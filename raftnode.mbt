// Copyright 2015 The etcd Authors
// Copyright 2026 Leo Cheng
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

///|
/// A message-driven Raft server: the consensus core (`Node`) wrapped with
/// everything a real deployment needs to run it — the peer set and configuration,
/// per-follower replication progress, election and heartbeat timers with
/// randomized timeouts, pre-vote, and a deterministic PRNG so tests can replay a
/// run exactly. It communicates only through `Message`s: `tick` and `step`
/// return the messages to send, and a transport (real or simulated) delivers
/// them. This mirrors etcd's `raft.Node`/`Ready` split of logic from I/O.
pub struct RaftNode {
  core : Node
  id : String
  peers : Array[String]
  config : Membership
  progress : Map[String, Progress]
  votes : Map[String, Bool]
  pre_vote : Bool
  mut check_quorum : Bool
  mut in_pre_campaign : Bool
  mut leader_id : String?
  mut election_elapsed : Int
  mut heartbeat_elapsed : Int
  election_timeout : Int
  heartbeat_timeout : Int
  mut randomized_election_timeout : Int
  max_entries : UInt64
  // Byte cap on a single outbound AppendEntries batch (etcd's MaxSizePerMsg).
  max_msg_bytes : UInt64
  // Byte cap on the uncommitted log tail (etcd's MaxUncommittedEntriesSize); 0
  // disables the check. `uncommitted_size` is the running tally.
  max_uncommitted_size : UInt64
  mut uncommitted_size : UInt64
  mut rng : UInt64
  // Highest log index whose configuration change has been folded into the live
  // configuration, so a committed ConfChange is applied exactly once.
  mut conf_applied : UInt64
  // The target of an in-flight leadership transfer, if any (Raft §3.10). While
  // set, the leader stops accepting proposals and hands off once the target is
  // caught up; it is cleared on timeout, step-down, or the target's removal.
  mut lead_transferee : String?
  // Set when an applied EnterJoint asked to auto-leave, so the leader appends
  // the matching LeaveJoint on the next apply pass (Raft §4.3).
  mut auto_leave_pending : Bool
  // Linearizable read-index bookkeeping (Raft §6.4).
  read_only : ReadOnly
}

///|
/// Create a server with id `id`, the other voters `peers`, and a deterministic
/// seed. Election timeouts randomize in [election_timeout, 2*election_timeout),
/// the spread that keeps split votes rare (Raft §5.2). Heartbeats go out every
/// `heartbeat_timeout` ticks, which must be well below the election timeout.
pub fn RaftNode::new(
  id : String,
  peers : Array[String],
  seed? : UInt64 = 1,
  election_timeout? : Int = 10,
  heartbeat_timeout? : Int = 1,
  max_entries? : UInt64 = 256,
  max_msg_bytes? : UInt64 = 18446744073709551615UL,
  max_uncommitted_size? : UInt64 = 0,
  pre_vote? : Bool = true,
) -> RaftNode {
  let all = [id]
  for p in peers {
    all.push(p)
  }
  let node = {
    core: Node::new(id),
    id,
    peers: peers.copy(),
    config: Membership::new(all),
    progress: {},
    votes: {},
    pre_vote,
    check_quorum: false,
    in_pre_campaign: false,
    leader_id: None,
    election_elapsed: 0,
    heartbeat_elapsed: 0,
    election_timeout,
    heartbeat_timeout,
    randomized_election_timeout: election_timeout,
    max_entries,
    max_msg_bytes,
    max_uncommitted_size,
    uncommitted_size: 0,
    rng: seed,
    conf_applied: 0,
    lead_transferee: None,
    auto_leave_pending: false,
    read_only: ReadOnly::new(),
  }
  node.reset_timeout()
  node
}

///|
/// The next value of the node's deterministic PRNG (a 64-bit LCG). Randomizing
/// election timeouts from a per-node seed makes whole-cluster runs reproducible.
fn RaftNode::next_rand(self : RaftNode) -> UInt64 {
  self.rng = self.rng * 6364136223846793005UL + 1442695040888963407UL
  self.rng
}

///|
/// Reset both timers and pick a fresh randomized election timeout in
/// [election_timeout, 2*election_timeout). The spread is taken from the *high*
/// bits of the LCG: an LCG's low bits are notoriously non-random (the lowest
/// cycles with period 2), so `next_rand() % et` on the raw value never reaches
/// some residues — using the top bits gives the uniform spread §5.2 needs.
fn RaftNode::reset_timeout(self : RaftNode) -> Unit {
  self.election_elapsed = 0
  self.heartbeat_elapsed = 0
  let spread = ((self.next_rand() >> 33) % self.election_timeout.to_uint64()).to_int()
  self.randomized_election_timeout = self.election_timeout + spread
}

///|
/// This server's id.
pub fn RaftNode::id(self : RaftNode) -> String {
  self.id
}

///|
/// The current role of the underlying consensus core.
pub fn RaftNode::role(self : RaftNode) -> Role {
  self.core.role()
}

///|
/// The randomized election timeout this server is currently counting toward, in
/// ticks. Exposed for tests that check the timeout stays within its band.
pub fn RaftNode::election_deadline(self : RaftNode) -> Int {
  self.randomized_election_timeout
}

///|
/// The current term.
pub fn RaftNode::term(self : RaftNode) -> UInt64 {
  self.core.current_term()
}

///|
/// Whether this server currently believes it is the leader.
pub fn RaftNode::is_leader(self : RaftNode) -> Bool {
  self.core.role() == Leader
}

///|
/// The leader this server last heard from, if any.
pub fn RaftNode::leader(self : RaftNode) -> String? {
  self.leader_id
}

///|
/// The underlying consensus core, for reading log/commit state in tests.
pub fn RaftNode::node(self : RaftNode) -> Node {
  self.core
}

///|
/// The commit index of the underlying core.
pub fn RaftNode::commit_index(self : RaftNode) -> UInt64 {
  self.core.commit_index
}

///|
/// The ids that have granted this server (pre-)votes so far, itself included.
fn RaftNode::granted(self : RaftNode) -> Array[String] {
  let out : Array[String] = []
  for id, ok in self.votes {
    if ok {
      out.push(id)
    }
  }
  out
}

///|
/// Advance the logical clock by one tick, returning the messages to send. A
/// leader beats every `heartbeat_timeout` ticks; a follower or candidate that
/// reaches its randomized election timeout starts a new campaign.
pub fn RaftNode::tick(self : RaftNode) -> Array[Message] {
  if self.core.role() == Leader {
    self.heartbeat_elapsed = self.heartbeat_elapsed + 1
    self.election_elapsed = self.election_elapsed + 1
    let out : Array[Message] = []
    // Once per election timeout, close the liveness window. With check-quorum
    // on, a leader that has not heard from a majority in that window steps down
    // so a partitioned-away leader cannot linger (etcd's CheckQuorum, §6.4).
    if self.election_elapsed >= self.election_timeout {
      self.election_elapsed = 0
      // A leadership transfer that has not completed within an election timeout
      // is abandoned, so proposals are not blocked indefinitely (§3.10).
      self.lead_transferee = None
      if self.check_quorum && !self.quorum_active() {
        self.step_down()
        return []
      }
      self.reset_recent_active()
    }
    if self.heartbeat_elapsed >= self.heartbeat_timeout {
      self.heartbeat_elapsed = 0
      for m in self.bcast_heartbeat() {
        out.push(m)
      }
    }
    return out
  }
  self.election_elapsed = self.election_elapsed + 1
  if self.election_elapsed >= self.randomized_election_timeout {
    return self.campaign()
  }
  []
}

///|
/// Turn on check-quorum and lease reads for this server.
pub fn RaftNode::enable_check_quorum(self : RaftNode) -> Unit {
  self.check_quorum = true
}

///|
/// Stand down to follower in the current term, e.g. on losing quorum contact.
fn RaftNode::step_down(self : RaftNode) -> Unit {
  self.core.become_follower(self.core.current_term())
  self.leader_id = None
  self.lead_transferee = None
  self.reset_timeout()
}

///|
/// Start a campaign. With pre-vote enabled the node first runs a pre-vote round
/// under a hypothetical next term without touching its own term or vote, so a
/// partitioned node cannot force real elections and inflate terms (Raft §9.6,
/// pre-vote). Winning the pre-vote — or pre-vote being disabled — starts the
/// real election.
pub fn RaftNode::campaign(self : RaftNode) -> Array[Message] {
  // Only a voter may stand for election; a learner (non-voting member) never
  // campaigns even if its election timer fires (§4.2.1, promotable).
  if !self.config.contains(self.id) {
    return []
  }
  self.reset_timeout()
  if self.pre_vote {
    self.start_pre_campaign()
  } else {
    self.start_real_campaign()
  }
}

///|
fn RaftNode::start_pre_campaign(self : RaftNode) -> Array[Message] {
  self.in_pre_campaign = true
  self.votes.clear()
  self.votes[self.id] = true
  let out : Array[Message] = []
  let args = {
    term: self.core.current_term() + 1,
    candidate_id: self.id,
    last_log_index: self.core.last_log_index(),
    last_log_term: self.core.last_log_term(),
  }
  for p in self.peers {
    out.push(Message::new(self.id, p, PreVote(args)))
  }
  // A single-node cluster wins its own pre-vote immediately.
  if self.config.has_majority(self.granted()) {
    return self.start_real_campaign()
  }
  out
}

///|
fn RaftNode::start_real_campaign(self : RaftNode) -> Array[Message] {
  self.in_pre_campaign = false
  self.core.become_candidate()
  self.leader_id = None
  self.votes.clear()
  self.votes[self.id] = true
  let out : Array[Message] = []
  let args = self.core.request_vote_args()
  for p in self.peers {
    out.push(Message::new(self.id, p, Vote(args)))
  }
  if self.config.has_majority(self.granted()) {
    self.become_leader()
    return self.bcast_append()
  }
  out
}

///|
/// Take leadership: initialise per-follower progress, then append a no-op entry
/// under the new term. Committing that entry commits everything before it,
/// which is how a new leader safely learns its true commit index (Raft §5.4.2).
fn RaftNode::become_leader(self : RaftNode) -> Unit {
  self.core.become_leader()
  self.leader_id = Some(self.id)
  self.lead_transferee = None
  self.progress.clear()
  let last = self.core.last_log_index()
  for p in self.peers {
    self.progress[p] = Progress::new(last + 1)
  }
  let _ = self.core.append(self.core.current_term(), b"")
  // Like etcd's reset() on every state change, re-randomize the election
  // timeout and clear the timers.
  self.reset_timeout()
  // In a single-node cluster the no-op commits at once, which also arms reads.
  self.maybe_commit() |> ignore
}

///|
/// Transition to follower in `term`, re-randomizing the election timeout. Every
/// state change routes through the timer reset (Raft §5.2), which is what makes
/// the per-node timeout fresh on each transition and keeps split votes rare.
pub fn RaftNode::become_follower(self : RaftNode, term : UInt64) -> Unit {
  self.core.become_follower(term)
  self.leader_id = None
  self.in_pre_campaign = false
  self.reset_timeout()
}

///|
/// Transition to candidate (advancing the term and voting for self), also
/// re-randomizing the election timeout.
pub fn RaftNode::become_candidate(self : RaftNode) -> Unit {
  self.core.become_candidate()
  self.leader_id = None
  self.reset_timeout()
}

///|
/// Append a client command to the leader's log and replicate it. A no-op on a
/// non-leader. Returns the AppendEntries messages to send.
pub fn RaftNode::propose(self : RaftNode, command : Bytes) -> Array[Message] {
  // A leader in the middle of handing off leadership stops accepting new
  // proposals, so the transfer target can catch up to a fixed log (§3.10).
  if self.core.role() != Leader || self.lead_transferee is Some(_) {
    return []
  }
  // Drop the proposal if it would push the uncommitted tail over its quota
  // (etcd's ErrProposalDropped), unless the tail is currently empty.
  if !self.increase_uncommitted_size(command.length().to_uint64()) {
    return []
  }
  let _ = self.core.append(self.core.current_term(), command)
  self.maybe_commit() |> ignore
  self.bcast_append()
}

///|
/// Append a configuration change to the leader's log and replicate it, so every
/// server folds the same membership change into its configuration at the same
/// log position once it commits (Raft §6). A no-op on a non-leader.
pub fn RaftNode::propose_conf(
  self : RaftNode,
  change : ConfChange,
) -> Array[Message] {
  if self.core.role() != Leader || self.lead_transferee is Some(_) {
    return []
  }
  let _ = self.core.append_conf(self.core.current_term(), change.encode())
  self.maybe_commit() |> ignore
  self.bcast_append()
}

///|
/// Append a batch configuration change (joint consensus, Raft §4.3) and
/// replicate it. Entering joint with a non-empty batch moves the cluster to
/// C(old,new); once committed, `auto_leave` has the leader append the matching
/// leave automatically. A no-op on a non-leader or mid-transfer.
pub fn RaftNode::propose_conf_v2(
  self : RaftNode,
  change : ConfChangeV2,
) -> Array[Message] {
  if self.core.role() != Leader || self.lead_transferee is Some(_) {
    return []
  }
  let _ = self.core.append_conf(self.core.current_term(), change.encode())
  self.maybe_commit() |> ignore
  self.bcast_append()
}

///|
/// Begin transferring leadership to `target` (Raft §3.10). If the target is
/// already caught up it is sent a TimeoutNow so it campaigns immediately;
/// otherwise it is first sent the entries it lacks and the caller retries the
/// transfer once it has caught up. A no-op on a non-leader or an unknown target.
pub fn RaftNode::transfer_leadership(
  self : RaftNode,
  target : String,
) -> Array[Message] {
  // A non-leader, a transfer to ourselves, or an unknown/non-member target is a
  // no-op (§3.10). Transferring to a learner is likewise refused.
  if self.core.role() != Leader ||
    target == self.id ||
    !self.config.contains(target) {
    return []
  }
  self.lead_transferee = Some(target)
  self.election_elapsed = 0
  match self.progress.get(target) {
    None => []
    Some(p) =>
      if p.match_index == self.core.last_log_index() {
        [Message::new(self.id, target, TimeoutNow(self.core.current_term()))]
      } else {
        [self.send_to(target)]
      }
  }
}

///|
/// Build the AppendEntries or InstallSnapshot message for one follower, based on
/// its progress. If the entry the follower needs has been compacted away, a
/// snapshot is sent instead (Raft §7).
fn RaftNode::send_to(self : RaftNode, peer : String) -> Message {
  let p = self.progress[peer]
  let prev = p.next_index - 1
  if prev < self.core.snapshot_index {
    p.become_snapshot(self.core.snapshot_index)
    let snap : Snapshot = {
      last_index: self.core.snapshot_index,
      last_term: self.core.snapshot_term,
      data: b"",
    }
    return Message::new(
      self.id,
      peer,
      Snapshot(
        InstallSnapshotArgs::whole(self.core.current_term(), self.id, snap),
      ),
    )
  }
  // A streaming follower whose in-flight window is full is sent a content-free
  // probe (no new entries, no window slot consumed), rather than more entries it
  // has no room to track — matching etcd's ShouldSendMsgApp (§5.3 flow control).
  let entries = if p.state == Replicate && p.inflights.full() {
    []
  } else {
    // Cap the batch by both count (max_entries) and encoded byte size
    // (MaxSizePerMsg); limit_size always keeps at least one entry.
    limit_size(
      self.core.entries_after_limited(prev, self.max_entries)[:],
      self.max_msg_bytes,
    )
  }
  let has = entries.length() > 0
  let last = if has { entries[entries.length() - 1].index } else { prev }
  // Advance the flow controller: stream (consume a window slot) or pause a probe.
  p.sent_entries(last, has)
  Message::new(
    self.id,
    peer,
    Append({
      term: self.core.current_term(),
      leader_id: self.id,
      prev_log_index: prev,
      prev_log_term: self.core.term_at(prev),
      entries,
      leader_commit: self.core.commit_index,
    }),
  )
}

///|
/// Send an AppendEntries (or snapshot) to every follower.
fn RaftNode::bcast_append(self : RaftNode) -> Array[Message] {
  let out : Array[Message] = []
  for peer in self.peers {
    // Skip a throttled follower: a streaming one whose in-flight window is full,
    // or one still digesting a snapshot. A prober is sent its one probe.
    let p = self.progress[peer]
    if p.is_paused() {
      continue
    }
    out.push(self.send_to(peer))
  }
  out
}

///|
/// Send a heartbeat to every follower. The `commit` carried is capped at what
/// the follower is known to hold, so it never learns of a commit index past its
/// own log (Raft §5.3).
fn RaftNode::bcast_heartbeat(
  self : RaftNode,
  context? : Bytes = b"",
) -> Array[Message] {
  let out : Array[Message] = []
  for peer in self.peers {
    let p = self.progress[peer]
    let commit = if p.match_index < self.core.commit_index {
      p.match_index
    } else {
      self.core.commit_index
    }
    out.push(
      Message::new(
        self.id,
        peer,
        Heartbeat({
          term: self.core.current_term(),
          leader_id: self.id,
          commit,
          context,
        }),
      ),
    )
  }
  out
}

///|
/// Recompute the commit index from replication progress. An index is committed
/// once it is stored on a majority of voters *and* belongs to the current term;
/// a leader never commits an entry from an earlier term by counting replicas
/// alone (Raft §5.4.2). Self counts as having stored up to its last index.
fn RaftNode::maybe_commit(self : RaftNode) -> Bool {
  let mut any = false
  // Committing a configuration change can shrink the quorum, which may in turn
  // let a further entry commit, so re-evaluate until the commit index is stable.
  // A leader that removed itself steps down inside apply, ending the loop.
  while self.core.role() == Leader {
    // The highest index a quorum of the configuration has stored (the leader
    // itself included, at its last index). In a joint configuration this is the
    // smaller of the two halves' agreed indices.
    let acked : Map[String, UInt64] = {}
    acked[self.id] = self.core.last_log_index()
    for peer, p in self.progress {
      acked[peer] = p.match_index
    }
    let mci = self.config.committed_index(acked)
    // §5.4.2: a leader only commits an index whose entry is from its own term;
    // since terms are monotonic in the log, no lower current-term index can be
    // committed if this one is not.
    if mci > self.core.commit_index &&
      self.core.term_at(mci) == self.core.current_term() {
      let old = self.core.commit_index
      self.core.advance_commit(mci)
      // Newly committed entries leave the uncommitted tail; release their bytes.
      let mut freed = 0UL
      let mut i = old + 1
      while i <= mci {
        match self.core.entry_at(i) {
          Some(e) => freed = freed + payload_size(e)
          None => ()
        }
        i = i + 1
      }
      self.reduce_uncommitted_size(freed)
      self.apply_committed_conf()
      any = true
    } else {
      break
    }
  }
  any
}

///|
/// Account for `size` bytes of newly-proposed payload against the uncommitted
/// quota (etcd's increaseUncommittedSize). Returns false — meaning the proposal
/// must be dropped — only when the tail is already non-empty and this would push
/// it over `max_uncommitted_size`; an empty tail always accepts at least one
/// proposal, however large, and empty (zero-byte) entries always pass.
fn RaftNode::increase_uncommitted_size(self : RaftNode, size : UInt64) -> Bool {
  if self.max_uncommitted_size > 0 &&
    self.uncommitted_size > 0 &&
    size > 0 &&
    self.uncommitted_size + size > self.max_uncommitted_size {
    return false
  }
  self.uncommitted_size = self.uncommitted_size + size
  true
}

///|
/// Release `size` bytes from the uncommitted tally as entries commit or apply
/// (etcd's reduceUncommittedSize). Never goes below zero.
pub fn RaftNode::reduce_uncommitted_size(
  self : RaftNode,
  size : UInt64,
) -> Unit {
  self.uncommitted_size = if size > self.uncommitted_size {
    0
  } else {
    self.uncommitted_size - size
  }
}

///|
/// The current size of the uncommitted log tail, in payload bytes.
pub fn RaftNode::uncommitted_size(self : RaftNode) -> UInt64 {
  self.uncommitted_size
}

///|
/// Fold every committed-but-unapplied `ConfChange` entry into the live
/// configuration, replication progress and peer set, in log order (Raft §6).
/// This is what actually makes an added server a voter, stops a removed server
/// counting toward quorum, and steps down a leader that removed itself.
fn RaftNode::apply_committed_conf(self : RaftNode) -> Unit {
  while self.conf_applied < self.core.commit_index {
    let idx = self.conf_applied + 1
    match self.core.entry_at(idx) {
      Some(e) =>
        if e.is_conf_change() {
          // A batch (ConfChangeV2) is tagged 'V'; anything else is a single change.
          match ConfChangeV2::decode(e.command) {
            Some(v2) => self.apply_conf_change_v2(v2)
            None =>
              match ConfChange::decode(e.command) {
                Some(cc) => {
                  cc.apply_to(self.config)
                  self.reconcile_peers()
                }
                None => ()
              }
          }
        }
      None => ()
    }
    self.conf_applied = idx
  }
  // Auto-leave: once an EnterJoint that requested it has been applied, the
  // leader appends the matching LeaveJoint so the cluster settles on the new
  // configuration without a second client request (§4.3).
  if self.auto_leave_pending && self.core.role() == Leader {
    self.auto_leave_pending = false
    let _ = self.core.append_conf(
      self.core.current_term(),
      ConfChangeV2::leave_joint().encode(),
    )
  }
}

///|
/// Apply one committed batch configuration change (joint enter/leave).
fn RaftNode::apply_conf_change_v2(self : RaftNode, v2 : ConfChangeV2) -> Unit {
  if v2.is_leave() {
    self.config.leave_joint()
  } else {
    // Enter joint consensus: the current voters become the outgoing half, and
    // the batch's changes are folded into the incoming half. Decisions then
    // need a majority of *both* halves until the matching leave commits (§4.3).
    let outgoing = self.config.members.copy()
    for c in v2.changes {
      c.apply_to(self.config)
    }
    self.config.begin_joint(outgoing)
    if v2.auto_leave {
      self.auto_leave_pending = true
    }
  }
  self.reconcile_peers()
}

///|
/// Bring the peer set and replication progress into line with the current
/// configuration after a change: add newcomers (with progress, if leading),
/// drop departed servers, abort a transfer to a removed target, and step down if
/// we ourselves are no longer part of the configuration (Raft §6).
fn RaftNode::reconcile_peers(self : RaftNode) -> Unit {
  let desired = self.config.nodes()
  self.peers.retain(fn(p) { desired.contains(p) })
  let stale : Array[String] = []
  for peer, _ in self.progress {
    if !desired.contains(peer) {
      stale.push(peer)
    }
  }
  for peer in stale {
    self.progress.remove(peer)
  }
  for id in desired {
    if id != self.id {
      if !self.peers.contains(id) {
        self.peers.push(id)
      }
      if self.core.role() == Leader && !self.progress.contains(id) {
        self.progress[id] = Progress::new(self.core.last_log_index() + 1)
      }
      // Keep each progress's learner flag in step with the configuration.
      match self.progress.get(id) {
        Some(p) => p.is_learner = self.config.is_learner(id)
        None => ()
      }
    }
  }
  match self.lead_transferee {
    Some(t) => if !self.config.contains(t) { self.lead_transferee = None }
    None => ()
  }
  if self.core.role() == Leader && !self.config.contains(self.id) {
    self.step_down()
  }
}
