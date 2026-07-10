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
  // Per-follower replication flow-control window: at most `max_inflight`
  // outstanding AppendEntries and, when non-zero, at most `max_inflight_bytes`
  // outstanding payload bytes (etcd's MaxInflightMsgs / MaxInflightBytes). Both
  // are handed to each follower's `Progress` so its `Inflights` enforces them.
  max_inflight : Int
  max_inflight_bytes : UInt64
  mut rng : UInt64
  // Highest log index whose configuration change has been folded into the live
  // configuration, so a committed ConfChange is applied exactly once.
  mut conf_applied : UInt64
  // The target of an in-flight leadership transfer, if any (Raft §3.10). While
  // set, the leader stops accepting proposals and hands off once the target is
  // caught up; it is cleared on timeout, step-down, or the target's removal.
  mut lead_transferee : String?
  // Linearizable read-index bookkeeping (Raft §6.4).
  read_only : ReadOnly
  // Read-index requests (origin id, opaque context) received before the leader
  // has committed an entry in its current term. etcd holds these in
  // `pendingReadIndexMessages` and releases them on the first in-term commit
  // rather than dropping the read.
  pending_read_index : Array[(String, Bytes)]
  // etcd's DisableProposalForwarding: when set, a follower drops client proposals
  // instead of forwarding them to the leader (for deployments that route writes
  // to the leader out-of-band). Off by default: a follower forwards.
  mut no_forward : Bool
  // The highest log index that carries a not-yet-applied configuration change
  // (etcd's `pendingConfIndex`). A leader refuses to propose a further change
  // until its applied index passes this, so at most one change is in flight.
  // Set conservatively to the last index on becoming leader, and reset on any
  // other state change.
  mut pending_conf_index : UInt64
  // etcd's DisableConfChangeValidation: turns off the propose-time checks below
  // (only-one-in-flight, joint-ordering). Off by default.
  no_conf_change_validation : Bool
  // etcd's StepDownOnRemoval: when set, a leader removed from the configuration
  // (or demoted to a learner) steps down. Off by default — matching etcd, whose
  // zero-valued Config leaves a removed leader in place until the new
  // configuration elects a replacement (etcd's `switchToConfig`).
  step_down_on_removal : Bool
  // Diagnostics sink (etcd's `Config.Logger`); a no-op by default.
  logger : &Logger
  // State-transition tracer (etcd's `Config.TraceLogger`); a no-op by default.
  tracer : &Tracer
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
  max_inflight? : Int = 256,
  max_inflight_bytes? : UInt64 = 0,
  check_quorum? : Bool = false,
  // etcd's Config.PreVote is opt-in (zero value false); default off to match.
  pre_vote? : Bool = false,
  step_down_on_removal? : Bool = false,
  disable_conf_change_validation? : Bool = false,
  // etcd's zero value is ReadOnlySafe ("default and suggested"); a LeaseBased
  // default without check-quorum is a configuration etcd's validate() rejects.
  read_only_option? : ReadOnlyOption = Safe,
  logger? : &Logger = NopLogger::{  },
  tracer? : &Tracer = NopTracer::{  },
) -> RaftNode {
  // Reject an unusable configuration at construction, as etcd's newRaft panics on
  // an invalid Config.validate() (raft.go:440). This convenience constructor
  // shares the exact checks with the explicit Config path.
  Config::new(
    id,
    peers,
    election_tick=election_timeout,
    heartbeat_tick=heartbeat_timeout,
    max_msg_bytes~,
    max_inflight~,
    max_inflight_bytes~,
    check_quorum~,
    read_only_option~,
  ).validate() catch {
    EmptyId => abort("invalid raft config: empty id")
    HeartbeatTickNotPositive =>
      abort("invalid raft config: heartbeat tick <= 0")
    ElectionTickNotGreater =>
      abort("invalid raft config: election tick <= heartbeat tick")
    MaxInflightNotPositive => abort("invalid raft config: max inflight <= 0")
    MaxInflightBytesTooSmall =>
      abort("invalid raft config: max inflight bytes < max message size")
    LeaseBasedNeedsCheckQuorum =>
      abort("invalid raft config: ReadOnlyLeaseBased requires CheckQuorum")
  }
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
    check_quorum,
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
    max_inflight,
    max_inflight_bytes,
    rng: seed,
    conf_applied: 0,
    lead_transferee: None,
    read_only: ReadOnly::new(),
    pending_read_index: [],
    no_forward: false,
    pending_conf_index: 0,
    no_conf_change_validation: disable_conf_change_validation,
    step_down_on_removal,
    logger,
    tracer,
  }
  node.read_only.safe = read_only_option is Safe
  node.tracer.on_event(InitState(id))
  node.reset_timeout()
  node
}

///|
/// The voters of the current configuration, sorted (etcd's `VoterNodes`), for
/// status and diagnostics. Sorting makes the list deterministic regardless of
/// the order members were added.
pub fn RaftNode::voter_nodes(self : RaftNode) -> Array[String] {
  let out = self.config.voters()
  out.sort()
  out
}

///|
/// Whether this node may stand for election (etcd's `promotable`): it must be a
/// voter of the current configuration. A learner or a node that has been removed
/// is not promotable. (etcd also refuses while a snapshot is being applied; this
/// port installs snapshots synchronously, so there is no such in-progress state.)
pub fn RaftNode::promotable(self : RaftNode) -> Bool {
  self.config.contains(self.id)
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
/// The current role, as reported to callers (etcd's `SoftState.RaftState`).
/// During the pre-vote probe the underlying core is still internally a follower
/// (it has not adopted the hypothetical term), but the observable state is
/// `PreCandidate` — a first-class Raft state, not a hidden flag.
pub fn RaftNode::role(self : RaftNode) -> Role {
  if self.in_pre_campaign {
    PreCandidate
  } else {
    self.core.role()
  }
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
  // A leader ignores a fresh campaign (etcd's `hup`: "already leader"): it must
  // not step itself down to candidate and inflate the term.
  if self.core.role() == Leader {
    return []
  }
  // Only a promotable voter may stand for election; a learner or a node no
  // longer in the configuration never campaigns (§4.2.1, promotable).
  if !self.promotable() {
    return []
  }
  // Do not campaign while a committed configuration change is still unapplied:
  // electing a leader under a stale view of the membership could split the
  // quorum (etcd's `hup`/`hasUnappliedConfChanges`, raft.go:983).
  if self.has_unapplied_conf_changes() {
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
fn RaftNode::start_real_campaign(
  self : RaftNode,
  force? : Bool = false,
) -> Array[Message] {
  self.in_pre_campaign = false
  self.core.become_candidate()
  self.leader_id = None
  self.votes.clear()
  self.votes[self.id] = true
  let out : Array[Message] = []
  // A leadership-transfer campaign marks its votes forced so a follower still
  // leased to the outgoing leader grants them, letting the handover complete
  // instead of stalling out a full election timeout (§3.10).
  let args = self.core.request_vote_args()
  for p in self.peers {
    out.push(Message::new(self.id, p, Vote(args), force~))
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
  // Conservatively assume any inherited tail may hold a pending conf change:
  // block new ones until the applied index passes it (etcd sets pendingConfIndex
  // to lastIndex here, before appending the no-op below). For a fresh leader with
  // an empty log this is 0, so the first conf change is allowed at once.
  self.pending_conf_index = last
  for p in self.peers {
    self.progress[p] = Progress::new(
      last + 1,
      max_inflight=self.max_inflight,
      max_inflight_bytes=self.max_inflight_bytes,
    )
  }
  let _ = self.core.append(self.core.current_term(), b"")
  // Like etcd's reset() on every state change, re-randomize the election
  // timeout and clear the timers.
  self.reset_timeout()
  // In a single-node cluster the no-op commits at once, which also arms reads.
  self.maybe_commit() |> ignore
  // A leader inheriting a committed auto-leave joint config (whose LeaveJoint
  // was never committed, e.g. truncated on the term change) appends the leave.
  self.maybe_append_auto_leave()
  self.tracer.on_event(StateChange(self.id, Leader, self.core.current_term()))
  self.logger.log(
    Info,
    "\{self.id} became leader at term \{self.core.current_term()}",
  )
}

///|
/// Transition to follower in `term`, re-randomizing the election timeout. Every
/// state change routes through the timer reset (Raft §5.2), which is what makes
/// the per-node timeout fresh on each transition and keeps split votes rare.
pub fn RaftNode::become_follower(self : RaftNode, term : UInt64) -> Unit {
  self.core.become_follower(term)
  self.leader_id = None
  self.in_pre_campaign = false
  self.pending_conf_index = 0
  self.lead_transferee = None
  self.reset_timeout()
  self.tracer.on_event(StateChange(self.id, Follower, term))
  self.logger.log(Info, "\{self.id} became follower at term \{term}")
}

///|
/// Transition to candidate (advancing the term and voting for self), also
/// re-randomizing the election timeout.
pub fn RaftNode::become_candidate(self : RaftNode) -> Unit {
  self.core.become_candidate()
  self.leader_id = None
  self.in_pre_campaign = false
  self.pending_conf_index = 0
  self.lead_transferee = None
  self.reset_timeout()
  self.tracer.on_event(
    StateChange(self.id, Candidate, self.core.current_term()),
  )
  self.logger.log(
    Info,
    "\{self.id} became candidate at term \{self.core.current_term()}",
  )
}

///|
/// Turn off proposal forwarding (etcd's DisableProposalForwarding): a follower
/// will drop client proposals instead of forwarding them to the leader.
pub fn RaftNode::disable_proposal_forwarding(self : RaftNode) -> Unit {
  self.no_forward = true
}

///|
/// Append a client command and replicate it. On a leader the command is appended
/// and the resulting AppendEntries returned; on a follower it is forwarded to the
/// leader (unless forwarding is disabled or no leader is known); on a candidate
/// it is dropped. This routes through the same `Propose` message path a remote
/// proposal takes, so local and forwarded proposals behave identically.
pub fn RaftNode::propose(self : RaftNode, command : Bytes) -> Array[Message] {
  self.step_propose(self.id, [
    Entry::normal(self.core.current_term(), 0, command),
  ])
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
  // A single ConfChange is never a joint leave; refuse it if one is already
  // pending (etcd's stepLeader validation). The refused proposal is simply
  // dropped rather than appended.
  if self.conf_change_refused(false) {
    return []
  }
  let _ = self.core.append_conf(self.core.current_term(), change.encode())
  self.pending_conf_index = self.core.last_log_index()
  self.maybe_commit() |> ignore
  self.bcast_append()
}

///|
/// Whether a configuration change carrying `is_leave` must be refused at propose
/// time (etcd's stepLeader checks, raft.go:1326): a change is refused while one
/// is already pending (applied has not caught up to `pending_conf_index`), while
/// a non-leave change is proposed in a joint config, or while a leave is proposed
/// outside one. `DisableConfChangeValidation` turns the checks off.
fn RaftNode::conf_change_refused(self : RaftNode, is_leave : Bool) -> Bool {
  if self.no_conf_change_validation {
    return false
  }
  // etcd compares `pendingConfIndex` against the applied index. In this port a
  // committed conf change is folded into the configuration immediately (tracked
  // by `conf_applied`), which is the moral equivalent of etcd applying it — so a
  // further change is allowed once the pending one has committed, and refused
  // while it is still an uncommitted, un-folded tail entry.
  let already_pending = self.pending_conf_index > self.conf_applied
  let already_joint = self.config.is_joint()
  already_pending ||
  (already_joint && !is_leave) ||
  (!already_joint && is_leave)
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
  if self.conf_change_refused(change.is_leave()) {
    return []
  }
  let _ = self.core.append_conf(self.core.current_term(), change.encode())
  self.pending_conf_index = self.core.last_log_index()
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
  if self.core.role() != Leader {
    return []
  }
  // A learner can never be leader, so a transfer to one is ignored outright,
  // without disturbing any transfer already under way (etcd checks pr.IsLearner
  // first).
  if self.config.is_learner(target) {
    return []
  }
  // A transfer already in progress (§3.10): a repeat to the same target is
  // ignored (the handoff continues); a request naming a different target aborts
  // the old one before the new is considered (etcd's stepLeader).
  match self.lead_transferee {
    Some(last) =>
      if last == target {
        return []
      } else {
        self.lead_transferee = None
      }
    None => ()
  }
  // Transferring to ourselves is a no-op — but note it still aborts a pending
  // transfer above, which is how a leader cancels a handoff (etcd's
  // "already leader" branch runs after abortLeaderTransfer).
  if target == self.id {
    return []
  }
  // An unknown / non-member target is a no-op.
  if !self.config.contains(target) {
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
        match self.send_to(target) {
          Some(m) => [m]
          None => []
        }
      }
  }
}

///|
/// Request that leadership move to `target` (etcd's TransferLeader). On a leader
/// this begins the handoff; on a follower the request is forwarded to the leader,
/// so a transfer may be initiated from any server. Routes through the same
/// `TransferLeader` message path a forwarded request takes.
pub fn RaftNode::request_transfer_leader(
  self : RaftNode,
  target : String,
) -> Array[Message] {
  self.step_transfer_leader(self.id, target)
}

///|
/// Forget the currently-recognised leader (etcd's MsgForgetLeader): a follower
/// clears its leader so it may grant (pre)votes at once instead of waiting out a
/// check-quorum lease, without moving its term or resetting its election timer.
/// Ignored under lease-based reads; a no-op on a candidate or leader.
pub fn RaftNode::forget_leader(self : RaftNode) -> Unit {
  self.step(Message::new(self.id, self.id, ForgetLeader)) |> ignore
}

///|
/// Build the AppendEntries or InstallSnapshot message for one follower, based on
/// its progress. If the entry the follower needs has been compacted away, a
/// snapshot is sent instead (Raft §7). Returns `None` when nothing should be
/// sent — currently only when a snapshot would be needed but the follower has
/// not been heard from this liveness window (etcd's `maybeSendSnapshot` refuses
/// to ship a snapshot to a `!RecentActive` follower, since it is probably gone
/// and the snapshot would be wasted bandwidth).
fn RaftNode::send_to(self : RaftNode, peer : String) -> Message? {
  let p = self.progress[peer]
  let prev = p.next_index - 1
  if prev < self.core.snapshot_index {
    if !p.is_active() {
      return None
    }
    p.become_snapshot(self.core.snapshot_index)
    let snap : Snapshot = {
      last_index: self.core.snapshot_index,
      last_term: self.core.snapshot_term,
      data: b"",
      // Stamp the current membership so the follower rebuilds it on install.
      conf_state: self.conf_state(),
    }
    return Some(
      Message::new(
        self.id,
        peer,
        Snapshot(
          InstallSnapshotArgs::whole(self.core.current_term(), self.id, snap),
        ),
      ),
    )
  }
  // A streaming follower whose in-flight window is full is sent a content-free
  // probe (no new entries, no window slot consumed), rather than more entries it
  // has no room to track — matching etcd's ShouldSendMsgApp (§5.3 flow control).
  let entries = if p.state == Replicate && p.inflights.full() {
    []
  } else {
    // Cap the batch by encoded byte size (etcd's MaxSizePerMsg) and, as an
    // extra bound etcd does NOT have, by entry count (max_entries, default 256).
    // DIVERGENT (we do more): etcd caps a MsgApp only by bytes; the count cap is
    // a belt-and-suspenders limit, latent while max_msg_bytes is finite and the
    // in-flight message/byte windows bound throughput anyway. limit_size always
    // keeps at least one entry.
    limit_size(
      self.core.entries_after_limited(prev, self.max_entries)[:],
      self.max_msg_bytes,
    )
  }
  let has = entries.length() > 0
  let last = if has { entries[entries.length() - 1].index } else { prev }
  // Total payload bytes in this batch, for the in-flight byte budget
  // (etcd's payloadsSize). Inert unless MaxInflightBytes is set.
  let mut ents_size = 0UL
  for e in entries {
    ents_size = ents_size + payload_size(e)
  }
  // Advance the flow controller: stream (consume a window slot) or pause a probe.
  p.sent_entries(last, has, bytes=ents_size)
  // Record the commit index just put in flight to this follower, so a later
  // eager commit-only send can be skipped when it would tell the follower
  // nothing new (etcd's SentCommit / CanBumpCommit optimization).
  p.note_commit_sent(self.core.commit_index)
  Some(
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
    ),
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
    match self.send_to(peer) {
      Some(m) => out.push(m)
      None => ()
    }
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
    // A heartbeat carries a commit index too, so it also advances what the
    // follower is known to have been told (etcd's sendHeartbeat SentCommit).
    p.note_commit_sent(commit)
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
      self.core.advance_commit(mci)
      self.tracer.on_event(Commit(self.id, mci))
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
/// Advance the applied watermark to `index`, releasing the uncommitted-tail
/// bytes of the entries now applied (etcd releases the quota at *apply* time,
/// on `MsgStorageApplyResp`, not at commit time). Deferring the release to apply
/// is what bounds the un-applied tail even for a single-voter leader — which
/// commits its own appends instantly, so a commit-time release could never form
/// a bounded backlog. The application (or the RawNode/Advance layer) calls this
/// once it has applied committed entries.
pub fn RaftNode::advance_applied(self : RaftNode, index : UInt64) -> Unit {
  if index <= self.core.last_applied {
    return
  }
  let mut freed = 0UL
  let mut i = self.core.last_applied + 1
  while i <= index {
    match self.core.entry_at(i) {
      Some(e) => freed = freed + payload_size(e)
      None => ()
    }
    i = i + 1
  }
  self.core.mark_applied(index)
  self.reduce_uncommitted_size(freed)
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
  // Auto-leave: a leader in an auto-leave joint config appends the matching
  // LeaveJoint (§4.3). This is driven by the *durable* config flag, not a
  // one-shot marker, and re-checked whenever the committed prefix advances, so
  // if the LeaveJoint is truncated before committing the leader appends it
  // again — the flag is only cleared when the LeaveJoint itself commits.
  self.maybe_append_auto_leave()
}

///|
/// If we are a leader in an auto-leave joint config and no LeaveJoint entry is
/// already in flight, append one. Robust to truncation: `has_pending_leave`
/// scans the uncommitted tail, so a truncated LeaveJoint is simply re-appended.
fn RaftNode::maybe_append_auto_leave(self : RaftNode) -> Unit {
  if self.core.role() == Leader &&
    self.config.is_joint() &&
    self.config.auto_leave &&
    !self.has_pending_leave() {
    let _ = self.core.append_conf(
      self.core.current_term(),
      ConfChangeV2::leave_joint().encode(),
    )
    // The auto-appended leave is itself now the pending change, so no other
    // conf change is proposed until it applies (etcd tracks pendingConfIndex for
    // the automatic transition too).
    self.pending_conf_index = self.core.last_log_index()
  }
}

///|
/// Whether any committed-but-unapplied entry is a configuration change (etcd's
/// `hasUnappliedConfChanges`). A server refuses to campaign while one is pending,
/// so it does not become leader under a membership it has not yet folded in.
fn RaftNode::has_unapplied_conf_changes(self : RaftNode) -> Bool {
  if self.core.last_applied >= self.core.commit_index {
    return false
  }
  let mut i = self.core.last_applied + 1
  while i <= self.core.commit_index {
    match self.core.entry_at(i) {
      Some(e) => if e.is_conf_change() { return true }
      None => ()
    }
    i = i + 1
  }
  false
}

///|
/// Whether a LeaveJoint conf-change entry is already present in the uncommitted
/// tail (so we should not append another).
fn RaftNode::has_pending_leave(self : RaftNode) -> Bool {
  let mut i = self.core.commit_index + 1
  while i <= self.core.last_log_index() {
    match self.core.entry_at(i) {
      Some(e) =>
        if e.is_conf_change() {
          match ConfChangeV2::decode(e.command) {
            Some(v2) => if v2.is_leave() { return true }
            None => ()
          }
        }
      None => ()
    }
    i = i + 1
  }
  false
}

///|
/// The current membership as a `ConfState`, to stamp into a snapshot so a
/// follower restoring from it rebuilds the same voter/learner sets (§7).
pub fn RaftNode::conf_state(self : RaftNode) -> ConfState {
  {
    voters: self.config.members.copy(),
    learners: self.config.learners.copy(),
    voters_outgoing: if self.config.joint {
      self.config.outgoing.copy()
    } else {
      []
    },
    learners_next: self.config.learners_next.copy(),
    auto_leave: self.config.auto_leave,
  }
}

///|
/// A read-only view of one follower's replication progress, surfaced for status
/// and monitoring (etcd's `tracker.Progress` as exposed through `Status` and
/// `WithProgress`). It is a copied value, so reading it never disturbs the
/// running protocol.
pub(all) struct ProgressStatus {
  id : String
  match_index : UInt64
  next_index : UInt64
  state : ProgressState
  paused : Bool
  pending_snapshot : UInt64
  is_learner : Bool
} derive(Eq)

///|
fn progress_status_of(id : String, p : Progress) -> ProgressStatus {
  {
    id,
    match_index: p.match_index,
    next_index: p.next_index,
    state: p.state,
    paused: p.is_paused(),
    pending_snapshot: p.pending_snapshot,
    is_learner: p.is_learner,
  }
}

///|
/// The full status of the server (etcd's `Status`): the basic status, a snapshot
/// of every tracked follower's replication progress — including the leader's own
/// entry, fully caught up — and the current configuration as a `ConfState`.
pub(all) struct FullStatus {
  basic : RaftStatus
  progress : Array[ProgressStatus]
  config : ConfState
}

///|
/// A snapshot of every tracked follower's progress. On a leader this includes
/// the leader's own entry (caught up to its last index); a non-leader tracks no
/// progress and returns an empty view.
pub fn RaftNode::progress_status(self : RaftNode) -> Array[ProgressStatus] {
  let out : Array[ProgressStatus] = []
  if self.core.role() == Leader {
    out.push({
      id: self.id,
      match_index: self.core.last_log_index(),
      next_index: self.core.last_log_index() + 1,
      state: Replicate,
      paused: false,
      pending_snapshot: 0,
      is_learner: false,
    })
  }
  for id, p in self.progress {
    out.push(progress_status_of(id, p))
  }
  out
}

///|
/// One member's progress view, or `None` if it is not tracked. A leader also
/// answers for itself (etcd tracks the leader in its own progress map).
pub fn RaftNode::progress_of(self : RaftNode, id : String) -> ProgressStatus? {
  if id == self.id && self.core.role() == Leader {
    return Some({
      id,
      match_index: self.core.last_log_index(),
      next_index: self.core.last_log_index() + 1,
      state: Replicate,
      paused: false,
      pending_snapshot: 0,
      is_learner: false,
    })
  }
  match self.progress.get(id) {
    Some(p) => Some(progress_status_of(id, p))
    None => None
  }
}

///|
/// Visit every tracked follower's progress (etcd's `WithProgress`). The visitor
/// must not retain the `Progress` beyond the call; it is handed a copied view.
pub fn RaftNode::with_progress(
  self : RaftNode,
  visit : (String, ProgressStatus) -> Unit,
) -> Unit {
  for entry in self.progress_status() {
    visit(entry.id, entry)
  }
}

///|
/// The full status of this server (etcd's `Status`): basic status + per-follower
/// progress + the current configuration.
pub fn RaftNode::full_status(self : RaftNode) -> FullStatus {
  {
    basic: self.status(),
    progress: self.progress_status(),
    config: self.conf_state(),
  }
}

///|
/// Rebuild the live configuration from a snapshot's `ConfState` — the voters,
/// the outgoing half (so the cluster resumes mid-joint if the snapshot was taken
/// then), and the learners — then reconcile peers/progress. This is what stops a
/// snapshot restore from silently losing the membership.
fn RaftNode::restore_conf_state(self : RaftNode, cs : ConfState) -> Unit {
  self.config.members.clear()
  for v in cs.voters {
    self.config.members.push(v)
  }
  self.config.outgoing.clear()
  for v in cs.voters_outgoing {
    self.config.outgoing.push(v)
  }
  self.config.joint = cs.voters_outgoing.length() > 0
  self.config.learners.clear()
  for l in cs.learners {
    self.config.learners.push(l)
  }
  self.config.learners_next.clear()
  for l in cs.learners_next {
    self.config.learners_next.push(l)
  }
  self.reconcile_peers()
}

///|
/// Apply one committed batch configuration change (joint enter/leave).
fn RaftNode::apply_conf_change_v2(self : RaftNode, v2 : ConfChangeV2) -> Unit {
  if v2.is_leave() {
    self.config.leave_joint()
  } else {
    let (auto, joint) = v2.enters_joint()
    if joint {
      // Enter joint consensus: snapshot the current voters as the outgoing half
      // *before* applying the batch, so a voter demoted here is seen to still be
      // an outgoing voter and is staged in learners_next rather than losing its
      // vote mid-transition. Decisions then need a majority of *both* halves
      // until the matching leave commits (§4.3).
      self.config.begin_joint(self.config.members.copy())
      for c in v2.changes {
        c.apply_to(self.config)
      }
      // Record auto-leave as *durable* config state (from this committed entry),
      // not a transient flag, so a truncated LeaveJoint cannot lose the intent.
      self.config.auto_leave = auto
    } else {
      // Applied simply (an `Auto` transition changing at most one voter): no
      // joint transition, matching etcd's ConfChangeV2 `Auto` path.
      for c in v2.changes {
        c.apply_to(self.config)
      }
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
        self.progress[id] = Progress::new(
          self.core.last_log_index() + 1,
          max_inflight=self.max_inflight,
          max_inflight_bytes=self.max_inflight_bytes,
        )
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
  // etcd's `switchToConfig`: a leader that is removed (or demoted to a learner)
  // steps down only when `StepDownOnRemoval` is set. By default it stays leader
  // over a configuration it is no longer part of, until the survivors elect a
  // replacement — matching etcd, whose zero-valued Config does not step down.
  if self.core.role() == Leader &&
    self.step_down_on_removal &&
    (!self.config.contains(self.id) || self.config.is_learner(self.id)) {
    self.logger.log(
      Info,
      "\{self.id} stepping down: removed from configuration",
    )
    self.step_down()
  }
}
