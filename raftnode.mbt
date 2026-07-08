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
  mut rng : UInt64
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
    pre_vote: true,
    check_quorum: false,
    in_pre_campaign: false,
    leader_id: None,
    election_elapsed: 0,
    heartbeat_elapsed: 0,
    election_timeout,
    heartbeat_timeout,
    randomized_election_timeout: election_timeout,
    rng: seed,
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
/// Reset both timers and pick a fresh randomized election timeout.
fn RaftNode::reset_timeout(self : RaftNode) -> Unit {
  self.election_elapsed = 0
  self.heartbeat_elapsed = 0
  let spread = (self.next_rand() % self.election_timeout.to_uint64()).to_int()
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
  self.reset_timeout()
}

///|
/// Start a campaign. With pre-vote enabled the node first runs a pre-vote round
/// under a hypothetical next term without touching its own term or vote, so a
/// partitioned node cannot force real elections and inflate terms (Raft §9.6,
/// pre-vote). Winning the pre-vote — or pre-vote being disabled — starts the
/// real election.
pub fn RaftNode::campaign(self : RaftNode) -> Array[Message] {
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
  self.progress.clear()
  let last = self.core.last_log_index()
  for p in self.peers {
    self.progress[p] = Progress::new(last + 1)
  }
  let _ = self.core.append(self.core.current_term(), b"")
  self.heartbeat_elapsed = 0
  self.election_elapsed = 0
  // In a single-node cluster the no-op commits at once, which also arms reads.
  self.maybe_commit() |> ignore
}

///|
/// Append a client command to the leader's log and replicate it. A no-op on a
/// non-leader. Returns the AppendEntries messages to send.
pub fn RaftNode::propose(self : RaftNode, command : Bytes) -> Array[Message] {
  if self.core.role() != Leader {
    return []
  }
  let _ = self.core.append(self.core.current_term(), command)
  self.maybe_commit() |> ignore
  self.bcast_append()
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
  let entries = self.core.entries_after(prev)
  if p.state == Replicate && entries.length() > 0 {
    p.optimistic_advance(entries[entries.length() - 1].index)
  }
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
  for p in self.peers {
    out.push(self.send_to(p))
  }
  out
}

///|
/// Send a heartbeat — an empty AppendEntries anchored at the follower's known
/// match point — to every follower.
fn RaftNode::bcast_heartbeat(self : RaftNode) -> Array[Message] {
  let out : Array[Message] = []
  for peer in self.peers {
    let p = self.progress[peer]
    let prev = if p.match_index < self.core.last_log_index() {
      p.match_index
    } else {
      self.core.last_log_index()
    }
    let anchor = if prev < self.core.snapshot_index {
      self.core.snapshot_index
    } else {
      prev
    }
    out.push(
      Message::new(
        self.id,
        peer,
        Append({
          term: self.core.current_term(),
          leader_id: self.id,
          prev_log_index: anchor,
          prev_log_term: self.core.term_at(anchor),
          entries: [],
          leader_commit: self.core.commit_index,
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
  let last = self.core.last_log_index()
  let mut candidate = self.core.commit_index
  let mut n = last
  while n > self.core.commit_index {
    if self.core.term_at(n) == self.core.current_term() {
      let acked : Array[String] = [self.id]
      for peer, p in self.progress {
        if p.match_index >= n {
          acked.push(peer)
        }
      }
      if self.config.has_majority(acked) {
        candidate = n
        break
      }
    }
    n = n - 1
  }
  if candidate > self.core.commit_index {
    self.core.advance_commit(candidate)
    true
  } else {
    false
  }
}
