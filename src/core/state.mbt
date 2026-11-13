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
/// A single Raft node: its identity, current role, and the state it holds.
///
/// `current_term`, `voted_for` and `log` make up the state a real deployment
/// must persist to stable storage before replying to any RPC, so the node
/// recovers correctly after a crash. `commit_index` and `last_applied` are
/// volatile: they are safe to lose and are rebuilt after a restart.
pub struct Node {
  id : String
  mut role : Role
  mut current_term : UInt64
  mut voted_for : String?
  log : Array[Entry]
  mut commit_index : UInt64
  mut last_applied : UInt64
  mut snapshot_index : UInt64
  mut snapshot_term : UInt64
  // The state image the installed snapshot carried. The driver applies it to the
  // state machine, so it has to reach `Ready.snapshot` intact; the index and term
  // alone name the baseline but cannot rebuild what was applied.
  priv mut snapshot_data : Bytes
  // How many snapshots this node has *installed from a leader*. A local
  // compaction also advances `snapshot_index` — it moves the durable floor — but
  // installs nothing, so only this counter makes a snapshot real to the driver.
  mut snapshots_installed : UInt64
}

///|
/// Create a fresh node that starts as a follower at term 0 with an empty log.
pub fn Node::new(id : String) -> Node {
  {
    id,
    role: Follower,
    current_term: 0,
    voted_for: None,
    log: [],
    commit_index: 0,
    last_applied: 0,
    snapshot_index: 0,
    snapshot_term: 0,
    snapshot_data: b"",
    snapshots_installed: 0,
  }
}

///|
/// The node's current role.
pub fn Node::role(self : Node) -> Role {
  self.role
}

///|
/// The node's current term.
pub fn Node::current_term(self : Node) -> UInt64 {
  self.current_term
}

///|
/// Step down to follower and adopt a newly observed, higher term, clearing
/// any vote cast in the old term.
pub fn Node::become_follower(self : Node, term : UInt64) -> Unit {
  self.role = Follower
  // etcd's `reset` only clears a vote when the term actually moves; a same-term
  // step-down (check-quorum, a lost election) keeps the vote it cast, which is
  // what leaves the HardState unchanged and the next Ready MustSync=false.
  if self.current_term != term {
    self.current_term = term
    self.voted_for = None
  }
}

///|
/// Enter the pre-vote probe phase (etcd's `becomePreCandidate`): change the role
/// but NOT the term or vote — a pre-candidate solicits votes under a hypothetical
/// next term it has not adopted.
pub fn Node::become_pre_candidate(self : Node) -> Unit {
  self.role = PreCandidate
}

///|
/// Start a new election: advance the term and vote for self.
pub fn Node::become_candidate(self : Node) -> Unit {
  self.current_term = self.current_term + 1
  self.role = Candidate
  self.voted_for = Some(self.id)
}

///|
/// Take leadership after winning a majority of votes in the current term.
pub fn Node::become_leader(self : Node) -> Unit {
  self.role = Leader
}

///|
/// Advance the commit index once a higher index is known to be committed.
/// Never moves backwards.
pub fn Node::advance_commit(self : Node, index : UInt64) -> Unit {
  if index > self.commit_index {
    self.commit_index = index
  }
}

///|
/// Record that the state machine has applied entries up through `index`.
pub fn Node::mark_applied(self : Node, index : UInt64) -> Unit {
  self.last_applied = index
}
