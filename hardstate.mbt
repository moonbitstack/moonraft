///|
/// The small piece of durable state Raft must flush before answering any RPC:
/// the current term, the vote cast in that term, and the commit index. etcd
/// calls this the HardState; persisting it is what lets a node rejoin without
/// violating the election-safety or state-machine-safety properties (§5.3).
pub(all) struct HardState {
  term : UInt64
  vote : String?
  commit : UInt64
} derive(Eq)

///|
/// The HardState a fresh node starts from: term 0, no vote, nothing committed.
pub fn HardState::initial() -> HardState {
  { term: 0, vote: None, commit: 0 }
}

///|
/// Capture this node's HardState for durable storage.
pub fn Node::hard_state(self : Node) -> HardState {
  { term: self.current_term, vote: self.voted_for, commit: self.commit_index }
}

///|
/// Adopt a HardState read back from storage. The commit index never moves
/// backwards: a stale HardState replayed after a snapshot must not un-commit
/// entries the snapshot already covers.
pub fn Node::apply_hard_state(self : Node, hs : HardState) -> Unit {
  self.current_term = hs.term
  self.voted_for = hs.vote
  if hs.commit > self.commit_index {
    self.commit_index = hs.commit
  }
}
