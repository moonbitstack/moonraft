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
