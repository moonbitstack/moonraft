///|
/// The persistent state a Raft node must save to stable storage before
/// responding to any RPC (Raft §5.3): the current term, the vote cast in that
/// term, and the log. It is exactly what a node reloads to recover after a
/// crash.
pub(all) struct Persisted {
  term : UInt64
  voted_for : String?
  log : Array[Entry]
}

///|
/// Durable storage for a node's persistent state. A real deployment backs
/// this with a file or a database; the tests use an in-memory implementation.
/// Keeping it a trait decouples the consensus core from any particular I/O.
pub trait LogStore {
  fn persist(Self, Persisted) -> Unit
  fn restore(Self) -> Persisted?
}

///|
/// Capture this node's persistent state so it can be written to a `LogStore`.
/// The log is copied, so the returned value is a stable point-in-time image.
pub fn Node::persisted(self : Node) -> Persisted {
  { term: self.current_term, voted_for: self.voted_for, log: self.log.copy() }
}

///|
/// Reload persistent state into this node after a restart, replacing whatever
/// it currently holds.
pub fn Node::recover(self : Node, state : Persisted) -> Unit {
  self.current_term = state.term
  self.voted_for = state.voted_for
  self.log.clear()
  for entry in state.log {
    self.log.push(entry)
  }
}
