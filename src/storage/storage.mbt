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
pub(open) trait LogStore {
  fn persist(Self, Persisted) -> Unit
  fn restore(Self) -> Persisted?
}
