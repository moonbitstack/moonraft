///|
/// One command entry in the replicated log.
///
/// `term` is the leader's term — a monotonically increasing logical clock —
/// at the moment the entry was created. `index` is the entry's 1-based
/// position in the log; index 0 is the empty position before the first entry.
/// `command` is the opaque payload handed to the state machine once the entry
/// is committed.
pub(all) struct Entry {
  term : UInt64
  index : UInt64
  command : Bytes
} derive(Eq, Show)
