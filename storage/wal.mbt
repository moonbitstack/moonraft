///|
/// One record in the write-ahead log. A real Raft node appends these to a file
/// (fsync'd before it replies to any RPC) and replays them in order after a
/// crash to rebuild exactly the state it had promised (Raft §5.3, §7). A
/// `WalSnapshot` marks a compaction baseline; the `WalEntry` records after it
/// carry the log tail; `WalHardState` records the term/vote/commit at the time.
pub(all) enum WalRecord {
  WalHardState(HardState)
  WalEntry(Entry)
  WalSnapshot(Snapshot)
} derive(Eq)

///|
/// Durable, append-only write-ahead log. Implemented over a file in production;
/// the in-memory `MemWal` in the tests is used for tests and single-process
/// runs.
pub(open) trait WalStore {
  fn append_record(Self, WalRecord) -> Unit
  fn load(Self) -> Array[WalRecord]
}
