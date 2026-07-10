///|
/// One record in the write-ahead log. A real Raft node appends these to a file
/// (fsync'd before it replies to any RPC) and replays them in order after a
/// crash to rebuild exactly the state it had promised (Raft §5.3, §7). A
/// `WalSnapshot` marks a compaction baseline; the `WalEntry` records after it
/// carry the log tail; `WalHardState` records the term/vote/commit at the time.
pub enum WalRecord {
  WalHardState(HardState)
  WalEntry(Entry)
  WalSnapshot(Snapshot)
} derive(Eq)

///|
/// Durable, append-only write-ahead log. Implemented over a file in production;
/// the in-memory `MemWal` in the tests is used for tests and single-process
/// runs.
pub trait WalStore {
  fn append_record(Self, WalRecord) -> Unit
  fn load(Self) -> Array[WalRecord]
}

///|
/// Persist a node's full state to a write-ahead log as a snapshot baseline (if
/// any), the current HardState, and every in-memory log entry. This is the
/// checkpoint a node writes so a later `replay` reconstructs it.
pub fn Node::save_to(self : Node, wal : &WalStore) -> Unit {
  if self.snapshot_index > 0 {
    wal.append_record(
      WalSnapshot({
        last_index: self.snapshot_index,
        last_term: self.snapshot_term,
        data: b"",
        conf_state: ConfState::empty(),
      }),
    )
  }
  wal.append_record(WalHardState(self.hard_state()))
  for entry in self.log {
    wal.append_record(WalEntry(entry))
  }
}

///|
/// Accept a single log entry during replay, appending it or overwriting a
/// conflicting suffix so the rebuilt log matches what was durably recorded.
fn Node::accept_entry(self : Node, e : Entry) -> Unit {
  if e.index <= self.snapshot_index {
    return
  }
  if e.index > self.last_log_index() {
    if e.index == self.last_log_index() + 1 {
      self.log.push(e)
    }
  } else if self.term_at(e.index) != e.term {
    self.truncate_from(e.index)
    self.log.push(e)
  }
}

///|
/// Replay a write-ahead log into this node, rebuilding its durable state in
/// record order: snapshots set the baseline, HardState records set term, vote
/// and commit, and entry records rebuild the log tail. This is the crash-
/// recovery path a node runs on restart before serving any request.
pub fn Node::replay(self : Node, records : Array[WalRecord]) -> Unit {
  for record in records {
    match record {
      WalSnapshot(snap) => self.install_snapshot(snap)
      WalHardState(hs) => self.apply_hard_state(hs)
      WalEntry(entry) => self.accept_entry(entry)
    }
  }
}

///|
/// Rebuild a node from a write-ahead log, the whole restart path in one call.
pub fn Node::recover_from(self : Node, wal : &WalStore) -> Unit {
  self.replay(wal.load())
}
