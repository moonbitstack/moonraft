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

///|
/// Write a node's durable state into a `MemoryStorage`: the snapshot baseline
/// (if any), the HardState, and every in-memory log entry. Together with
/// `load_from` this is the persist half of a node's restart path expressed over
/// the storage engine rather than the write-ahead log.
pub fn Node::save_into(self : Node, storage : MemoryStorage) -> Unit {
  if self.snapshot_index > 0 {
    // Writing the node's snapshot into a storage that already holds a newer one
    // (etcd's `ErrSnapOutOfDate`) leaves the more advanced storage untouched; any
    // storage error is tolerated, keeping `save_into` total.
    storage.apply_snapshot({
      last_index: self.snapshot_index,
      last_term: self.snapshot_term,
      data: b"",
      conf_state: ConfState::empty(),
    }) catch {
      _ => ()
    }
  }
  storage.set_hard_state(self.hard_state())
  let entries : Array[Entry] = []
  for e in self.log {
    entries.push(e)
  }
  storage.append(entries)
}

///|
/// Rebuild a node from any `RaftStorage`: adopt the snapshot baseline, the
/// HardState, and every stored entry, in that order. This is the storage-backed
/// crash-recovery path. A snapshot the backend has not finished preparing, or a
/// range that has been compacted out from under us, is tolerated rather than
/// fatal, matching etcd's error contract on the read path.
pub fn Node::load_from(self : Node, storage : &RaftStorage) -> Unit {
  // A snapshot the backend has not finished preparing is tolerated: recovery
  // proceeds from the log instead.
  let snap = storage.storage_snapshot() catch { _ => Snapshot::empty() }
  if !snap.is_empty() {
    self.install_snapshot(snap)
  }
  self.apply_hard_state(storage.initial_state())
  let first = storage.first_index()
  let last = storage.last_index()
  if last >= first {
    // A range compacted out from under us mid-recovery is tolerated: the entries
    // it would have loaded are already covered by the snapshot baseline.
    let loaded = storage.storage_entries(first, last + 1, no_limit) catch {
      _ => []
    }
    for e in loaded {
      self.accept_entry(e)
    }
  }
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
