///|
/// Write a node's durable state into a `MemoryStorage`: the snapshot baseline
/// (if any), the HardState, and every in-memory log entry. Together with
/// `load_from` this is the persist half of a node's restart path expressed over
/// the storage engine rather than the write-ahead log.
pub fn Node::save_into(self : Node, storage : MemoryStorage) -> Unit {
  if self.snapshot_index > 0 {
    // A fresh storage has an empty snapshot, so this never actually predates
    // anything; the catch keeps `save_into` total in the face of the etcd
    // `SnapOutOfDate` contract.
    storage.apply_snapshot({
      last_index: self.snapshot_index,
      last_term: self.snapshot_term,
      data: b"",
    }) catch {
      SnapOutOfDate => ()
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
  let snap = storage.storage_snapshot() catch {
    SnapshotTemporarilyUnavailable => Snapshot::empty()
    _ => Snapshot::empty()
  }
  if !snap.is_empty() {
    self.install_snapshot(snap)
  }
  self.apply_hard_state(storage.initial_state())
  let first = storage.first_index()
  let last = storage.last_index()
  if last >= first {
    let loaded = storage.storage_entries(first, last + 1, no_limit) catch {
      Compacted => []
      Unavailable => []
      _ => []
    }
    for e in loaded {
      self.accept_entry(e)
    }
  }
}
