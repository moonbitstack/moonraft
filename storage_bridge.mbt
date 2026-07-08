///|
/// Write a node's durable state into a `MemoryStorage`: the snapshot baseline
/// (if any), the HardState, and every in-memory log entry. Together with
/// `load_from` this is the persist half of a node's restart path expressed over
/// the storage engine rather than the write-ahead log.
pub fn Node::save_into(self : Node, storage : MemoryStorage) -> Unit {
  if self.snapshot_index > 0 {
    storage.apply_snapshot({
      last_index: self.snapshot_index,
      last_term: self.snapshot_term,
      data: b"",
    })
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
/// crash-recovery path.
pub fn Node::load_from(self : Node, storage : &RaftStorage) -> Unit {
  let snap = storage.storage_snapshot()
  if !snap.is_empty() {
    self.install_snapshot(snap)
  }
  self.apply_hard_state(storage.storage_hard_state())
  let first = storage.first_index()
  let last = storage.last_index()
  if last >= first {
    for e in storage.slice(first, last + 1) {
      self.accept_entry(e)
    }
  }
}
