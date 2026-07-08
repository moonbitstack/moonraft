///|
/// Read access to a node's durable log, modelled on etcd's Storage interface.
/// The consensus core reads entries, terms and the snapshot back through this
/// trait, which lets the same core run over memory, a file or a database.
pub trait RaftStorage {
  fn first_index(Self) -> UInt64
  fn last_index(Self) -> UInt64
  fn storage_term(Self, UInt64) -> UInt64?
  fn slice(Self, UInt64, UInt64) -> Array[Entry]
  fn storage_snapshot(Self) -> Snapshot
  fn storage_hard_state(Self) -> HardState
}

///|
/// An in-memory `RaftStorage`. `ents[0]` is a sentinel whose index and term are
/// the snapshot baseline, so `ents[i]` always holds the entry at absolute index
/// `ents[0].index + i`. This mirrors etcd's MemoryStorage layout, which keeps
/// index arithmetic branch-free.
pub struct MemoryStorage {
  mut hard : HardState
  mut snap : Snapshot
  ents : Array[Entry]
}

///|
/// Create empty storage: an initial HardState, the empty snapshot, and a lone
/// sentinel entry at index 0.
pub fn MemoryStorage::new() -> MemoryStorage {
  {
    hard: HardState::initial(),
    snap: Snapshot::empty(),
    ents: [Entry::normal(0, 0, b"")],
  }
}

///|
/// The absolute index of the sentinel: one below the first real entry.
fn MemoryStorage::offset(self : MemoryStorage) -> UInt64 {
  self.ents[0].index
}

///|
impl RaftStorage for MemoryStorage with fn first_index(self) {
  self.offset() + 1
}

///|
impl RaftStorage for MemoryStorage with fn last_index(self) {
  self.offset() + self.ents.length().to_uint64() - 1
}

///|
impl RaftStorage for MemoryStorage with fn storage_term(self, index) {
  let off = self.offset()
  if index < off || index > self.last_index() {
    None
  } else {
    Some(self.ents[(index - off).to_int()].term)
  }
}

///|
impl RaftStorage for MemoryStorage with fn slice(self, lo, hi) {
  let out : Array[Entry] = []
  let off = self.offset()
  let start = if lo <= off { off + 1 } else { lo }
  let last = self.last_index()
  let stop = if hi > last + 1 { last + 1 } else { hi }
  let mut i = start
  while i < stop {
    out.push(self.ents[(i - off).to_int()])
    i = i + 1
  }
  out
}

///|
impl RaftStorage for MemoryStorage with fn storage_snapshot(self) {
  self.snap
}

///|
impl RaftStorage for MemoryStorage with fn storage_hard_state(self) {
  self.hard
}

///|
/// Record the HardState (term, vote, commit) for the next restart.
pub fn MemoryStorage::set_hard_state(
  self : MemoryStorage,
  hs : HardState,
) -> Unit {
  self.hard = hs
}

///|
/// Append entries to the log, overwriting any conflicting suffix. Entries at or
/// before the sentinel are already compacted away and are ignored.
pub fn MemoryStorage::append(
  self : MemoryStorage,
  entries : Array[Entry],
) -> Unit {
  if entries.length() == 0 {
    return
  }
  let off = self.offset()
  let first = entries[0].index
  let last = entries[entries.length() - 1].index
  if last <= off {
    return
  }
  // Drop any prefix the sentinel already covers.
  let mut start = 0
  while start < entries.length() && entries[start].index <= off {
    start = start + 1
  }
  // Keep our entries up to just before the first incoming index, then append.
  let keep_upto = (first - off).to_int()
  let cutoff = if keep_upto < 1 { 1 } else { keep_upto }
  while self.ents.length() > cutoff {
    self.ents.pop() |> ignore
  }
  let mut i = start
  while i < entries.length() {
    self.ents.push(entries[i])
    i = i + 1
  }
}

///|
/// Replace the whole log with a snapshot baseline, resetting the sentinel to
/// the snapshot's index and term (Raft §7). The commit index follows the
/// snapshot forward.
pub fn MemoryStorage::apply_snapshot(
  self : MemoryStorage,
  snapshot : Snapshot,
) -> Unit {
  self.snap = snapshot
  self.ents.clear()
  self.ents.push(Entry::normal(snapshot.last_term, snapshot.last_index, b""))
  if self.hard.commit < snapshot.last_index {
    self.hard = { ..self.hard, commit: snapshot.last_index }
  }
}

///|
/// Discard every entry at or before `compact_index`, moving the sentinel up to
/// that index. The index must be committed and still within the in-memory log.
/// Returns whether the log was compacted.
pub fn MemoryStorage::compact(
  self : MemoryStorage,
  compact_index : UInt64,
) -> Bool {
  let off = self.offset()
  if compact_index <= off || compact_index > self.last_index() {
    return false
  }
  let cut = (compact_index - off).to_int()
  let kept : Array[Entry] = []
  // The entry at compact_index becomes the new sentinel (index/term kept, no
  // payload) so slice arithmetic stays valid.
  let base = self.ents[cut]
  kept.push(Entry::normal(base.term, base.index, b""))
  let mut i = cut + 1
  while i < self.ents.length() {
    kept.push(self.ents[i])
    i = i + 1
  }
  self.ents.clear()
  for e in kept {
    self.ents.push(e)
  }
  true
}

///|
/// Build a snapshot at `index` carrying `data`, and remember it as the storage
/// snapshot. The index must be within the log; returns the snapshot.
pub fn MemoryStorage::create_snapshot(
  self : MemoryStorage,
  index : UInt64,
  data : Bytes,
) -> Snapshot? {
  match self.storage_term(index) {
    Some(term) => {
      let snap : Snapshot = { last_index: index, last_term: term, data }
      self.snap = snap
      Some(snap)
    }
    None => None
  }
}
