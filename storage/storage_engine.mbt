// Copyright 2015 The etcd Authors
// Copyright 2026 Leo Cheng
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

///|
/// The failure modes a `RaftStorage` can report, one variant per distinguishable
/// condition so a caller can tell "this index is gone forever" apart from "this
/// index has not arrived yet" — a distinction etcd draws with distinct sentinel
/// errors and one the consensus core depends on to decide between sending a
/// snapshot and simply waiting.
pub(all) suberror StorageError {
  /// The requested index predates the last snapshot: it has been compacted away.
  Compacted
  /// The requested entry is past the end of the log: not available (yet).
  Unavailable
  /// A snapshot older than the one already stored was offered.
  SnapOutOfDate
  /// The backend needs more time to materialize the snapshot; retry later.
  SnapshotTemporarilyUnavailable
} derive(Eq)

///|
/// Read access to a node's durable log, modelled on etcd's Storage interface.
/// The consensus core reads entries, terms and the snapshot back through this
/// trait, which lets the same core run over memory, a file or a database.
///
/// The index/term accessors raise `StorageError` so a compacted index is
/// reported distinctly from an unavailable one, exactly as etcd's `Storage`
/// contract requires.
pub(open) trait RaftStorage {
  /// The persisted HardState (term, vote, commit) to resume from.
  fn initial_state(Self) -> HardState
  /// Consecutive entries in `[lo, hi)`, capped so their total encoding size does
  /// not exceed `max_size` (but at least one entry is always returned). Raises
  /// `Compacted` if `lo` is compacted, `Unavailable` if the range is empty.
  fn storage_entries(Self, UInt64, UInt64, UInt64) -> Array[Entry] raise StorageError
  /// The term of the entry at `index`, which must lie in
  /// `[first_index-1, last_index]`. Raises `Compacted`/`Unavailable` otherwise.
  fn storage_term(Self, UInt64) -> UInt64 raise StorageError
  /// The index of the first entry still available (one past the snapshot).
  fn first_index(Self) -> UInt64
  /// The index of the last entry in the log.
  fn last_index(Self) -> UInt64
  /// The most recent snapshot. Raises `SnapshotTemporarilyUnavailable` when the
  /// backend is still preparing it.
  fn storage_snapshot(Self) -> Snapshot raise StorageError
  /// Persist `entries`, extending the stable log. The port's `RaftLog` writes the
  /// confirmed unstable prefix back through this (etcd persists through the
  /// application; the port folds it into the storage abstraction), so the log can
  /// be driven over any backend, not only `MemoryStorage`.
  fn append(Self, Array[Entry]) -> Unit
}

///|
/// An in-memory `RaftStorage`. `ents[0]` is a sentinel whose index and term are
/// the snapshot baseline, so `ents[i]` always holds the entry at absolute index
/// `ents[0].index + i`. This mirrors etcd's MemoryStorage layout, which keeps
/// index arithmetic branch-free.
pub struct MemoryStorage {
  mut hard : HardState
  mut snap : Snapshot
  mut ents : Array[Entry]
  // When set, `storage_snapshot` reports the snapshot as not-yet-ready. This
  // models etcd's contract where a backend may need time to prepare a snapshot;
  // the consensus core then knows to wait rather than treat it as an error.
  mut snapshot_pending : Bool
}

///|
/// Create empty storage: an initial HardState, the empty snapshot, and a lone
/// sentinel entry at index 0.
pub fn MemoryStorage::new() -> MemoryStorage {
  {
    hard: HardState::initial(),
    snap: Snapshot::empty(),
    ents: [Entry::normal(0, 0, b"")],
    snapshot_pending: false,
  }
}

///|
/// Build storage directly over a given entry array, treating `ents[0]` as the
/// compaction sentinel. Mirrors etcd's `&MemoryStorage{ents: ents}` test setup.
pub fn MemoryStorage::from_ents(ents : Array[Entry]) -> MemoryStorage {
  {
    hard: HardState::initial(),
    snap: Snapshot::empty(),
    ents,
    snapshot_pending: false,
  }
}

///|
/// The raw entry array, sentinel included. Test-facing, to assert the exact
/// post-compaction/append layout the way etcd's storage tests do.
pub fn MemoryStorage::raw_ents(self : MemoryStorage) -> Array[Entry] {
  self.ents
}

///|
/// The absolute index of the sentinel: one below the first real entry.
fn MemoryStorage::offset(self : MemoryStorage) -> UInt64 {
  self.ents[0].index
}

///|
fn MemoryStorage::last_index_of(self : MemoryStorage) -> UInt64 {
  self.offset() + self.ents.length().to_uint64() - 1
}

///|
pub impl RaftStorage for MemoryStorage with fn first_index(self) {
  self.offset() + 1
}

///|
pub impl RaftStorage for MemoryStorage with fn last_index(self) {
  self.last_index_of()
}

///|
pub impl RaftStorage for MemoryStorage with fn storage_term(self, index) {
  let off = self.offset()
  if index < off {
    raise Compacted
  }
  if (index - off).to_int() >= self.ents.length() {
    raise Unavailable
  }
  self.ents[(index - off).to_int()].term
}

///|
pub impl RaftStorage for MemoryStorage with fn storage_entries(
  self,
  lo,
  hi,
  max_size,
) {
  let off = self.offset()
  if lo <= off {
    raise Compacted
  }
  if hi > self.last_index_of() + 1 {
    abort("entries hi is out of bound of last index")
  }
  // Only the sentinel remains: no real entries to hand back.
  if self.ents.length() == 1 {
    raise Unavailable
  }
  limit_size(self.ents[(lo - off).to_int():(hi - off).to_int()], max_size)
}

///|
pub impl RaftStorage for MemoryStorage with fn storage_snapshot(self) {
  if self.snapshot_pending {
    raise SnapshotTemporarilyUnavailable
  }
  self.snap
}

///|
pub impl RaftStorage for MemoryStorage with fn initial_state(self) {
  self.hard
}

///|
pub impl RaftStorage for MemoryStorage with fn append(self, entries) {
  MemoryStorage::append(self, entries)
}

///|
/// Report the snapshot as not-yet-ready (true) or ready (false). Lets a backend
/// signal that `storage_snapshot()` should be retried rather than treated as an
/// error.
pub fn MemoryStorage::set_snapshot_pending(
  self : MemoryStorage,
  pending : Bool,
) -> Unit {
  self.snapshot_pending = pending
}

///|
/// A best-effort, clamping read of entries with indices in `[lo, hi)`: indices
/// at or before the sentinel and past the last entry are simply skipped rather
/// than raising. Used where an approximate window is wanted without the strict
/// etcd error contract.
pub fn MemoryStorage::slice(
  self : MemoryStorage,
  lo : UInt64,
  hi : UInt64,
) -> Array[Entry] {
  let out : Array[Entry] = []
  let off = self.offset()
  let start = if lo <= off { off + 1 } else { lo }
  let last = self.last_index_of()
  let stop = if hi > last + 1 { last + 1 } else { hi }
  let mut i = start
  while i < stop {
    out.push(self.ents[(i - off).to_int()])
    i = i + 1
  }
  out
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
/// Append entries to the log, overwriting any conflicting suffix. Entries whose
/// indices fall at or before the sentinel are already compacted and are
/// dropped; a gap between the log and the incoming entries is a programming
/// error and aborts, matching etcd's panic.
pub fn MemoryStorage::append(
  self : MemoryStorage,
  entries : Array[Entry],
) -> Unit {
  if entries.is_empty() {
    return
  }
  let first = self.first_index()
  let ent0 = entries[0].index
  let last = ent0 + entries.length().to_uint64() - 1
  // Nothing here is newer than what we already hold.
  if last < first {
    return
  }
  // Drop the prefix the sentinel already covers.
  let start = if first > ent0 { (first - ent0).to_int() } else { 0 }
  let off = (entries[start].index - self.offset()).to_int()
  if off > self.ents.length() {
    abort("missing log entry: append leaves a gap")
  }
  while self.ents.length() > off {
    self.ents.pop() |> ignore
  }
  let mut i = start
  while i < entries.length() {
    self.ents.push(entries[i])
    i = i + 1
  }
}

///|
/// Replace the whole log with a snapshot baseline, resetting the sentinel to the
/// snapshot's index and term (Raft §7). A snapshot no newer than the one already
/// stored is rejected with `SnapOutOfDate`.
pub fn MemoryStorage::apply_snapshot(
  self : MemoryStorage,
  snapshot : Snapshot,
) -> Unit raise StorageError {
  let ms_index = self.snap.last_index
  let snap_index = snapshot.last_index
  // During bootstrap only the ConfState may be set, leaving index and term 0;
  // that case (ms_index == 0) is allowed through.
  if ms_index != 0 && ms_index >= snap_index {
    raise SnapOutOfDate
  }
  self.seed_snapshot(snapshot)
}

///|
/// Install a snapshot baseline unconditionally, resetting the sentinel to its
/// index and term. This is the write half of `apply_snapshot` without the
/// out-of-date guard, for seeding a freshly created storage whose empty baseline
/// cannot predate anything.
pub fn MemoryStorage::seed_snapshot(
  self : MemoryStorage,
  snapshot : Snapshot,
) -> Unit {
  self.snap = snapshot
  self.ents = [Entry::normal(snapshot.last_term, snapshot.last_index, b"")]
}

///|
/// Discard every entry at or before `compact_index`, moving the sentinel up to
/// that index. An index at or before the current sentinel is `Compacted`; one
/// past the last entry aborts (etcd panics), since it is a caller error.
pub fn MemoryStorage::compact(
  self : MemoryStorage,
  compact_index : UInt64,
) -> Unit raise StorageError {
  let off = self.offset()
  if compact_index <= off {
    raise Compacted
  }
  if compact_index > self.last_index_of() {
    abort("compact index is out of bound of last index")
  }
  let cut = (compact_index - off).to_int()
  let base = self.ents[cut]
  let kept : Array[Entry] = [Entry::normal(base.term, base.index, b"")]
  let mut i = cut + 1
  while i < self.ents.length() {
    kept.push(self.ents[i])
    i = i + 1
  }
  self.ents = kept
}

///|
/// Build a snapshot at `index` carrying `data`, remember it, and return it. An
/// index at or before the current snapshot is `SnapOutOfDate`; one past the last
/// entry aborts (etcd panics).
pub fn MemoryStorage::create_snapshot(
  self : MemoryStorage,
  index : UInt64,
  data : Bytes,
  conf_state? : ConfState? = None,
) -> Snapshot raise StorageError {
  if index <= self.snap.last_index {
    raise SnapOutOfDate
  }
  if index > self.last_index_of() {
    abort("snapshot index is out of bound of last index")
  }
  let off = self.offset()
  let term = self.ents[(index - off).to_int()].term
  // etcd's CreateSnapshot(i, cs, data): a supplied ConfState is recorded;
  // otherwise the snapshot keeps the membership already on record, rather than
  // silently dropping it (which would lose the cluster config on restore).
  let snap : Snapshot = {
    last_index: index,
    last_term: term,
    data,
    conf_state: conf_state.unwrap_or(self.snap.conf_state),
  }
  self.snap = snap
  snap
}
