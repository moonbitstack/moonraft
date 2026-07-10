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
fn u64_min(a : UInt64, b : UInt64) -> UInt64 {
  if a < b {
    a
  } else {
    b
  }
}

///|
fn u64_max(a : UInt64, b : UInt64) -> UInt64 {
  if a > b {
    a
  } else {
    b
  }
}

///|
/// The raft log (etcd's `raftLog`): a `MemoryStorage` of durable entries with an
/// `Unstable` in-memory tail layered on top, plus the commit/apply cursors.
///
/// `committed` is the highest index known committed on a quorum; `applying` and
/// `applied` track how far the state machine has been told to apply and has
/// finished applying. `applying_ents_size` / `max_applying_ents_size` bound the
/// bytes of committed-but-unapplied entries handed out at once, so a burst of
/// commits cannot force an unbounded apply batch (byte-level pagination).
pub struct RaftLog {
  storage : MemoryStorage
  unstable : Unstable
  mut committed : UInt64
  mut applying : UInt64
  mut applied : UInt64
  max_applying_ents_size : UInt64
  mut applying_ents_size : UInt64
  mut applying_ents_paused : Bool
}

///|
/// Recover a log from `storage`, positioned at the last compaction: committed,
/// applying and applied all start at `first_index - 1`, and the unstable tail is
/// empty just past `last_index`.
pub fn RaftLog::new(storage : MemoryStorage) -> RaftLog {
  RaftLog::new_with_size(storage, no_limit)
}

///|
/// As `new`, but capping the byte size of entries returned per apply batch.
pub fn RaftLog::new_with_size(
  storage : MemoryStorage,
  max_applying_ents_size : UInt64,
) -> RaftLog {
  let first = storage.first_index()
  let last = storage.last_index()
  {
    storage,
    unstable: Unstable::new(last + 1),
    committed: first - 1,
    applying: first - 1,
    applied: first - 1,
    max_applying_ents_size,
    applying_ents_size: 0,
    applying_ents_paused: false,
  }
}

///|
/// The underlying unstable tail, for drivers that persist and stabilize it.
pub fn RaftLog::unstable_view(self : RaftLog) -> Unstable {
  self.unstable
}

///|
/// The committed index.
pub fn RaftLog::committed(self : RaftLog) -> UInt64 {
  self.committed
}

///|
/// The applied index.
pub fn RaftLog::applied(self : RaftLog) -> UInt64 {
  self.applied
}

///|
/// The first index still readable (one past the snapshot).
pub fn RaftLog::first_index(self : RaftLog) -> UInt64 {
  match self.unstable.maybe_first_index() {
    Some(i) => i
    None => self.storage.first_index()
  }
}

///|
/// The index of the last entry in the log.
pub fn RaftLog::last_index(self : RaftLog) -> UInt64 {
  match self.unstable.maybe_last_index() {
    Some(i) => i
    None => self.storage.last_index()
  }
}

///|
/// The term of entry `i`. `Compacted` if it predates the first index,
/// `Unavailable` if it is past the last. The term at `first_index-1` is retained
/// for log-matching even though the entry itself is gone.
pub fn RaftLog::term(self : RaftLog, i : UInt64) -> UInt64 raise StorageError {
  // Consult the unstable tail first; a hit there is always in range.
  match self.unstable.maybe_term(i) {
    Some(t) => t
    None => {
      if i + 1 < self.first_index() {
        raise Compacted
      }
      if i > self.last_index() {
        raise Unavailable
      }
      self.storage.storage_term(i)
    }
  }
}

///|
/// The term at `i`, or 0 when the index is out of bounds (etcd's
/// `zeroTermOnOutOfBounds`), used where a missing term is simply "no match".
pub fn RaftLog::zero_term_on_out_of_bounds(
  self : RaftLog,
  i : UInt64,
) -> UInt64 {
  self.term(i) catch {
    Compacted => 0
    Unavailable => 0
    _ => 0
  }
}

///|
/// Whether the log holds the identified entry with that exact term.
pub fn RaftLog::match_term(self : RaftLog, id : EntryId) -> Bool {
  (Some(self.term(id.index)) catch { _ => None }) == Some(id.term)
}

///|
/// The identity of the last entry.
pub fn RaftLog::last_entry_id(self : RaftLog) -> EntryId {
  let index = self.last_index()
  let t = self.term(index) catch {
    _ => abort("unexpected error getting last term")
  }
  { term: t, index }
}

///|
/// Whether the log ending at `their` is at least as up-to-date as ours (§5.4.1).
pub fn RaftLog::is_up_to_date(self : RaftLog, their : EntryId) -> Bool {
  let our = self.last_entry_id()
  their.term > our.term || (their.term == our.term && their.index >= our.index)
}

///|
/// Try to append the slice `a` after verifying its `prev` matches our log. On a
/// match, splice in any genuinely new tail (a conflict with a committed entry
/// aborts), advance commit to `min(committed, last_new)`, and return the new
/// last index; otherwise return `None`.
pub fn RaftLog::maybe_append(
  self : RaftLog,
  a : LogSlice,
  committed : UInt64,
) -> UInt64? {
  if !self.match_term(a.prev) {
    return None
  }
  let lastnewi = a.prev.index + a.entries.length().to_uint64()
  let ci = self.find_conflict(a.entries[:])
  if ci == 0 {
    ()
  } else if ci <= self.committed {
    abort("entry conflict with committed entry")
  } else {
    let offset = a.prev.index + 1
    if ci - offset > a.entries.length().to_uint64() {
      abort("index out of range")
    }
    self.append(a.entries[(ci - offset).to_int():].to_owned()) |> ignore
  }
  self.commit_to(u64_min(committed, lastnewi))
  Some(lastnewi)
}

///|
/// Append `ents` to the unstable tail and return the new last index. Appending
/// at or before the commit index is a corruption and aborts.
pub fn RaftLog::append(self : RaftLog, ents : Array[Entry]) -> UInt64 {
  if ents.length() == 0 {
    return self.last_index()
  }
  let after = ents[0].index - 1
  if after < self.committed {
    abort("append after is out of range [committed]")
  }
  self.unstable.truncate_and_append(ents)
  self.last_index()
}

///|
/// The index of the first entry in `ents` that conflicts with our log (same
/// index, different term), or the first genuinely new index, or 0 if all match.
pub fn RaftLog::find_conflict(
  self : RaftLog,
  ents : ArrayView[Entry],
) -> UInt64 {
  for e in ents {
    let id = e.id()
    if !self.match_term(id) {
      return id.index
    }
  }
  0
}

///|
/// A best guess at where our log stops matching another whose only known point
/// is `(index, term)`: the greatest `i <= index` with `term(i) <= term`, or with
/// an unknown (compacted/unstored) term. Returns `(i, term(i)-or-0)`.
pub fn RaftLog::find_conflict_by_term(
  self : RaftLog,
  index : UInt64,
  term : UInt64,
) -> (UInt64, UInt64) {
  let mut i = index
  while i > 0 {
    match (Some(self.term(i)) catch { _ => None }) {
      None => return (i, 0)
      Some(our) => if our <= term { return (i, our) }
    }
    i = i - 1
  }
  (0, 0)
}

///|
/// The committed entries whose term matches — advance commit to `at.index`.
pub fn RaftLog::maybe_commit(self : RaftLog, at : EntryId) -> Bool {
  if at.term != 0 && at.index > self.committed && self.match_term(at) {
    self.commit_to(at.index)
    true
  } else {
    false
  }
}

///|
/// Advance the commit index (never backwards). Committing past the last index is
/// a corruption and aborts.
pub fn RaftLog::commit_to(self : RaftLog, tocommit : UInt64) -> Unit {
  if self.committed < tocommit {
    if self.last_index() < tocommit {
      abort("tocommit is out of range [lastIndex]")
    }
    self.committed = tocommit
  }
}

///|
/// Record that the state machine has finished applying up to `i`, releasing
/// `size` bytes of the outstanding apply budget.
pub fn RaftLog::applied_to(self : RaftLog, i : UInt64, size : UInt64) -> Unit {
  if self.committed < i || i < self.applied {
    abort("applied is out of range [prevApplied, committed]")
  }
  self.applied = i
  self.applying = u64_max(self.applying, i)
  self.applying_ents_size = if self.applying_ents_size > size {
    self.applying_ents_size - size
  } else {
    0
  }
  self.applying_ents_paused = self.applying_ents_size >=
    self.max_applying_ents_size
}

///|
/// Record that the application has been handed entries up to `i` to apply,
/// charging `size` bytes against the budget and pausing when it is exhausted or
/// when the next entry would overshoot it.
pub fn RaftLog::accept_applying(
  self : RaftLog,
  i : UInt64,
  size : UInt64,
  allow_unstable : Bool,
) -> Unit {
  if self.committed < i {
    abort("applying is out of range [prevApplying, committed]")
  }
  self.applying = i
  self.applying_ents_size = self.applying_ents_size + size
  self.applying_ents_paused = self.applying_ents_size >=
    self.max_applying_ents_size ||
    i < self.max_appliable_index(allow_unstable)
}

///|
/// The highest index that may be applied: the commit index, capped at the last
/// stable index unless unstable entries are allowed.
fn RaftLog::max_appliable_index(
  self : RaftLog,
  allow_unstable : Bool,
) -> UInt64 {
  let hi = self.committed
  if !allow_unstable {
    u64_min(hi, self.unstable.offset - 1)
  } else {
    hi
  }
}

///|
/// Confirm the unstable tail entries up to `id` are durably stored.
pub fn RaftLog::stable_to(self : RaftLog, id : EntryId) -> Unit {
  self.unstable.stable_to(id)
}

///|
/// Confirm the unstable snapshot at index `i` is durably stored.
pub fn RaftLog::stable_snap_to(self : RaftLog, i : UInt64) -> Unit {
  self.unstable.stable_snap_to(i)
}

///|
/// Mark the current unstable entries and snapshot as being written.
pub fn RaftLog::accept_unstable(self : RaftLog) -> Unit {
  self.unstable.accept_in_progress()
}

///|
/// The unstable entries ready to be written and not already in progress.
pub fn RaftLog::next_unstable_ents(self : RaftLog) -> Array[Entry] {
  self.unstable.next_entries()
}

///|
/// Whether any unstable entries are ready to be written.
pub fn RaftLog::has_next_unstable_ents(self : RaftLog) -> Bool {
  self.next_unstable_ents().length() > 0
}

///|
/// Whether there are any unstable entries, whether or not already in progress.
pub fn RaftLog::has_next_or_in_progress_unstable_ents(self : RaftLog) -> Bool {
  self.unstable.entries.length() > 0
}

///|
fn RaftLog::has_next_or_in_progress_snapshot(self : RaftLog) -> Bool {
  match self.unstable.snapshot {
    Some(_) => true
    None => false
  }
}

///|
/// The unstable snapshot ready to be applied and not already in progress.
pub fn RaftLog::next_unstable_snapshot(self : RaftLog) -> Snapshot? {
  self.unstable.next_snapshot()
}

///|
/// Whether an unstable snapshot is ready to be applied.
pub fn RaftLog::has_next_unstable_snapshot(self : RaftLog) -> Bool {
  match self.unstable.next_snapshot() {
    Some(_) => true
    None => false
  }
}

///|
/// The committed-but-unapplied entries ready for execution, subject to the apply
/// pause, any pending snapshot, and the byte budget. `allow_unstable` lets
/// committed entries still in the unstable tail be applied.
pub fn RaftLog::next_committed_ents(
  self : RaftLog,
  allow_unstable : Bool,
) -> Array[Entry] {
  if self.applying_ents_paused {
    return []
  }
  if self.has_next_or_in_progress_snapshot() {
    return []
  }
  let lo = self.applying + 1
  let hi = self.max_appliable_index(allow_unstable) + 1
  if lo >= hi {
    return []
  }
  let max_size = self.max_applying_ents_size - self.applying_ents_size
  self.slice(lo, hi, max_size) catch {
    _ => abort("unexpected error getting unapplied entries")
  }
}

///|
/// Whether any committed-but-unapplied entries are ready (a light check that
/// avoids the `slice` in `next_committed_ents`).
pub fn RaftLog::has_next_committed_ents(
  self : RaftLog,
  allow_unstable : Bool,
) -> Bool {
  if self.applying_ents_paused {
    return false
  }
  if self.has_next_or_in_progress_snapshot() {
    return false
  }
  let lo = self.applying + 1
  let hi = self.max_appliable_index(allow_unstable) + 1
  lo < hi
}

///|
/// Restore the log to a snapshot baseline: commit follows the snapshot forward,
/// and the unstable tail is replaced by it.
pub fn RaftLog::restore(self : RaftLog, s : Snapshot) -> Unit {
  self.committed = s.last_index
  self.unstable.restore(s)
}

///|
/// The most recent snapshot: the unstable one if present, else storage's.
pub fn RaftLog::snapshot(self : RaftLog) -> Snapshot raise StorageError {
  match self.unstable.snapshot {
    Some(s) => s
    None => self.storage.storage_snapshot()
  }
}

///|
/// Entries starting at `i`, size-capped by `max_size`. Empty if `i` is past the
/// end; raises `Compacted` if `i` has been compacted.
pub fn RaftLog::entries(
  self : RaftLog,
  i : UInt64,
  max_size : UInt64,
) -> Array[Entry] raise StorageError {
  if i > self.last_index() {
    return []
  }
  self.slice(i, self.last_index() + 1, max_size)
}

///|
/// Every entry currently in the log.
pub fn RaftLog::all_entries(self : RaftLog) -> Array[Entry] {
  self.entries(self.first_index(), no_limit) catch {
    Compacted => self.all_entries() // racing compaction: retry
    _ => abort("unexpected error in all_entries")
  }
}

///|
/// Visit `[lo, hi)` in consecutive byte-bounded pages, passing each page to `v`.
/// `v` may raise to stop early. Each page holds at least one entry and at most
/// `page_size` bytes (unless a single entry exceeds it).
pub fn RaftLog::scan(
  self : RaftLog,
  lo : UInt64,
  hi : UInt64,
  page_size : UInt64,
  v : (ArrayView[Entry]) -> Unit raise,
) -> Unit raise {
  let mut lo = lo
  while lo < hi {
    let ents = self.slice(lo, hi, page_size)
    if ents.length() == 0 {
      abort("scan got 0 entries")
    }
    v(ents[:])
    lo = lo + ents.length().to_uint64()
  }
}

///|
/// Entries with indices in `[lo, hi)`, size-capped by `max_size`, drawn from the
/// stable storage and the unstable tail and stitched together. `Compacted` if
/// `lo` predates the first index.
pub fn RaftLog::slice(
  self : RaftLog,
  lo : UInt64,
  hi : UInt64,
  max_size : UInt64,
) -> Array[Entry] raise StorageError {
  self.must_check_out_of_bounds(lo, hi)
  if lo == hi {
    return []
  }
  if lo >= self.unstable.offset {
    return limit_size(self.unstable.slice(lo, hi)[:], max_size)
  }
  let cut = u64_min(hi, self.unstable.offset)
  let ents = self.storage.storage_entries(lo, cut, max_size) catch {
    Compacted => raise Compacted
    Unavailable => abort("entries are unavailable from storage")
    e => raise e
  }
  if hi <= self.unstable.offset {
    return ents
  }
  // The storage read may already have hit the size cap.
  if ents.length().to_uint64() < cut - lo {
    return ents
  }
  let size = ents_size(ents[:])
  if size >= max_size {
    return ents
  }
  let unstable_part = limit_size(
    self.unstable.slice(self.unstable.offset, hi)[:],
    max_size - size,
  )
  // A lone over-budget unstable entry is dropped rather than exceeding the cap.
  if unstable_part.length() == 1 &&
    size + ents_size(unstable_part[:]) > max_size {
    return ents
  }
  let out : Array[Entry] = []
  for e in ents {
    out.push(e)
  }
  for e in unstable_part {
    out.push(e)
  }
  out
}

///|
/// Guard: `first_index <= lo <= hi <= last_index + 1`. `Compacted` when `lo`
/// predates the first index; a high bound past the end aborts (etcd panics).
pub fn RaftLog::must_check_out_of_bounds(
  self : RaftLog,
  lo : UInt64,
  hi : UInt64,
) -> Unit raise StorageError {
  if lo > hi {
    abort("invalid slice: lo > hi")
  }
  let fi = self.first_index()
  if lo < fi {
    raise Compacted
  }
  let length = self.last_index() + 1 - fi
  if hi > fi + length {
    abort("slice out of bound")
  }
}
