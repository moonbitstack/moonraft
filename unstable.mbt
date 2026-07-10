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
/// The in-memory tail of the log that has not yet been written to `Storage`
/// (etcd's `unstable`). It holds newly appended entries and, optionally, a
/// snapshot waiting to be applied, until they are handed to a `Ready` and their
/// writes are confirmed. `entries[i]` sits at absolute log index `i + offset`.
///
/// `offset_in_progress` (exclusive, `>= offset`) marks how far the entries have
/// begun being written; `snapshot_in_progress` says the snapshot write has
/// begun. Following etcd, the "in progress" cursors are what let the same
/// entries be exposed once for persistence and then withheld until stabilized.
/// The snapshot is an `Option` rather than a sentinel — its absence is a real
/// type state, not an index-0 magic value.
pub struct Unstable {
  mut snapshot : Snapshot?
  mut entries : Array[Entry]
  mut offset : UInt64
  mut offset_in_progress : UInt64
  mut snapshot_in_progress : Bool
}

///|
/// A fresh unstable tail anchored just past the last stable entry.
pub fn Unstable::new(offset : UInt64) -> Unstable {
  {
    snapshot: None,
    entries: [],
    offset,
    offset_in_progress: offset,
    snapshot_in_progress: false,
  }
}

///|
/// The first index the unstable region can speak to — one past the snapshot —
/// or `None` when there is no snapshot.
pub fn Unstable::maybe_first_index(self : Unstable) -> UInt64? {
  match self.snapshot {
    Some(s) => Some(s.last_index + 1)
    None => None
  }
}

///|
/// The last index covered, if there is at least one entry or a snapshot.
pub fn Unstable::maybe_last_index(self : Unstable) -> UInt64? {
  let l = self.entries.length()
  if l != 0 {
    Some(self.offset + l.to_uint64() - 1)
  } else {
    match self.snapshot {
      Some(s) => Some(s.last_index)
      None => None
    }
  }
}

///|
/// The term of the entry at `i`, if the unstable region knows it — either from
/// an entry it holds or from the snapshot baseline.
pub fn Unstable::maybe_term(self : Unstable, i : UInt64) -> UInt64? {
  if i < self.offset {
    match self.snapshot {
      Some(s) => if s.last_index == i { Some(s.last_term) } else { None }
      None => None
    }
  } else {
    match self.maybe_last_index() {
      None => None
      Some(last) =>
        if i > last {
          None
        } else {
          Some(self.entries[(i - self.offset).to_int()].term)
        }
    }
  }
}

///|
/// The entries not already in the process of being written to storage.
pub fn Unstable::next_entries(self : Unstable) -> Array[Entry] {
  let in_progress = (self.offset_in_progress - self.offset).to_int()
  if self.entries.length() == in_progress {
    []
  } else {
    self.entries[in_progress:].to_owned()
  }
}

///|
/// The snapshot to write, if one is present and not already being written.
pub fn Unstable::next_snapshot(self : Unstable) -> Snapshot? {
  match self.snapshot {
    Some(_) => if self.snapshot_in_progress { None } else { self.snapshot }
    None => None
  }
}

///|
/// Mark every held entry and the snapshot as having begun their write, so they
/// are withheld from later `next_entries`/`next_snapshot` until stabilized.
pub fn Unstable::accept_in_progress(self : Unstable) -> Unit {
  let n = self.entries.length()
  if n > 0 {
    self.offset_in_progress = self.entries[n - 1].index + 1
  }
  match self.snapshot {
    Some(_) => self.snapshot_in_progress = true
    None => ()
  }
}

///|
/// Discard the entries up to and including `id` now that they are durably
/// stored. Ignored if the entry is missing, matched only the snapshot baseline,
/// or the term no longer matches (the unstable tail was replaced meanwhile).
pub fn Unstable::stable_to(self : Unstable, id : EntryId) -> Unit {
  match self.maybe_term(id.index) {
    None => return
    Some(gt) => {
      if id.index < self.offset {
        return
      }
      if gt != id.term {
        return
      }
      let num = (id.index + 1 - self.offset).to_int()
      self.entries = self.entries[num:].to_owned()
      self.offset = id.index + 1
      self.offset_in_progress = if self.offset_in_progress > self.offset {
        self.offset_in_progress
      } else {
        self.offset
      }
    }
  }
}

///|
/// Drop the snapshot once it has been written to storage.
pub fn Unstable::stable_snap_to(self : Unstable, i : UInt64) -> Unit {
  match self.snapshot {
    Some(s) =>
      if s.last_index == i {
        self.snapshot = None
        self.snapshot_in_progress = false
      }
    None => ()
  }
}

///|
/// Replace the unstable tail with a snapshot baseline: the log restarts just
/// past `s`, with no in-memory entries.
pub fn Unstable::restore(self : Unstable, s : Snapshot) -> Unit {
  self.offset = s.last_index + 1
  self.offset_in_progress = self.offset
  self.entries = []
  self.snapshot = Some(s)
  self.snapshot_in_progress = false
}

///|
/// Splice `ents` onto the tail: append directly when they follow the last held
/// entry, replace the whole tail when they start at or before `offset`, or
/// truncate the divergent suffix and append otherwise. Only in-progress entries
/// before the truncation point stay in progress.
pub fn Unstable::truncate_and_append(
  self : Unstable,
  ents : Array[Entry],
) -> Unit {
  let from_index = ents[0].index
  if from_index == self.offset + self.entries.length().to_uint64() {
    for e in ents {
      self.entries.push(e)
    }
  } else if from_index <= self.offset {
    self.entries = ents
    self.offset = from_index
    self.offset_in_progress = self.offset
  } else {
    let merged = self.slice(self.offset, from_index)
    for e in ents {
      merged.push(e)
    }
    self.entries = merged
    self.offset_in_progress = if self.offset_in_progress < from_index {
      self.offset_in_progress
    } else {
      from_index
    }
  }
}

///|
/// The held entries with indices in `[lo, hi)`. The whole range must lie within
/// the unstable region, otherwise this aborts (etcd panics).
pub fn Unstable::slice(
  self : Unstable,
  lo : UInt64,
  hi : UInt64,
) -> Array[Entry] {
  self.must_check_out_of_bounds(lo, hi)
  self.entries[(lo - self.offset).to_int():(hi - self.offset).to_int()].to_owned()
}

///|
/// Guard: `offset <= lo <= hi <= offset + len(entries)`.
fn Unstable::must_check_out_of_bounds(
  self : Unstable,
  lo : UInt64,
  hi : UInt64,
) -> Unit {
  if lo > hi {
    abort("invalid unstable.slice: lo > hi")
  }
  let upper = self.offset + self.entries.length().to_uint64()
  if lo < self.offset || hi > upper {
    abort("unstable.slice out of bound")
  }
}
