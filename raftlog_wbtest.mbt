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
//
// Ported from etcd-io/raft log_test.go against `RaftLog`. Cases that assert a Go
// panic (`wpanic`) are noted individually: MoonBit `abort` is not catchable, so
// the panic itself cannot be trapped (same limitation as inflights); the
// non-panic cases of those tables are all reproduced.

///|
/// Entries at consecutive indices from `start`, with consecutive terms `[from,
/// to)`. Mirrors etcd's `index(start).termRange(from, to)`.
fn ents_range(start : UInt64, from : UInt64, to : UInt64) -> Array[Entry] {
  let out : Array[Entry] = []
  let mut idx = start
  let mut t = from
  while t < to {
    out.push(Entry::normal(t, idx, b""))
    idx = idx + 1
    t = t + 1
  }
  out
}

///|
/// A fresh raftLog over empty storage with `ents` appended to the unstable tail.
fn log_appended(ents : Array[Entry]) -> RaftLog {
  let l = RaftLog::new(MemoryStorage::new())
  l.append(ents) |> ignore
  l
}

///|
test "TestFindConflict" {
  let prev = ents_at(1, [1, 2, 3])
  fn conflict(ents : Array[Entry]) -> UInt64 {
    log_appended(prev).find_conflict(ents[:])
  }

  assert_eq(conflict([]), 0)
  assert_eq(conflict(ents_at(1, [1, 2, 3])), 0)
  assert_eq(conflict(ents_at(2, [2, 3])), 0)
  assert_eq(conflict(ents_at(3, [3])), 0)
  assert_eq(conflict(ents_at(1, [1, 2, 3, 4, 4])), 4)
  assert_eq(conflict(ents_at(2, [2, 3, 4, 5])), 4)
  assert_eq(conflict(ents_at(3, [3, 4, 4])), 4)
  assert_eq(conflict(ents_at(4, [4, 4])), 4)
  assert_eq(conflict(ents_at(1, [4, 4])), 1)
  assert_eq(conflict(ents_at(2, [1, 4, 4])), 2)
  assert_eq(conflict(ents_at(3, [1, 2, 4, 4])), 3)
}

///|
fn check_fcbt(
  ents : Array[Entry],
  index : UInt64,
  term : UInt64,
  want : UInt64,
) -> Unit raise {
  let st = MemoryStorage::new()
  st.apply_snapshot({
    last_index: ents[0].index,
    last_term: ents[0].term,
    data: b"",
  })
  let l = RaftLog::new(st)
  l.append(ents[1:].to_owned()) |> ignore
  let (index2, term2) = l.find_conflict_by_term(index, term)
  assert_eq(index2, want)
  assert_eq(term2, l.zero_term_on_out_of_bounds(index2))
}

///|
test "TestFindConflictByTerm" {
  let a = [ent(0, 0), ent(1, 2), ent(2, 2), ent(3, 5), ent(4, 5), ent(5, 5)]
  check_fcbt(a, 100, 2, 100)
  check_fcbt(a, 5, 6, 5)
  check_fcbt(a, 5, 5, 5)
  check_fcbt(a, 5, 4, 2)
  check_fcbt(a, 5, 2, 2)
  check_fcbt(a, 5, 1, 0)
  check_fcbt(a, 1, 2, 1)
  check_fcbt(a, 1, 1, 0)
  check_fcbt(a, 0, 0, 0)
  let b = [
    ent(10, 3),
    ent(11, 3),
    ent(12, 3),
    ent(13, 4),
    ent(14, 4),
    ent(15, 4),
  ]
  check_fcbt(b, 30, 3, 30)
  check_fcbt(b, 14, 9, 14)
  check_fcbt(b, 14, 4, 14)
  check_fcbt(b, 14, 3, 12)
  check_fcbt(b, 14, 2, 9)
  check_fcbt(b, 11, 5, 11)
  check_fcbt(b, 10, 5, 10)
  check_fcbt(b, 10, 3, 10)
  check_fcbt(b, 10, 2, 9)
  check_fcbt(b, 9, 2, 9)
  check_fcbt(b, 4, 2, 4)
  check_fcbt(b, 0, 0, 0)
}

///|
test "TestIsUpToDate" {
  let l = log_appended(ents_at(1, [1, 2, 3]))
  let last = l.last_index()
  assert_true(l.is_up_to_date({ term: 4, index: last - 1 }))
  assert_true(l.is_up_to_date({ term: 4, index: last }))
  assert_true(l.is_up_to_date({ term: 4, index: last + 1 }))
  assert_false(l.is_up_to_date({ term: 2, index: last - 1 }))
  assert_false(l.is_up_to_date({ term: 2, index: last }))
  assert_false(l.is_up_to_date({ term: 2, index: last + 1 }))
  assert_false(l.is_up_to_date({ term: 3, index: last - 1 }))
  assert_true(l.is_up_to_date({ term: 3, index: last }))
  assert_true(l.is_up_to_date({ term: 3, index: last + 1 }))
}

///|
fn check_log_append(
  toappend : Array[Entry],
  windex : UInt64,
  wents : Array[Entry],
  wunstable : UInt64,
) -> Unit raise {
  let storage = MemoryStorage::new()
  storage.append(ents_at(1, [1, 2]))
  let l = RaftLog::new(storage)
  assert_eq(l.append(toappend), windex)
  assert_entries(l.entries(1, no_limit), wents)
  assert_eq(l.unstable.offset, wunstable)
}

///|
test "TestAppend" {
  check_log_append([], 2, ents_at(1, [1, 2]), 3)
  check_log_append(ents_at(3, [2]), 3, ents_at(1, [1, 2, 2]), 3)
  check_log_append(ents_at(1, [2]), 1, ents_at(1, [2]), 1)
  check_log_append(ents_at(2, [3, 3]), 3, ents_at(1, [1, 3, 3]), 2)
}

///|
fn check_maybe_append(
  prev : EntryId,
  committed : UInt64,
  ents : Array[Entry],
  wlasti : UInt64,
  wappend : Bool,
  wcommit : UInt64,
) -> Unit raise {
  let l = log_appended(ents_at(1, [1, 2, 3]))
  l.commit_to(1)
  let app : LogSlice = { term: 100, prev, entries: ents }
  assert_true(app.valid())
  match l.maybe_append(app, committed) {
    Some(i) => {
      assert_true(wappend)
      assert_eq(i, wlasti)
    }
    None => assert_false(wappend)
  }
  assert_eq(l.committed, wcommit)
  if wappend && ents.length() != 0 {
    let gents = l.slice(
      l.last_index() - ents.length().to_uint64() + 1,
      l.last_index() + 1,
      no_limit,
    )
    assert_entries(gents, ents)
  }
}

///|
test "TestLogMaybeAppend" {
  // lastindex=3, lastterm=3, commit=1. The etcd wpanic case (prev={0,0}, a
  // conflict with the committed entry at index 1) aborts and cannot be trapped,
  // so it is omitted; all 14 non-panic cases run.
  check_maybe_append({ term: 2, index: 3 }, 3, ents_at(4, [4]), 0, false, 1)
  check_maybe_append({ term: 3, index: 4 }, 3, ents_at(5, [4]), 0, false, 1)
  check_maybe_append({ term: 3, index: 3 }, 3, [], 3, true, 3)
  check_maybe_append({ term: 3, index: 3 }, 4, [], 3, true, 3)
  check_maybe_append({ term: 3, index: 3 }, 2, [], 3, true, 2)
  check_maybe_append({ term: 3, index: 3 }, 0, [], 3, true, 1)
  check_maybe_append({ term: 0, index: 0 }, 3, [], 0, true, 1)
  check_maybe_append({ term: 3, index: 3 }, 3, ents_at(4, [4]), 4, true, 3)
  check_maybe_append({ term: 3, index: 3 }, 4, ents_at(4, [4]), 4, true, 4)
  check_maybe_append({ term: 3, index: 3 }, 5, ents_at(4, [4]), 4, true, 4)
  check_maybe_append({ term: 3, index: 3 }, 5, ents_at(4, [4, 4]), 5, true, 5)
  check_maybe_append({ term: 2, index: 2 }, 3, ents_at(3, [4]), 3, true, 3)
  check_maybe_append({ term: 1, index: 1 }, 3, ents_at(2, [4]), 2, true, 2)
  check_maybe_append({ term: 1, index: 1 }, 3, ents_at(2, [4, 4]), 3, true, 3)
}

///|
test "TestCompactionSideEffects" {
  let last_index : UInt64 = 1000
  let unstable_index : UInt64 = 750
  let storage = MemoryStorage::new()
  storage.append(ents_range(1, 1, unstable_index + 1))
  let l = RaftLog::new(storage)
  l.append(ents_range(unstable_index + 1, unstable_index + 1, last_index + 1))
  |> ignore
  assert_true(l.maybe_commit(l.last_entry_id()))
  l.applied_to(l.committed, 0)
  let offset : UInt64 = 500
  storage.compact(offset)
  assert_eq(l.last_index(), last_index)
  let mut j = offset
  while j <= l.last_index() {
    assert_eq(l.term(j), j)
    j = j + 1
  }
  j = offset
  while j <= l.last_index() {
    assert_true(l.match_term({ term: j, index: j }))
    j = j + 1
  }
  let unstable_ents = l.next_unstable_ents()
  assert_eq(unstable_ents.length(), 250)
  assert_eq(unstable_ents[0].index, 751)
  let prev = l.last_index()
  l.append([Entry::normal(l.last_index() + 1, l.last_index() + 1, b"")])
  |> ignore
  assert_eq(l.last_index(), prev + 1)
  let ents = l.entries(l.last_index(), no_limit)
  assert_eq(ents.length(), 1)
}

///|
/// Build the raftLog used by the apply-pacing tests: snapshot at (1,3), one
/// stable entry at index 4, three unstable entries, committed to 5, applied 3.
fn apply_log(max_size : UInt64) -> RaftLog raise {
  let snap : Snapshot = { last_index: 3, last_term: 1, data: b"" }
  let ents = ents_at(4, [1, 1, 1])
  let storage = MemoryStorage::new()
  storage.apply_snapshot(snap)
  storage.append(ents[0:1].to_owned())
  let l = RaftLog::new_with_size(storage, max_size)
  l.append(ents) |> ignore
  l.stable_to({ term: 1, index: 4 })
  l.maybe_commit({ term: 1, index: 5 }) |> ignore
  l
}

///|
fn check_has_next_committed(
  applied : UInt64,
  applying : UInt64,
  allow_unstable : Bool,
  paused : Bool,
  snap : Bool,
  whas_next : Bool,
) -> Unit raise {
  let l = apply_log(no_limit)
  l.applied_to(applied, 0)
  l.accept_applying(applying, 0, allow_unstable)
  l.applying_ents_paused = paused
  if snap {
    l.restore({ last_index: 4, last_term: 1, data: b"" })
  }
  assert_eq(l.has_next_committed_ents(allow_unstable), whas_next)
}

///|
test "TestHasNextCommittedEnts" {
  check_has_next_committed(3, 3, true, false, false, true)
  check_has_next_committed(3, 4, true, false, false, true)
  check_has_next_committed(3, 5, true, false, false, false)
  check_has_next_committed(4, 4, true, false, false, true)
  check_has_next_committed(4, 5, true, false, false, false)
  check_has_next_committed(5, 5, true, false, false, false)
  check_has_next_committed(3, 3, false, false, false, true)
  check_has_next_committed(3, 4, false, false, false, false)
  check_has_next_committed(3, 5, false, false, false, false)
  check_has_next_committed(4, 4, false, false, false, false)
  check_has_next_committed(4, 5, false, false, false, false)
  check_has_next_committed(5, 5, false, false, false, false)
  check_has_next_committed(3, 3, true, true, false, false)
  check_has_next_committed(3, 3, true, false, true, false)
}

///|
fn check_next_committed(
  applied : UInt64,
  applying : UInt64,
  allow_unstable : Bool,
  paused : Bool,
  snap : Bool,
  wents : Array[Entry],
) -> Unit raise {
  let l = apply_log(no_limit)
  l.applied_to(applied, 0)
  l.accept_applying(applying, 0, allow_unstable)
  l.applying_ents_paused = paused
  if snap {
    l.restore({ last_index: 4, last_term: 1, data: b"" })
  }
  assert_entries(l.next_committed_ents(allow_unstable), wents)
}

///|
test "TestNextCommittedEnts" {
  check_next_committed(3, 3, true, false, false, ents_at(4, [1, 1]))
  check_next_committed(3, 4, true, false, false, ents_at(5, [1]))
  check_next_committed(3, 5, true, false, false, [])
  check_next_committed(4, 4, true, false, false, ents_at(5, [1]))
  check_next_committed(4, 5, true, false, false, [])
  check_next_committed(5, 5, true, false, false, [])
  check_next_committed(3, 3, false, false, false, ents_at(4, [1]))
  check_next_committed(3, 4, false, false, false, [])
  check_next_committed(3, 5, false, false, false, [])
  check_next_committed(4, 4, false, false, false, [])
  check_next_committed(4, 5, false, false, false, [])
  check_next_committed(5, 5, false, false, false, [])
  check_next_committed(3, 3, true, true, false, [])
  check_next_committed(3, 3, true, false, true, [])
}

///|
fn check_accept_applying(
  index : UInt64,
  allow_unstable : Bool,
  size : UInt64,
  wpaused : Bool,
) -> Unit raise {
  let l = apply_log(100)
  l.applied_to(3, 0)
  l.accept_applying(index, size, allow_unstable)
  assert_eq(l.applying_ents_paused, wpaused)
}

///|
test "TestAcceptApplying" {
  let max = 100UL
  check_accept_applying(3, true, max - 1, true)
  check_accept_applying(3, true, max, true)
  check_accept_applying(3, true, max + 1, true)
  check_accept_applying(4, true, max - 1, true)
  check_accept_applying(4, true, max, true)
  check_accept_applying(4, true, max + 1, true)
  check_accept_applying(5, true, max - 1, false)
  check_accept_applying(5, true, max, true)
  check_accept_applying(5, true, max + 1, true)
  check_accept_applying(3, false, max - 1, true)
  check_accept_applying(3, false, max, true)
  check_accept_applying(3, false, max + 1, true)
  check_accept_applying(4, false, max - 1, false)
  check_accept_applying(4, false, max, true)
  check_accept_applying(4, false, max + 1, true)
  check_accept_applying(5, false, max - 1, false)
  check_accept_applying(5, false, max, true)
  check_accept_applying(5, false, max + 1, true)
}

///|
fn check_applied_to(
  index : UInt64,
  size : UInt64,
  wapplying_size : UInt64,
  wpaused : Bool,
) -> Unit raise {
  let max = 100UL
  let overshoot = 5UL
  let l = apply_log(max)
  l.applied_to(3, 0)
  l.accept_applying(5, max + overshoot, false)
  l.applied_to(index, size)
  assert_eq(l.applied, index)
  assert_eq(l.applying, 5)
  assert_eq(l.applying_ents_size, wapplying_size)
  assert_eq(l.applying_ents_paused, wpaused)
}

///|
test "TestAppliedTo" {
  let max = 100UL
  let overshoot = 5UL
  check_applied_to(4, overshoot - 1, max + 1, true)
  check_applied_to(4, overshoot, max, true)
  check_applied_to(4, overshoot + 1, max - 1, false)
  check_applied_to(5, overshoot - 1, max + 1, true)
  check_applied_to(5, overshoot, max, true)
  check_applied_to(5, overshoot + 1, max - 1, false)
  check_applied_to(4, max + overshoot, 0, false)
  check_applied_to(4, max + overshoot + 1, 0, false)
}

///|
fn check_next_unstable(unstable : UInt64, wents : Array[Entry]) -> Unit raise {
  let previous = ents_at(1, [1, 2])
  let storage = MemoryStorage::new()
  storage.append(previous[0:(unstable - 1).to_int()].to_owned())
  let l = RaftLog::new(storage)
  l.append(previous[(unstable - 1).to_int():].to_owned()) |> ignore
  let ents = l.next_unstable_ents()
  let n = ents.length()
  if n > 0 {
    l.stable_to(ents[n - 1].id())
  }
  assert_entries(ents, wents)
  assert_eq(l.unstable.offset, previous[previous.length() - 1].index + 1)
}

///|
test "TestNextUnstableEnts" {
  check_next_unstable(3, [])
  check_next_unstable(1, ents_at(1, [1, 2]))
}

///|
test "TestCommitTo" {
  // {3,3}, {1,2 never decrease}. The {4 -> panic} case aborts (commit past the
  // last index) and cannot be trapped.
  let l0 = log_appended(ents_at(1, [1, 2, 3]))
  l0.commit_to(2)
  l0.commit_to(3)
  assert_eq(l0.committed, 3)
  let l1 = log_appended(ents_at(1, [1, 2, 3]))
  l1.commit_to(2)
  l1.commit_to(1)
  assert_eq(l1.committed, 2)
}

///|
fn check_stable_to_log(
  stablei : UInt64,
  stablet : UInt64,
  wunstable : UInt64,
) -> Unit raise {
  let l = log_appended(ents_at(1, [1, 2]))
  l.stable_to({ term: stablet, index: stablei })
  assert_eq(l.unstable.offset, wunstable)
}

///|
test "TestStableTo" {
  check_stable_to_log(1, 1, 2)
  check_stable_to_log(2, 2, 3)
  check_stable_to_log(2, 1, 1) // bad term
  check_stable_to_log(3, 1, 1) // bad index
}

///|
fn check_stable_to_snap(
  stablei : UInt64,
  stablet : UInt64,
  new_ents : Array[Entry],
  wunstable : UInt64,
) -> Unit raise {
  let snapi = 5UL
  let snapt = 2UL
  let s = MemoryStorage::new()
  s.apply_snapshot({ last_index: snapi, last_term: snapt, data: b"" })
  let l = RaftLog::new(s)
  l.append(new_ents) |> ignore
  l.stable_to({ term: stablet, index: stablei })
  assert_eq(l.unstable.offset, wunstable)
}

///|
test "TestStableToWithSnap" {
  let snapi = 5UL
  let snapt = 2UL
  check_stable_to_snap(snapi + 1, snapt, [], snapi + 1)
  check_stable_to_snap(snapi, snapt, [], snapi + 1)
  check_stable_to_snap(snapi - 1, snapt, [], snapi + 1)
  check_stable_to_snap(snapi + 1, snapt + 1, [], snapi + 1)
  check_stable_to_snap(snapi, snapt + 1, [], snapi + 1)
  check_stable_to_snap(snapi - 1, snapt + 1, [], snapi + 1)
  check_stable_to_snap(snapi + 1, snapt, ents_at(snapi + 1, [snapt]), snapi + 2)
  check_stable_to_snap(snapi, snapt, ents_at(snapi + 1, [snapt]), snapi + 1)
  check_stable_to_snap(snapi - 1, snapt, ents_at(snapi + 1, [snapt]), snapi + 1)
  check_stable_to_snap(
    snapi + 1,
    snapt + 1,
    ents_at(snapi + 1, [snapt]),
    snapi + 1,
  )
  check_stable_to_snap(snapi, snapt + 1, ents_at(snapi + 1, [snapt]), snapi + 1)
  check_stable_to_snap(
    snapi - 1,
    snapt + 1,
    ents_at(snapi + 1, [snapt]),
    snapi + 1,
  )
}

///|
test "TestCompaction" {
  // {1000, [300,500,800,900] -> [700,500,200,100], allow}. The out-of-bound
  // compact (index 1001) and the below-lower-bound second compact panic/err;
  // the ErrCompacted path (299 after 300) is catchable and covered.
  let storage = MemoryStorage::new()
  storage.append(ents_range(1, 1, 1001))
  let l = RaftLog::new(storage)
  l.maybe_commit({ term: 0, index: 1000 }) |> ignore
  l.applied_to(l.committed, 0)
  let compacts = [300UL, 500UL, 800UL, 900UL]
  let wleft = [700, 500, 200, 100]
  let mut k = 0
  while k < compacts.length() {
    storage.compact(compacts[k])
    assert_eq(l.all_entries().length(), wleft[k])
    k = k + 1
  }
  // Lower-bound: compacting an already-compacted index raises ErrCompacted.
  let s2 = MemoryStorage::new()
  s2.append(ents_range(1, 1, 1001))
  let l2 = RaftLog::new(s2)
  l2.maybe_commit({ term: 0, index: 1000 }) |> ignore
  l2.applied_to(l2.committed, 0)
  s2.compact(300)
  assert_eq(l2.all_entries().length(), 700)
  // Lower bound: compacting an already-compacted index raises ErrCompacted.
  assert_raises(() => s2.compact(299), Compacted)
}

///|
test "TestLogRestore" {
  let index = 1000UL
  let term = 1000UL
  let storage = MemoryStorage::new()
  storage.apply_snapshot({ last_index: index, last_term: term, data: b"" })
  let l = RaftLog::new(storage)
  assert_eq(l.all_entries().length(), 0)
  assert_eq(l.first_index(), index + 1)
  assert_eq(l.committed, index)
  assert_eq(l.unstable.offset, index + 1)
  assert_eq(l.term(index), term)
}

///|
test "TestIsOutOfBounds" {
  // ErrCompacted for lo below firstIndex; nil within range. The two wpanic cases
  // (hi past the end) abort and are not trapped.
  let offset = 100UL
  let num = 100UL
  let storage = MemoryStorage::new()
  storage.apply_snapshot({ last_index: offset, last_term: 0, data: b"" })
  let l = RaftLog::new(storage)
  l.append(ents_range(offset + 1, offset + 1, offset + num + 1)) |> ignore
  let first = offset + 1
  // lo below firstIndex raises ErrCompacted.
  assert_raises(
    () => l.must_check_out_of_bounds(first - 2, first + 1),
    Compacted,
  )
  assert_raises(
    () => l.must_check_out_of_bounds(first - 1, first + 1),
    Compacted,
  )
  // In-range checks raise nothing.
  l.must_check_out_of_bounds(first, first)
  l.must_check_out_of_bounds(first + num / 2, first + num / 2)
  l.must_check_out_of_bounds(first + num - 1, first + num - 1)
  l.must_check_out_of_bounds(first + num, first + num)
}

///|
test "TestTerm" {
  let offset = 100UL
  let num = 100UL
  let storage = MemoryStorage::new()
  storage.apply_snapshot({ last_index: offset, last_term: 1, data: b"" })
  let l = RaftLog::new(storage)
  l.append(ents_range(offset + 1, 1, num)) |> ignore
  assert_raises(() => l.term(offset - 1) |> ignore, Compacted)
  assert_eq(l.term(offset), 1)
  assert_eq(l.term(offset + num / 2), num / 2)
  assert_eq(l.term(offset + num - 1), num - 1)
  assert_raises(() => l.term(offset + num) |> ignore, Unavailable)
}

///|
test "TestTermWithUnstableSnapshot" {
  let storagesnapi = 100UL
  let unstablesnapi = storagesnapi + 5
  let storage = MemoryStorage::new()
  storage.apply_snapshot({ last_index: storagesnapi, last_term: 1, data: b"" })
  let l = RaftLog::new(storage)
  l.restore({ last_index: unstablesnapi, last_term: 1, data: b"" })
  // cannot get term from storage
  assert_raises(() => l.term(storagesnapi) |> ignore, Compacted)
  // cannot get term from the gap between storage ents and unstable snapshot
  assert_raises(() => l.term(storagesnapi + 1) |> ignore, Compacted)
  assert_raises(() => l.term(unstablesnapi - 1) |> ignore, Compacted)
  // get term from unstable snapshot index
  assert_eq(l.term(unstablesnapi), 1)
  // the log beyond the unstable snapshot is empty
  assert_raises(() => l.term(unstablesnapi + 1) |> ignore, Unavailable)
}

///|
test "TestSlice" {
  let offset = 100UL
  let num = 100UL
  let last = offset + num
  let half = offset + num / 2
  let halfe = Entry::normal(half, half, b"")
  let esz = entry_encoding_size(halfe)
  fn entries(from : UInt64, to : UInt64) -> Array[Entry] {
    ents_range(from, from, to)
  }

  let storage = MemoryStorage::new()
  storage.apply_snapshot({ last_index: offset, last_term: 0, data: b"" })
  storage.append(entries(offset + 1, half))
  let l = RaftLog::new(storage)
  l.append(entries(half, last)) |> ignore
  fn sl(lo : UInt64, hi : UInt64, lim : UInt64) -> Array[Entry] raise {
    l.slice(lo, hi, lim)
  }

  // ErrCompacted.
  assert_raises(
    () => l.slice(offset - 1, offset + 1, no_limit) |> ignore,
    Compacted,
  )
  assert_raises(
    () => l.slice(offset, offset + 1, no_limit) |> ignore,
    Compacted,
  )
  // No limit.
  assert_eq(sl(offset + 1, offset + 1, no_limit).length(), 0)
  assert_entries(
    sl(offset + 1, half - 1, no_limit),
    entries(offset + 1, half - 1),
  )
  assert_entries(sl(offset + 1, half, no_limit), entries(offset + 1, half))
  assert_entries(
    sl(offset + 1, half + 1, no_limit),
    entries(offset + 1, half + 1),
  )
  assert_entries(sl(offset + 1, last, no_limit), entries(offset + 1, last))
  assert_entries(sl(half - 1, half, no_limit), entries(half - 1, half))
  assert_entries(sl(half - 1, half + 1, no_limit), entries(half - 1, half + 1))
  assert_entries(sl(half - 1, last, no_limit), entries(half - 1, last))
  assert_entries(sl(half, half + 1, no_limit), entries(half, half + 1))
  assert_entries(sl(half, last, no_limit), entries(half, last))
  assert_entries(sl(last - 1, last, no_limit), entries(last - 1, last))
  // At least one entry is always returned.
  assert_entries(sl(offset + 1, last, 0), entries(offset + 1, offset + 2))
  assert_entries(sl(half - 1, half + 1, 0), entries(half - 1, half))
  assert_entries(sl(half, last, 0), entries(half, half + 1))
  assert_entries(sl(half + 1, last, 0), entries(half + 1, half + 2))
  // Low limit (just under one entry).
  assert_entries(sl(offset + 1, last, esz - 1), entries(offset + 1, offset + 2))
  assert_entries(sl(half - 1, half + 1, esz - 1), entries(half - 1, half))
  assert_entries(sl(half, last, esz - 1), entries(half, half + 1))
  // Just enough for one.
  assert_entries(sl(offset + 1, last, esz), entries(offset + 1, offset + 2))
  assert_entries(sl(half - 1, half + 1, esz), entries(half - 1, half))
  assert_entries(sl(half, last, esz), entries(half, half + 1))
  // Not enough for two.
  assert_entries(sl(offset + 1, last, esz + 1), entries(offset + 1, offset + 2))
  assert_entries(sl(half - 1, half + 1, esz + 1), entries(half - 1, half))
  assert_entries(sl(half, last, esz + 1), entries(half, half + 1))
  // Enough for two.
  assert_entries(sl(offset + 1, last, esz * 2), entries(offset + 1, offset + 3))
  assert_entries(sl(half - 2, half + 1, esz * 2), entries(half - 2, half))
  assert_entries(sl(half - 1, half + 1, esz * 2), entries(half - 1, half + 1))
  assert_entries(sl(half, last, esz * 2), entries(half, half + 2))
  // Not enough for three.
  assert_entries(sl(half - 2, half + 1, esz * 3 - 1), entries(half - 2, half))
  // Enough for three.
  assert_entries(sl(half - 1, half + 2, esz * 3), entries(half - 1, half + 2))
}

///|
test "TestScan" {
  let offset = 47UL
  let num = 20UL
  let last = offset + num
  let half = offset + num / 2
  fn entries(from : UInt64, to : UInt64) -> Array[Entry] {
    ents_range(from, from, to)
  }

  let entry_size = ents_size(entries(half, half + 1)[:])
  let storage = MemoryStorage::new()
  storage.apply_snapshot({ last_index: offset, last_term: 0, data: b"" })
  storage.append(entries(offset + 1, half))
  let l = RaftLog::new(storage)
  l.append(entries(half, last)) |> ignore

  // scan() returns the same entries as slice(), on all inputs.
  for page_size in [0UL, 1, 10, 100, entry_size, entry_size + 1] {
    let mut lo = offset + 1
    while lo < last {
      let mut hi = lo
      while hi <= last {
        let got : Array[Entry] = []
        l.scan(lo, hi, page_size, es => {
          for e in es {
            got.push(e)
          }
          assert_true(es.length() == 1 || ents_size(es) <= page_size)
        })
        let want = l.slice(lo, hi, no_limit)
        assert_entries(got, want)
        hi = hi + 1
      }
      lo = lo + 1
    }
  }

  // The callback error is propagated (used to break the scan early).
  let iters : Ref[Int] = { val: 0 }
  let broke = try {
    l.scan(offset + 1, half, 0, _es => {
      iters.val = iters.val + 1
      if iters.val == 2 {
        raise ScanBreak
      }
    })
    false
  } catch {
    ScanBreak => true
    _ => false
  }
  assert_true(broke)
  assert_eq(iters.val, 2)

  // Max out the limit rather than always returning a single entry (the range
  // length is even).
  l.scan(offset + 1, offset + 11, entry_size * 2, es => {
    assert_eq(es.length(), 2)
    assert_eq(ents_size(es), entry_size * 2)
  })
}

///|
/// A break signal for the scan callback, matching etcd's `errBreak`.
suberror ScanBreak
