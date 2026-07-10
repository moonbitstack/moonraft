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
// Ported from etcd-io/raft log_unstable_test.go against `Unstable`.

///|
fn mk_snap(index : UInt64, term : UInt64) -> Snapshot {
  { last_index: index, last_term: term, data: b"" }
}

///|
fn us(
  entries : Array[Entry],
  offset : UInt64,
  snap : Snapshot?,
  oip : UInt64,
  sip : Bool,
) -> Unstable {
  {
    snapshot: snap,
    entries,
    offset,
    offset_in_progress: oip,
    snapshot_in_progress: sip,
  }
}

///|
fn oeq(got : UInt64?, want : UInt64?) -> Unit raise {
  assert_true(got == want)
}

///|
test "TestUnstableMaybeFirstIndex" {
  // no snapshot
  oeq(us(ents_at(5, [1]), 5, None, 5, false).maybe_first_index(), None)
  oeq(us([], 0, None, 0, false).maybe_first_index(), None)
  // has snapshot
  oeq(
    us(ents_at(5, [1]), 5, Some(mk_snap(4, 1)), 5, false).maybe_first_index(),
    Some(5),
  )
  oeq(us([], 5, Some(mk_snap(4, 1)), 5, false).maybe_first_index(), Some(5))
}

///|
test "TestMaybeLastIndex" {
  // last in entries
  oeq(us(ents_at(5, [1]), 5, None, 5, false).maybe_last_index(), Some(5))
  oeq(
    us(ents_at(5, [1]), 5, Some(mk_snap(4, 1)), 5, false).maybe_last_index(),
    Some(5),
  )
  // last in snapshot
  oeq(us([], 5, Some(mk_snap(4, 1)), 5, false).maybe_last_index(), Some(4))
  // empty unstable
  oeq(us([], 0, None, 0, false).maybe_last_index(), None)
}

///|
test "TestUnstableMaybeTerm" {
  // term from entries
  oeq(us(ents_at(5, [1]), 5, None, 5, false).maybe_term(5), Some(1))
  oeq(us(ents_at(5, [1]), 5, None, 5, false).maybe_term(6), None)
  oeq(us(ents_at(5, [1]), 5, None, 5, false).maybe_term(4), None)
  oeq(
    us(ents_at(5, [1]), 5, Some(mk_snap(4, 1)), 5, false).maybe_term(5),
    Some(1),
  )
  oeq(us(ents_at(5, [1]), 5, Some(mk_snap(4, 1)), 5, false).maybe_term(6), None)
  // term from snapshot
  oeq(
    us(ents_at(5, [1]), 5, Some(mk_snap(4, 1)), 5, false).maybe_term(4),
    Some(1),
  )
  oeq(us(ents_at(5, [1]), 5, Some(mk_snap(4, 1)), 5, false).maybe_term(3), None)
  oeq(us([], 5, Some(mk_snap(4, 1)), 5, false).maybe_term(5), None)
  oeq(us([], 5, Some(mk_snap(4, 1)), 5, false).maybe_term(4), Some(1))
  oeq(us([], 0, None, 0, false).maybe_term(5), None)
}

///|
test "TestUnstableRestore" {
  let u = us(ents_at(5, [1]), 5, Some(mk_snap(4, 1)), 6, true)
  u.restore(mk_snap(6, 2))
  assert_eq(u.offset, 7)
  assert_eq(u.offset_in_progress, 7)
  assert_eq(u.entries.length(), 0)
  assert_true(u.snapshot == Some(mk_snap(6, 2)))
  assert_false(u.snapshot_in_progress)
}

///|
test "TestUnstableNextEntries" {
  // nothing in progress
  assert_entries(
    us(ents_at(5, [1, 1]), 5, None, 5, false).next_entries(),
    ents_at(5, [1, 1]),
  )
  // partially in progress
  assert_entries(
    us(ents_at(5, [1, 1]), 5, None, 6, false).next_entries(),
    ents_at(6, [1]),
  )
  // everything in progress
  assert_eq(
    us(ents_at(5, [1, 1]), 5, None, 7, false).next_entries().length(),
    0,
  )
}

///|
test "TestUnstableNextSnapshot" {
  let s = mk_snap(4, 1)
  // snapshot not unstable
  assert_true(us([], 0, None, 0, false).next_snapshot() == None)
  // snapshot not in progress
  assert_true(us([], 0, Some(s), 0, false).next_snapshot() == Some(s))
  // snapshot in progress
  assert_true(us([], 0, Some(s), 0, true).next_snapshot() == None)
}

///|
fn check_accept_in_progress(
  entries : Array[Entry],
  snap : Snapshot?,
  oip : UInt64,
  sip : Bool,
  woip : UInt64,
  wsip : Bool,
) -> Unit raise {
  let u = us(entries, 5, snap, oip, sip)
  u.accept_in_progress()
  assert_eq(u.offset_in_progress, woip)
  assert_eq(u.snapshot_in_progress, wsip)
}

///|
test "TestUnstableAcceptInProgress" {
  let s = mk_snap(4, 1)
  // no snapshot
  check_accept_in_progress([], None, 5, false, 5, false)
  check_accept_in_progress(ents_at(5, [1]), None, 5, false, 6, false)
  check_accept_in_progress(ents_at(5, [1, 1]), None, 5, false, 7, false)
  check_accept_in_progress(ents_at(5, [1, 1]), None, 6, false, 7, false)
  check_accept_in_progress(ents_at(5, [1, 1]), None, 7, false, 7, false)
  // with snapshot
  check_accept_in_progress([], Some(s), 5, false, 5, true)
  check_accept_in_progress(ents_at(5, [1]), Some(s), 5, false, 6, true)
  check_accept_in_progress(ents_at(5, [1, 1]), Some(s), 5, false, 7, true)
  check_accept_in_progress(ents_at(5, [1, 1]), Some(s), 6, false, 7, true)
  check_accept_in_progress(ents_at(5, [1, 1]), Some(s), 7, false, 7, true)
  check_accept_in_progress([], Some(s), 5, true, 5, true)
  check_accept_in_progress(ents_at(5, [1]), Some(s), 5, true, 6, true)
  check_accept_in_progress(ents_at(5, [1, 1]), Some(s), 5, true, 7, true)
  check_accept_in_progress(ents_at(5, [1, 1]), Some(s), 6, true, 7, true)
  check_accept_in_progress(ents_at(5, [1, 1]), Some(s), 7, true, 7, true)
}

///|
fn check_stable_to(
  entries : Array[Entry],
  offset : UInt64,
  oip : UInt64,
  snap : Snapshot?,
  index : UInt64,
  term : UInt64,
  woffset : UInt64,
  woip : UInt64,
  wlen : Int,
) -> Unit raise {
  let u = us(entries, offset, snap, oip, false)
  u.stable_to({ term, index })
  assert_eq(u.offset, woffset)
  assert_eq(u.offset_in_progress, woip)
  assert_eq(u.entries.length(), wlen)
}

///|
test "TestUnstableStableTo" {
  check_stable_to([], 0, 0, None, 5, 1, 0, 0, 0)
  // stable to the first entry
  check_stable_to(ents_at(5, [1]), 5, 6, None, 5, 1, 6, 6, 0)
  check_stable_to(ents_at(5, [1, 1]), 5, 6, None, 5, 1, 6, 6, 1)
  // stable to the first entry and in-progress ahead
  check_stable_to(ents_at(5, [1, 1]), 5, 7, None, 5, 1, 6, 7, 1)
  // stable to the first entry and term mismatch
  check_stable_to(ents_at(6, [2]), 6, 7, None, 6, 1, 6, 7, 1)
  // stable to old entry
  check_stable_to(ents_at(5, [1]), 5, 6, None, 4, 1, 5, 6, 1)
  check_stable_to(ents_at(5, [1]), 5, 6, None, 4, 2, 5, 6, 1)
  // with snapshot: stable to the first entry
  check_stable_to(ents_at(5, [1]), 5, 6, Some(mk_snap(4, 1)), 5, 1, 6, 6, 0)
  check_stable_to(ents_at(5, [1, 1]), 5, 6, Some(mk_snap(4, 1)), 5, 1, 6, 6, 1)
  // stable to the first entry and in-progress ahead
  check_stable_to(ents_at(5, [1, 1]), 5, 7, Some(mk_snap(4, 1)), 5, 1, 6, 7, 1)
  // stable to the first entry and term mismatch
  check_stable_to(ents_at(6, [2]), 6, 7, Some(mk_snap(5, 1)), 6, 1, 6, 7, 1)
  // stable to snapshot
  check_stable_to(ents_at(5, [1]), 5, 6, Some(mk_snap(4, 1)), 4, 1, 5, 6, 1)
  // stable to old entry
  check_stable_to(ents_at(5, [2]), 5, 6, Some(mk_snap(4, 2)), 4, 1, 5, 6, 1)
}

///|
fn check_truncate_and_append(
  entries : Array[Entry],
  offset : UInt64,
  oip : UInt64,
  snap : Snapshot?,
  toappend : Array[Entry],
  woffset : UInt64,
  woip : UInt64,
  wentries : Array[Entry],
) -> Unit raise {
  let u = us(entries, offset, snap, oip, false)
  u.truncate_and_append(toappend)
  assert_eq(u.offset, woffset)
  assert_eq(u.offset_in_progress, woip)
  assert_entries(u.entries, wentries)
}

///|
test "TestUnstableTruncateAndAppend" {
  // append to the end
  check_truncate_and_append(
    ents_at(5, [1]),
    5,
    5,
    None,
    ents_at(6, [1, 1]),
    5,
    5,
    ents_at(5, [1, 1, 1]),
  )
  check_truncate_and_append(
    ents_at(5, [1]),
    5,
    6,
    None,
    ents_at(6, [1, 1]),
    5,
    6,
    ents_at(5, [1, 1, 1]),
  )
  // replace the unstable entries
  check_truncate_and_append(
    ents_at(5, [1]),
    5,
    5,
    None,
    ents_at(5, [2, 2]),
    5,
    5,
    ents_at(5, [2, 2]),
  )
  check_truncate_and_append(
    ents_at(5, [1]),
    5,
    5,
    None,
    ents_at(4, [2, 2, 2]),
    4,
    4,
    ents_at(4, [2, 2, 2]),
  )
  check_truncate_and_append(
    ents_at(5, [1]),
    5,
    6,
    None,
    ents_at(5, [2, 2]),
    5,
    5,
    ents_at(5, [2, 2]),
  )
  // truncate the existing entries and append
  check_truncate_and_append(
    ents_at(5, [1, 1, 1]),
    5,
    5,
    None,
    ents_at(6, [2]),
    5,
    5,
    ents_at(5, [1, 2]),
  )
  check_truncate_and_append(
    ents_at(5, [1, 1, 1]),
    5,
    5,
    None,
    ents_at(7, [2, 2]),
    5,
    5,
    ents_at(5, [1, 1, 2, 2]),
  )
  check_truncate_and_append(
    ents_at(5, [1, 1, 1]),
    5,
    6,
    None,
    ents_at(6, [2]),
    5,
    6,
    ents_at(5, [1, 2]),
  )
  check_truncate_and_append(
    ents_at(5, [1, 1, 1]),
    5,
    7,
    None,
    ents_at(6, [2]),
    5,
    6,
    ents_at(5, [1, 2]),
  )
}
