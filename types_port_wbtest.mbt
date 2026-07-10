// Copyright 2024 The etcd Authors
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
// Ported from etcd-io/raft types_test.go.

///|
/// etcd's `id(index, term)` helper — note the argument order is (index, term).
fn eid(index : UInt64, term : UInt64) -> EntryId {
  { term, index }
}

///|
/// etcd's `e(index, term)` helper: a bare entry at (index, term).
fn ent(index : UInt64, term : UInt64) -> Entry {
  Entry::normal(term, index, b"")
}

///|
test "TestEntryID" {
  assert_true(eid(10, 5) == eid(10, 5))
  assert_true(eid(10, 4) != eid(10, 5))
  assert_true(eid(9, 5) != eid(10, 5))

  // pbEntryID equivalents.
  assert_true(Entry::normal(0, 0, b"").id() == eid(0, 0))
  assert_true(Entry::normal(1, 2, b"data").id() == eid(2, 1))
  assert_true(Entry::normal(10, 123, b"").id() == eid(123, 10))
}

///|
/// Build the LogSlice, assert its validity, and — when valid — that its last
/// entry id and last index agree with the expectation.
fn check_slice(
  term : UInt64,
  prev : EntryId,
  entries : Array[Entry],
  not_ok : Bool,
  last : EntryId,
) -> Unit raise {
  let s : LogSlice = { term, prev, entries }
  assert_eq(s.valid(), !not_ok)
  if !not_ok {
    assert_true(s.last_entry_id() == last)
    assert_eq(s.last_entry_id().index, s.last_index())
  }
}

///|
test "TestLogSlice" {
  // Empty "dummy" slice at the (0,0) origin.
  check_slice(0, eid(0, 0), [], false, eid(0, 0))
  // Empty slice with a given prev ID; valid only if term >= prev.term.
  check_slice(0, eid(123, 10), [], true, eid(0, 0))
  check_slice(9, eid(123, 10), [], true, eid(0, 0))
  check_slice(10, eid(123, 10), [], false, eid(123, 10))
  check_slice(11, eid(123, 10), [], false, eid(123, 10))
  // A single entry.
  check_slice(0, eid(0, 0), [ent(1, 1)], true, eid(0, 0))
  check_slice(1, eid(0, 0), [ent(1, 1)], false, eid(1, 1))
  check_slice(2, eid(0, 0), [ent(1, 1)], false, eid(1, 1))
  // Multiple entries.
  check_slice(2, eid(0, 0), [ent(2, 1), ent(3, 1), ent(4, 2)], true, eid(0, 0))
  check_slice(1, eid(1, 1), [ent(2, 1), ent(3, 1), ent(4, 2)], true, eid(0, 0))
  check_slice(2, eid(1, 1), [ent(2, 1), ent(3, 1), ent(4, 2)], false, eid(4, 2))
  // First entry inconsistent with prev.
  check_slice(10, eid(123, 5), [ent(111, 5)], true, eid(0, 0))
  check_slice(10, eid(123, 5), [ent(124, 4)], true, eid(0, 0))
  check_slice(10, eid(123, 5), [ent(234, 6)], true, eid(0, 0))
  check_slice(10, eid(123, 5), [ent(124, 6)], false, eid(124, 6))
  // Inconsistent entries.
  check_slice(10, eid(12, 2), [ent(13, 2), ent(12, 2)], true, eid(0, 0))
  check_slice(10, eid(12, 2), [ent(13, 2), ent(15, 2)], true, eid(0, 0))
  check_slice(10, eid(12, 2), [ent(13, 2), ent(14, 1)], true, eid(0, 0))
  check_slice(10, eid(12, 2), [ent(13, 2), ent(14, 3)], false, eid(14, 3))
}
