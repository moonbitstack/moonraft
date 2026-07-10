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
// Ported from etcd-io/raft storage_test.go. Each Go table-driven case is
// reproduced; MemoryStorage is now etcd-faithful, distinguishing ErrCompacted
// from ErrUnavailable and rejecting stale snapshots with ErrSnapOutOfDate.

///|
/// Build `[start, start+len)` entries carrying the given terms and empty
/// payloads. Mirrors etcd's `index(start).terms(...)`.
fn ents_at(start : UInt64, terms : Array[UInt64]) -> Array[Entry] {
  let out : Array[Entry] = []
  let mut i = start
  for t in terms {
    out.push(Entry::normal(t, i, b""))
    i = i + 1
  }
  out
}

///|
/// Assert two entry runs match on index and term (etcd's requireEqualEntries).
fn assert_entries(got : Array[Entry], want : Array[Entry]) -> Unit raise {
  assert_eq(got.length(), want.length())
  for i in 0..<got.length() {
    assert_eq(got[i].index, want[i].index)
    assert_eq(got[i].term, want[i].term)
  }
}

///|
/// Assert that `thunk` raises exactly `want`.
fn assert_raises(
  thunk : () -> Unit raise StorageError,
  want : StorageError,
) -> Unit raise {
  match
    (try {
      thunk()
      None
    } catch {
      e => Some(e)
    }) {
    Some(e) => assert_true(e == want)
    None => abort("expected a StorageError but the call succeeded")
  }
}

///|
test "TestStorageTerm" {
  let ents = ents_at(3, [3, 4, 5])
  // {2, ErrCompacted, 0}
  let s = MemoryStorage::from_ents(ents)
  assert_raises(() => s.storage_term(2) |> ignore, Compacted)
  // {3, nil, 3}
  assert_eq(s.storage_term(3), 3)
  // {4, nil, 4}
  assert_eq(s.storage_term(4), 4)
  // {5, nil, 5}
  assert_eq(s.storage_term(5), 5)
  // {6, ErrUnavailable, 0}
  assert_raises(() => s.storage_term(6) |> ignore, Unavailable)
}

///|
test "TestStorageEntries" {
  let ents = ents_at(3, [3, 4, 5, 6])
  let sz = fn(i : Int) { entry_encoding_size(ents[i]) }
  let big = no_limit
  // {2, 6, max, ErrCompacted, nil}
  let s = MemoryStorage::from_ents(ents)
  assert_raises(() => s.storage_entries(2, 6, big) |> ignore, Compacted)
  // {3, 4, max, ErrCompacted, nil}
  assert_raises(() => s.storage_entries(3, 4, big) |> ignore, Compacted)
  // {4, 5, max, nil, index(4).terms(4)}
  assert_entries(s.storage_entries(4, 5, big), ents_at(4, [4]))
  // {4, 6, max, nil, index(4).terms(4,5)}
  assert_entries(s.storage_entries(4, 6, big), ents_at(4, [4, 5]))
  // {4, 7, max, nil, index(4).terms(4,5,6)}
  assert_entries(s.storage_entries(4, 7, big), ents_at(4, [4, 5, 6]))
  // even if maxsize is zero, the first entry is returned
  assert_entries(s.storage_entries(4, 7, 0), ents_at(4, [4]))
  // limit to 2 (size of ents[1]+ents[2])
  assert_entries(s.storage_entries(4, 7, sz(1) + sz(2)), ents_at(4, [4, 5]))
  // limit to 2 (size of ents[1]+ents[2]+ents[3]/2)
  assert_entries(
    s.storage_entries(4, 7, sz(1) + sz(2) + sz(3) / 2),
    ents_at(4, [4, 5]),
  )
  // limit to 2 (ents[1]+ents[2]+ents[3]-1)
  assert_entries(
    s.storage_entries(4, 7, sz(1) + sz(2) + sz(3) - 1),
    ents_at(4, [4, 5]),
  )
  // all (ents[1]+ents[2]+ents[3])
  assert_entries(
    s.storage_entries(4, 7, sz(1) + sz(2) + sz(3)),
    ents_at(4, [4, 5, 6]),
  )
}

///|
test "TestStorageLastIndex" {
  let s = MemoryStorage::from_ents(ents_at(3, [3, 4, 5]))
  assert_eq(s.last_index(), 5)
  s.append(ents_at(6, [5]))
  assert_eq(s.last_index(), 6)
}

///|
test "TestStorageFirstIndex" {
  let s = MemoryStorage::from_ents(ents_at(3, [3, 4, 5]))
  assert_eq(s.first_index(), 4)
  s.compact(4)
  assert_eq(s.first_index(), 5)
}

///|
test "TestStorageCompact" {
  // {2, ErrCompacted, 3, 3, 3}
  let s0 = MemoryStorage::from_ents(ents_at(3, [3, 4, 5]))
  assert_raises(() => s0.compact(2), Compacted)
  assert_eq(s0.raw_ents()[0].index, 3)
  assert_eq(s0.raw_ents()[0].term, 3)
  assert_eq(s0.raw_ents().length(), 3)
  // {3, ErrCompacted, 3, 3, 3}
  let s1 = MemoryStorage::from_ents(ents_at(3, [3, 4, 5]))
  assert_raises(() => s1.compact(3), Compacted)
  assert_eq(s1.raw_ents()[0].index, 3)
  assert_eq(s1.raw_ents()[0].term, 3)
  assert_eq(s1.raw_ents().length(), 3)
  // {4, nil, 4, 4, 2}
  let s2 = MemoryStorage::from_ents(ents_at(3, [3, 4, 5]))
  s2.compact(4)
  assert_eq(s2.raw_ents()[0].index, 4)
  assert_eq(s2.raw_ents()[0].term, 4)
  assert_eq(s2.raw_ents().length(), 2)
  // {5, nil, 5, 5, 1}
  let s3 = MemoryStorage::from_ents(ents_at(3, [3, 4, 5]))
  s3.compact(5)
  assert_eq(s3.raw_ents()[0].index, 5)
  assert_eq(s3.raw_ents()[0].term, 5)
  assert_eq(s3.raw_ents().length(), 1)
}

///|
test "TestStorageCreateSnapshot" {
  // {4, nil, snap(index=4, term=4)}
  let s0 = MemoryStorage::from_ents(ents_at(3, [3, 4, 5]))
  let snap0 = s0.create_snapshot(4, b"data")
  assert_eq(snap0.last_index, 4)
  assert_eq(snap0.last_term, 4)
  assert_true(snap0.data == b"data")
  // {5, nil, snap(index=5, term=5)}
  let s1 = MemoryStorage::from_ents(ents_at(3, [3, 4, 5]))
  let snap1 = s1.create_snapshot(5, b"data")
  assert_eq(snap1.last_index, 5)
  assert_eq(snap1.last_term, 5)
  assert_true(snap1.data == b"data")
}

///|
test "TestStorageAppend" {
  // {index(1).terms(1,2), index(3).terms(3,4,5)}
  let a = MemoryStorage::from_ents(ents_at(3, [3, 4, 5]))
  a.append(ents_at(1, [1, 2]))
  assert_entries(a.raw_ents(), ents_at(3, [3, 4, 5]))
  // {index(3).terms(3,4,5), index(3).terms(3,4,5)}
  let b = MemoryStorage::from_ents(ents_at(3, [3, 4, 5]))
  b.append(ents_at(3, [3, 4, 5]))
  assert_entries(b.raw_ents(), ents_at(3, [3, 4, 5]))
  // {index(3).terms(3,6,6), index(3).terms(3,6,6)}
  let c = MemoryStorage::from_ents(ents_at(3, [3, 4, 5]))
  c.append(ents_at(3, [3, 6, 6]))
  assert_entries(c.raw_ents(), ents_at(3, [3, 6, 6]))
  // {index(3).terms(3,4,5,5), index(3).terms(3,4,5,5)}
  let d = MemoryStorage::from_ents(ents_at(3, [3, 4, 5]))
  d.append(ents_at(3, [3, 4, 5, 5]))
  assert_entries(d.raw_ents(), ents_at(3, [3, 4, 5, 5]))
  // truncate incoming, truncate existing and append: {index(2).terms(3,3,5), index(3).terms(3,5)}
  let e = MemoryStorage::from_ents(ents_at(3, [3, 4, 5]))
  e.append(ents_at(2, [3, 3, 5]))
  assert_entries(e.raw_ents(), ents_at(3, [3, 5]))
  // truncate existing and append: {index(4).terms(5), index(3).terms(3,5)}
  let f = MemoryStorage::from_ents(ents_at(3, [3, 4, 5]))
  f.append(ents_at(4, [5]))
  assert_entries(f.raw_ents(), ents_at(3, [3, 5]))
  // direct append: {index(6).terms(5), index(3).terms(3,4,5,5)}
  let g = MemoryStorage::from_ents(ents_at(3, [3, 4, 5]))
  g.append(ents_at(6, [5]))
  assert_entries(g.raw_ents(), ents_at(3, [3, 4, 5, 5]))
}

///|
test "TestStorageApplySnapshot" {
  // normal case: apply snap(index=4, term=4)
  let s0 = MemoryStorage::new()
  s0.apply_snapshot({ last_index: 4, last_term: 4, data: b"data" })
  assert_eq(s0.storage_snapshot().last_index, 4)
  // snapshot out of date: apply index=4 then index=3 -> ErrSnapOutOfDate
  let s1 = MemoryStorage::new()
  s1.apply_snapshot({ last_index: 4, last_term: 4, data: b"data" })
  assert_raises(
    () => s1.apply_snapshot({ last_index: 3, last_term: 3, data: b"data" }),
    SnapOutOfDate,
  )
  // bootstrap with confState only (index/term 0): allowed
  let s2 = MemoryStorage::new()
  s2.apply_snapshot({ last_index: 0, last_term: 0, data: b"data" })
  assert_eq(s2.storage_snapshot().last_index, 0)
}
