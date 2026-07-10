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

// The `panic ...` tests below drive each abort/`Panicf` guard to its illegal
// precondition. MoonBit's test harness runs a `panic`-named test as an assertion
// that the body aborts, and the coverage instrumentation records the abort line
// as executed, so a faithful transcription of an etcd `logger.Panicf(...)` is
// covered rather than left as an untestable contract.

// --- raftlog.mbt guards (etcd log.go) -----------------------------------------

///|
/// etcd log.go maybeAppend `Panicf("entry %d conflict with committed entry")`:
/// a slice whose entries conflict at or before the commit index.
test "panic maybe_append conflicting with a committed entry aborts" {
  let l = log_appended(ents_at(1, [1, 2, 3]))
  l.commit_to(1)
  let app : LogSlice = {
    term: 100,
    prev: { term: 0, index: 0 },
    entries: ents_at(1, [3, 3]),
  }
  l.maybe_append(app, 3) |> ignore
}

///|
/// etcd log.go maybeAppend `Panicf("index, %d, is out of range")`: a conflict
/// index that runs past the end of the appended slice (a non-contiguous slice).
test "panic maybe_append with a conflict index past the slice aborts" {
  let l = log_appended(ents_at(1, [1, 1, 1, 1, 1, 1]))
  let app : LogSlice = {
    term: 100,
    prev: { term: 1, index: 5 },
    entries: [Entry::normal(9, 9, b"")],
  }
  l.maybe_append(app, 0) |> ignore
}

///|
/// etcd log.go append `Panicf("after(%d) is out of range [committed(%d)]")`.
test "panic append below the commit index aborts" {
  let l = log_appended(ents_at(1, [1, 2, 3]))
  l.commit_to(3)
  l.append([Entry::normal(4, 2, b"")]) |> ignore
}

///|
/// etcd log.go commitTo `Panicf("tocommit(%d) is out of range [lastIndex(%d)]")`.
test "panic commit_to beyond the last index aborts" {
  let l = log_appended(ents_at(1, [1, 2, 3]))
  l.commit_to(99)
}

///|
/// etcd log.go appliedTo `Panicf("applied(%d) is out of range [prevApplied(%d),
/// committed(%d)]")`.
test "panic applied_to beyond the commit index aborts" {
  let l = log_appended(ents_at(1, [1, 2, 3]))
  l.commit_to(3)
  l.applied_to(99, 0)
}

///|
/// etcd log.go acceptApplying `Panicf("applying(%d) is out of range
/// [prevApplying(%d), committed(%d)]")`.
test "panic accept_applying beyond the commit index aborts" {
  let l = log_appended(ents_at(1, [1, 2, 3]))
  l.commit_to(3)
  l.accept_applying(99, 0, false)
}

///|
/// etcd log.go mustCheckOutOfBounds `Panicf("invalid slice %d > %d")`.
test "panic slice with lo greater than hi aborts" {
  let l = log_appended(ents_at(1, [1, 2, 3]))
  l.slice(3, 1, no_limit) |> ignore
}

///|
/// etcd log.go mustCheckOutOfBounds `Panicf("slice[%d,%d) out of bound
/// [%d,%d]")`.
test "panic slice past the last index aborts" {
  let l = log_appended(ents_at(1, [1, 2, 3]))
  l.slice(1, 99, no_limit) |> ignore
}

// --- storage_engine.mbt guards (etcd storage.go MemoryStorage) ----------------

///|
/// etcd storage.go Entries `Panicf("entries' hi(%d) is out of bound
/// lastindex(%d)")`.
test "panic storage entries past the last index aborts" {
  let s = MemoryStorage::new()
  s.append(ents_at(1, [1, 2, 3]))
  s.storage_entries(1, 99, no_limit) |> ignore
}

///|
/// etcd storage.go Append `Panicf("missing log entry [last: %d, append at: %d]")`.
test "panic storage append leaving a gap aborts" {
  let s = MemoryStorage::new()
  s.append(ents_at(1, [1, 2, 3]))
  s.append([Entry::normal(1, 9, b"")])
}

///|
/// etcd storage.go Compact `Panicf("compact %d is out of bound lastindex(%d)")`.
test "panic storage compact past the last index aborts" {
  let s = MemoryStorage::new()
  s.append(ents_at(1, [1, 2, 3]))
  s.compact(99) |> ignore
}

///|
/// etcd storage.go CreateSnapshot `Panicf("snapshot %d is out of bound
/// lastindex(%d)")`.
test "panic storage create_snapshot past the last index aborts" {
  let s = MemoryStorage::new()
  s.append(ents_at(1, [1, 2, 3]))
  s.create_snapshot(99, b"") |> ignore
}

// --- unstable.mbt guards (etcd log_unstable.go) -------------------------------

///|
/// etcd log_unstable.go slice `Panicf("invalid unstable.slice %d > %d")`.
test "panic unstable slice with lo greater than hi aborts" {
  let u = Unstable::new(10)
  u.slice(5, 3) |> ignore
}

///|
/// etcd log_unstable.go slice `Panicf("unstable.slice[%d,%d) out of bound
/// [%d,%d]")`.
test "panic unstable slice out of bounds aborts" {
  let u = Unstable::new(10)
  u.slice(3, 5) |> ignore
}

// --- non-panic reachable arms -------------------------------------------------

///|
/// `save_into` applies the node's snapshot to the target storage; when that
/// storage already holds a newer snapshot the apply is a no-op (etcd
/// `ApplySnapshot` returning `ErrSnapOutOfDate`), tolerated rather than fatal.
test "save_into tolerates a storage snapshot newer than the node's" {
  let a = Node::new("a")
  let _ = a.append(1, b"x")
  let _ = a.append(1, b"y")
  let _ = a.append(2, b"z")
  a.advance_commit(3)
  let _ = a.compact(2, b"snap@2")
  let storage = MemoryStorage::new()
  storage.apply_snapshot({
    last_index: 5,
    last_term: 3,
    data: b"",
    conf_state: ConfState::empty(),
  })
  a.save_into(storage)
  assert_eq(storage.last_index(), 5)
}

///|
/// `has_ready` reports outstanding work when the core has appended entries the
/// driver has not yet surfaced, independently of any pending message or state
/// change (etcd `HasReady`'s `hasNextUnstableEnts()` check).
test "has_ready reports work from unsurfaced unstable entries" {
  let rn = fresh_raw()
  rn.node().node().append(1, b"x") |> ignore
  assert_true(rn.has_ready())
}

///|
/// A storage backend that reports a non-empty log but raises `Compacted` for the
/// entry range — the compaction race `load_from` tolerates: the compaction point
/// advanced past `first_index` between reading it and fetching the entries.
struct RacingCompactionStorage {}

///|
impl RaftStorage for RacingCompactionStorage with fn initial_state(_self) {
  HardState::initial()
}

///|
impl RaftStorage for RacingCompactionStorage with fn storage_entries(
  _self,
  _lo,
  _hi,
  _max,
) {
  raise Compacted
}

///|
impl RaftStorage for RacingCompactionStorage with fn storage_term(_self, _i) {
  raise Compacted
}

///|
impl RaftStorage for RacingCompactionStorage with fn first_index(_self) {
  1
}

///|
impl RaftStorage for RacingCompactionStorage with fn last_index(_self) {
  3
}

///|
impl RaftStorage for RacingCompactionStorage with fn storage_snapshot(_self) {
  Snapshot::empty()
}

///|
/// `load_from` tolerates a range compacted out from under it: the entry read
/// raises `Compacted`, and recovery proceeds with no loaded entries rather than
/// aborting (etcd's read path returns `ErrCompacted`, handled not fatal).
test "load_from tolerates the entry range being compacted concurrently" {
  let n = Node::new("a")
  n.load_from(RacingCompactionStorage::{  } as &RaftStorage)
  assert_eq(n.last_log_index(), 0)
}
