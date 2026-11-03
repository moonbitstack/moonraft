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

///|
/// An in-memory write-ahead log: an ordered list of records standing in for an
/// append-only file. Used for tests and single-process runs.
struct MemWal {
  records : Array[WalRecord]
}

///|
fn MemWal::new() -> MemWal {
  { records: [], }
}

///|
impl @storage.WalStore for MemWal with fn append_record(self, record) {
  self.records.push(record)
}

///|
impl @storage.WalStore for MemWal with fn load(self) {
  self.records.copy()
}

///|
fn MemWal::len(self : MemWal) -> Int32 {
  self.records.length()
}

///|
test "a node round-trips through a write-ahead log" {
  let a = Node::new("a")
  a.become_candidate()
  let _ = a.append(1, b"x")
  let _ = a.append(1, b"y")
  a.advance_commit(2)
  let wal = MemWal::new()
  a.save_to(wal)

  // Crash and restart from the log alone.
  let b = Node::new("a")
  b.recover_from(wal)
  assert_eq(b.current_term(), 1)
  assert_true(b.voted_for == Some("a"))
  assert_eq(b.last_log_index(), 2)
  assert_eq(b.commit_index, 2)
  assert_eq(b.term_at(2), 1)
}

///|
test "replay rebuilds a snapshot baseline plus the log tail" {
  let a = Node::new("a")
  let _ = a.append(1, b"x")
  let _ = a.append(1, b"y")
  let _ = a.append(2, b"z")
  a.advance_commit(3)
  let _ = a.compact(2, b"state@2")
  let wal = MemWal::new()
  a.save_to(wal)
  let b = Node::new("a")
  b.recover_from(wal)
  assert_eq(b.last_log_index(), 3)
  assert_eq(b.term_at(2), 1)
  assert_eq(b.term_at(3), 2)
  assert_true(b.entry_at(1) == None)
}

///|
test "replaying a conflicting entry overwrites the tail" {
  let node = Node::new("n")
  let _ = node.append(1, b"old")
  node.replay([
    WalHardState({ term: 2, vote: None, commit: 0, }),
    WalEntry(Entry::normal(2, 1, b"new")),
  ])
  assert_eq(node.current_term(), 2)
  assert_eq(node.term_at(1), 2)
  assert_eq(node.last_log_index(), 1)
}

///|
test "a fresh write-ahead log holds one record set per save" {
  let a = Node::new("a")
  let _ = a.append(1, b"x")
  let wal = MemWal::new()
  a.save_to(wal)
  // One HardState record plus one entry record.
  assert_eq(wal.len(), 2)
}
