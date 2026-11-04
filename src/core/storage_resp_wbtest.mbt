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
/// `stableTo` against the four things an acknowledgement can hit: nothing to
/// confirm, an index the tail no longer holds, the snapshot baseline, and a tail
/// that has since been rewritten under a newer term.
test "UnstableStableTo names every ignored acknowledgement" {
  let l = RaftLog::new(MemoryStorage::new())
  // An acknowledgement with no entries to confirm asks nothing.
  assert_true(l.async_stabilize_checked(0, 1) is Stabilized)
  // Nothing in the tail holds index 7.
  assert_true(l.async_stabilize_checked(7, 1) is Missing)
  // A snapshot baseline answers for its own index, but confirms no entry.
  l.restore({
    last_index: 5,
    last_term: 2,
    data: b"",
    conf_state: ConfState::empty(),
  })
  assert_true(l.async_stabilize_checked(5, 2) is MatchedSnapshot)
  // A fresh log whose tail holds (3, 12): an acknowledgement for (12, 2) lost
  // the race and must not confirm the entry that replaced it.
  let tail = RaftLog::new(MemoryStorage::new())
  let tail_ents : Array[Entry] = []
  for i = 1; i <= 11; i = i + 1 {
    tail_ents.push(Entry::normal(1, i.to_uint64(), b""))
  }
  tail_ents.push(Entry::normal(3, 12, b""))
  tail.append(tail_ents) |> ignore
  match tail.async_stabilize_checked(12, 2) {
    TermMismatch(existing_term~) => assert_eq(existing_term, 3UL)
    _ => fail("expected a term mismatch on (12, 2)")
  }
  // The same acknowledgement replayed with the right term is the match that
  // stabilises it.
  assert_true(tail.async_stabilize_checked(12, 3) is Stabilized)
}

///|
/// The durable view `raft-log` reads: empty before anything is written, and the
/// written range after.
test "RaftLog stable_entries reads storage, not the unstable tail" {
  let l = RaftLog::new(MemoryStorage::new())
  assert_eq(l.stable_entries().length(), 0)
  assert_eq(l.storage_first_index(), 1UL)
  assert_eq(l.storage_last_index(), 0UL)
  // An unstable append is not durable yet.
  l.append([Entry::normal(1, 1, b"a"), Entry::normal(1, 2, b"b")]) |> ignore
  assert_eq(l.stable_entries().length(), 0)
  assert_eq(l.storage_last_index(), 0UL)
  // Writing it through makes it durable, and then it is what the reader sees.
  l.append_stable(l.next_unstable_ents())
  assert_eq(l.stable_entries().length(), 2)
  assert_eq(l.storage_last_index(), 2UL)
  assert_eq(l.stable_entries()[1].command, b"b")
}

///|
/// The driver adds the diagnostics etcd prints when a storage acknowledgement is
/// ignored, one line per reason.
test "RawNode step_append_resp reports an ignored acknowledgement" {
  let logger = CapturingLogger::new()
  let rn = RaftNode::new("1", [], logger~).raw()
  rn.campaign()
  let term = rn.node().node().current_term()
  // Missing: the tail holds nothing at index 99.
  rn.step(
    Message::new(
      "AppendThread",
      "1",
      StorageAppendResp({ index: 99, log_term: 1, term, }),
    ),
  )
  // Matched the snapshot baseline rather than an entry.
  rn.log.restore({
    last_index: 4,
    last_term: 1,
    data: b"",
    conf_state: ConfState::empty(),
  })
  rn.step(
    Message::new(
      "AppendThread",
      "1",
      StorageAppendResp({ index: 4, log_term: 1, term, }),
    ),
  )
  // The tail was rewritten at index 5 under a newer term.
  rn.log.restore({
    last_index: 4,
    last_term: 1,
    data: b"",
    conf_state: ConfState::empty(),
  })
  rn.log.append([Entry::normal(3, 5, b"")]) |> ignore
  rn.step(
    Message::new(
      "AppendThread",
      "1",
      StorageAppendResp({ index: 5, log_term: 2, term, }),
    ),
  )
  // A response from a term the node has left is dropped with its own line.
  rn.step(
    Message::new(
      "AppendThread",
      "1",
      StorageAppendResp({ index: 5, log_term: 3, term: term - 1UL, }),
    ),
  )
  let lines = logger.messages()
  // The constructor and the campaign log too; the four lines under test are the
  // last four.
  let base = lines.length() - 4
  assert_true(
    lines[base].contains("entry at index 99 missing from unstable log"),
  )
  assert_true(
    lines[base + 1].contains("entry at index 4 matched unstable snapshot"),
  )
  assert_true(
    lines[base + 2].contains(
      "entry at (index,term)=(5,2) mismatched with entry at (5,3) in unstable log",
    ),
  )
  assert_true(lines[base + 3].contains("ignored entry appends"))
}

///|
/// A compaction request the durable log cannot honour is ignored rather than
/// aborting: the mirror only holds what the driver has been handed.
test "RawNode compact_storage ignores an out-of-range request" {
  let rn = RaftNode::new("1", [], logger=NopLogger::{ }).raw()
  // Nothing is durable yet, so neither a zero index nor one past the end can be
  // compacted to.
  rn.compact_storage(0)
  rn.compact_storage(9)
  assert_eq(rn.log.storage_last_index(), 0UL)
  // A node recovered from a snapshot has a durable baseline but no entries past
  // it, so a compaction to the baseline itself is one the storage refuses
  // (`Compacted`) rather than one the driver can honour.
  let snap : Snapshot = {
    last_index: 5,
    last_term: 1,
    data: b"",
    conf_state: ConfState::empty(),
  }
  let recovered = RaftNode::new("2", [], snapshot=snap).raw()
  recovered.compact_storage(5)
  assert_eq(recovered.log.storage_last_index(), 5UL)
}
