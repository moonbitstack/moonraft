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

// Ported from etcd-io/raft (Apache-2.0): raft_test.go TestUncommittedEntryLimit
// (MaxUncommittedEntriesSize) and the MaxSizePerMsg batch cap. `ack_from` is
// shared from confchange_apply_wbtest.mbt.

///|
test "TestUncommittedEntryLimit" {
  let max_entries = 1024
  let test_data = b"testdata" // 8-byte payload
  let max_entry_size = (max_entries * 8).to_uint64()
  let r = RaftNode::new(
    "1",
    ["2", "3"],
    pre_vote=false,
    max_uncommitted_size=max_entry_size,
  )
  r.node().become_candidate()
  r.become_leader()
  // The leader's initial no-op has no payload, so the tally stays zero.
  assert_eq(r.uncommitted_size(), 0)
  r.progress["2"].become_replicate()
  r.progress["3"].become_replicate()
  r.uncommitted_size = 0

  // The first 1024 proposals are appended; the tally reaches exactly the cap.
  for _i in 0..<max_entries {
    let before = r.node().last_log_index()
    r.propose(test_data) |> ignore
    assert_eq(r.node().last_log_index(), before + 1)
  }
  // One more is dropped.
  let before = r.node().last_log_index()
  r.propose(test_data) |> ignore
  assert_eq(r.node().last_log_index(), before)

  // Release the bytes as if the entries had committed.
  r.reduce_uncommitted_size(max_entry_size)
  assert_eq(r.uncommitted_size(), 0)

  // A single large proposal is accepted because the tail was empty beforehand,
  // even though it pushes past the cap.
  let large = Bytes::make(2 * max_entries * 8, b'a')
  let b2 = r.node().last_log_index()
  r.propose(large) |> ignore
  assert_eq(r.node().last_log_index(), b2 + 1)
  // Now a normal proposal is dropped again.
  let b3 = r.node().last_log_index()
  r.propose(test_data) |> ignore
  assert_eq(r.node().last_log_index(), b3)
  // But an empty (zero-byte) entry always appends.
  let b4 = r.node().last_log_index()
  r.propose(b"") |> ignore
  assert_eq(r.node().last_log_index(), b4 + 1)
  r.reduce_uncommitted_size((2 * max_entries * 8).to_uint64())
  assert_eq(r.uncommitted_size(), 0)
}

///|
/// MaxSizePerMsg caps a single AppendEntries batch by encoded byte size, always
/// keeping at least one entry.
test "MaxSizePerMsg bounds the AppendEntries batch" {
  let cap = 250UL
  let r = RaftNode::new("1", ["2"], pre_vote=false, max_msg_bytes=cap)
  r.node().become_candidate()
  r.become_leader()
  // Append several ~100-byte entries beyond the no-op.
  for _i in 0..<10 {
    let _ = r.node().append(r.term(), Bytes::make(100, b'x'))
  }
  // Probe from the start of the log; the batch must respect the byte cap.
  r.progress["2"].next_index = 1
  let msg = r.send_to("2")
  match msg.payload {
    Append(a) => {
      assert_true(a.entries.length() >= 1)
      assert_true(ents_size(a.entries[:]) <= cap || a.entries.length() == 1)
      // The cap admits fewer than the full run of 11 entries.
      assert_true(a.entries.length() < 11)
    }
    _ => abort("expected an Append")
  }
}
