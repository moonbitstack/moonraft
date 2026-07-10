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
// Byte-level commit pagination, ported from etcd node_test.go / rawnode_test.go.
// These exercise the RaftLog's `max_applying_ents_size` cap now wired into
// RawNode via `raw_with_max_committed_size`.

///|
test "TestCommitPagination" {
  let rn = RaftNode::new("1", []).raw_with_max_committed_size(2048)
  rn.campaign()
  // Persist and apply the vote and the election no-op.
  rn.stabilize() |> ignore
  let blob = Bytes::make(1000, b'a')
  rn.propose(blob)
  rn.propose(blob)
  rn.propose(blob)
  // First Ready: all three proposals to persist, and — since the caller persists
  // before applying — the first paginated commit batch to apply, together. The
  // 2048-byte cap fits two ~1009-byte entries, so the batch is 2 of 3.
  let rd1 = rn.ready()
  assert_eq(rd1.entries.length(), 3)
  assert_eq(rd1.committed_entries.length(), 2)
  rn.store(rd1)
  rn.advance(rd1)
  // Second Ready: the last commit batch (1), no more to persist.
  let rd2 = rn.ready()
  assert_eq(rd2.committed_entries.length(), 1)
  rn.store(rd2)
  rn.advance(rd2)
}

///|
test "TestRawNodeCommitPaginationAfterRestart" {
  // A node restarts with 11 committed entries and a small per-Ready commit cap.
  // Every entry must still be applied, in order, with no gaps and none dropped
  // (the etcd regression: the last committed entry being silently paginated
  // away). Simplification: commit is set directly on restart rather than nudged
  // to 11 by a MsgHeartbeat, which is a replication-core concern.
  let rn0 = RaftNode::new("1", [])
  let node = rn0.node()
  let mut k = 1
  while k <= 11 {
    node.append(1, b"a") |> ignore
    k = k + 1
  }
  node.apply_hard_state({ term: 1, vote: Some("1"), commit: 11 })
  let esz = entry_encoding_size(Entry::normal(1, 1, b"a"))
  let raw = rn0.raw_with_max_committed_size(esz * 5)
  let applied : Array[UInt64] = []
  let mut highest : UInt64 = 0
  while highest != 11 {
    let rd = raw.ready()
    // Progress must be made every round.
    assert_true(rd.committed_entries.length() > 0)
    let first = rd.committed_entries[0].index
    // No gap between rounds.
    if highest != 0 {
      assert_eq(first, highest + 1)
    }
    for e in rd.committed_entries {
      applied.push(e.index)
    }
    highest = rd.committed_entries[rd.committed_entries.length() - 1].index
    raw.store(rd)
    raw.advance(rd)
  }
  // Exactly indices 1..11 applied, in order.
  assert_eq(applied.length(), 11)
  let mut idx = 0
  while idx < 11 {
    assert_eq(applied[idx], (idx + 1).to_uint64())
    idx = idx + 1
  }
}
