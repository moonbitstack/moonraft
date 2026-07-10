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
// Ported from etcd node_test.go:TestAppendPagination and
// rawnode_test.go:TestRawNodeBoundedLogGrowthWithPartition. `ack_from` is shared
// from confchange_apply_wbtest.mbt.

///|
test "TestAppendPagination" {
  // A leader with entries to send to a lagging follower must split its
  // AppendEntries into byte-bounded batches: never over MaxSizePerMsg, and (as
  // long as more than one entry fits) at least one batch over half the cap.
  let cap = 2048UL
  let r = RaftNode::new("1", ["2"], pre_vote=false, max_msg_bytes=cap)
  r.node().become_candidate()
  r.become_leader()
  r.progress["2"].become_replicate()
  // Five ~1000-byte proposals accumulate on the leader (follower 2 is behind).
  let blob = Bytes::make(1000, b'a')
  for _i in 0..<5 {
    r.node().append(r.term(), blob) |> ignore
  }
  // Catch the follower up one AppendEntries at a time, inspecting every batch.
  r.progress["2"].next_index = 1
  let mut seen_full = false
  let mut steps = 0
  while r.progress["2"].next_index <= r.node().last_log_index() && steps < 100 {
    steps = steps + 1
    let msg = r.send_to("2").unwrap()
    match msg.payload {
      Append(a) => {
        let n = a.entries.length()
        if n == 0 {
          break
        }
        let size = ents_size(a.entries[:])
        // Never exceed the cap, unless a lone entry is itself larger.
        assert_true(size <= cap || n == 1)
        if size > cap / 2 {
          seen_full = true
        }
        r.step(ack_from("2", a.entries[n - 1].index, r.term())) |> ignore
      }
      _ => break
    }
  }
  assert_true(seen_full)
}

///|
test "TestRawNodeBoundedLogGrowthWithPartitionedLeader" {
  // etcd's single-voter form: a partitioned leader keeps proposing. Its own
  // appends commit immediately (single-voter quorum), but the uncommitted-tail
  // quota is released only at *apply* time (RawNode Advance), so the tail is
  // bounded by MaxUncommittedEntriesSize and further proposals are dropped until
  // the committed entries are applied.
  let max_entries = 16
  let data = b"testdata" // 8-byte payload
  let cap = (max_entries * 8).to_uint64()
  let r = RaftNode::new("1", [], pre_vote=false, max_uncommitted_size=cap)
  let raw = r.raw()
  raw.campaign()
  // Persist and apply the leader's initial no-op, so the tally starts clean.
  let rd0 = raw.ready()
  raw.store(rd0)
  raw.advance(rd0)
  assert_eq(r.uncommitted_size(), 0)
  let before = r.node().last_log_index()

  // Propose far more than the cap; only `max_entries` are appended.
  let mut i = 0
  while i < 1024 {
    raw.propose(data)
    i = i + 1
  }
  assert_eq(r.node().last_log_index(), before + max_entries.to_uint64())
  // The tail is bounded, and — committed but not yet applied — stays accounted.
  assert_eq(r.uncommitted_size(), cap)

  // Drive Ready/Advance until the committed entries are applied; only then is
  // the quota released (apply-time, matching etcd).
  let mut rounds = 0
  while r.uncommitted_size() > 0 && rounds < 10 {
    rounds = rounds + 1
    let rd = raw.ready()
    raw.store(rd)
    raw.advance(rd)
  }
  assert_eq(r.uncommitted_size(), 0)
}
