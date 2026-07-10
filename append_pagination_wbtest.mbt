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
    let msg = r.send_to("2")
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
  // A leader partitioned from its followers keeps proposing; the uncommitted log
  // tail must stay bounded by MaxUncommittedEntriesSize, and the bytes are
  // released once the entries make progress. (Driven at the RaftNode/RawNode
  // seam: a 3-voter leader whose followers never ack, so commit — and thus the
  // release — is blocked until a quorum acknowledges. See PORTING.md for why the
  // etcd single-voter form is not reproducible here.)
  let max_entries = 16
  let data = b"testdata" // 8-byte payload
  let cap = (max_entries * 8).to_uint64()
  let r = RaftNode::new(
    "1",
    ["2", "3"],
    pre_vote=false,
    max_uncommitted_size=cap,
  )
  r.node().become_candidate()
  r.become_leader()
  r.progress["2"].become_replicate()
  r.progress["3"].become_replicate()
  // The election no-op carries no payload, so start the tally clean.
  r.uncommitted_size = 0
  let raw = r.raw()

  // Propose a burst far larger than the cap; only `max_entries` are accepted.
  let before = r.node().last_log_index()
  let mut i = 0
  while i < 1024 {
    raw.propose(data)
    i = i + 1
  }
  assert_eq(r.uncommitted_size(), cap)
  assert_eq(r.node().last_log_index(), before + max_entries.to_uint64())

  // The leader still surfaces the appended (but uncommitted) tail to persist.
  let rd = raw.ready()
  assert_eq(rd.entries.length(), max_entries)
  raw.advance(rd)
  // Appended, not yet committed: the tally is unchanged.
  assert_eq(r.uncommitted_size(), cap)

  // Recover: a quorum acknowledges, the tail commits and its bytes are released.
  let last = r.node().last_log_index()
  raw.step(ack_from("2", last, r.term()))
  raw.step(ack_from("3", last, r.term()))
  assert_eq(r.uncommitted_size(), 0)
}
