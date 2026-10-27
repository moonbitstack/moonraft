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

// Port of raft_test.go's TestProgressLeader (raft_test.go:142): the leader keeps
// its own entry in the progress map and self-acks its appends, so its own match
// advances to the last index. etcd stages the self-ack through msgsAfterAppend
// (advanceMessagesAfterAppend); this port appends synchronously, so the leader's
// own match tracks its log immediately.

///|
test "TestProgressLeader" {
  let r = RaftNode::new("1", ["2"])
  r.become_candidate()
  r.become_leader()
  r.progress["2"].become_replicate()

  // The leader tracks itself in its own progress map (etcd's r.trk.Progress[1]).
  assert_true(r.progress.contains("1"))

  // Five proposals; with the election no-op that is six log entries.
  for _i in 0..<5 {
    r.propose(b"foo") |> ignore
  }
  assert_eq(r.node().last_log_index(), 6)

  // The leader's own progress has folded in every self-append.
  assert_eq(r.progress["1"].match_index, 6)
  assert_eq(r.progress["1"].next_index, 7)

  // The follower it has not heard from is untouched at match 0.
  assert_eq(r.progress["2"].match_index, 0)
}
