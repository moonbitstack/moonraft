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

// Ports of raft_test.go progress/send-append cases. See GAP_core.md census.
// etcd's pause-gated `sendAppend(to)` is `bcast_append()` here: it iterates the
// single follower and skips it while its flow is paused, whereas the unconditional
// `send_to` mirrors etcd's `maybeSendAppend` core without the pause gate. MsgBeat,
// which etcd routes to `bcastHeartbeat`, is `bcast_heartbeat()` here.

///|
/// TestSendAppendForProgressProbe: a probing follower is sent exactly one
/// AppendEntries per heartbeat round; further appends are withheld until a
/// heartbeat response clears the flow throttle (raft_test.go:2402).
test "TestSendAppendForProgressProbe" {
  let r = RaftNode::new("1", ["2"], pre_vote=false)
  r.node().become_candidate()
  r.become_leader()
  r.progress["2"].become_probe()
  for i in 0..<3 {
    if i == 0 {
      // The first send of a round emits one probe carrying the log; its
      // prev_log_index is 0 (probing from the start).
      r.node().append(r.term(), b"somedata") |> ignore
      let msgs = r.bcast_append()
      assert_eq(msgs.length(), 1)
      match msgs[0].payload {
        Append(a) => assert_eq(a.prev_log_index, 0)
        _ => abort("expected an Append")
      }
    }
    assert_true(r.progress["2"].msg_app_flow_paused)
    // A paused prober is sent nothing, however much its log grows.
    for _j in 0..<10 {
      r.node().append(r.term(), b"somedata") |> ignore
      assert_eq(r.bcast_append().length(), 0)
    }
    // A heartbeat round does not clear the throttle.
    let hb = r.bcast_heartbeat()
    assert_true(r.progress["2"].msg_app_flow_paused)
    assert_eq(hb.length(), 1)
    assert_true(hb[0].kind() == "Heartbeat")
  }
  // A heartbeat response releases exactly one more message.
  let out = r.step(
    Message::new("2", "1", HeartbeatResp({ term: r.term(), context: b"" })),
  )
  assert_eq(out.length(), 1)
  match out[0].payload {
    Append(a) => assert_eq(a.prev_log_index, 0)
    _ => abort("expected an Append")
  }
  assert_true(r.progress["2"].msg_app_flow_paused)
}

///|
/// TestSendAppendForProgressReplicate: a streaming follower is sent an
/// AppendEntries on every send, its in-flight window admitting each one
/// (raft_test.go:2449).
test "TestSendAppendForProgressReplicate" {
  let r = RaftNode::new("1", ["2"], pre_vote=false)
  r.node().become_candidate()
  r.become_leader()
  r.progress["2"].become_replicate()
  for _i in 0..<10 {
    r.node().append(r.term(), b"somedata") |> ignore
    let msgs = r.bcast_append()
    assert_eq(msgs.length(), 1)
  }
}

///|
/// TestSendAppendForProgressSnapshot: a follower being sent a snapshot is sent
/// no AppendEntries (raft_test.go:2464). Its progress is pinned in the snapshot
/// state, which `is_paused` reports so `bcast_append` skips it.
test "TestSendAppendForProgressSnapshot" {
  let r = RaftNode::new("1", ["2"], pre_vote=false)
  r.node().become_candidate()
  r.become_leader()
  r.progress["2"].become_snapshot(10)
  for _i in 0..<10 {
    r.node().append(r.term(), b"somedata") |> ignore
    assert_eq(r.bcast_append().length(), 0)
  }
}

///|
/// TestLeaderIncreaseNext: proposing under a follower in the replicate state
/// optimistically advances its next index past the whole log; under the probe
/// state it does not move (raft_test.go:2372). Both table cases.
test "TestLeaderIncreaseNext" {
  // previousEnts = [1@1, 2@2, 3@3]; noop@4 on becoming leader; propose @5.
  // Replicate: next jumps to last+1 = 6. Probe: next stays at the preset 2.
  for tc in [(true, 6UL), (false, 2UL)] {
    let (replicate, wnext) = tc
    let r = RaftNode::new("1", ["2"], pre_vote=false)
    r.node().append(1, b"") |> ignore
    r.node().append(2, b"") |> ignore
    r.node().append(3, b"") |> ignore
    r.node().become_candidate()
    r.become_leader()
    if replicate {
      r.progress["2"].become_replicate()
    } else {
      r.progress["2"].become_probe()
    }
    r.progress["2"].next_index = 2
    r.propose(b"somedata") |> ignore
    assert_eq(r.progress["2"].next_index, wnext)
  }
}
