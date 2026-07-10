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

// Ported from etcd-io/raft (Apache-2.0): raft_flow_control_test.go.
// Exercises the Inflights sliding window through the live leader send/ack path
// (proving the tracker is wired in, not an isolated data structure).

///|
fn accept_append(
  from : String,
  to : String,
  term : UInt64,
  index : UInt64,
) -> Message {
  Message::new(
    from,
    to,
    AppendResp({
      term,
      success: true,
      match_index: index,
      conflict_index: 0,
      conflict_term: 0,
    }),
  )
}

///|
/// TestMsgAppFlowControlFull: MsgApp fills the sending window until full, then
/// no more MsgApp is sent.
test "TestMsgAppFlowControlFull" {
  let r = RaftNode::new("1", ["2"], pre_vote=false, election_timeout=5)
  r.node().become_candidate()
  r.become_leader()
  let pr2 = r.progress["2"]
  pr2.become_replicate()
  let max_inflight = 256
  // 1. fill the window: each proposal emits exactly one MsgApp.
  for _i in 0..<max_inflight {
    let ms = r.propose(b"somedata")
    assert_eq(ms.length(), 1)
    assert_true(ms[0].kind() == "Append")
  }
  // ensure 1: the window is full.
  assert_true(pr2.is_paused())
  // ensure 2: while full, no more MsgApp is sent.
  for _i in 0..<10 {
    let ms = r.propose(b"somedata")
    assert_eq(ms.length(), 0)
  }
}

///|
/// TestMsgAppFlowControlMoveForward: a valid MsgAppResp slides the window past
/// all indices at or below its index; an out-of-date MsgAppResp has no effect.
test "TestMsgAppFlowControlMoveForward" {
  let r = RaftNode::new("1", ["2"], pre_vote=false, election_timeout=5)
  r.node().become_candidate()
  r.become_leader()
  let pr2 = r.progress["2"]
  pr2.become_replicate()
  let max_inflight = 256
  for _i in 0..<max_inflight {
    r.propose(b"somedata") |> ignore
  }
  // 1 is the noop, 2 is the first proposal; start acking from 2.
  let mut tt = 2
  while tt < max_inflight {
    // A valid ack moves the window forward.
    r.step(accept_append("2", "1", 1, tt.to_uint64())) |> ignore
    // Refill the window with one proposal → one MsgApp.
    let ms = r.propose(b"somedata")
    assert_eq(ms.length(), 1)
    assert_true(ms[0].kind() == "Append")
    // ensure 1: full again.
    assert_true(pr2.is_paused())
    // ensure 2: out-of-date acks leave the window untouched.
    for i in 0..<tt {
      r.step(accept_append("2", "1", 1, i.to_uint64())) |> ignore
      assert_true(pr2.is_paused())
    }
    tt = tt + 1
  }
}

///|
/// TestMsgAppFlowControlRecvHeartbeat: a heartbeat response frees the flow so
/// exactly one (empty) MsgApp probe is sent while the window stays full.
test "TestMsgAppFlowControlRecvHeartbeat" {
  let r = RaftNode::new("1", ["2"], pre_vote=false, election_timeout=5)
  r.node().become_candidate()
  r.become_leader()
  let pr2 = r.progress["2"]
  pr2.become_replicate()
  let max_inflight = 256
  for _i in 0..<max_inflight {
    r.propose(b"somedata") |> ignore
  }
  let mut tt = 1
  while tt < 5 {
    // Each heartbeat response releases the flow for exactly one empty probe.
    for _i in 0..<tt {
      assert_true(pr2.is_paused())
      let ms = r.step(Message::new("2", "1", HeartbeatResp(r.term())))
      assert_eq(ms.length(), 1)
      match ms[0].payload {
        Append(a) => assert_eq(a.entries.length(), 0)
        _ => abort("expected an empty Append probe")
      }
    }
    // No further appends are sent while the window is full and no heartbeat.
    for _i in 0..<10 {
      assert_true(pr2.is_paused())
      assert_eq(r.propose(b"somedata").length(), 0)
    }
    r.step(Message::new("2", "1", HeartbeatResp(r.term()))) |> ignore
    tt = tt + 1
  }
}
