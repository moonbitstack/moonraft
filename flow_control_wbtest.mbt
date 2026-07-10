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
    AppendResp({ term, success: true, match_index: index, conflict_index: 0 }),
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
