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

// Ports of raft_test.go step-driven cases. See GAP_core.md census.

///|
/// TestPromotable: only a voter of the current configuration may campaign; a
/// node not in the voter set (empty config, or a config that excludes it) is not
/// promotable (raft_test.go:2938).
test "TestPromotable" {
  // {self} → promotable.
  let a = RaftNode::new("1", [], pre_vote=false)
  assert_true(a.promotable())
  // {1,2,3} → promotable.
  let b = RaftNode::new("1", ["2", "3"], pre_vote=false)
  assert_true(b.promotable())
  // {} (self removed) → not promotable.
  let c = RaftNode::new("1", [], pre_vote=false)
  c.config.members.retain(fn(m) { m != "1" })
  assert_false(c.promotable())
  // {2,3} (self not a voter) → not promotable.
  let d = RaftNode::new("1", ["2", "3"], pre_vote=false)
  d.config.members.retain(fn(m) { m != "1" })
  assert_false(d.promotable())
}

///|
/// TestHandleHeartbeat: a heartbeat advances the follower's commit index toward
/// the leader's (capped at its own log), never decreasing it, and always draws a
/// HeartbeatResp (raft_test.go:1181).
test "TestHandleHeartbeat" {
  for tc in [(3UL, 3UL), (1UL, 2UL)] {
    let (hb_commit, want) = tc
    let r = RaftNode::new("1", ["2"], pre_vote=false)
    r.node().append(1, b"a") |> ignore
    r.node().append(2, b"b") |> ignore
    r.node().append(3, b"c") |> ignore
    r.become_follower(2)
    r.node().advance_commit(2)
    let out = r.step(
      Message::new(
        "2",
        "1",
        Heartbeat({ term: 2, leader_id: "2", commit: hb_commit, context: b"" }),
      ),
    )
    assert_eq(r.commit_index(), want)
    assert_eq(out.length(), 1)
    assert_true(out[0].kind() == "HeartbeatResp")
  }
}

///|
/// TestHandleHeartbeatResp: a heartbeat response from a follower that is behind
/// re-sends it AppendEntries; once it has acknowledged the log, further heartbeat
/// responses send nothing (raft_test.go:1206).
test "TestHandleHeartbeatResp" {
  let r = RaftNode::new("1", ["2"], pre_vote=false)
  r.node().append(1, b"a") |> ignore
  r.node().append(2, b"b") |> ignore
  r.node().append(3, b"c") |> ignore
  r.node().become_candidate()
  r.become_leader()
  r.node().advance_commit(r.node().last_log_index())
  // Behind follower: a heartbeat response prompts an append.
  let m1 = r.step(
    Message::new("2", "1", HeartbeatResp({ term: r.term(), context: b"" })),
  )
  assert_eq(m1.length(), 1)
  assert_true(m1[0].kind() == "Append" || m1[0].kind() == "Probe")
  // A second heartbeat response prompts another append.
  let m2 = r.step(
    Message::new("2", "1", HeartbeatResp({ term: r.term(), context: b"" })),
  )
  assert_eq(m2.length(), 1)
  // Once the follower has acknowledged the last index, heartbeats send nothing.
  r.step(ack_from("2", r.node().last_log_index(), r.term())) |> ignore
  let m3 = r.step(
    Message::new("2", "1", HeartbeatResp({ term: r.term(), context: b"" })),
  )
  assert_eq(m3.length(), 0)
}

///|
/// TestMsgAppRespWaitReset: after one follower's ack commits an entry, a new
/// proposal is broadcast only to followers not still in a probe wait; a lagging
/// follower gets the entry once its own ack releases it (raft_test.go:1268).
test "TestMsgAppRespWaitReset" {
  let r = RaftNode::new("1", ["2", "3"], pre_vote=false)
  r.node().become_candidate()
  r.become_leader() // no-op @1
  // Node 2 acks @1 → committed; node 3 has not answered (still probing).
  r.step(ack_from("2", 1, r.term())) |> ignore
  assert_eq(r.commit_index(), 1)
  // Propose @2: it goes to node 2 (streaming), not node 3 (still in probe wait).
  let msgs = r.propose(b"x")
  let mut to2 = 0
  let mut to3 = 0
  for m in msgs {
    if m.kind() == "Append" {
      if m.to == "2" {
        to2 = to2 + 1
      }
      if m.to == "3" {
        to3 = to3 + 1
      }
    }
  }
  assert_true(to2 >= 1)
  assert_eq(to3, 0)
  // Node 3 acks @1: this releases it, and it is sent entry @2.
  let out = r.step(ack_from("3", 1, r.term()))
  let mut got2 = false
  for m in out {
    if m.kind() == "Append" && m.to == "3" {
      got2 = true
    }
  }
  assert_true(got2)
}
