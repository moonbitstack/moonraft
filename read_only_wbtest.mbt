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

// Ported from etcd-io/raft (Apache-2.0) raft_test.go:
// TestReadOnlyOptionSafe / TestReadOnlyOptionLease / TestReadOnlyForNewLeader.
// (`ack_from` is shared from confchange_apply_wbtest.mbt.)

///|
fn leader_with_committed_noop() -> RaftNode {
  let r = RaftNode::new("1", ["2", "3"], pre_vote=false)
  r.node().become_candidate()
  r.become_leader() // no-op @1
  // Commit the no-op so the commit index is from the current term (§5.4.2),
  // a precondition for serving a read.
  r.step(ack_from("2", 1, r.term())) |> ignore
  r
}

///|
test "ReadOnlySafe: a read is confirmed only by a fresh heartbeat quorum" {
  let r = leader_with_committed_noop()
  r.enable_read_only_safe()
  let msgs = r.request_read_index(b"read-1")
  // Safe mode broadcasts a heartbeat carrying the read context to each peer.
  assert_eq(msgs.length(), 2)
  for m in msgs {
    assert_true(m.kind() == "Heartbeat")
  }
  // Only the leader has acked so far — not yet a quorum.
  assert_eq(r.take_read_states().length(), 0)
  // A follower acks the heartbeat, echoing the context: now a quorum agrees.
  r.step(
    Message::new(
      "2",
      "1",
      HeartbeatResp({ term: r.term(), context: b"read-1" }),
    ),
  )
  |> ignore
  let rs = r.take_read_states()
  assert_eq(rs.length(), 1)
  assert_eq(rs[0].index, 1)
  assert_eq(rs[0].request_ctx, b"read-1")
  // Draining is idempotent.
  assert_eq(r.take_read_states().length(), 0)
}

///|
test "ReadOnlySafe: an ack for the latest context confirms earlier reads too" {
  let r = leader_with_committed_noop()
  r.enable_read_only_safe()
  r.request_read_index(b"r1") |> ignore
  r.request_read_index(b"r2") |> ignore
  // A quorum ack on the later context confirms both reads, in order.
  r.step(
    Message::new("2", "1", HeartbeatResp({ term: r.term(), context: b"r2" })),
  )
  |> ignore
  let rs = r.take_read_states()
  assert_eq(rs.length(), 2)
  assert_eq(rs[0].request_ctx, b"r1")
  assert_eq(rs[1].request_ctx, b"r2")
}

///|
test "ReadOnlyLeaseBased: a read is confirmed at once under a valid lease" {
  let r = leader_with_committed_noop() // node 2's ack marked it active
  r.set_read_only_option(LeaseBased)
  // Lease mode: no heartbeat round-trip is needed.
  let msgs = r.request_read_index(b"lease-read")
  assert_eq(msgs.length(), 0)
  let rs = r.take_read_states()
  assert_eq(rs.length(), 1)
  assert_eq(rs[0].index, 1)
}

///|
test "a follower serves no read" {
  let r = RaftNode::new("1", ["2", "3"], pre_vote=false)
  r.node().become_follower(1)
  assert_eq(r.request_read_index(b"x").length(), 0)
  assert_eq(r.take_read_states().length(), 0)
}

///|
test "a single-node leader serves a read immediately" {
  let r = RaftNode::new("1", [], pre_vote=false)
  r.campaign() |> ignore
  assert_eq(r.request_read_index(b"x").length(), 0)
  let rs = r.take_read_states()
  assert_eq(rs.length(), 1)
  assert_eq(rs[0].index, r.commit_index())
}
