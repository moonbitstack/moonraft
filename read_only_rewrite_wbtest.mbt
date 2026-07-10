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

///|
/// The HeartbeatResp follower `from` sends to leader `to`, echoing the leader's
/// current read-confirmation position (etcd echoes the heartbeat Context).
fn hb_ack(from : String, to : String, leader : RaftNode) -> Message {
  Message::new(
    from,
    to,
    HeartbeatResp({
      term: leader.term(),
      context: leader.read_only.heartbeat_ctx(),
    }),
  )
}

///|
/// DIVERGENT fix (ReadOnly ack-coverage): the port originally copied etcd's
/// *pre-refactor* read-only design, keying the heartbeat ack set on the caller's
/// context bytes. Two reads sharing a context collided — the second request
/// overwrote the first's ack set and, once one confirmed, the other could never
/// confirm. etcd's current design carries an internal position counter, so one
/// quorum round confirms every pending read. Red (pre-fix): only 1 of the 2
/// shared-context reads confirmed; green: both.
test "ReadOnly: two reads sharing a context both confirm on one quorum round" {
  let r = cov_leader()
  r.set_read_only_option(Safe)
  r.step(ack_from("2", 1, r.term())) |> ignore // commit the no-op @1
  r.request_read_index(b"same") |> ignore
  r.request_read_index(b"same") |> ignore
  // One follower ack (self + node 2 of 3) confirms the position, releasing both.
  r.step(hb_ack("2", "1", r)) |> ignore
  assert_eq(r.take_read_states().length(), 2)
}

///|
/// A single read confirms on a quorum heartbeat, at the leader's commit index.
test "ReadOnly: a single read confirms at the commit index" {
  let r = cov_leader()
  r.set_read_only_option(Safe)
  r.step(ack_from("2", 1, r.term())) |> ignore
  r.request_read_index(b"rc") |> ignore
  r.step(hb_ack("2", "1", r)) |> ignore
  let rs = r.take_read_states()
  assert_eq(rs.length(), 1)
  assert_eq(rs[0].index, 1)
}

///|
/// Several distinct reads are all confirmed by one quorum round (batching): etcd
/// carries the latest position, so N pending reads need one round, not N.
test "ReadOnly: many pending reads confirm in one round" {
  let r = cov_leader()
  r.set_read_only_option(Safe)
  r.step(ack_from("2", 1, r.term())) |> ignore
  r.request_read_index(b"a") |> ignore
  r.request_read_index(b"b") |> ignore
  r.request_read_index(b"c") |> ignore
  r.step(hb_ack("2", "1", r)) |> ignore
  assert_eq(r.take_read_states().length(), 3)
}

///|
/// A read ack short of a quorum leaves the reads unconfirmed (5-node cluster,
/// one follower ack: self + one is two of five, not a majority).
test "ReadOnly: an ack short of a quorum confirms nothing" {
  let r = RaftNode::new("1", ["2", "3", "4", "5"], pre_vote=false)
  r.node().become_candidate()
  r.become_leader()
  r.set_read_only_option(Safe)
  r.step(ack_from("2", 1, r.term())) |> ignore
  r.step(ack_from("3", 1, r.term())) |> ignore // commit @1
  r.request_read_index(b"rc") |> ignore
  r.step(hb_ack("2", "1", r)) |> ignore
  assert_eq(r.take_read_states().length(), 0)
}

///|
/// heartbeat_ctx is empty when no read is pending, and an 8-byte little-endian
/// encoding of the position otherwise; recv_ack round-trips it.
test "ReadOnly: heartbeat_ctx encodes the pending-read position" {
  let r = cov_leader()
  r.set_read_only_option(Safe)
  r.step(ack_from("2", 1, r.term())) |> ignore
  assert_eq(r.read_only.heartbeat_ctx().length(), 0) // nothing pending
  r.request_read_index(b"x") |> ignore
  let ctx = r.read_only.heartbeat_ctx()
  assert_eq(ctx.length(), 8) // 8-byte position
}

///|
/// A queued read (before the first in-term commit) is released in ReadOnlySafe
/// by broadcasting confirming heartbeats when the commit lands.
test "ReadOnly: a queued read is released by heartbeats on the first commit" {
  let r = RaftNode::new("1", ["2", "3"], pre_vote=false) // Safe default
  r.node().become_candidate()
  r.become_leader()
  r.request_read_index(b"rc") |> ignore // queued, no in-term commit yet
  let out = r.step(ack_from("2", 1, r.term())) // commit → release
  let mut hb = 0
  for m in out {
    if m.kind() == "Heartbeat" {
      hb = hb + 1
    }
  }
  assert_true(hb > 0)
}

///|
/// Lease-based read with no active quorum serves nothing.
test "ReadOnly: lease read without an active quorum returns nothing" {
  let r = cov_leader()
  r.set_read_only_option(LeaseBased)
  r.step(ack_from("2", 1, r.term())) |> ignore
  r.reset_recent_active()
  assert_eq(r.request_read_index(b"rc").length(), 0)
  assert_eq(r.take_read_states().length(), 0)
}

///|
/// read_index() returns None before an in-term commit and again once committed
/// but without an active quorum.
test "ReadOnly: read_index None before in-term commit and without a quorum" {
  let r = cov_leader()
  assert_true(r.read_index() is None)
  r.step(ack_from("2", 1, r.term())) |> ignore
  r.reset_recent_active()
  assert_true(r.read_index() is None)
}
