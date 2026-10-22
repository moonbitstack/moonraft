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
      HeartbeatResp({ term: r.term(), context: b"read-1", }),
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
    Message::new("2", "1", HeartbeatResp({ term: r.term(), context: b"r2", })),
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

///|
test "TestReadOnlyDuplicateRequest: a duplicated, delayed read is never stale" {
  let net = Net::new(["1", "2", "3"]) // ReadOnlySafe by default

  // A hook that delays (saves, does not re-deliver) only heartbeat responses, so
  // a read's confirmation can be held back and replayed after a leader change.
  let delayed : Array[Message] = []
  let delay_heartbeat_resps = fn(m : Message) -> Bool {
    if m.kind() == "HeartbeatResp" {
      delayed.push(m)
      false
    } else {
      true
    }
  }

  // Elect node 1 leader; its no-op commits so it can serve reads.
  net.campaign("1")
  assert_true(net.node("1").is_leader())

  // readIndexMsgA, forwarded by follower 2; its confirmed index must be at least
  // the leader's committed index now.
  let read_index_a = Message::new("2", "1", ReadIndex(b"A"))
  let read_index_minimum_a = net.node("1").commit_index()

  // Explicitly duplicate readIndexMsgA for later replay — as the network, or a
  // client retrying ReadIndex(), would.
  delayed.push(read_index_a)

  // Process the read, but hold its confirming heartbeat responses back.
  net.msg_hook = Some(delay_heartbeat_resps)
  net.inject(read_index_a)
  net.msg_hook = None

  // A heartbeat round now finishes processing readIndexA at the correct index.
  net.beat("1")

  net.isolate("1")

  // Elect node 2 leader and commit a new entry, advancing its committed index.
  net.campaign("2")
  net.propose("2", b"someOp")
  let read_index_minimum_b = net.node("2").commit_index()

  // Read from the disconnected, stale leader r1.
  let read_index_b = Message::new("1", "1", ReadIndex(b"B"))
  net.inject(read_index_b)

  net.recover()
  // Replay the delayed readIndexA duplicate and the delayed heartbeats; this must
  // not confirm B against the stale leader.
  net.deliver(delayed)

  // No confirmed read index may be smaller than the commit index it was issued
  // against — the linearizability property.
  let mut confirmed_a = 0
  for id in ["1", "2", "3"] {
    for rs in net.node(id).take_read_states() {
      if rs.request_ctx == b"A" {
        confirmed_a = confirmed_a + 1
        assert_true(rs.index >= read_index_minimum_a)
      } else if rs.request_ctx == b"B" {
        assert_true(rs.index >= read_index_minimum_b)
      }
    }
  }
  // Non-vacuity: read A is actually confirmed (its heartbeat round completed);
  // the linearizability check above is therefore exercised, not merely empty.
  assert_true(confirmed_a >= 1)
}

///|
/// A single voter plus a learner is a *singleton* for read purposes (etcd's
/// `trk.IsSingleton`, which counts voters only): the read is answered at once at
/// the commit index, in ReadOnlySafe mode, with no heartbeat round-trip and no
/// dependence on the learner acknowledging anything.
test "ReadOnlyWithLearner: a lone voter answers reads without the learner" {
  let r = RaftNode::new("1", [], pre_vote=false) // Safe by default
  r.campaign() |> ignore // sole voter -> leader, no-op committed @1
  r.propose_conf(ConfChange::add_learner("2")) |> ignore // learner committed @2
  assert_true(r.config.is_learner("2"))
  assert_false(r.config.contains("2")) // not a voter
  assert_eq(r.config.size(), 1) // quorum is still the single voter
  assert_true(r.peers.contains("2")) // learner is replicated to
  r.enable_read_only_safe() // etcd's default; assert it explicitly

  // The learner has acked nothing, yet the read is served at the commit index.
  r.request_read_index(b"ctx1") |> ignore
  let rs = r.take_read_states()
  assert_eq(rs.length(), 1)
  assert_eq(rs[0].request_ctx, b"ctx1")
  assert_eq(rs[0].index, r.commit_index())
}

///|
/// The served read index tracks the leader's commit index as it advances under
/// the single voter — proposals commit without the learner, and each read
/// reflects the latest committed index (upstream drives ten proposals before
/// each read and checks the returned index climbs).
test "ReadOnlyWithLearner: read index tracks the single voter's commit" {
  let r = RaftNode::new("1", [], pre_vote=false)
  r.campaign() |> ignore
  r.propose_conf(ConfChange::add_learner("2")) |> ignore
  r.enable_read_only_safe()
  let mut last = r.commit_index()
  for _round in 0..<4 {
    for _j in 0..<3 {
      r.propose(b"x") |> ignore // commits under the sole voter
    }
    let ci = r.commit_index()
    assert_true(ci > last)
    r.request_read_index(b"ctx") |> ignore
    let rs = r.take_read_states()
    assert_eq(rs.length(), 1)
    assert_eq(rs[0].index, ci)
    last = ci
  }
}

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
