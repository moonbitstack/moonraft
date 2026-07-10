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

// Routable client-request messages (etcd's local, term-0 messages) and their
// forwarding. Ported from etcd-io/raft (Apache-2.0):
//   raft_test.go:TestDisableProposalForwarding
//   node_test.go:TestNodeReadIndexToOldLeader, TestNodeProposeWaitDropped
//   testdata/forget_leader*.txt
// `ack_from` is shared from confchange_apply_wbtest.mbt.

///|
/// A follower that recognises leader 1 in term 1 (its election timer left at a
/// non-zero value so tests can check whether an operation resets it).
fn follower_of_leader_1(id : String, peers : Array[String]) -> RaftNode {
  let r = RaftNode::new(id, peers, pre_vote=false)
  r.node().become_follower(1)
  r.leader_id = Some("1")
  r
}

///|
/// TestDisableProposalForwarding: a follower forwards a client proposal to the
/// leader by default, but drops it when DisableProposalForwarding is set.
test "TestDisableProposalForwarding: follower forwards, disabled follower drops" {
  let entries = [Entry::normal(1, 0, b"testdata")]

  // r2 forwards (default): one message, addressed to the leader, still a Propose
  // carrying the original entries, with the origin preserved as `from`.
  let r2 = follower_of_leader_1("2", ["1", "3"])
  let out2 = r2.step(Message::new("2", "2", Propose(entries)))
  assert_eq(out2.length(), 1)
  assert_eq(out2[0].from, "2")
  assert_eq(out2[0].to, "1")
  match out2[0].payload {
    Propose(es) => assert_eq(es[0].command, b"testdata")
    _ => abort("expected a forwarded Propose")
  }

  // r3 drops it (forwarding disabled).
  let r3 = follower_of_leader_1("3", ["1", "2"])
  r3.disable_proposal_forwarding()
  let out3 = r3.step(Message::new("3", "3", Propose(entries)))
  assert_eq(out3.length(), 0)
}

///|
/// A follower that knows no leader has nowhere to forward a proposal, so it drops
/// it (etcd's ErrProposalDropped) rather than appending on a non-leader.
test "a follower with no leader drops a proposal" {
  let r = RaftNode::new("2", ["1", "3"], pre_vote=false)
  r.node().become_follower(1) // leader_id left None
  assert_eq(
    r
    .step(Message::new("2", "2", Propose([Entry::normal(1, 0, b"x")])))
    .length(),
    0,
  )
  assert_eq(r.node().last_log_index(), 0) // nothing appended
}

///|
/// TestNodeProposeWaitDropped (non-goroutine part): a candidate cannot serve a
/// proposal — it has no leader to forward to and must not append — so the
/// proposal is dropped and produces no messages, leaving the log untouched.
test "TestNodeProposeWaitDropped: a candidate drops a proposal" {
  let r = RaftNode::new("1", ["2", "3"], pre_vote=false)
  r.node().become_candidate()
  let before = r.node().last_log_index()
  let out = r.step(
    Message::new("1", "1", Propose([Entry::normal(1, 0, b"test_dropping")])),
  )
  assert_eq(out.length(), 0)
  assert_eq(r.node().last_log_index(), before)
}

///|
/// TestNodeReadIndexToOldLeader: a follower forwards a read-index request to the
/// leader it knows, preserving the origin as `from` and stamping no term, so the
/// answer routes back to the requester even across a leader change.
test "TestNodeReadIndexToOldLeader: read index forwards to the current leader" {
  let ctx = b"testdata"

  // r2 and r3 forward to leader 1, preserving their own ids as the origin.
  let r2 = follower_of_leader_1("2", ["1", "3"])
  let fwd2 = r2.step(Message::new("2", "2", ReadIndex(ctx)))
  assert_eq(fwd2.length(), 1)
  assert_eq(fwd2[0].from, "2")
  assert_eq(fwd2[0].to, "1")
  assert_eq(fwd2[0].term(), 0) // term not set on a forwarded read
  let r3 = follower_of_leader_1("3", ["1", "2"])
  let fwd3 = r3.step(Message::new("3", "3", ReadIndex(ctx)))
  assert_eq(fwd3[0].from, "3")
  assert_eq(fwd3[0].to, "1")

  // Leadership moves to 3; the old leader 1 is now a follower recognising 3. It
  // re-forwards the two messages it holds to the new leader, still preserving
  // each read's original origin.
  let r1 = RaftNode::new("1", ["2", "3"], pre_vote=false)
  r1.node().become_follower(2)
  r1.leader_id = Some("3")
  let again2 = r1.step(fwd2[0])
  assert_eq(again2.length(), 1)
  assert_eq(again2[0].from, "2") // origin preserved through the second hop
  assert_eq(again2[0].to, "3")
  let again3 = r1.step(fwd3[0])
  assert_eq(again3[0].from, "3")
  assert_eq(again3[0].to, "3")
}

///|
/// End-to-end read-index across a forward: a follower forwards a read to the
/// leader; the leader confirms its leadership with a heartbeat quorum and answers
/// the originator with a ReadIndexResp; the follower records the confirmed read
/// index. This exercises the whole MsgReadIndex/MsgReadIndexResp loop.
test "read index forwarded to the leader is answered to the requester" {
  let l = RaftNode::new("1", ["2", "3"], pre_vote=false)
  l.node().become_candidate()
  l.become_leader() // no-op @1
  l.step(ack_from("2", 1, l.term())) |> ignore // commit the no-op in this term
  l.enable_read_only_safe()

  // A read forwarded by follower 2 (to == leader 1) broadcasts confirming
  // heartbeats carrying the read context.
  let hbs = l.step(Message::new("2", "1", ReadIndex(b"rctx")))
  assert_eq(hbs.length(), 2)
  // The leader itself records nothing yet — the read belongs to follower 2.
  assert_eq(l.take_read_states().length(), 0)

  // A quorum ack (follower 2 echoing the context) confirms the read; the leader
  // answers the originator rather than recording it locally.
  let resp = l.step(
    Message::new("2", "1", HeartbeatResp({ term: l.term(), context: b"rctx" })),
  )
  assert_eq(l.take_read_states().length(), 0)
  assert_eq(resp.length(), 1)
  assert_eq(resp[0].to, "2")
  match resp[0].payload {
    ReadIndexResp(r) => {
      assert_eq(r.index, 1)
      assert_eq(r.context, b"rctx")
    }
    _ => abort("expected a ReadIndexResp to the requester")
  }

  // The requester records the confirmed read index for its caller.
  let f = follower_of_leader_1("2", ["1", "3"])
  f.step(resp[0]) |> ignore
  let rs = f.take_read_states()
  assert_eq(rs.length(), 1)
  assert_eq(rs[0].index, 1)
  assert_eq(rs[0].request_ctx, b"rctx")
}

///|
/// A follower forwards a leadership-transfer request to the leader, so a transfer
/// may be initiated from any server (etcd's MsgTransferLeader). The origin is
/// preserved and the target is carried in the payload.
test "MsgTransferLeader forwards from a follower to the leader" {
  let f = follower_of_leader_1("2", ["1", "3"])
  let out = f.request_transfer_leader("3")
  assert_eq(out.length(), 1)
  assert_eq(out[0].from, "2")
  assert_eq(out[0].to, "1")
  match out[0].payload {
    TransferLeader(target) => assert_eq(target, "3")
    _ => abort("expected a forwarded TransferLeader")
  }
}

///|
/// forget_leader.txt: a follower forgetting its leader clears `lead` without
/// moving its term or resetting its election timer; forget-leader on a candidate
/// (which recognises no leader) is a no-op.
test "forget_leader: a follower clears its leader without moving term or timer" {
  let n = follower_of_leader_1("2", ["1", "3"])
  n.enable_read_only_safe() // default etcd read-only mode
  n.election_elapsed = 3 // partway through the election timeout
  n.forget_leader()
  assert_true(n.leader() is None) // lead cleared
  assert_eq(n.term(), 1) // term unchanged
  assert_eq(n.election_elapsed, 3) // election timer NOT reset

  // A candidate has no current leader; forgetting is a no-op.
  let c = RaftNode::new("3", ["1", "2"], pre_vote=false)
  c.enable_read_only_safe()
  c.node().become_candidate()
  c.forget_leader()
  assert_true(c.role() == Candidate)
}

///|
/// forget_leader_prevote_checkquorum.txt (essence): a leased follower rejects a
/// pre-vote; once it forgets its leader the lease is gone, so it grants the
/// pre-vote — the mechanism that lets a healthy candidate win after a partition.
test "forget_leader lets a leased follower grant a pre-vote again" {
  let n = RaftNode::new("2", ["1", "3"])
  n.enable_check_quorum()
  n.enable_read_only_safe()
  n.node().become_follower(1)
  n.leader_id = Some("1")
  n.election_elapsed = 0 // within the lease
  let pv : Payload = PreVote({
    term: 2,
    candidate_id: "3",
    last_log_index: 0,
    last_log_term: 0,
  })
  // Leased: the pre-vote is refused.
  match n.step(Message::new("3", "2", pv))[0].payload {
    PreVoteResp(r) => assert_false(r.vote_granted)
    _ => abort("expected PreVoteResp")
  }
  // Forget the leader: the lease is gone.
  n.forget_leader()
  assert_true(n.leader() is None)
  match n.step(Message::new("3", "2", pv))[0].payload {
    PreVoteResp(r) => assert_true(r.vote_granted)
    _ => abort("expected PreVoteResp")
  }
}

///|
/// forget_leader_read_only_lease_based.txt: under lease-based reads, forget-leader
/// is ignored — dropping the leader would undermine the lease reads depend on.
test "forget_leader is ignored under lease-based reads" {
  let n = follower_of_leader_1("2", ["1", "3"])
  n.set_read_only_option(LeaseBased) // lease-based reads: forget-leader is ignored
  n.forget_leader()
  assert_true(n.leader() is Some(_)) // leader retained
  match n.leader() {
    Some(l) => assert_eq(l, "1")
    None => abort("leader should be retained")
  }
}

///|
/// A ReadIndexResp is a response that makes a round trip across terms, so a late
/// one from a superseded term must be dropped, not recorded — the same class of
/// bug as B1 (a stale ack must be discarded by term, never matched by content).
/// The centralized term classification in `step` drops it before it reaches the
/// handler; a current-term reply is still recorded.
test "a stale ReadIndexResp is dropped by term" {
  let n = RaftNode::new("2", ["1", "3"], pre_vote=false)
  n.node().become_follower(5)
  n.leader_id = Some("1")
  // A reply stamped with a superseded term (3 < 5) is ignored.
  n.step(
    Message::new(
      "1",
      "2",
      ReadIndexResp({ term: 3, index: 9, context: b"old" }),
    ),
  )
  |> ignore
  assert_eq(n.take_read_states().length(), 0)
  // A reply at our current term is recorded for the caller.
  n.step(
    Message::new(
      "1",
      "2",
      ReadIndexResp({ term: 5, index: 9, context: b"now" }),
    ),
  )
  |> ignore
  let rs = n.take_read_states()
  assert_eq(rs.length(), 1)
  assert_eq(rs[0].index, 9)
  assert_eq(rs[0].request_ctx, b"now")
}
