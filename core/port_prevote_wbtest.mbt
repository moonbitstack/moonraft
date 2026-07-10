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

// Ports of raft_test.go pre-vote and leader-transfer network cases. See
// GAP_core.md census.

///|
/// Campaign several nodes "at once": their first-round messages are seeded into a
/// single delivery queue and interleaved in FIFO order, reproducing etcd's batched
/// `nt.send([hup, hup]...)`. This matters for split votes, where delivering one
/// node's whole campaign before the other's would let the first win outright.
fn Net::campaign_both(self : Net, ids : Array[String]) -> Unit {
  let init : Array[Message] = []
  for id in ids {
    for m in self.node(id).campaign() {
      init.push(m)
    }
  }
  self.deliver(init)
}

///|
/// Bring every node to a common baseline term (etcd's `becomeFollower(1, None)`
/// before the network is built), so later election terms line up with etcd's.
fn Net::base_term(self : Net, term : UInt64) -> Unit {
  for _id, n in self.nodes {
    n.become_follower(term)
  }
}

///|
/// etcd's `checkLeaderTransferState`: after a transfer resolves, the observed node
/// is in `state`, follows `lead`, and holds no pending transferee.
fn check_transfer_state(
  r : RaftNode,
  is_leader : Bool,
  lead : String,
) -> Unit raise {
  if is_leader {
    assert_true(r.is_leader())
  } else {
    assert_true(r.role() == Follower)
  }
  assert_true(r.leader() == Some(lead))
  assert_true(r.lead_transferee == None)
}

///|
/// TestLeaderTransferReceiveHigherTermVote: a pending transfer to an isolated
/// target is abandoned when another node wins an election at a higher term
/// (raft_test.go:3263).
test "TestLeaderTransferReceiveHigherTermVote" {
  let net = Net::new(["1", "2", "3"])
  net.campaign("1")
  net.isolate("3")
  // Transfer to the isolated node: it cannot catch up, so the transfer pends.
  net.transfer("3", "1")
  assert_true(net.node("1").lead_transferee == Some("3"))
  // Node 2 campaigns and wins at a higher term; the leader steps down and the
  // transfer is aborted.
  net.campaign("2")
  check_transfer_state(net.node("1"), false, "2")
}

///|
/// TestNodeWithSmallerTermCanCompleteElection: a partitioned pre-candidate that
/// fell behind on term still lets the connected majority elect a leader, and on
/// recovery a leader emerges (raft_test.go:3414).
test "TestNodeWithSmallerTermCanCompleteElection" {
  let net = Net::new(["1", "2", "3"], pre_vote=true)
  net.base_term(1)
  net.cut("1", "3")
  net.cut("2", "3")
  net.campaign("1")
  assert_true(net.node("1").is_leader())
  assert_true(net.node("2").role() == Follower)
  // The isolated node becomes a pre-candidate but cannot advance its term.
  net.campaign("3")
  assert_true(net.node("3").role() == PreCandidate)
  // Node 2 campaigns; it displaces the leader at a higher term.
  net.campaign("2")
  assert_eq(net.node("1").term(), 3)
  assert_eq(net.node("2").term(), 3)
  assert_eq(net.node("3").term(), 1)
  assert_true(net.node("1").role() == Follower)
  assert_true(net.node("2").is_leader())
  assert_true(net.node("3").role() == PreCandidate)
  // Kill the current leader (node 2) and let the rest elect: a leader emerges.
  net.recover()
  net.cut("2", "1")
  net.cut("2", "3")
  net.campaign("3")
  net.campaign("1")
  assert_true(net.node("1").is_leader() || net.node("3").is_leader())
}

///|
/// TestPreVoteWithSplitVote: after a split vote both nodes become candidates at
/// the same term, and a further campaign by one completes the election in the
/// next round (raft_test.go:3481).
test "TestPreVoteWithSplitVote" {
  let net = Net::new(["1", "2", "3"], pre_vote=true)
  net.base_term(1)
  net.campaign("1")
  // The leader goes down; 2 and 3 start campaigning at the same time.
  net.isolate("1")
  net.campaign_both(["2", "3"])
  assert_eq(net.node("2").term(), 3)
  assert_eq(net.node("3").term(), 3)
  assert_true(net.node("2").role() == Candidate)
  assert_true(net.node("3").role() == Candidate)
  // Node 2 times out first and wins the next round.
  net.campaign("2")
  assert_eq(net.node("2").term(), 4)
  assert_eq(net.node("3").term(), 4)
  assert_true(net.node("2").is_leader())
  assert_true(net.node("3").role() == Follower)
}

///|
/// TestPreVoteWithCheckQuorum: with pre-vote and check-quorum both on, an isolated
/// leader's followers still hold its lease, so a challenger cannot immediately win;
/// once a challenger sheds its own lease another can complete the election
/// (raft_test.go:3534).
test "TestPreVoteWithCheckQuorum" {
  let net = Net::new(["1", "2", "3"], pre_vote=true)
  net.base_term(1)
  for id in ["1", "2", "3"] {
    net.node(id).enable_check_quorum()
  }
  net.campaign("1")
  net.isolate("1")
  assert_true(net.node("1").is_leader())
  assert_true(net.node("2").role() == Follower)
  assert_true(net.node("3").role() == Follower)
  // Node 2 ignores node 3's pre-vote while it still leases to 1; then node 2
  // campaigns and, node 3 having shed its lease, completes the election.
  net.campaign("3")
  net.campaign("2")
  assert_true(net.node("2").is_leader() || net.node("3").role() == Follower)
}
