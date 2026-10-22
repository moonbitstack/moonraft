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

///|
/// etcd's `newPreVoteMigrationCluster`: n1 and n2 run pre-vote, n3 does not (yet).
/// n1 leads at term 2; n3 is then isolated, an entry is proposed to the majority,
/// and n3 times out twice into a stuck candidacy at term 4. Pre-vote is finally
/// enabled on n3 and the network healed.
fn pre_vote_migration_cluster() -> Net raise {
  let net = Net::new(["1", "2", "3"])
  net.base_term(1)
  net.node("1").set_pre_vote(true)
  net.node("2").set_pre_vote(true)
  // n3 intentionally left without pre-vote, simulating a mid-restart mixed cluster.
  net.campaign("1")
  net.isolate("3")
  net.propose("1", b"some data")
  // n3, isolated and without pre-vote, bumps its term on each timeout but can win
  // nothing, landing at term 4 as a candidate.
  net.campaign("3")
  net.campaign("3")

  // State: n1 leader (2), n2 follower (2), n3 candidate (4).
  assert_true(net.node("1").is_leader())
  assert_true(net.node("2").role() == Follower)
  assert_true(net.node("3").role() == Candidate)
  assert_eq(net.node("1").term(), 2)
  assert_eq(net.node("2").term(), 2)
  assert_eq(net.node("3").term(), 4)

  // Enable pre-vote on n3, then heal the network.
  net.node("3").set_pre_vote(true)
  net.recover()
  net
}

///|
/// TestPreVoteMigrationCanCompleteElection (raft_test.go:3645): with the leader
/// down, n2 and n3 campaign; the cluster still elects a leader once the pre-vote
/// migration has settled.
test "TestPreVoteMigrationCanCompleteElection" {
  let net = pre_vote_migration_cluster()

  // Simulate the leader going down.
  net.isolate("1")

  net.campaign("3")
  net.campaign("2")

  assert_true(net.node("2").role() == Follower)
  assert_true(net.node("3").role() == PreCandidate)

  net.campaign("3")
  net.campaign("2")

  // A leader emerges: n2 wins, or n3 has yielded to it.
  assert_true(net.node("2").is_leader() || net.node("3").role() == Follower)
}

///|
/// TestPreVoteMigrationWithFreeStuckPreCandidate (raft_test.go:3672): a stuck
/// pre-candidate at a higher term cannot win (its log is behind), but a heartbeat
/// from the leader carrying a lower term is answered with the higher term, which
/// frees the cluster by making the leader step down.
test "TestPreVoteMigrationWithFreeStuckPreCandidate" {
  let net = pre_vote_migration_cluster()

  net.campaign("3")
  assert_true(net.node("1").is_leader())
  assert_true(net.node("2").role() == Follower)
  assert_true(net.node("3").role() == PreCandidate)

  // Pre-vote again for safety; nothing changes.
  net.campaign("3")
  assert_true(net.node("1").is_leader())
  assert_true(net.node("2").role() == Follower)
  assert_true(net.node("3").role() == PreCandidate)

  // The leader heartbeats the stuck pre-candidate at its own (lower) term; the
  // pre-candidate answers with its higher term, disrupting the leader.
  let lead_term = net.node("1").term()
  net.inject(
    Message::new(
      "1",
      "3",
      Heartbeat({ term: lead_term, leader_id: "1", commit: 0, context: b"", }),
    ),
  )

  assert_true(net.node("1").role() == Follower)
  assert_eq(net.node("1").term(), net.node("3").term())
}
