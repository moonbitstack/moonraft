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

// Ports of raft_test.go's pre-vote migration cases (raft_test.go:3604): a
// mixed-version cluster where pre-vote is enabled replica by replica, as a
// rolling restart would. This needs a node's `pre_vote` to flip mid-run, which
// `set_pre_vote` now allows.

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
      Heartbeat({ term: lead_term, leader_id: "1", commit: 0, context: b"" }),
    ),
  )

  assert_true(net.node("1").role() == Follower)
  assert_eq(net.node("1").term(), net.node("3").term())
}
