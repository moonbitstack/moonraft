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

// Ports of raft_test.go check-quorum cases. See GAP_core.md census. etcd's
// `setRandomizedElectionTimeout` is a direct write to `randomized_election_timeout`
// here; ticking a follower `election_timeout` times drives its `election_elapsed`
// to the lease boundary without campaigning, so its lease to the current leader
// expires (etcd's `in_lease`: check_quorum && leader && election_elapsed <
// election_timeout).

///|
/// Tick a node `n` times, discarding whatever it emits; used to expire a
/// follower's lease deterministically.
fn tick_n(r : RaftNode, n : Int) -> Unit {
  for _i in 0..<n {
    r.tick() |> ignore
  }
}

///|
/// TestLeaderStepdownWhenQuorumActive: a check-quorum leader that keeps hearing
/// from a majority never steps itself down, even past an election timeout
/// (raft_test.go:1646).
test "TestLeaderStepdownWhenQuorumActive" {
  let r = RaftNode::new(
    "1",
    ["2", "3"],
    pre_vote=false,
    check_quorum=true,
    election_timeout=5,
  )
  r.node().become_candidate()
  r.become_leader()
  for _i in 0..<(5 + 1) {
    r.step(
      Message::new("2", "1", HeartbeatResp({ term: r.term(), context: b"" })),
    )
    |> ignore
    r.tick() |> ignore
  }
  assert_true(r.is_leader())
}

///|
/// TestLeaderStepdownWhenQuorumLost: a check-quorum leader that hears from no one
/// steps down after an election timeout (raft_test.go:1662). The lost-quorum half
/// of the pair above.
test "TestLeaderStepdownWhenQuorumLost" {
  let r = RaftNode::new(
    "1",
    ["2", "3"],
    pre_vote=false,
    check_quorum=true,
    election_timeout=5,
  )
  r.node().become_candidate()
  r.become_leader()
  tick_n(r, 5 + 1)
  assert_true(r.role() == Follower)
}

///|
/// TestLeaderSupersedingWithCheckQuorum: a challenger cannot win while a follower
/// still holds a lease to the incumbent, but succeeds once that follower's lease
/// has expired (raft_test.go:1677).
test "TestLeaderSupersedingWithCheckQuorum" {
  let net = Net::new(["1", "2", "3"])
  for id in ["1", "2", "3"] {
    net.node(id).enable_check_quorum()
  }
  net.node("2").randomized_election_timeout = 11
  tick_n(net.node("2"), 10)
  net.campaign("1")
  assert_true(net.node("1").is_leader())
  assert_true(net.node("3").role() == Follower)
  // b (node 2) is still leased to leader 1, so it rejects 3's vote: 3 stalls.
  net.campaign("3")
  assert_true(net.node("3").role() == Candidate)
  // Expire b's lease; 3 now gathers b's vote and wins.
  tick_n(net.node("2"), 10)
  net.campaign("3")
  assert_true(net.node("3").is_leader())
}

///|
/// TestLeaderElectionWithCheckQuorum: right after creation votes are cast
/// regardless of the election timeout, so node 1 wins; once 1's followers hold a
/// lease, a later challenger displaces it only after their leases lapse
/// (raft_test.go:1711).
test "TestLeaderElectionWithCheckQuorum" {
  let net = Net::new(["1", "2", "3"])
  for id in ["1", "2", "3"] {
    net.node(id).enable_check_quorum()
  }
  net.node("1").randomized_election_timeout = 11
  net.node("2").randomized_election_timeout = 12
  // Immediately after creation, no node holds a lease, so 1 is elected.
  net.campaign("1")
  assert_true(net.node("1").is_leader())
  assert_true(net.node("3").role() == Follower)
  // Expire the leases of 1 and 2, then let 3 campaign: 1 steps down, 3 wins.
  net.node("1").randomized_election_timeout = 11
  net.node("2").randomized_election_timeout = 12
  tick_n(net.node("1"), 10)
  tick_n(net.node("2"), 10)
  net.campaign("3")
  assert_true(net.node("1").role() == Follower)
  assert_true(net.node("3").is_leader())
}

///|
/// TestFreeStuckCandidateWithCheckQuorum: a candidate stuck at a higher term
/// (having disrupted no one because the leader was isolated) is freed when the
/// isolated leader rejoins: the leader's heartbeat reveals the higher term, the
/// leader steps down, and the candidate then wins (raft_test.go:1750).
test "TestFreeStuckCandidateWithCheckQuorum" {
  let net = Net::new(["1", "2", "3"])
  for id in ["1", "2", "3"] {
    net.node(id).enable_check_quorum()
  }
  net.node("2").randomized_election_timeout = 11
  tick_n(net.node("2"), 10)
  net.campaign("1")
  // Isolate the leader; 3 campaigns. b (node 2) has lost contact with 1 but its
  // lease has not expired, so it rejects 3 — 3 becomes a stuck candidate whose
  // term keeps climbing.
  net.isolate("1")
  net.campaign("3")
  assert_true(net.node("2").role() == Follower)
  assert_true(net.node("3").role() == Candidate)
  assert_eq(net.node("3").term(), net.node("2").term() + 1)
  net.campaign("3")
  assert_true(net.node("2").role() == Follower)
  assert_true(net.node("3").role() == Candidate)
  assert_eq(net.node("3").term(), net.node("2").term() + 2)
  // The leader rejoins and sends a heartbeat to 3; the higher term forces the
  // leader to step down, freeing the stuck candidate.
  net.recover()
  net.inject(
    Message::new(
      "1",
      "3",
      Heartbeat({
        term: net.node("1").term(),
        leader_id: "1",
        commit: 0,
        context: b"",
      }),
    ),
  )
  assert_true(net.node("1").role() == Follower)
  assert_eq(net.node("1").term(), net.node("3").term())
  // 3 now wins.
  net.campaign("3")
  assert_true(net.node("3").is_leader())
}

///|
/// TestNonPromotableVoterWithCheckQuorum: a node removed from the configuration
/// is not promotable, yet still follows a leader elected by the remaining voter
/// (raft_test.go:1794).
test "TestNonPromotableVoterWithCheckQuorum" {
  let net = Net::new(["1", "2"])
  for id in ["1", "2"] {
    net.node(id).enable_check_quorum()
  }
  // Remove node 2 from its own view of the configuration: it is now a
  // non-promotable voter.
  net.node("2").config.members.retain(fn(m) { m != "2" })
  assert_false(net.node("2").promotable())
  net.node("2").randomized_election_timeout = 11
  tick_n(net.node("2"), 10)
  net.campaign("1")
  assert_true(net.node("1").is_leader())
  assert_true(net.node("2").role() == Follower)
  assert_true(net.node("2").leader() == Some("1"))
}
