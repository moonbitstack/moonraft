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

// Ports of raft_test.go network election cases. See GAP_core.md census.

///|
/// TestDuelingCandidates: with the link between 1 and 3 cut, 1 wins term 1 while
/// 3 stalls as a candidate (rejected by 2, which has already voted). After the
/// partition heals, 3's fresh campaign at a higher term is rejected by both 1 and
/// 2 because its log is too short, so all three end as followers of the highest
/// term reached (raft_test.go:782).
test "TestDuelingCandidates" {
  let net = Net::new(["1", "2", "3"])
  net.cut("1", "3")
  net.campaign("1")
  net.campaign("3")
  assert_true(net.node("1").is_leader())
  // 3 got its own vote and a rejection from 2: still a candidate.
  assert_true(net.node("3").role() == Candidate)
  net.recover()
  // 3 raises its term and tries again; a quorum with longer logs rejects it.
  net.campaign("3")
  for tc in [("1", 2UL, 1UL), ("2", 2UL, 1UL), ("3", 2UL, 0UL)] {
    let (id, term, last) = tc
    assert_true(net.node(id).role() == Follower)
    assert_eq(net.node(id).term(), term)
    assert_eq(net.node(id).node().last_log_index(), last)
  }
}

///|
/// TestDuelingPreCandidates: the same partition, but with pre-vote enabled the
/// stuck node reverts to follower without a rejected pre-vote inflating anyone's
/// term, so the incumbent leader at term 1 is never disrupted (raft_test.go:830).
test "TestDuelingPreCandidates" {
  let net = Net::new(["1", "2", "3"], pre_vote=true)
  net.cut("1", "3")
  net.campaign("1")
  net.campaign("3")
  assert_true(net.node("1").is_leader())
  // 3's pre-vote is rejected; it never left follower for real.
  assert_true(net.node("3").role() == Follower)
  net.recover()
  net.campaign("3")
  for
    tc in [
      ("1", true, 1UL, 1UL),
      ("2", false, 1UL, 1UL),
      ("3", false, 1UL, 0UL),
    ] {
    let (id, leader, term, last) = tc
    if leader {
      assert_true(net.node(id).is_leader())
    } else {
      assert_true(net.node(id).role() == Follower)
    }
    assert_eq(net.node(id).term(), term)
    assert_eq(net.node(id).node().last_log_index(), last)
  }
}

///|
/// A candidate that receives a MsgHeartbeat or MsgApp from a leader at a higher
/// term resets its term to the leader's and reverts to follower
/// (raft_test.go:1500, testCandidateResetTerm). `mt` chooses the disrupting
/// message kind.
fn candidate_reset_term(use_append : Bool) -> Unit raise {
  let net = Net::new(["1", "2", "3"])
  net.campaign("1")
  assert_true(net.node("1").is_leader())
  assert_true(net.node("2").role() == Follower)
  assert_true(net.node("3").role() == Follower)
  // Isolate 3, then raise the term among the rest by two elections.
  net.isolate("3")
  net.campaign("2")
  net.campaign("1")
  assert_true(net.node("1").is_leader())
  assert_true(net.node("2").role() == Follower)
  // The isolated node times out into a candidacy.
  net.node("3").randomized_election_timeout = 10
  tick_n(net.node("3"), 10)
  assert_true(net.node("3").role() == Candidate)
  net.recover()
  // The leader reaches the candidate; its higher term forces it back to follower.
  let leader_term = net.node("1").term()
  let disrupt = if use_append {
    Message::new(
      "1",
      "3",
      Append({
        term: leader_term,
        leader_id: "1",
        prev_log_index: 0,
        prev_log_term: 0,
        entries: [],
        leader_commit: 0,
      }),
    )
  } else {
    Message::new(
      "1",
      "3",
      Heartbeat({ term: leader_term, leader_id: "1", commit: 0, context: b"" }),
    )
  }
  net.inject(disrupt)
  assert_true(net.node("3").role() == Follower)
  assert_eq(net.node("3").term(), leader_term)
}

///|
/// TestCandidateResetTermMsgHeartbeat (raft_test.go:1500).
test "TestCandidateResetTermMsgHeartbeat" {
  candidate_reset_term(false)
}

///|
/// TestCandidateResetTermMsgApp (raft_test.go:1504).
test "TestCandidateResetTermMsgApp" {
  candidate_reset_term(true)
}
