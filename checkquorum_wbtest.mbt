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

// Tests for B3: with check-quorum on, a server that is still in contact with a
// leader rejects disruptive (pre)vote solicitations without moving its term.
// Adapted from etcd-io/raft (Apache-2.0) raft_test.go:
// TestLeaderSupersedingWithCheckQuorum / TestDisruptiveFollower.

///|
fn leased_follower() -> RaftNode {
  let r = RaftNode::new("2", ["1", "3"], pre_vote=false)
  r.enable_check_quorum()
  r.node().become_follower(5)
  r.leader_id = Some("1") // recognises leader 1
  r.election_elapsed = 0 // fresh contact: within the lease
  r
}

///|
test "B3: leased follower rejects a higher-term vote without bumping its term" {
  let r = leased_follower()
  let out = r.step(
    Message::new(
      "3",
      "2",
      Vote({ term: 6, candidate_id: "3", last_log_index: 0, last_log_term: 0 }),
    ),
  )
  assert_eq(out.length(), 1)
  match out[0].payload {
    VoteResp(reply) => {
      assert_false(reply.vote_granted)
      assert_eq(reply.term, 5) // term NOT adopted
    }
    _ => abort("expected VoteResp")
  }
  assert_eq(r.term(), 5)
  assert_true(r.role() == Follower)
}

///|
test "B3: leased follower rejects a pre-vote" {
  let r = leased_follower()
  let out = r.step(
    Message::new(
      "3",
      "2",
      PreVote({
        term: 6,
        candidate_id: "3",
        last_log_index: 0,
        last_log_term: 0,
      }),
    ),
  )
  assert_eq(out.length(), 1)
  match out[0].payload {
    PreVoteResp(reply) => assert_false(reply.vote_granted)
    _ => abort("expected PreVoteResp")
  }
  assert_eq(r.term(), 5)
}

///|
test "B3: once the lease expires the vote is granted again" {
  let r = leased_follower()
  r.election_elapsed = r.election_timeout // lease has lapsed
  let out = r.step(
    Message::new(
      "3",
      "2",
      Vote({ term: 6, candidate_id: "3", last_log_index: 0, last_log_term: 0 }),
    ),
  )
  match out[0].payload {
    VoteResp(reply) => assert_true(reply.vote_granted)
    _ => abort("expected VoteResp")
  }
  assert_eq(r.term(), 6)
}
