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

///|
/// TestStateTransition (raft_test.go:1395): the reachable rows, checking the
/// resulting state and term. `PreCandidate` is now a first-class Role reported
/// through SoftState (etcd's StatePreCandidate), so the pre-vote transition is
/// observable. The invalid transitions etcd guards with a panic
/// (Follower→Leader, Leader→PreCandidate, Leader→Candidate) are not reproduced —
/// this port does not install those panic guards (N/A-wpanic).
test "TestStateTransition: reportable states and their terms" {
  // Initial state is Follower.
  let r = RaftNode::new("1", ["2", "3"], pre_vote=true)
  assert_true(r.role() == Follower)
  // Follower → PreCandidate (pre-vote probe): term is NOT advanced yet.
  r.campaign() |> ignore
  assert_true(r.role() == PreCandidate)
  assert_eq(r.term(), 0)
  // PreCandidate → Candidate: the real campaign advances the term.
  r.become_candidate()
  assert_true(r.role() == Candidate)
  assert_eq(r.term(), 1)
  // Candidate → Leader.
  r.become_leader()
  assert_true(r.role() == Leader)
  // Leader → Follower at a given term.
  r.become_follower(5)
  assert_true(r.role() == Follower)
  assert_eq(r.term(), 5)
}

///|
/// PreCandidate is reported through SoftState (etcd's SoftState.RaftState), not
/// hidden as an internal flag — a monitoring caller sees the pre-vote phase.
test "PreCandidate is reported through SoftState" {
  let r = RaftNode::new("1", ["2", "3"], pre_vote=true)
  assert_true(r.soft_state().state == Follower)
  r.campaign() |> ignore
  assert_true(r.soft_state().state == PreCandidate)
  // Winning the pre-vote and becoming a real candidate flips the reported state.
  r.become_candidate()
  assert_true(r.soft_state().state == Candidate)
}

///|
/// PreCandidate is now the genuine core state during pre-vote (etcd's
/// becomePreCandidate), not a flag derived at the reporting boundary — the core
/// Node itself reports it. Before the wiring the core stayed Follower.
test "PreCandidate is the real core state during pre-vote" {
  let r = RaftNode::new("1", ["2", "3"], pre_vote=true)
  r.campaign() |> ignore
  assert_true(r.node().role() == PreCandidate)
  // The pre-candidate has forgotten its leader (etcd's becomePreCandidate lead=None)
  // and, like a candidate, drops a client proposal rather than forwarding it.
  assert_true(r.leader() is None)
  assert_eq(r.propose(b"x").length(), 0)
}
