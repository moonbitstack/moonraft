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

// Ports of raft_test.go cases not previously covered by name-and-assertion.
// See GAP_core.md for the census. `ack_from` / `vote_resp` are shared helpers.

///|
/// TestLearnerCanVote: a learner is non-voting for the purpose of *campaigning*,
/// but it still grants its vote when a candidate solicits it (raft_test.go:442).
test "TestLearnerCanVote" {
  let n2 = RaftNode::new("2", ["1"], pre_vote=false)
  // Make this node a learner of the configuration {voter:1, learner:2}.
  n2.config.members.retain(fn(m) { m != "2" })
  n2.config.add_learner("2")
  n2.become_follower(1)
  let out = n2.step(
    Message::new(
      "1",
      "2",
      Vote({ term: 2, candidate_id: "1", last_log_index: 11, last_log_term: 11 }),
    ),
  )
  assert_eq(out.length(), 1)
  assert_true(vote_granted(out))
}

///|
/// TestRecvMsgUnreachable: reporting a streaming (Replicate) follower unreachable
/// drops it back to Probe, resuming from just past its match point
/// (raft_test.go:2479).
test "TestRecvMsgUnreachable" {
  let r = RaftNode::new("1", ["2"], pre_vote=false)
  r.node().become_candidate()
  r.become_leader()
  let p = r.progress["2"]
  p.maybe_update(3) |> ignore
  p.become_replicate()
  p.next_index = 6
  r.report_unreachable("2")
  assert_true(p.state == Probe)
  assert_eq(p.next_index, p.match_index + 1)
}

///|
/// TestCampaignWhileLeader: a leader that is told to campaign again ignores it —
/// it stays leader and does not bump the term (raft_test.go:2975). Before the
/// fix this stepped the leader down to candidate.
test "TestCampaignWhileLeader" {
  let r = RaftNode::new("1", [], pre_vote=false)
  r.campaign() |> ignore
  assert_true(r.is_leader())
  let term = r.term()
  r.campaign() |> ignore
  assert_true(r.is_leader())
  assert_eq(r.term(), term)
}

///|
/// TestPreCampaignWhileLeader: the same, with pre-vote enabled.
test "TestPreCampaignWhileLeader" {
  let r = RaftNode::new("1", [], pre_vote=true)
  r.campaign() |> ignore
  assert_true(r.is_leader())
  let term = r.term()
  r.campaign() |> ignore
  assert_true(r.is_leader())
  assert_eq(r.term(), term)
}

///|
/// TestLeaderTransferIgnoreProposal: while a leadership transfer is pending the
/// leader stops accepting proposals so the target can catch up to a fixed log
/// (raft_test.go:3240).
test "TestLeaderTransferIgnoreProposal" {
  let r = RaftNode::new("1", ["2", "3"], pre_vote=false)
  r.node().become_candidate()
  r.become_leader()
  // Node 3 lags, so the transfer stays pending (no TimeoutNow sent yet).
  r.transfer_leadership("3") |> ignore
  assert_true(r.lead_transferee == Some("3"))
  let last = r.node().last_log_index()
  // A proposal during the pending transfer is dropped: the log does not grow.
  r.propose(b"data") |> ignore
  assert_eq(r.node().last_log_index(), last)
}

///|
/// TestLeaderTransferBack: transferring to a lagging node leaves the transfer
/// pending; transferring back to self then cancels it, and the leader stays
/// leader (raft_test.go:3328). Before the fix the pending transferee was not
/// cleared by a transfer to self.
test "TestLeaderTransferBack" {
  let r = RaftNode::new("1", ["2", "3"], pre_vote=false)
  r.node().become_candidate()
  r.become_leader()
  r.transfer_leadership("3") |> ignore
  assert_true(r.lead_transferee == Some("3"))
  // Transfer back to self: cancels the handoff, stays leader.
  r.transfer_leadership("1") |> ignore
  assert_true(r.is_leader())
  assert_true(r.lead_transferee == None)
}

///|
/// TestLeaderTransferSecondTransferToSameNode: a repeat request to the same
/// pending target is ignored — the handoff simply continues (raft_test.go:3366).
test "TestLeaderTransferSecondTransferToSameNode" {
  let r = RaftNode::new("1", ["2", "3"], pre_vote=false)
  r.node().become_candidate()
  r.become_leader()
  r.transfer_leadership("3") |> ignore
  r.election_elapsed = 5
  // Same target again: ignored (no reset of the transfer bookkeeping).
  let out = r.transfer_leadership("3")
  assert_eq(out.length(), 0)
  assert_true(r.lead_transferee == Some("3"))
}
