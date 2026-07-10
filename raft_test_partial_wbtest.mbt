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

// Completing raft_test.go cases previously only PARTIAL. See GAP_core.md census.

///|
/// TestNewLeaderPendingConfig: on becoming leader, pendingConfIndex is set
/// conservatively to the last log index — 0 with an empty log, 1 if an entry was
/// already appended (raft_test.go:2823). Both table cases.
test "TestNewLeaderPendingConfig" {
  // No pre-existing entry: pending_conf_index == 0.
  let r0 = RaftNode::new("1", ["2"], pre_vote=false)
  r0.node().become_candidate()
  r0.become_leader()
  assert_eq(r0.pending_conf_index, 0)
  // One pre-existing entry: pending_conf_index == 1.
  let r1 = RaftNode::new("1", ["2"], pre_vote=false)
  r1.node().append(1, b"") |> ignore
  r1.node().become_candidate()
  r1.become_leader()
  assert_eq(r1.pending_conf_index, 1)
}

///|
/// TestRaftNodes: VoterNodes returns the voters sorted, independent of insertion
/// order (raft_test.go:2955). Both table cases.
test "TestRaftNodes" {
  let a = RaftNode::new("1", ["2", "3"], pre_vote=false)
  assert_true(a.voter_nodes() == ["1", "2", "3"])
  // Built in a different order: still sorted.
  let b = RaftNode::new("3", ["2", "1"], pre_vote=false)
  assert_true(b.voter_nodes() == ["1", "2", "3"])
}

///|
/// TestLearnerCampaign: a node that has become a learner ignores a MsgTimeoutNow
/// and does not campaign — the transfer-time learner check can race with the
/// demotion, so the recipient must also refuse (raft_test.go:3577).
test "TestLearnerCampaign" {
  let n2 = RaftNode::new("2", ["1"], pre_vote=false)
  n2.config.members.retain(fn(m) { m != "2" })
  n2.config.add_learner("2")
  assert_true(n2.config.is_learner("2"))
  // A MsgTimeoutNow to the learner is ignored: it stays a follower.
  let out = n2.step(Message::new("1", "2", TimeoutNow(1)))
  assert_eq(out.length(), 0)
  assert_true(n2.role() == Follower)
}

///|
/// TestLeaderTransferToUpToDateNodeFromFollower: a transfer requested from a
/// follower is forwarded to the leader, which hands off to an up-to-date target;
/// leadership can then be transferred back (raft_test.go:3081).
test "TestLeaderTransferToUpToDateNodeFromFollower" {
  let net = Net::new(["1", "2", "3"])
  net.campaign("1")
  assert_true(net.node("1").is_leader())
  // Node 2 requests the transfer (to itself), forwarded to leader 1.
  net.transfer("2", "2")
  assert_true(net.node("2").is_leader())
  assert_true(net.node("1").role() == Follower)
  assert_true(net.node("1").lead_transferee == None)
  // Replicate, then transfer back to 1.
  net.propose("2", b"x")
  net.transfer("1", "1")
  assert_true(net.node("1").is_leader())
  assert_true(net.node("2").lead_transferee == None)
}

///|
/// TestLeaderTransferSecondTransferToAnotherNode: a second transfer naming a
/// different target aborts the first (pending) one and completes to the new
/// target (raft_test.go:3347).
test "TestLeaderTransferSecondTransferToAnotherNode" {
  let net = Net::new(["1", "2", "3"])
  net.campaign("1")
  net.isolate("3")
  // Transfer to isolated 3: stays pending (3 cannot catch up).
  net.transfer("3", "1")
  assert_true(net.node("1").lead_transferee == Some("3"))
  // Transfer to 2 (up-to-date): aborts the old, hands off to 2.
  net.transfer("2", "1")
  assert_true(net.node("2").is_leader())
  assert_true(net.node("1").role() == Follower)
  assert_true(net.node("1").lead_transferee == None)
}
