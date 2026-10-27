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

// Tests for learner (non-voting) members. Adapted from etcd-io/raft (Apache-2.0)
// raft_test.go:TestLearnerElectionTimeout / TestLearnerPromotion.

///|
/// A committed AddLearner adds a non-voting member: it joins the peer set and
/// gets replication progress, but is not a voter and does not change the quorum.
/// A later AddNode promotes it to a full voter (Raft §4.2.1).
test "learner joins non-voting, then is promoted to voter" {
  let r = RaftNode::new("1", [], pre_vote=false)
  r.campaign() |> ignore // single voter becomes leader
  r.propose_conf(ConfChange::add_learner("2")) |> ignore
  // "2" is a learner: replicated to, but not a voter and not counted in quorum.
  assert_true(r.config.is_learner("2"))
  assert_false(r.config.contains("2"))
  assert_eq(r.config.size(), 1)
  assert_true(r.peers.contains("2"))
  assert_true(r.progress.contains("2"))
  // Promote: AddNode on the learner makes it a voter and clears the learner tag.
  r.propose_conf(ConfChange::add("2")) |> ignore
  assert_true(r.config.contains("2"))
  assert_false(r.config.is_learner("2"))
  assert_eq(r.config.size(), 2)
}

///|
/// A learner never stands for election even when its election timer fires.
test "learner does not campaign" {
  let r = RaftNode::new("2", ["1"], pre_vote=false)
  // Reconfigure so this node is a learner rather than a voter.
  r.config.members.retain(fn(m) { m != "2" })
  r.config.add_learner("2")
  assert_true(r.config.is_learner("2"))
  let out = r.campaign()
  assert_eq(out.length(), 0)
  assert_true(r.role() == Follower)
  // Even after many election timeouts it stays a follower.
  for _i in 0..<40 {
    r.tick() |> ignore
  }
  assert_true(r.role() == Follower)
}

///|
/// A learner receives replicated entries from the leader (it is in the peer set
/// and has progress), so the leader sends it AppendEntries.
test "leader replicates to a learner" {
  let r = RaftNode::new("1", [], pre_vote=false)
  r.campaign() |> ignore
  r.propose_conf(ConfChange::add_learner("2")) |> ignore
  let msgs = r.propose(b"payload")
  // The proposal is broadcast to the learner as an AppendEntries.
  let mut to_learner = 0
  for m in msgs {
    if m.kind() == "Append" {
      to_learner = to_learner + 1
    }
  }
  assert_true(to_learner >= 1)
}
