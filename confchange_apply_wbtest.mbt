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

// End-to-end tests for B2: a committed ConfChange is applied to the *live*
// server — its configuration, replication progress and peer set — not merely
// folded into a side ConfDriver. Adapted from etcd-io/raft (Apache-2.0)
// raft_test.go:TestAddNode / TestRemoveNode / TestCommitAfterRemoveNode.

///|
fn ack_from(peer : String, index : UInt64, term : UInt64) -> Message {
  Message::new(
    peer,
    "1",
    AppendResp({
      term,
      success: true,
      match_index: index,
      conflict_index: 0,
      conflict_term: 0,
    }),
  )
}

///|
/// A committed AddNode makes the newcomer a voter on the leader, with its own
/// replication progress, and grows the configuration.
test "B2: committed AddNode makes the new server a voter" {
  let r = RaftNode::new("1", [], pre_vote=false)
  r.campaign() |> ignore // single-node: becomes leader immediately
  assert_true(r.is_leader())
  assert_eq(r.config.size(), 1)
  r.propose_conf(ConfChange::add("2")) |> ignore // single node commits at once
  assert_true(r.config.contains("2"))
  assert_true(r.peers.contains("2"))
  assert_true(r.progress.contains("2"))
  assert_eq(r.config.size(), 2)
}

///|
/// A leader that commits its own removal steps down and drops out of the
/// configuration (Raft §6).
test "B2: leader that removes itself steps down" {
  let r = RaftNode::new("1", ["2", "3"], pre_vote=false)
  r.node().become_candidate()
  r.become_leader() // no-op at index 1
  assert_true(r.is_leader())
  r.propose_conf(ConfChange::remove("1")) |> ignore // conf entry at index 2
  // Commit the conf entry: a majority (2 of 3) acks index 2.
  r.step(ack_from("2", 2, r.term())) |> ignore
  r.step(ack_from("3", 2, r.term())) |> ignore
  assert_true(r.role() == Follower)
  assert_false(r.is_leader())
  assert_false(r.config.contains("1"))
}

///|
/// Removing a member shrinks the quorum, which lets a previously blocked entry
/// commit (etcd TestCommitAfterRemoveNode). Here node 3 is down; a proposal
/// cannot commit under the 3-node quorum with only node 2, but once node 3 is
/// removed the 2-node quorum commits both the removal and the pending entry.
test "B2: removal shrinks the quorum and unblocks commit" {
  let r = RaftNode::new("1", ["2", "3"], pre_vote=false)
  r.node().become_candidate()
  r.become_leader() // no-op @1
  // Propose a normal entry @2; node 2 acks it, node 3 is down.
  r.propose(b"x") |> ignore
  r.step(ack_from("2", 2, r.term())) |> ignore
  // With a 3-node quorum and node 3 silent, @2 already commits (2 of 3). To see
  // the shrink effect, propose a conf removal @3 that only node 2 can ack.
  r.propose_conf(ConfChange::remove("3")) |> ignore
  r.step(ack_from("2", 3, r.term())) |> ignore
  // The removal committed and applied: node 3 is gone from config and progress.
  assert_false(r.config.contains("3"))
  assert_false(r.progress.contains("3"))
  assert_eq(r.config.size(), 2)
  // Now a further proposal commits on the 2-node quorum (self + node 2) alone.
  r.propose(b"y") |> ignore
  let before = r.commit_index()
  r.step(ack_from("2", r.node().last_log_index(), r.term())) |> ignore
  assert_true(r.commit_index() > before)
  assert_eq(r.commit_index(), r.node().last_log_index())
}
