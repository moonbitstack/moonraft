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

// Joint-consensus tests (Raft §4.3). Adapted from etcd-io/raft (Apache-2.0)
// confchange/*, rawnode_test.go:TestRawNodeJointAutoLeave. `ack_from` is shared
// from confchange_apply_wbtest.mbt.

///|
test "ConfChangeV2 round-trips through its log encoding" {
  let v = ConfChangeV2::enter_joint(
    [
      ConfChange::add("4"),
      ConfChange::remove("3"),
      ConfChange::add_learner("5"),
    ],
    auto_leave=true,
  )
  match ConfChangeV2::decode(v.encode()) {
    Some(d) => {
      assert_true(d.auto_leave())
      assert_eq(d.changes.length(), 3)
      assert_true(d.changes[0] == ConfChange::add("4"))
      assert_true(d.changes[1] == ConfChange::remove("3"))
      assert_true(d.changes[2] == ConfChange::add_learner("5"))
      assert_false(d.is_leave())
    }
    None => abort("expected a ConfChangeV2")
  }
  match ConfChangeV2::decode(ConfChangeV2::leave_joint().encode()) {
    Some(d) => {
      assert_true(d.is_leave())
      assert_false(d.auto_leave())
    }
    None => abort("expected a ConfChangeV2")
  }
  // A single-change command is not mistaken for a batch.
  assert_true(ConfChangeV2::decode(ConfChange::add("2").encode()) == None)
}

///|
/// A joint membership change enters C(old,new), needs both halves to commit,
/// and — with auto_leave — settles on the new configuration on its own.
test "joint consensus enters both halves then auto-leaves" {
  let r = RaftNode::new("1", ["2", "3"], pre_vote=false)
  r.node().become_candidate()
  r.become_leader() // no-op @1
  r.step(ack_from("2", 1, r.term())) |> ignore // commit the no-op
  // Replace node 3 with node 4: enter joint {1,2,4} over outgoing {1,2,3}.
  let v2 = ConfChangeV2::enter_joint(
    [ConfChange::remove("3"), ConfChange::add("4")],
    auto_leave=true,
  )
  r.propose_conf_v2(v2) |> ignore // enter-joint entry @2
  // Node 2 is in both halves, so its ack alone commits the entry.
  r.step(ack_from("2", 2, r.term())) |> ignore
  // Now in joint consensus, with the newcomer tracked and the leave auto-queued.
  assert_true(r.config.is_joint())
  assert_true(r.config.contains("4"))
  assert_true(r.progress.contains("4"))
  assert_eq(r.node().last_log_index(), 3) // leave entry appended @3
  // Commit the auto-appended leave; the cluster settles on {1,2,4}.
  r.step(ack_from("2", 3, r.term())) |> ignore
  assert_false(r.config.is_joint())
  assert_true(r.config.contains("4"))
  assert_false(r.config.contains("3"))
  assert_eq(r.config.size(), 3)
}

///|
/// During the joint phase, a decision needs a majority of *both* halves: an ack
/// from a node present only in the outgoing half cannot, with the leader alone,
/// carry the incoming half.
test "joint consensus requires both majorities to commit" {
  let r = RaftNode::new("1", ["2", "3"], pre_vote=false)
  r.node().become_candidate()
  r.become_leader()
  r.step(ack_from("2", 1, r.term())) |> ignore
  // Enter joint {1,4,5} over outgoing {1,2,3} — disjoint new voters, no auto-leave.
  let v2 = ConfChangeV2::enter_joint(
    [
      ConfChange::remove("2"),
      ConfChange::remove("3"),
      ConfChange::add("4"),
      ConfChange::add("5"),
    ],
    auto_leave=false,
  )
  r.propose_conf_v2(v2) |> ignore // @2
  r.step(ack_from("2", 2, r.term())) |> ignore // commits @2 under old config
  assert_true(r.config.is_joint())
  // Propose a normal entry @3. An outgoing-only node (2 or 3) acking cannot make
  // the incoming half {1,4,5}: the leader has only its own vote there.
  r.propose(b"x") |> ignore
  r.step(ack_from("3", 3, r.term())) |> ignore
  assert_true(r.commit_index() < 3)
  // A new (incoming) node acking supplies the incoming majority, and node 3 the
  // outgoing majority, so @3 commits.
  r.step(ack_from("4", 3, r.term())) |> ignore
  assert_eq(r.commit_index(), 3)
}
