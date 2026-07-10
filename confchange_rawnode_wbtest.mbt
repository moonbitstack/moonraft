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
//
// Ported from etcd rawnode_test.go / node_test.go: the ConfChangeV2 / joint /
// learner / LearnersNext behaviour, driven through the RawNode contract now that
// the confchange/membership engine is in place. This port applies committed conf
// changes in the core (not via a manual `ApplyConfChange` that returns the
// ConfState), and its `ConfChangeV2` carries `auto_leave` (implicit vs explicit
// joint) rather than etcd's `Transition` enum, so the tests are re-expressed as
// assertions on the resulting `ConfState` (voters / voters_outgoing / learners /
// learners_next) reached through `RawNode::propose_conf_v2` + `conf_state`. See
// PORTING.md for the two residual gaps (the simple-`Auto` single-change apply and
// the transient `auto_leave` flag).

///|
fn sorted_copy(a : Array[String]) -> Array[String] {
  let b = a.copy()
  b.sort()
  b
}

///|
/// A sorted, comma-joined rendering of a set, for order-independent String
/// comparison (avoids `assert_eq` on `Array[String]`, which prefers Debug).
fn set_key(a : Array[String]) -> String {
  let b = sorted_copy(a)
  let mut s = ""
  for x in b {
    s = s + x + ","
  }
  s
}

///|
/// Assert the four membership sets of `cs`, order-independent. (The derived
/// `auto_leave` flag is asserted separately where it is stable — this port clears
/// its pending-auto-leave marker when the leave entry is appended, not when it
/// commits, so the flag is transient while a leave is in flight.)
fn assert_conf_sets(
  cs : ConfState,
  voters : Array[String],
  voters_outgoing : Array[String],
  learners : Array[String],
  learners_next : Array[String],
) -> Unit raise {
  assert_eq(set_key(cs.voters), set_key(voters))
  assert_eq(set_key(cs.voters_outgoing), set_key(voters_outgoing))
  assert_eq(set_key(cs.learners), set_key(learners))
  assert_eq(set_key(cs.learners_next), set_key(learners_next))
}

///|
/// A single-voter leader with its no-op committed and applied — the state the
/// etcd tests reach before proposing a conf change.
fn conf_leader() -> RawNode {
  let rn = RaftNode::new("1", [], pre_vote=false).raw()
  rn.campaign()
  rn.stabilize() |> ignore
  rn
}

///|
test "TestRawNodeProposeAndConfChange: V2 add-learner settles to a simple config" {
  // etcd case 3 (V2 AddLearnerNode, Auto -> simple). A learner add keeps the
  // voter set at {1}, so the implicit joint's leave commits at once and the
  // cluster settles on the simple result — the same end state etcd's simple
  // apply reaches.
  let rn = conf_leader()
  rn.propose_conf_v2(
    ConfChangeV2::enter_joint([ConfChange::add_learner("2")], auto_leave=true),
  )
  assert_conf_sets(rn.conf_state(), ["1"], [], ["2"], [])
  assert_false(rn.node().config.is_joint())
}

///|
test "TestRawNodeProposeAndConfChange: V2 explicit joint learner, then manual leave" {
  // etcd case 4 (JointExplicit). Without auto-leave the config stays joint until
  // an explicit leave is proposed.
  let rn = conf_leader()
  rn.propose_conf_v2(
    ConfChangeV2::enter_joint([ConfChange::add_learner("2")], auto_leave=false),
  )
  // exp: joint {voters 1, outgoing 1, learners 2}.
  assert_conf_sets(rn.conf_state(), ["1"], ["1"], ["2"], [])
  assert_true(rn.node().config.is_joint())
  // Leave joint manually -> exp2: simple {voters 1, learners 2}.
  rn.propose_conf_v2(ConfChangeV2::leave_joint())
  assert_conf_sets(rn.conf_state(), ["1"], [], ["2"], [])
  assert_false(rn.node().config.is_joint())
}

///|
test "TestRawNodeProposeAndConfChange: V2 implicit joint learner auto-leaves" {
  // etcd case 5 (JointImplicit). The auto-leave settles the config on its own.
  let rn = conf_leader()
  rn.propose_conf_v2(
    ConfChangeV2::enter_joint([ConfChange::add_learner("2")], auto_leave=true),
  )
  assert_conf_sets(rn.conf_state(), ["1"], [], ["2"], [])
  assert_false(rn.node().config.is_joint())
}

///|
test "TestRawNodeProposeAndConfChange: V2 add-and-demote uses LearnersNext (implicit)" {
  // etcd case 6 (add 2, demote 1, add learner 3; Auto -> joint because demoting a
  // voter is unsafe). The new voter {2} cannot yet acknowledge, so the leave is
  // held and the joint config — with n1 staged in LearnersNext — is observed.
  let rn = conf_leader()
  rn.propose_conf_v2(
    ConfChangeV2::enter_joint(
      [
        ConfChange::add("2"),
        ConfChange::add_learner("1"),
        ConfChange::add_learner("3"),
      ],
      auto_leave=true,
    ),
  )
  // exp: joint {voters 2, outgoing 1, learners 3, learners_next 1}.
  assert_conf_sets(rn.conf_state(), ["2"], ["1"], ["3"], ["1"])
  assert_true(rn.node().config.is_joint())
}

///|
test "TestRawNodeProposeAndConfChange: V2 add-and-demote uses LearnersNext (explicit)" {
  // etcd case 7 (same batch, JointExplicit). Same joint config; no auto-leave.
  let rn = conf_leader()
  rn.propose_conf_v2(
    ConfChangeV2::enter_joint(
      [
        ConfChange::add("2"),
        ConfChange::add_learner("1"),
        ConfChange::add_learner("3"),
      ],
      auto_leave=false,
    ),
  )
  assert_conf_sets(rn.conf_state(), ["2"], ["1"], ["3"], ["1"])
  assert_true(rn.node().config.is_joint())
}

///|
test "TestRawNodeJointAutoLeave: an implicit joint change settles automatically" {
  // etcd rawnode_test.go:TestRawNodeJointAutoLeave. A learner-add under implicit
  // (auto-leave) joint consensus enters C(old,new) and then leaves on its own,
  // settling on {voters 1, learners 2}. (etcd's version parks the config in the
  // joint state by stepping the leader down and re-electing before the auto-leave
  // fires; this port applies committed conf changes in the core and leaves as
  // soon as the joint quorum allows, so the settled end state is asserted — the
  // enter-joint/auto-leave lifecycle itself is covered end to end by
  // joint_wbtest.mbt.)
  let rn = conf_leader()
  rn.propose_conf_v2(
    ConfChangeV2::enter_joint([ConfChange::add_learner("2")], auto_leave=true),
  )
  let cs = rn.conf_state()
  assert_conf_sets(cs, ["1"], [], ["2"], [])
  assert_false(rn.node().config.is_joint())
  // No auto-leave remains pending once the config has settled.
  assert_false(cs.auto_leave)
}

///|
test "TestNodeProposeAddLearnerNode: adding a learner does not change the voters" {
  // etcd node_test.go:TestNodeProposeAddLearnerNode. Proposing an AddLearnerNode
  // conf change makes node 2 a learner while the voter set stays {1}; the
  // committed entry is a conf change decoding to that learner add.
  let rn = conf_leader()
  rn.propose_conf(ConfChange::add_learner("2"))
  let cs = rn.conf_state()
  assert_conf_sets(cs, ["1"], [], ["2"], [])
  assert_true(rn.node().config.is_learner("2"))
  // The proposal is on the log at index 2 as a conf change for the learner add.
  let e = rn.node().node().entry_at(2).unwrap()
  assert_true(e.is_conf_change())
  assert_true(
    ConfChange::decode(e.command) == Some(ConfChange::add_learner("2")),
  )
}

///|
test "TestRawNodeStatus: status reports per-follower progress and the configuration" {
  // The other half of etcd rawnode_test.go:TestRawNodeStatus — status.Progress[1]
  // and tracker.Config — now that `full_status` exposes both.
  let c = InteractionCluster::new(["1", "2", "3"])
  c.at("1").campaign()
  c.stabilize() |> ignore
  let st = c.at("1").node().full_status()
  // The leader's own progress entry is present and is not a learner.
  match c.at("1").node().progress_of("1") {
    Some(p) => {
      assert_true(p.match_index >= 1)
      assert_false(p.is_learner)
    }
    None => abort("expected a progress entry for the leader")
  }
  // Progress is tracked for all three members.
  assert_eq(st.progress.length(), 3)
  // The configuration (etcd's tracker.Config) reports the three voters.
  assert_eq(set_key(st.config.voters), "1,2,3,")
  assert_eq(st.config.learners.length(), 0)
}
