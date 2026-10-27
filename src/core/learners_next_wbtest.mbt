// Copyright 2019 The etcd Authors
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

// End-to-end proof that the LIVE path (not just the offline Changer) honours the
// learners_next invariant: a voter demoted during a joint change keeps voting
// through the outgoing half, and only becomes a non-voting learner on leave —
// with its Progress preserved, not recreated. `ack_from` is shared from
// confchange_apply_wbtest.mbt.

///|
test "learners_next: demoted voter votes via the outgoing half until leave" {
  let r = RaftNode::new("1", ["2", "3", "4"], pre_vote=false)
  r.node().become_candidate()
  r.become_leader() // no-op @1
  // Commit the no-op (3 of 4).
  r.step(ack_from("2", 1, r.term())) |> ignore
  r.step(ack_from("3", 1, r.term())) |> ignore

  // Enter joint demoting node 4: incoming {1,2,3}, outgoing {1,2,3,4}. The
  // enter-joint entry @2 commits under the OLD config {1,2,3,4} (3 of 4).
  r.propose_conf_v2(
    ConfChangeV2::enter_joint([ConfChange::add_learner("4")], auto_leave=false),
  )
  |> ignore
  r.step(ack_from("2", 2, r.term())) |> ignore
  r.step(ack_from("3", 2, r.term())) |> ignore

  // Node 4 is staged, still a voter via the outgoing half, not yet a learner.
  assert_true(r.config.is_joint())
  assert_true(r.config.contains("4"))
  assert_false(r.config.is_learner("4"))
  assert_true(r.config.learners_next.contains("4"))
  assert_true(r.progress.contains("4"))
  let round4 = r.progress["4"].next_index

  // Propose X @3, acked only by node 2. The incoming half {1,2,3} has its
  // majority ({1,2}), but the outgoing half {1,2,3,4} needs 3 of 4 — and node 4
  // IS counted there — so X must NOT commit yet. (Were node 4 not in the
  // outgoing half, {1,2} would carry {1,2,3} and X would commit.)
  r.propose(b"x") |> ignore
  let x = r.node().last_log_index()
  r.step(ack_from("2", x, r.term())) |> ignore
  assert_true(r.commit_index() < x)

  // A third ack (node 3) satisfies the outgoing half's 3-of-4; X commits.
  r.step(ack_from("3", x, r.term())) |> ignore
  assert_eq(r.commit_index(), x)

  // Leave the joint config; the leave entry commits under both halves.
  r.propose_conf_v2(ConfChangeV2::leave_joint()) |> ignore
  let leave = r.node().last_log_index()
  r.step(ack_from("2", leave, r.term())) |> ignore
  r.step(ack_from("3", leave, r.term())) |> ignore

  // Now node 4 is a non-voting learner; it no longer counts, and — crucially —
  // its Progress was preserved across the transition, not recreated.
  assert_false(r.config.is_joint())
  assert_false(r.config.contains("4"))
  assert_true(r.config.is_learner("4"))
  assert_eq(r.config.learners_next.length(), 0)
  assert_true(r.progress.contains("4"))
  assert_eq(r.progress["4"].next_index, round4)
  assert_true(r.progress["4"].is_learner)
}

///|
test "learners_next: a snapshot ConfState restores the staged learner" {
  let f = RaftNode::new("2", ["1"], pre_vote=false)
  let cs : ConfState = {
    voters: ["1", "2", "3"],
    learners: [],
    voters_outgoing: ["1", "2", "3", "4"],
    learners_next: ["4"],
    auto_leave: false,
  }
  let snap : Snapshot = {
    last_index: 9,
    last_term: 4,
    data: b"",
    conf_state: cs,
  }
  f.step(
    Message::new("1", "2", Snapshot(InstallSnapshotArgs::whole(4, "1", snap))),
  )
  |> ignore
  // The staged demotion survives the restore: node 4 still votes via outgoing.
  assert_true(f.config.is_joint())
  assert_true(f.config.contains("4"))
  assert_false(f.config.is_learner("4"))
  assert_true(f.config.learners_next.contains("4"))
}
