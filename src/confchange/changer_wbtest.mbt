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

// Ported from etcd-io/raft (Apache-2.0): confchange/testdata/* driven through
// the Changer, and confchange/restore_test.go. Each datadriven command is
// expanded into an explicit step; the harness bumps `last_index` once per
// command (advance_index), so `next` reveals the round a progress was created.

///|
/// confchange/testdata/joint_learners_next.txt — a voter demoted during a joint
/// change is staged in learners_next and only becomes a learner (keeping its
/// progress, hence `next`) on leave-joint.
test "confchange datadriven: joint_learners_next" {
  let c = Changer::new()
  assert_true(c.simple([("v", "1")]) is None)
  assert_eq(c.describe(), "voters=(1)\n1: StateProbe match=0 next=1\n")
  c.advance_index()
  assert_true(c.enter_joint(false, [("v", "2"), ("l", "1")]) is None)
  assert_eq(
    c.describe(),
    "voters=(2)&&(1) learners_next=(1)\n" +
    "1: StateProbe match=0 next=1\n" +
    "2: StateProbe match=0 next=1\n",
  )
  c.advance_index()
  assert_true(c.leave_joint() is None)
  assert_eq(
    c.describe(),
    "voters=(2) learners=(1)\n" +
    "1: StateProbe match=0 next=1 learner\n" +
    "2: StateProbe match=0 next=1\n",
  )
}

///|
/// confchange/testdata/simple_promote_demote.txt (voter/learner sets).
test "confchange datadriven: simple promote_demote" {
  let c = Changer::new()
  assert_true(c.simple([("v", "1")]) is None)
  assert_eq(c.describe(), "voters=(1)\n1: StateProbe match=0 next=1\n")
  c.advance_index()
  assert_true(c.simple([("v", "2")]) is None)
  c.advance_index()
  assert_true(c.simple([("v", "3")]) is None)
  assert_eq(
    c.describe(),
    "voters=(1 2 3)\n" +
    "1: StateProbe match=0 next=1\n" +
    "2: StateProbe match=0 next=1\n" +
    "3: StateProbe match=0 next=2\n",
  )
  c.advance_index()
  // Atomically demote+promote 1 — no change.
  assert_true(c.simple([("l", "1"), ("v", "1")]) is None)
  c.advance_index()
  // Demote a voter.
  assert_true(c.simple([("l", "2")]) is None)
  assert_eq(
    c.describe(),
    "voters=(1 3) learners=(2)\n" +
    "1: StateProbe match=0 next=1\n" +
    "2: StateProbe match=0 next=1 learner\n" +
    "3: StateProbe match=0 next=2\n",
  )
  c.advance_index()
  // Promote it back.
  assert_true(c.simple([("v", "2")]) is None)
  assert_eq(
    c.describe(),
    "voters=(1 2 3)\n" +
    "1: StateProbe match=0 next=1\n" +
    "2: StateProbe match=0 next=1\n" +
    "3: StateProbe match=0 next=2\n",
  )
}

///|
/// A simple change may not alter more than one voter, nor run while joint.
test "confchange datadriven: simple safety errors" {
  let c = Changer::new()
  assert_true(c.simple([("v", "1"), ("v", "2"), ("v", "3")]) is Some(_))
  // The rejected change left the config untouched.
  assert_eq(c.describe(), "voters=()\n")
  // Enter joint, then a simple change is refused.
  assert_true(c.simple([("v", "1")]) is None)
  assert_true(c.enter_joint(false, [("v", "2")]) is None)
  assert_true(c.simple([("v", "3")]) is Some(_))
}

///|
/// confchange/restore_test.go: Restore rebuilds a joint configuration from a
/// ConfState (the worked example in restore.go).
test "confchange Restore: joint config from a ConfState" {
  let c = Changer::new(last_index=10)
  let cs : ConfState = {
    voters: ["1", "2", "3"],
    learners: ["5"],
    voters_outgoing: ["1", "2", "4", "6"],
    learners_next: ["4"],
    auto_leave: false,
  }
  assert_true(c.restore(cs) is None)
  assert_eq(
    c.describe(),
    "voters=(1 2 3)&&(1 2 4 6) learners=(5) learners_next=(4)\n" +
    "1: StateProbe match=0 next=10\n" +
    "2: StateProbe match=0 next=10\n" +
    "3: StateProbe match=0 next=10\n" +
    "4: StateProbe match=0 next=10\n" +
    "5: StateProbe match=0 next=10 learner\n" +
    "6: StateProbe match=0 next=10\n",
  )
}

///|
/// Restore of a plain (non-joint) ConfState.
test "confchange Restore: simple config from a ConfState" {
  let c = Changer::new(last_index=5)
  let cs : ConfState = {
    voters: ["1", "2"],
    learners: ["3"],
    voters_outgoing: [],
    learners_next: [],
    auto_leave: false,
  }
  assert_true(c.restore(cs) is None)
  assert_eq(
    c.describe(),
    "voters=(1 2) learners=(3)\n" +
    "1: StateProbe match=0 next=5\n" +
    "2: StateProbe match=0 next=5\n" +
    "3: StateProbe match=0 next=5 learner\n",
  )
}
