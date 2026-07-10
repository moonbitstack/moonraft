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

// Ported from etcd-io/raft (Apache-2.0) raft_test.go, at the confchange-Changer
// layer (the configuration-reconstruction property etcd's raft.restore /
// applyConfChange delegates to confchange.Restore / Changer):
//   #70 TestRestoreWithLearner, #72 TestRestoreVoterToLearner,
//   #73 TestRestoreLearnerPromotion, #87 TestRemoveLearner.
// etcd's tests additionally assert raft-level state (raftLog.lastIndex/term,
// follower role) which lives in the ③ path (snapshot install) — those are
// covered separately there. Here we assert the voter/learner sets and the
// per-node learner flag, which is exactly what those etcd tests assert about
// the configuration.

///|
/// Restore `cs` into a fresh Changer and assert the resulting configuration:
/// the voter set, the learner set, and the learner flag on every tracked node.
/// Also checks the read-back ConfState is equivalent to the input.
fn assert_restored(
  cs : ConfState,
  want_voters : Array[String],
  want_learners : Array[String],
) -> Unit raise {
  let c = Changer::new(last_index=11)
  assert_true(c.restore(cs) is None)
  assert_true(set_eq(c.cfg.incoming, want_voters))
  assert_true(set_eq(c.cfg.learners, want_learners))
  for id in want_voters {
    assert_false(c.prs[id].is_learner)
  }
  for id in want_learners {
    assert_true(c.prs[id].is_learner)
  }
  // Every voter and learner has a tracked progress; no stragglers.
  assert_eq(c.prs.length(), want_voters.length() + want_learners.length())
  // The configuration reads back out as an equivalent ConfState.
  assert_true(c.conf_state().equivalent(cs))
}

///|
/// #70 TestRestoreWithLearner — restoring a ConfState with a learner yields the
/// voters as voters and the learner as a learner.
test "raft_test #70: TestRestoreWithLearner" {
  assert_restored(
    {
      voters: ["1", "2"],
      learners: ["3"],
      voters_outgoing: [],
      learners_next: [],
      auto_leave: false,
    },
    ["1", "2"],
    ["3"],
  )
}

///|
/// #72 TestRestoreVoterToLearner — a node that was a voter is restored as a
/// learner. (confchange.Restore rebuilds from empty, so the restored ConfState's
/// learner status is authoritative; we first show 3 as a voter to make the
/// before/after transition explicit.)
test "raft_test #72: TestRestoreVoterToLearner" {
  // Before: 3 is a voter.
  let before = Changer::new()
  assert_true(before.simple([("v", "1")]) is None)
  before.advance_index()
  assert_true(before.simple([("v", "2")]) is None)
  before.advance_index()
  assert_true(before.simple([("v", "3")]) is None)
  assert_false(before.prs["3"].is_learner)

  // After restoring a ConfState that lists 3 as a learner, 3 is a learner.
  assert_restored(
    {
      voters: ["1", "2"],
      learners: ["3"],
      voters_outgoing: [],
      learners_next: [],
      auto_leave: false,
    },
    ["1", "2"],
    ["3"],
  )
}

///|
/// #73 TestRestoreLearnerPromotion — a node that was a learner is restored as a
/// voter.
test "raft_test #73: TestRestoreLearnerPromotion" {
  // Before: 3 is a learner.
  let before = Changer::new()
  assert_true(before.simple([("v", "1")]) is None)
  before.advance_index()
  assert_true(before.simple([("v", "2")]) is None)
  before.advance_index()
  assert_true(before.simple([("l", "3")]) is None)
  assert_true(before.prs["3"].is_learner)

  // After restoring a ConfState with 3 as a voter (no learners), 3 is a voter.
  assert_restored(
    {
      voters: ["1", "2", "3"],
      learners: [],
      voters_outgoing: [],
      learners_next: [],
      auto_leave: false,
    },
    ["1", "2", "3"],
    [],
  )
}

///|
/// #87 TestRemoveLearner — removing the learner shrinks it away; removing the
/// last remaining voter is refused. etcd panics on "removed all voters"; this
/// port returns the error value Some("removed all voters") instead (abort is
/// uncatchable in MoonBit, so the error value is the faithful, better-behaved
/// expression of the same rejection — see GAP §八).
test "raft_test #87: TestRemoveLearner" {
  let c = Changer::new()
  assert_true(c.simple([("v", "1")]) is None)
  c.advance_index()
  assert_true(c.simple([("l", "2")]) is None)
  assert_true(set_eq(c.cfg.incoming, ["1"]))
  assert_true(set_eq(c.cfg.learners, ["2"]))
  c.advance_index()
  // Remove the learner: voters {1}, learners {}.
  assert_true(c.simple([("r", "2")]) is None)
  assert_true(set_eq(c.cfg.incoming, ["1"]))
  assert_eq(c.cfg.learners.length(), 0)
  c.advance_index()
  // Removing the last voter is refused (etcd panic ↔ our error value).
  assert_true(c.simple([("r", "1")]) == Some("removed all voters"))
  // The rejected change left the configuration untouched.
  assert_true(set_eq(c.cfg.incoming, ["1"]))
}
