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

// Tests closing genuinely-reachable branches the ported upstream tests never
// exercised (error/edge paths of confchange.Changer, quorum.Describe, the
// ConfChangeV2 decoder, Inflights.clone, and the Progress renderer). Each
// asserts a real contract; see GAP_peripheral.md for the per-line disposition
// (the remaining uncovered lines are defensive branches proven unreachable via
// the public API, or abort/panic contracts).

///|
/// The datadriven `_ => "unexpected conf type"` branch of Changer.apply
/// (etcd confchange.go apply's default), reachable with an unknown op tag.
test "changer refuses an unknown conf-change op" {
  let c = Changer::new()
  assert_true(c.simple([("v", "1")]) is None)
  c.advance_index()
  assert_true(c.simple([("x", "9")]) == Some("unexpected conf type x"))
  // The rejected change left the config untouched.
  assert_eq(c.describe(), "voters=(1)\n1: StateProbe match=0 next=1\n")
}

///|
/// EnterJoint whose batch removes every incoming voter is refused by apply's
/// "removed all voters" check (the enter_joint apply-error rollback path).
test "enter_joint that removes every voter is refused" {
  let c = Changer::new()
  assert_true(c.simple([("v", "1")]) is None)
  c.advance_index()
  assert_true(c.enter_joint(false, [("r", "1")]) == Some("removed all voters"))
  // Rolled back to the non-joint single-voter config.
  assert_eq(c.describe(), "voters=(1)\n1: StateProbe match=0 next=1\n")
}

///|
/// Restore of a ConfState with no voters propagates the "removed all voters"
/// error out of the per-change Simple (restore's non-joint error path).
test "restore of a ConfState with no voters is refused" {
  let c = Changer::new()
  let cs : ConfState = {
    voters: [],
    learners: ["3"],
    voters_outgoing: [],
    learners_next: [],
    auto_leave: false,
  }
  assert_true(c.restore(cs) == Some("removed all voters"))
}

///|
fn cov_prs(entries : Array[(String, Bool)]) -> Map[String, Progress] {
  let m : Map[String, Progress] = {}
  for e in entries {
    let (id, learner) = e
    let p = Progress::new(1)
    p.is_learner = learner
    m[id] = p
  }
  m
}

///|
fn assert_inv(
  cfg : ChangerConfig,
  prs : Map[String, Progress],
  want : String?,
) -> Unit raise {
  assert_true(check_invariants(cfg, prs) == want)
}

///|
/// Directly unit-test check_invariants (etcd's checkInvariants). etcd's own
/// datadriven/quick tests never trigger it — it is a defensive safety net that
/// only fires on a bug, and the public Changer API maintains the invariants — so
/// there is no upstream test to port; we verify the ported validator rejects
/// each malformed shape with the correct message.
test "check_invariants catches each malformed configuration" {
  // A voter without a progress.
  assert_inv(
    {
      incoming: ["1"],
      outgoing: [],
      learners: [],
      learners_next: [],
      auto_leave: false,
    },
    {},
    Some("no progress for 1"),
  )
  // A staged learner that is not in the outgoing half.
  assert_inv(
    {
      incoming: ["1"],
      outgoing: [],
      learners: [],
      learners_next: ["9"],
      auto_leave: false,
    },
    cov_prs([("1", false), ("9", false)]),
    Some("9 is in LearnersNext, but not Voters[1]"),
  )
  // A staged learner already marked as a learner.
  assert_inv(
    {
      incoming: ["1"],
      outgoing: ["9"],
      learners: [],
      learners_next: ["9"],
      auto_leave: false,
    },
    cov_prs([("1", false), ("9", true)]),
    Some("9 is in LearnersNext, but is already marked as learner"),
  )
  // A learner in the outgoing voter half.
  assert_inv(
    {
      incoming: ["1"],
      outgoing: ["3"],
      learners: ["3"],
      learners_next: [],
      auto_leave: false,
    },
    cov_prs([("1", false), ("3", true)]),
    Some("3 is in Learners and Voters[1]"),
  )
  // A learner in the incoming voter half.
  assert_inv(
    {
      incoming: ["1", "3"],
      outgoing: [],
      learners: ["3"],
      learners_next: [],
      auto_leave: false,
    },
    cov_prs([("1", false), ("3", true)]),
    Some("3 is in Learners and Voters[0]"),
  )
  // A learner not marked as a learner.
  assert_inv(
    {
      incoming: ["1"],
      outgoing: [],
      learners: ["3"],
      learners_next: [],
      auto_leave: false,
    },
    cov_prs([("1", false), ("3", false)]),
    Some("3 is in Learners, but is not marked as learner"),
  )
  // AutoLeave set on a non-joint config.
  assert_inv(
    {
      incoming: ["1"],
      outgoing: [],
      learners: [],
      learners_next: [],
      auto_leave: true,
    },
    cov_prs([("1", false)]),
    Some("AutoLeave must be false when not joint"),
  )
  // A well-formed configuration passes.
  assert_inv(
    {
      incoming: ["1", "2"],
      outgoing: [],
      learners: ["3"],
      learners_next: [],
      auto_leave: false,
    },
    cov_prs([("1", false), ("2", false), ("3", true)]),
    None,
  )
}

///|
/// The safety-net integration: if the configuration is somehow inconsistent, an
/// operation's post-change check_invariants (etcd's checkAndReturn) catches it
/// and rolls back rather than committing the change. We inject an inconsistency
/// the public API cannot produce — a learner with no progress, via same-package
/// field access — then confirm each op rejects and restores.
test "an operation rolls back when its result violates an invariant" {
  // simple
  let a = Changer::new()
  assert_true(a.simple([("v", "1")]) is None)
  a.cfg.learners.push("ghost")
  assert_true(a.simple([("u", "1")]) == Some("no progress for ghost"))

  // enter_joint
  let b = Changer::new()
  assert_true(b.simple([("v", "1")]) is None)
  b.cfg.learners.push("ghost")
  assert_true(b.enter_joint(false, []) == Some("no progress for ghost"))

  // leave_joint
  let c = Changer::new()
  assert_true(c.simple([("v", "1")]) is None)
  c.advance_index()
  assert_true(c.enter_joint(false, [("v", "2")]) is None)
  c.cfg.learners.push("ghost")
  assert_true(c.leave_joint() == Some("no progress for ghost"))
}

///|
/// Inflights.clone copies a non-empty buffer and shares no memory with it.
test "cloning a non-empty Inflights shares no buffer" {
  let a = Inflights::new(4, 0)
  a.add(1, 10)
  a.add(2, 20)
  let b = a.clone()
  assert_eq(b.count(), 2)
  assert_eq(b.bytes, 30)
  // Freeing in the clone leaves the original untouched.
  b.free_le(2)
  assert_eq(b.count(), 0)
  assert_eq(a.count(), 2)
}

///|
/// Progress renders the Replicate state (the StateReplicate arm of the state
/// name, which the Snapshot-only TestProgressString never reached).
test "Progress renders the Replicate state" {
  let p = Progress::new(3)
  p.become_replicate()
  assert_true(p.to_string().has_prefix("StateReplicate "))
}

///|
/// MajorityConfig.Describe on an empty quorum, and its wide-index padding.
test "describe: empty quorum and wide index" {
  assert_eq(@quorum.describe([], {}), "<empty majority quorum>")
  // An index wider than the 5-column field is printed in full (pad5 pass-through).
  let out = @quorum.describe(["1"], { "1": 123456UL })
  assert_true(out.contains("123456"))
}

///|
fn cov_cmd(s : String) -> Bytes {
  let arr : Array[Byte] = []
  for ch in s {
    let code = ch.to_int()
    arr.push((code & 0xff).to_byte())
    arr.push(((code >> 8) & 0xff).to_byte())
  }
  Bytes::from_array(arr)
}

///|
/// The ConfChangeV2 decoder tolerates a bare "V" (no transition byte → Auto) and
/// skips a segment carrying an unknown change tag.
test "ConfChangeV2 decode tolerates a bare V and skips a malformed segment" {
  // Bare "V": length < 2, transition defaults to Auto, no changes.
  match ConfChangeV2::decode(cov_cmd("V")) {
    Some(v) => assert_eq(v.changes.length(), 0)
    None => abort("expected Some for a bare V")
  }
  // Unknown segment tag "x" is skipped.
  match ConfChangeV2::decode(cov_cmd("Va;x5")) {
    Some(v) => assert_eq(v.changes.length(), 0)
    None => abort("expected Some")
  }
  // A well-formed segment still decodes.
  match ConfChangeV2::decode(cov_cmd("Va;+7")) {
    Some(v) => {
      assert_eq(v.changes.length(), 1)
      assert_true(v.changes[0] == ConfChange::add("7"))
    }
    None => abort("expected Some")
  }
}
