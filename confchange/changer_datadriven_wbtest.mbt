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

// Ported from etcd-io/raft (Apache-2.0): confchange/testdata/*, driven through
// the Changer exactly as confchange/datadriven_test.go does. That harness holds
// LastIndex at 0 and increments it once after *every* command (including the
// ones that error), so a follower added at command K is anchored at
// next = max(K, 1). We mirror this by calling `advance_index()` after each
// command. Every datadriven case is expanded into an explicit assertion.
//
// testdata/zero.txt is intentionally *not* ported: it exercises etcd's uint64
// NodeId==0 "ignore this change" sentinel, which has no counterpart in this
// port's string-id model (there "0" is an ordinary id). See GAP_peripheral.md.

///|
/// Parse a space-delimited change list ("v1 l2 r3 u4") into the Changer's
/// (op, id) form — the datadriven `simple`/`enter-joint` input syntax.
fn parse_ccs(s : String) -> Array[(String, String)] {
  let out : Array[(String, String)] = []
  for tok in s.split(" ") {
    let t = tok.to_owned()
    if t.length() >= 2 {
      out.push((t[0:1].to_owned(), t[1:].to_owned()))
    }
  }
  out
}

///|
/// Assert a Changer operation was rejected with exactly `msg` (comparing the
/// String option directly avoids the deprecated `Show`-based rendering path).
fn assert_err(actual : String?, msg : String) -> Unit raise {
  assert_true(actual == Some(msg))
}

///|
/// testdata/update.txt — ConfChangeUpdateNode (`u`) is a no-op passthrough.
test "confchange datadriven: update" {
  let c = Changer::new()
  assert_true(c.simple(parse_ccs("v1")) is None)
  assert_eq(c.describe(), "voters=(1)\n1: StateProbe match=0 next=1\n")
  c.advance_index()
  assert_true(c.simple(parse_ccs("v2 u1")) is None)
  assert_eq(
    c.describe(),
    "voters=(1 2)\n1: StateProbe match=0 next=1\n2: StateProbe match=0 next=1\n",
  )
  c.advance_index()
  assert_true(c.simple(parse_ccs("u1 u2 u3 u1 u2 u3")) is None)
  assert_eq(
    c.describe(),
    "voters=(1 2)\n1: StateProbe match=0 next=1\n2: StateProbe match=0 next=1\n",
  )
}

///|
/// testdata/simple_idempotency.txt — repeating a change is a no-op.
test "confchange datadriven: simple_idempotency" {
  let c = Changer::new()
  assert_true(c.simple(parse_ccs("v1")) is None)
  assert_eq(c.describe(), "voters=(1)\n1: StateProbe match=0 next=1\n")
  c.advance_index() // LI=1
  assert_true(c.simple(parse_ccs("v1")) is None)
  assert_eq(c.describe(), "voters=(1)\n1: StateProbe match=0 next=1\n")
  c.advance_index() // LI=2
  assert_true(c.simple(parse_ccs("v2")) is None)
  assert_eq(
    c.describe(),
    "voters=(1 2)\n1: StateProbe match=0 next=1\n2: StateProbe match=0 next=2\n",
  )
  c.advance_index() // LI=3
  assert_true(c.simple(parse_ccs("l1")) is None)
  assert_eq(
    c.describe(),
    "voters=(2) learners=(1)\n" +
    "1: StateProbe match=0 next=1 learner\n" +
    "2: StateProbe match=0 next=2\n",
  )
  c.advance_index() // LI=4
  assert_true(c.simple(parse_ccs("l1")) is None)
  assert_eq(
    c.describe(),
    "voters=(2) learners=(1)\n" +
    "1: StateProbe match=0 next=1 learner\n" +
    "2: StateProbe match=0 next=2\n",
  )
  c.advance_index() // LI=5
  assert_true(c.simple(parse_ccs("r1")) is None)
  assert_eq(c.describe(), "voters=(2)\n2: StateProbe match=0 next=2\n")
  c.advance_index() // LI=6
  assert_true(c.simple(parse_ccs("r1")) is None)
  assert_eq(c.describe(), "voters=(2)\n2: StateProbe match=0 next=2\n")
  c.advance_index() // LI=7
  assert_true(c.simple(parse_ccs("v3")) is None)
  assert_eq(
    c.describe(),
    "voters=(2 3)\n2: StateProbe match=0 next=2\n3: StateProbe match=0 next=7\n",
  )
  c.advance_index() // LI=8
  assert_true(c.simple(parse_ccs("r3")) is None)
  assert_eq(c.describe(), "voters=(2)\n2: StateProbe match=0 next=2\n")
  c.advance_index() // LI=9
  assert_true(c.simple(parse_ccs("r3")) is None)
  assert_eq(c.describe(), "voters=(2)\n2: StateProbe match=0 next=2\n")
  c.advance_index() // LI=10
  assert_true(c.simple(parse_ccs("r4")) is None)
  assert_eq(c.describe(), "voters=(2)\n2: StateProbe match=0 next=2\n")
}

///|
/// testdata/simple_safety.txt — the guardrails on simple (non-joint) changes.
test "confchange datadriven: simple_safety" {
  let c = Changer::new()
  // l1 with no voters removes all voters.
  assert_err(c.simple(parse_ccs("l1")), "removed all voters")
  assert_eq(c.describe(), "voters=()\n")
  c.advance_index() // LI=1
  assert_true(c.simple(parse_ccs("v1")) is None)
  assert_eq(c.describe(), "voters=(1)\n1: StateProbe match=0 next=1\n")
  c.advance_index() // LI=2
  assert_true(c.simple(parse_ccs("v2 l3")) is None)
  assert_eq(
    c.describe(),
    "voters=(1 2) learners=(3)\n" +
    "1: StateProbe match=0 next=1\n" +
    "2: StateProbe match=0 next=2\n" +
    "3: StateProbe match=0 next=2 learner\n",
  )
  c.advance_index() // LI=3
  assert_err(
    c.simple(parse_ccs("r1 v5")),
    "more than one voter changed without entering joint config",
  )
  c.advance_index() // LI=4
  assert_err(c.simple(parse_ccs("r1 r2")), "removed all voters")
  c.advance_index() // LI=5
  assert_err(
    c.simple(parse_ccs("v3 v4")),
    "more than one voter changed without entering joint config",
  )
  c.advance_index() // LI=6
  assert_err(
    c.simple(parse_ccs("l1 v5")),
    "more than one voter changed without entering joint config",
  )
  c.advance_index() // LI=7
  assert_err(c.simple(parse_ccs("l1 l2")), "removed all voters")
  c.advance_index() // LI=8
  assert_true(c.simple(parse_ccs("l2 l3 l4 l5")) is None)
  assert_eq(
    c.describe(),
    "voters=(1) learners=(2 3 4 5)\n" +
    "1: StateProbe match=0 next=1\n" +
    "2: StateProbe match=0 next=2 learner\n" +
    "3: StateProbe match=0 next=2 learner\n" +
    "4: StateProbe match=0 next=8 learner\n" +
    "5: StateProbe match=0 next=8 learner\n",
  )
  c.advance_index() // LI=9
  assert_err(c.simple(parse_ccs("r1")), "removed all voters")
  c.advance_index() // LI=10
  assert_true(c.simple(parse_ccs("r2 r3 r4 r5")) is None)
  assert_eq(c.describe(), "voters=(1)\n1: StateProbe match=0 next=1\n")
}

///|
/// testdata/joint_autoleave.txt — the autoleave flag is passed through to the
/// joint config and reflected by `describe`.
test "confchange datadriven: joint_autoleave" {
  let c = Changer::new()
  assert_true(c.simple(parse_ccs("v1")) is None)
  assert_eq(c.describe(), "voters=(1)\n1: StateProbe match=0 next=1\n")
  c.advance_index() // LI=1
  assert_true(c.enter_joint(true, parse_ccs("v2 v3")) is None)
  assert_eq(
    c.describe(),
    "voters=(1 2 3)&&(1) autoleave\n" +
    "1: StateProbe match=0 next=1\n" +
    "2: StateProbe match=0 next=1\n" +
    "3: StateProbe match=0 next=1\n",
  )
  c.advance_index() // LI=2
  // Can't enter joint twice, even if autoleave changes.
  assert_err(c.enter_joint(false, []), "config is already joint")
  c.advance_index() // LI=3
  assert_true(c.leave_joint() is None)
  assert_eq(
    c.describe(),
    "voters=(1 2 3)\n" +
    "1: StateProbe match=0 next=1\n" +
    "2: StateProbe match=0 next=1\n" +
    "3: StateProbe match=0 next=1\n",
  )
}

///|
/// testdata/joint_idempotency.txt — operations on entering a joint state are
/// idempotent (removing an absent node is fine, re-demoting is fine, etc.).
test "confchange datadriven: joint_idempotency" {
  let c = Changer::new()
  assert_true(c.simple(parse_ccs("v1")) is None)
  assert_eq(c.describe(), "voters=(1)\n1: StateProbe match=0 next=1\n")
  c.advance_index() // LI=1
  assert_true(
    c.enter_joint(
      false,
      parse_ccs("r1 r2 r9 v2 v3 v4 v2 v3 v4 l2 l2 r4 r4 l1 l1"),
    )
    is None,
  )
  assert_eq(
    c.describe(),
    "voters=(3)&&(1) learners=(2) learners_next=(1)\n" +
    "1: StateProbe match=0 next=1\n" +
    "2: StateProbe match=0 next=1 learner\n" +
    "3: StateProbe match=0 next=1\n",
  )
  c.advance_index() // LI=2
  assert_true(c.leave_joint() is None)
  assert_eq(
    c.describe(),
    "voters=(3) learners=(1 2)\n" +
    "1: StateProbe match=0 next=1 learner\n" +
    "2: StateProbe match=0 next=1 learner\n" +
    "3: StateProbe match=0 next=1\n",
  )
}

///|
/// testdata/joint_safety.txt — the guardrails on entering/leaving joint configs.
test "confchange datadriven: joint_safety" {
  let c = Changer::new()
  assert_err(c.leave_joint(), "can't leave a non-joint config")
  c.advance_index() // LI=1
  assert_err(c.enter_joint(false, []), "can't make a zero-voter config joint")
  c.advance_index() // LI=2
  assert_err(
    c.enter_joint(false, parse_ccs("v1")),
    "can't make a zero-voter config joint",
  )
  c.advance_index() // LI=3
  assert_true(c.simple(parse_ccs("v1")) is None)
  assert_eq(c.describe(), "voters=(1)\n1: StateProbe match=0 next=3\n")
  c.advance_index() // LI=4
  assert_err(c.leave_joint(), "can't leave a non-joint config")
  c.advance_index() // LI=5
  assert_true(c.enter_joint(false, []) is None)
  assert_eq(c.describe(), "voters=(1)&&(1)\n1: StateProbe match=0 next=3\n")
  c.advance_index() // LI=6
  assert_err(c.enter_joint(false, []), "config is already joint")
  c.advance_index() // LI=7
  assert_true(c.leave_joint() is None)
  assert_eq(c.describe(), "voters=(1)\n1: StateProbe match=0 next=3\n")
  c.advance_index() // LI=8
  assert_err(c.leave_joint(), "can't leave a non-joint config")
  c.advance_index() // LI=9
  assert_true(c.enter_joint(false, parse_ccs("r1 v2 v3 l4")) is None)
  assert_eq(
    c.describe(),
    "voters=(2 3)&&(1) learners=(4)\n" +
    "1: StateProbe match=0 next=3\n" +
    "2: StateProbe match=0 next=9\n" +
    "3: StateProbe match=0 next=9\n" +
    "4: StateProbe match=0 next=9 learner\n",
  )
  c.advance_index() // LI=10
  assert_err(c.enter_joint(false, []), "config is already joint")
  c.advance_index() // LI=11
  assert_err(c.enter_joint(false, parse_ccs("v12")), "config is already joint")
  c.advance_index() // LI=12
  assert_err(
    c.simple(parse_ccs("l15")),
    "can't apply simple config change in joint config",
  )
  c.advance_index() // LI=13
  assert_true(c.leave_joint() is None)
  assert_eq(
    c.describe(),
    "voters=(2 3) learners=(4)\n" +
    "2: StateProbe match=0 next=9\n" +
    "3: StateProbe match=0 next=9\n" +
    "4: StateProbe match=0 next=9 learner\n",
  )
  c.advance_index() // LI=14
  assert_true(c.simple(parse_ccs("l9")) is None)
  assert_eq(
    c.describe(),
    "voters=(2 3) learners=(4 9)\n" +
    "2: StateProbe match=0 next=9\n" +
    "3: StateProbe match=0 next=9\n" +
    "4: StateProbe match=0 next=9 learner\n" +
    "9: StateProbe match=0 next=14 learner\n",
  )
}
