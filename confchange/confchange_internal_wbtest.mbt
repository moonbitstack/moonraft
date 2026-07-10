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

// Ported from etcd-io/raft (Apache-2.0): confchange/testdata/*, asserting the
// internal voter/learner/joint state a batch of changes produces. Change codes
// mirror the datadriven DSL: v=add voter, l=demote/add learner, r=remove.
// (`learners_next` — a voter demoted *during* a joint change — is the one
// confchange-package case beyond this live model; see PORTING.md.)

///|
fn sorted_join(a : Array[String]) -> String {
  let c = a.copy()
  c.sort()
  let mut s = ""
  for x in c {
    s = s + x + ","
  }
  s
}

///|
fn apply_batch(m : Membership, changes : Array[(String, String)]) -> Unit {
  for c in changes {
    let (op, id) = c
    match op {
      "v" => m.add(id)
      "l" => m.add_learner(id)
      "r" => m.remove(id)
      _ => ()
    }
  }
}

///|
fn assert_config(
  m : Membership,
  voters : Array[String],
  learners : Array[String],
) -> Unit raise {
  assert_eq(sorted_join(m.members), sorted_join(voters))
  assert_eq(sorted_join(m.learners), sorted_join(learners))
}

///|
/// confchange/testdata/simple_promote_demote.txt — the whole sequence, which
/// shares one Config across commands.
test "confchange simple: promote and demote" {
  let m = Membership::new([])
  apply_batch(m, [("v", "1")])
  assert_config(m, ["1"], [])
  apply_batch(m, [("v", "2")])
  assert_config(m, ["1", "2"], [])
  apply_batch(m, [("v", "3")])
  assert_config(m, ["1", "2", "3"], [])
  // Atomically demote then promote 1 — net no change.
  apply_batch(m, [("l", "1"), ("v", "1")])
  assert_config(m, ["1", "2", "3"], [])
  // Demote a voter.
  apply_batch(m, [("l", "2")])
  assert_config(m, ["1", "3"], ["2"])
  // Atomically promote then demote 2 — net still a learner.
  apply_batch(m, [("v", "2"), ("l", "2")])
  assert_config(m, ["1", "3"], ["2"])
  // Promote it back.
  apply_batch(m, [("v", "2")])
  assert_config(m, ["1", "2", "3"], [])
}

///|
/// confchange/testdata/simple_safety.txt (the well-formed subset): add/remove.
test "confchange simple: add and remove voters and learners" {
  let m = Membership::new(["1"])
  apply_batch(m, [("v", "2"), ("l", "3")])
  assert_config(m, ["1", "2"], ["3"])
  apply_batch(m, [("r", "2")])
  assert_config(m, ["1"], ["3"])
  // Promote the learner 3.
  apply_batch(m, [("v", "3")])
  assert_config(m, ["1", "3"], [])
}

///|
/// A joint membership change: enter C(old,new), then leave. Mirrors the
/// enter-joint / leave-joint datadriven flow (without a learners_next demotion).
test "confchange joint: enter then leave" {
  let m = Membership::new(["1", "2", "3"])
  // enter-joint replacing {2,3} with {4,5}: outgoing = current voters.
  let outgoing = m.members.copy()
  apply_batch(m, [("r", "2"), ("r", "3"), ("v", "4"), ("v", "5")])
  m.begin_joint(outgoing)
  assert_true(m.is_joint())
  assert_config(m, ["1", "4", "5"], [])
  // During joint, both halves count as voters.
  assert_true(m.contains("2")) // still in the outgoing half
  assert_true(m.contains("4")) // in the incoming half
  // leave-joint: settle on the incoming configuration.
  m.leave_joint()
  assert_false(m.is_joint())
  assert_config(m, ["1", "4", "5"], [])
  assert_false(m.contains("2"))
}
