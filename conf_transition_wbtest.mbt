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

// ConfChangeV2 transitions (etcd's ConfChangeTransition): Auto applies a batch
// simply when it safely can (≤1 voter changed) and otherwise enters an
// auto-leaving joint; the explicit forms always enter joint. This is the gap
// that blocked case 2 of rawnode_test.go:TestRawNodeProposeAndConfChange.

///|
test "enters_joint decides simple vs joint per the transition" {
  // Auto + one change → apply simply (no joint).
  let (a1, j1) = ConfChangeV2::auto([ConfChange::add("2")]).enters_joint()
  assert_false(a1)
  assert_false(j1)
  // Auto + more than one change → joint, auto-leaving.
  let (a2, j2) = ConfChangeV2::auto([
    ConfChange::remove("2"),
    ConfChange::add("3"),
  ]).enters_joint()
  assert_true(a2)
  assert_true(j2)
  // Implicit joint (enter_joint auto_leave=true) → joint, auto-leaving, even for
  // a single change.
  let (a3, j3) = ConfChangeV2::enter_joint(
    [ConfChange::add("2")],
    auto_leave=true,
  ).enters_joint()
  assert_true(a3)
  assert_true(j3)
  // Explicit joint → joint, manual leave.
  let (a4, j4) = ConfChangeV2::enter_joint(
    [ConfChange::add("2")],
    auto_leave=false,
  ).enters_joint()
  assert_false(a4)
  assert_true(j4)
}

///|
/// TestRawNodeProposeAndConfChange case 2: an Auto batch adding a single voter
/// is applied *simply* — the config becomes {1,2} with no outgoing half.
test "Auto single-voter change applies simply, not joint" {
  let r = RaftNode::new("1", [], pre_vote=false)
  r.campaign() |> ignore
  r.propose_conf_v2(ConfChangeV2::auto([ConfChange::add("2")])) |> ignore
  assert_true(r.config.contains("2"))
  assert_false(r.config.is_joint())
  assert_false(r.config.auto_leave)
  assert_eq(r.config.size(), 2)
  assert_true(r.progress.contains("2"))
}

///|
/// An explicit-joint batch enters joint even for a single change (and does not
/// auto-leave).
test "explicit joint enters joint for a single change" {
  let r = RaftNode::new("1", [], pre_vote=false)
  r.campaign() |> ignore
  r.propose_conf_v2(
    ConfChangeV2::enter_joint([ConfChange::add("2")], auto_leave=false),
  )
  |> ignore
  assert_true(r.config.is_joint())
  assert_false(r.config.auto_leave)
}
