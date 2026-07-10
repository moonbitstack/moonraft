// Copyright 2021 The etcd Authors
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

// Ported from etcd-io/raft (Apache-2.0): raftpb/confstate_test.go
// TestConfState_Equivalent and the behavioural part of
// raftpb/confchange_test.go TestLeaveJoint. (TestLeaveJoint's reflect-based
// field-count guard is a Go struct-layout assertion — N/A here.)

///|
fn cs(
  voters? : Array[String] = [],
  learners? : Array[String] = [],
  voters_outgoing? : Array[String] = [],
  learners_next? : Array[String] = [],
  auto_leave? : Bool = false,
) -> ConfState {
  { voters, learners, voters_outgoing, learners_next, auto_leave }
}

///|
test "TestConfState_Equivalent" {
  // Reordered voters/learners/outgoing/next are equivalent.
  assert_true(
    cs(
      voters=["1", "2", "3"],
      learners=["5", "4", "6"],
      voters_outgoing=["9", "8", "7"],
      learners_next=["10", "20", "15"],
    ).equivalent(
      cs(
        voters=["1", "2", "3"],
        learners=["4", "5", "6"],
        voters_outgoing=["7", "9", "8"],
        learners_next=["20", "10", "15"],
      ),
    ),
  )
  // Empty and (nil) empty are equivalent.
  assert_true(cs(voters=[]).equivalent(cs()))
  // Non-equivalent voter sets.
  assert_false(
    cs(voters=["1", "2", "3", "4"]).equivalent(cs(voters=["2", "1", "3"])),
  )
  assert_false(
    cs(voters=["1", "4", "3"]).equivalent(cs(voters=["2", "1", "3"])),
  )
  // Sensitive to the auto-leave flag.
  assert_false(cs(auto_leave=true).equivalent(cs()))
}

///|
test "TestLeaveJoint: an empty batch is a leave, a non-empty one is not" {
  assert_true(ConfChangeV2::leave_joint().is_leave())
  assert_true(ConfChangeV2::enter_joint([], auto_leave=false).is_leave())
  assert_false(
    ConfChangeV2::enter_joint([ConfChange::add("2")], auto_leave=true).is_leave(),
  )
}
