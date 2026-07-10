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

// Regression test: the auto-leave intent must be *durable* config state derived
// from the committed EnterJoint, not a marker cleared when the LeaveJoint is
// merely appended. Only committed entries may change the state machine, so a
// LeaveJoint truncated before it commits must not have already "consumed" the
// auto-leave — the leader has to append it again. `ack_from` is shared from
// confchange_apply_wbtest.mbt.

///|
test "auto_leave survives truncation of an uncommitted LeaveJoint" {
  let r = RaftNode::new("1", ["2", "3"], pre_vote=false)
  r.node().become_candidate()
  r.become_leader() // no-op @1
  r.step(ack_from("2", 1, r.term())) |> ignore // commit the no-op

  // Enter an auto-leave joint config (demoting node 3 forces joint).
  r.propose_conf_v2(
    ConfChangeV2::enter_joint([ConfChange::add_learner("3")], auto_leave=true),
  )
  |> ignore
  // The EnterJoint entry @2 commits under the old config {1,2,3} (2 of 3).
  r.step(ack_from("2", 2, r.term())) |> ignore

  // Applied: an auto-leave joint config, and the leader has appended a LeaveJoint.
  assert_true(r.config.is_joint())
  assert_true(r.config.auto_leave)
  assert_true(r.has_pending_leave())
  let leave_idx = r.node().last_log_index()

  // A new leader at a higher term overwrites the uncommitted LeaveJoint @leave_idx
  // with a different entry (a legitimate log truncation, §5.3). Its commit index
  // stays at the EnterJoint, which was already committed.
  let t = r.term() + 2
  r.step(
    Message::new(
      "2",
      "1",
      Append({
        term: t,
        leader_id: "2",
        prev_log_index: leave_idx - 1,
        prev_log_term: r.node().term_at(leave_idx - 1),
        entries: [Entry::normal(t, leave_idx, b"z")],
        leader_commit: leave_idx - 1,
      }),
    ),
  )
  |> ignore

  // The committed EnterJoint stands, so the config is still an auto-leave joint —
  // and the LeaveJoint is gone. (Before the fix, the marker was cleared when the
  // LeaveJoint was appended, so `auto_leave` would already read false here.)
  assert_true(r.config.is_joint())
  assert_true(r.config.auto_leave)
  assert_false(r.has_pending_leave())

  // When this node leads again it re-appends the LeaveJoint, rather than being
  // stuck in joint forever. (Before the fix, the consumed marker meant it never
  // re-appended.)
  r.node().become_candidate()
  r.become_leader()
  assert_true(r.config.is_joint())
  assert_true(r.has_pending_leave())
}
