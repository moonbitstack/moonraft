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

// Ported from etcd-io/raft (Apache-2.0): raft_snap_test.go. We track the pending
// snapshot index as `next_index - 1` while in the Snapshot state (etcd keeps it
// in a dedicated PendingSnapshot field). `ack_from` is shared from
// confchange_apply_wbtest.mbt.

///|
fn snap_leader() -> RaftNode {
  let r = RaftNode::new("1", ["2"], pre_vote=false)
  // Restore a snapshot at index/term 11, then take leadership (no-op at 12).
  r
  .node()
  .install_snapshot({
    last_index: 11,
    last_term: 11,
    data: b"",
    conf_state: ConfState::empty(),
  })
  r.node().become_candidate()
  r.become_leader()
  r
}

///|
/// TestPendingSnapshotPauseReplication: a follower digesting a snapshot receives
/// no further AppendEntries from a proposal.
test "TestPendingSnapshotPauseReplication" {
  let r = snap_leader()
  r.progress["2"].become_snapshot(11)
  let msgs = r.propose(b"somedata")
  assert_eq(msgs.length(), 0)
}

///|
/// TestSnapshotAbort: a successful AppendResp at or past the snapshot index
/// aborts the pending snapshot and resumes streaming.
test "TestSnapshotAbort" {
  let r = snap_leader()
  let pr2 = r.progress["2"]
  pr2.become_snapshot(11)
  r.step(ack_from("2", 11, r.term())) |> ignore
  assert_true(pr2.state == Replicate)
  // Optimistically advanced past the no-op at index 12 that leadership appended.
  assert_eq(pr2.next_index, 13)
  assert_eq(pr2.inflights.count(), 1)
}

///|
/// TestSendingSnapshotSetPendingSnapshot: probing a follower whose needed entry
/// has been compacted moves it into the Snapshot state with the pending snapshot
/// index recorded (here 11).
test "TestSendingSnapshotSetPendingSnapshot" {
  let r = snap_leader()
  let pr2 = r.progress["2"]
  // Force a probe just past the snapshot baseline, then reject it: the follower
  // needs the compacted prefix, so a snapshot is sent.
  pr2.next_index = 12
  let out = r.step(
    Message::new(
      "2",
      "1",
      AppendResp({
        term: r.term(),
        success: false,
        match_index: 0,
        conflict_index: 1,
        conflict_term: 0,
      }),
    ),
  )
  assert_true(pr2.state == Snapshot)
  assert_eq(pr2.pending_snapshot, 11)
  assert_eq(out.length(), 1)
  assert_true(out[0].kind() == "Snapshot")
}

///|
/// TestSnapshotFailure: a rejected snapshot report discards the pending snapshot
/// and probes again from the match point.
test "TestSnapshotFailure" {
  let r = snap_leader()
  let pr2 = r.progress["2"]
  pr2.next_index = 1
  pr2.become_snapshot(11)
  r.report_snapshot("2", true)
  assert_eq(pr2.pending_snapshot, 0)
  assert_eq(pr2.next_index, 1)
  assert_true(pr2.msg_app_flow_paused)
}

///|
/// TestSnapshotSucceed: a successful snapshot report probes from just past the
/// snapshot.
test "TestSnapshotSucceed" {
  let r = snap_leader()
  let pr2 = r.progress["2"]
  pr2.next_index = 1
  pr2.become_snapshot(11)
  r.report_snapshot("2", false)
  assert_eq(pr2.pending_snapshot, 0)
  assert_eq(pr2.next_index, 12)
  assert_true(pr2.msg_app_flow_paused)
}
