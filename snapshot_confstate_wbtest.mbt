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

// Restoring the live membership from a snapshot's ConfState (Raft §7). Adapted
// from etcd raft_test.go:TestRestore / TestRestoreWithLearner and
// confchange/restore_test.go.

///|
fn install_snap(
  f : RaftNode,
  leader : String,
  term : UInt64,
  index : UInt64,
  cs : ConfState,
) -> Unit {
  let snap : Snapshot = {
    last_index: index,
    last_term: term,
    data: b"",
    conf_state: cs,
  }
  let args = InstallSnapshotArgs::whole(term, leader, snap)
  f.step(Message::new(leader, f.id(), Snapshot(args))) |> ignore
}

///|
/// A follower that installs a snapshot rebuilds its voter and learner sets from
/// the snapshot's ConfState — it does not silently lose its configuration.
test "TestRestore: snapshot ConfState rebuilds the membership" {
  let f = RaftNode::new("2", ["1"], pre_vote=false)
  let cs : ConfState = {
    voters: ["1", "2", "3"],
    learners: ["4"],
    voters_outgoing: [],
    learners_next: [],
    auto_leave: false,
  }
  install_snap(f, "1", 5, 11, cs)
  // The snapshot took effect: baseline advanced and the config was rebuilt.
  assert_eq(f.commit_index(), 11)
  assert_true(f.config.contains("3"))
  assert_true(f.config.is_learner("4"))
  assert_false(f.config.contains("4")) // a learner is not a voter
  assert_eq(f.config.size(), 3)
  assert_true(f.peers.contains("3"))
  assert_true(f.peers.contains("4"))
}

///|
/// A snapshot taken mid-joint restores both halves, so the cluster resumes in
/// the joint configuration.
test "TestRestoreWithVotersOutgoing: joint ConfState restores both halves" {
  let f = RaftNode::new("2", ["1"], pre_vote=false)
  let cs : ConfState = {
    voters: ["1", "2", "3"],
    learners: [],
    voters_outgoing: ["1", "2", "4"],
    learners_next: [],
    auto_leave: false,
  }
  install_snap(f, "1", 6, 20, cs)
  assert_true(f.config.is_joint())
  assert_true(f.config.contains("3")) // incoming voter
  assert_true(f.config.contains("4")) // still a voter via the outgoing half
}

///|
/// Round-trip: the ConfState a leader stamps into a snapshot (`conf_state`)
/// reflects its live configuration, and a follower restoring it recovers the
/// same voters and learners.
test "snapshot ConfState round-trips through a leader's configuration" {
  let lead = RaftNode::new("1", [], pre_vote=false)
  lead.campaign() |> ignore
  // Adding a learner keeps the single-voter quorum, so this commits immediately.
  lead.propose_conf(ConfChange::add_learner("2")) |> ignore
  assert_true(lead.config.is_learner("2"))
  let cs = lead.conf_state()
  // The stamped state matches the live configuration.
  assert_true(cs.voters.contains("1"))
  assert_true(cs.learners.contains("2"))
  // A follower restoring that state rebuilds the same voter/learner sets. The
  // restoring node must itself appear in the snapshot's ConfState — etcd refuses
  // to restore a snapshot that does not list the recipient (raft.go:1901), and a
  // real joining node is always in the config the snapshot records. Here node "2"
  // (the learner) restores its own membership.
  let f = RaftNode::new("2", [], pre_vote=false)
  install_snap(f, "1", 3, 5, cs)
  assert_true(f.config.contains("1"))
  assert_true(f.config.is_learner("2"))
  assert_false(f.config.contains("2"))
}
