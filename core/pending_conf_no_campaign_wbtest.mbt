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

///|
/// MISSING fix (M-hup-unapplied): etcd's `hup` refuses to start an election
/// while there are committed-but-unapplied configuration changes
/// (`hasUnappliedConfChanges`, raft.go:983,995) — campaigning with a pending
/// membership change could elect a leader under a stale view of the quorum.
/// Red: a follower whose log holds a committed, not-yet-applied ConfChange must
/// not become a candidate when its election timer fires.
test "M-hup-unapplied: does not campaign with a pending unapplied conf change" {
  let r = RaftNode::new("2", ["1", "3"], pre_vote=false)
  // A committed conf change at index 1 that has not been applied
  // (last_applied stays at 0; the config was never folded in).
  r.core.append_conf(1, ConfChange::add("4").encode()) |> ignore
  r.core.advance_commit(1)
  assert_true(r.core.last_applied < r.commit_index())
  let msgs = r.campaign()
  assert_eq(msgs.length(), 0)
  assert_true(r.role() == Follower)
}

///|
/// The gate also blocks a transfer-triggered campaign (MsgTimeoutNow routes
/// through `hup(campaignTransfer)`, which runs the same check).
test "M-hup-unapplied: TimeoutNow does not campaign with a pending conf change" {
  let r = RaftNode::new("2", ["1", "3"], pre_vote=false)
  r.core.append_conf(1, ConfChange::add("4").encode()) |> ignore
  r.core.advance_commit(1)
  let msgs = r.step(Message::new("1", "2", TimeoutNow(0)))
  assert_eq(msgs.length(), 0)
  assert_true(r.role() == Follower)
}

///|
/// With no pending conf change the follower campaigns normally, so the gate does
/// not block ordinary elections.
test "M-hup-unapplied: campaigns normally when nothing is pending" {
  let r = RaftNode::new("2", ["1", "3"], pre_vote=false)
  let msgs = r.campaign()
  assert_true(msgs.length() > 0)
  assert_true(r.role() == Candidate)
}

///|
/// A follower leased to leader "1" under check-quorum.
fn leased_to_leader() -> RaftNode {
  let f = RaftNode::new("2", ["1", "3"], pre_vote=false)
  f.enable_check_quorum()
  f.step(
    Message::new(
      "1",
      "2",
      Heartbeat({ term: 1, leader_id: "1", commit: 0, context: b"" }),
    ),
  )
  |> ignore
  f
}

///|
fn vote_granted(msgs : Array[Message]) -> Bool {
  guard msgs.length() > 0 else { return false }
  match msgs[0].payload {
    VoteResp(r) => r.vote_granted
    _ => false
  }
}

///|
/// DIVERGENT fix (D-transfer-force): a leadership-transfer campaign marks its
/// votes forced so a follower still leased to the outgoing leader grants them
/// (etcd carries `campaignTransfer` in the message context, raft.go:1102). Red:
/// a leased follower rejects an ordinary higher-term vote but must grant a
/// forced one.
test "D-transfer-force: leased follower rejects normal vote, grants forced vote" {
  let args : RequestVoteArgs = {
    term: 2,
    candidate_id: "3",
    last_log_index: 0,
    last_log_term: 0,
  }
  // Unforced higher-term vote → rejected, term not adopted (lease holds).
  let f1 = leased_to_leader()
  let normal = f1.step(Message::new("3", "2", Vote(args)))
  assert_false(vote_granted(normal))
  assert_eq(f1.term(), 1)
  // Forced higher-term vote → granted, term adopted (transfer bypasses lease).
  let f2 = leased_to_leader()
  let forced = f2.step(Message::new("3", "2", Vote(args), force=true))
  assert_true(vote_granted(forced))
  assert_eq(f2.term(), 2)
}

///|
/// A transfer-triggered campaign (TimeoutNow) emits forced votes, so it wins
/// even against leased followers — end-to-end wiring of the fix above.
test "D-transfer-force: TimeoutNow campaign emits forced votes" {
  let t = RaftNode::new("2", ["1", "3"], pre_vote=false)
  let msgs = t.step(Message::new("1", "2", TimeoutNow(0)))
  let mut saw_forced = false
  for m in msgs {
    if m.payload is Vote(_) && m.force {
      saw_forced = true
    }
  }
  assert_true(saw_forced)
}

///|
/// DIVERGENT fix (D-restore-notinconf): etcd's `restore` refuses a snapshot
/// whose ConfState does not list the recipient (defense-in-depth, raft.go:1901).
/// Red: a snapshot excluding this node must not be installed.
test "D-restore-notinconf: snapshot excluding self is ignored" {
  let r = RaftNode::new("2", ["1", "3"], pre_vote=false)
  let cs : ConfState = {
    voters: ["1", "3"],
    learners: [],
    voters_outgoing: [],
    learners_next: [],
    auto_leave: false,
  }
  let snap : Snapshot = {
    last_index: 5,
    last_term: 1,
    data: b"",
    conf_state: cs,
  }
  r.step(
    Message::new("1", "2", Snapshot(InstallSnapshotArgs::whole(1, "1", snap))),
  )
  |> ignore
  assert_eq(r.node().snapshot_index, 0)
}

///|
/// A snapshot that does list this node installs normally, so the guard only
/// rejects the pathological case.
test "D-restore-notinconf: snapshot including self installs" {
  let r = RaftNode::new("2", ["1", "3"], pre_vote=false)
  let cs : ConfState = {
    voters: ["1", "2", "3"],
    learners: [],
    voters_outgoing: [],
    learners_next: [],
    auto_leave: false,
  }
  let snap : Snapshot = {
    last_index: 5,
    last_term: 1,
    data: b"",
    conf_state: cs,
  }
  r.step(
    Message::new("1", "2", Snapshot(InstallSnapshotArgs::whole(1, "1", snap))),
  )
  |> ignore
  assert_eq(r.node().snapshot_index, 5)
}
