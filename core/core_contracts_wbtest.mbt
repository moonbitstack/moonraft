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

// Abort/panic-contract coverage. Each `panic` test names the illegal precondition
// that triggers the abort; MoonBit fails the test if the body does not abort, so
// these are assertions of the contract, not filler.

///|
/// etcd `Inflights.Add` panics on a full window ("cannot add into a Full
/// inflights", tracker/inflights.go).
test "panic adding to a full inflights window aborts" {
  let in_ = Inflights::new(1, 0)
  in_.add(1, 0)
  in_.add(2, 0)
}

///|
/// etcd `SentEntries` panics for any progress state other than Replicate/Probe;
/// the leader never sends an append while a snapshot is pending.
test "panic sent_entries in the snapshot state aborts" {
  let p = Progress::new(1)
  p.become_snapshot(5)
  p.sent_entries(6, true)
}

///|
/// B5 State-Machine Safety: an append that would overwrite a committed entry with
/// a different term must never occur for a correct leader; the store asserts it.
test "panic storing a conflict at a committed index aborts" {
  let n = Node::new("1")
  n.append(1, b"") |> ignore
  n.advance_commit(1)
  n.store_entries(0, [Entry::normal(2, 1, b"")]) |> ignore
}

///|
/// etcd `newRaft` panics on an invalid `Config.validate`; the convenience
/// constructor reproduces each branch.
test "panic RaftNode::new rejects an empty id" {
  RaftNode::new("", ["2"]) |> ignore
}

///|
test "panic RaftNode::new rejects a non-positive heartbeat tick" {
  RaftNode::new("1", ["2"], heartbeat_timeout=0) |> ignore
}

///|
test "panic RaftNode::new rejects an election tick not above the heartbeat tick" {
  RaftNode::new("1", ["2"], election_timeout=1, heartbeat_timeout=1) |> ignore
}

///|
test "panic RaftNode::new rejects a non-positive max inflight" {
  RaftNode::new("1", ["2"], max_inflight=0) |> ignore
}

///|
test "panic RaftNode::new rejects max inflight bytes below the message cap" {
  RaftNode::new("1", ["2"], max_msg_bytes=1000, max_inflight_bytes=500)
  |> ignore
}

///|
test "panic RaftNode::new rejects lease-based reads without check-quorum" {
  RaftNode::new("1", ["2"], read_only_option=LeaseBased, check_quorum=false)
  |> ignore
}

///|
/// `step` intercepts local, routable messages term-free, so a local message
/// reaching the term classifier is a routing bug.
test "panic a local message reaching on_stale aborts" {
  let r = RaftNode::new("1", ["2"], pre_vote=false)
  r.on_stale(Message::new("2", "1", Propose([]))) |> ignore
}

///|
test "panic a local message reaching dispatch aborts" {
  let r = RaftNode::new("1", ["2"], pre_vote=false)
  r.dispatch(Message::new("2", "1", Propose([]))) |> ignore
}

///|
/// A leadership transfer to a follower that is behind the compacted snapshot and
/// has not been heard from ships nothing (D-snap-recentactive): `send_to`
/// withholds even the snapshot, so the transfer produces no message and waits for
/// the target to become active.
test "transfer to an inactive follower behind the snapshot sends nothing" {
  let r = snap_leader()
  let p = r.progress["2"]
  p.next_index = 11
  assert_false(p.is_active())
  assert_eq(r.transfer_leadership("2").length(), 0)
}
