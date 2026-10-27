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
/// DIVERGENT fix (D-snap-recentactive): etcd's `maybeSendSnapshot` refuses to
/// ship a snapshot to a follower that has not been heard from this liveness
/// window (`if !pr.RecentActive { return false }`, raft.go:667) — it is probably
/// gone and the snapshot would be wasted. This node shipped one unconditionally.
/// Red: `send_to` returns a snapshot for an inactive follower; after the fix it
/// returns nothing until the follower is marked active.
test "D-snap-recentactive: no snapshot is sent to an inactive follower" {
  let r = snap_leader()
  let p = r.progress["2"]
  // Rewind the follower behind the snapshot baseline so it needs a snapshot.
  p.next_index = 11
  assert_false(p.is_active())
  // Inactive: nothing is shipped.
  assert_true(r.send_to("2") is None)
  // Once it answers (marked active), the snapshot goes out.
  p.mark_active()
  let m = r.send_to("2")
  assert_true(m is Some(_))
  assert_true(m.unwrap().kind() == "Snapshot")
}

///|
/// The guard is specific to snapshots: an inactive follower that only needs
/// ordinary entries (not a compacted prefix) is still sent an AppendEntries, so
/// the change does not stall normal replication.
test "D-snap-recentactive: an inactive follower still gets ordinary appends" {
  let r = snap_leader()
  let p = r.progress["2"]
  // next_index just past the snapshot baseline: an append, not a snapshot.
  p.next_index = 12
  assert_false(p.is_active())
  let m = r.send_to("2")
  assert_true(m is Some(_))
  assert_true(m.unwrap().kind() == "Probe" || m.unwrap().kind() == "Append")
}
