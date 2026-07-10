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

// Pins the Progress-side machinery that the ③-path replication loop will wire in
// the 0.4.0 Config pass: the `sent_commit` in-flight-commit tracker (etcd
// CanBumpCommit / SentCommit and the BecomeProbe/BecomeSnapshot/MaybeDecrTo
// regressions) and the `MaxInflightBytes` byte budget threaded Progress->
// Inflights. These exercise every staged member so it is not dead ("parameter
// orphan") code even before the send loop calls it. etcd verifies this behaviour
// only through raft-level integration tests (raft_test.go), so these are
// contract unit tests against the progress.go source semantics, not a port of an
// upstream progress_test.go case.

///|
test "sent_commit: fresh progress is zero; note/can_bump per CanBumpCommit" {
  let p = Progress::new(10) // match=0, next=10, sent_commit=0
  assert_eq(p.sent_commit, 0)
  // Nothing sent yet: bumping to 5 can help (0 < next-1 = 9).
  assert_true(p.can_bump_commit(5))
  p.note_commit_sent(5)
  assert_eq(p.sent_commit, 5)
  // Already sent 5: re-sending 5 or less cannot advance the follower.
  assert_false(p.can_bump_commit(5))
  assert_false(p.can_bump_commit(4))
  // 6 helps: 6 > 5 and 5 < 9.
  assert_true(p.can_bump_commit(6))
  // Once the in-flight commit reaches next-1, no further eager bump helps.
  p.note_commit_sent(9)
  assert_false(p.can_bump_commit(10))
}

///|
test "sent_commit: become_snapshot pins it to the snapshot index" {
  let p = Progress::new(5)
  p.note_commit_sent(3)
  p.become_snapshot(10)
  // In Snapshot, sent_commit == pending == next-1.
  assert_eq(p.sent_commit, 10)
  assert_eq(p.next_index, 11)
  assert_eq(p.pending_snapshot, 10)
  assert_false(p.can_bump_commit(20))
}

///|
test "sent_commit: become_probe regresses it to next-1" {
  let p = Progress::new(1)
  p.become_replicate() // next = match+1 = 1
  p.optimistic_advance(20) // stream far ahead: next = 21
  p.note_commit_sent(15) // 15 < next-1 = 20
  p.become_probe() // from Replicate: next = match+1 = 1
  assert_eq(p.next_index, 1)
  // sent_commit regressed to min(15, next-1 = 0) = 0.
  assert_eq(p.sent_commit, 0)
}

///|
test "sent_commit: maybe_decr_to regresses it (both branches)" {
  // Replicate branch.
  let a = Progress::new(1)
  a.match_index = 5
  a.next_index = 20
  a.state = Replicate
  a.note_commit_sent(15)
  assert_true(a.maybe_decr_to(19, 19))
  assert_eq(a.next_index, 6) // match+1
  assert_eq(a.sent_commit, 5) // min(15, next-1 = 5)

  // Probe branch.
  let b = Progress::new(1)
  b.match_index = 0
  b.next_index = 20
  b.state = Probe
  b.note_commit_sent(15)
  assert_true(b.maybe_decr_to(19, 2)) // next-1 = 19 == rejected
  assert_eq(b.next_index, 3) // max(min(19,3), 1)
  assert_eq(b.sent_commit, 2) // min(15, next-1 = 2)
}

///|
test "MaxInflightBytes: the byte budget throttles replication" {
  // A generous message-count window but a tight 400-byte budget.
  let p = Progress::new(1, max_inflight=100, max_inflight_bytes=400)
  p.become_replicate()
  // First 300-byte batch: 300 < 400, not paused.
  p.sent_entries(1, true, bytes=300)
  assert_false(p.is_paused())
  // Second 300-byte batch crosses the soft budget (600 >= 400) → paused, even
  // though only 2 of 100 message slots are used.
  p.sent_entries(2, true, bytes=300)
  assert_true(p.is_paused())
  // Freeing the acked prefix releases the byte budget again.
  p.free_le(2)
  assert_false(p.inflights.full())
}

///|
test "Changer threads MaxInflightBytes into each new progress" {
  let c = Changer::new(max_inflight_bytes=512)
  assert_true(c.simple([("v", "1")]) is None)
  assert_eq(c.prs["1"].inflights.max_bytes, 512)
}
