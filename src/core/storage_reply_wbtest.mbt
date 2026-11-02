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
/// A storage thread's reply is a routable message: it names the write it
/// acknowledges and the term that write was issued under, so a reply arriving
/// after a term change is identifiable as stale. The core has no cursors to move
/// (the Ready driver owns them), so stepping one there is a no-op rather than a
/// routing error.
test "storage replies name their write and are inert in the core" {
  let ack = Message::new(
    "AppendThread",
    "1",
    StorageAppendResp({ index: 12UL, log_term: 1UL, term: 2UL, }),
  )
  let applied = Message::new(
    "ApplyThread",
    "1",
    StorageApplyResp({ entries: [Entry::normal(1UL, 12UL, b"x")], }),
  )
  assert_eq(ack.type_name(), "MsgStorageAppendResp")
  assert_eq(applied.type_name(), "MsgStorageApplyResp")
  assert_eq(ack.kind(), "StorageAppendResp")
  assert_eq(applied.kind(), "StorageApplyResp")
  // The append reply carries the term of the write; committed entries are
  // term-independent, so the apply reply carries none.
  assert_eq(ack.term(), 2UL)
  assert_eq(applied.term(), 0UL)
  let r = RaftNode::new("1", [], pre_vote=false)
  assert_eq(r.step(ack).length(), 0)
  assert_eq(r.step(applied).length(), 0)
  // Stepping them changed nothing about the node's state.
  assert_eq(r.term(), 0UL)
  assert_eq(r.node().last_log_index(), 0UL)
}

///|
/// A leader that is told a follower is unreachable says so, and the line names
/// the follower's progress (`StateReplicate match=… next=…`) — the state the
/// report just changed.
test "an unreachable follower is reported with its progress" {
  let log = CapturingLogger::new()
  let r = RaftNode::new("1", ["2"], pre_vote=false, logger=log)
  r.campaign() |> ignore
  r.step(Message::new("2", "1", VoteResp({ term: 1UL, vote_granted: true, })))
  |> ignore
  assert_true(r.is_leader())
  r.report_unreachable("2")
  let lines = log.messages()
  let mut found = false
  for line in lines {
    if line.contains("failed to send message to 2 because it is unreachable") {
      found = true
      assert_true(line.contains("StateProbe match=0 next=1"))
    }
  }
  assert_true(found)
}

///|
/// The progress string a leader logs for a follower covers every state and every
/// flag: the replication state, the match/next pair, and then only the
/// annotations that apply (learner, paused, a pending snapshot, an in-flight
/// window and whether it is full).
test "progress text names every follower state" {
  // Streaming, with the flow-control window filled by one append.
  let replicate = Progress::new(6UL, max_inflight=1)
  replicate.match_index = 5UL
  replicate.state = Replicate
  replicate.inflights.add(5UL, 1UL)
  assert_eq(
    progress_text(replicate),
    // A full window pauses the flow, and a follower not heard from is inactive.
    "StateReplicate match=5 next=6 paused inactive inflight=1[full]",
  )
  // A learner that has not been heard from.
  let learner = Progress::new(3UL)
  learner.is_learner = true
  learner.recent_active = false
  assert_eq(
    progress_text(learner),
    "StateProbe match=0 next=3 learner inactive",
  )
  // A follower being caught up by a snapshot: replication is paused until it
  // reconnects past the snapshot.
  let snap = Progress::new(9UL)
  snap.state = Snapshot
  snap.pending_snapshot = 8UL
  assert_eq(
    progress_text(snap),
    "StateSnapshot match=0 next=9 paused pendingSnap=8 inactive",
  )
}
