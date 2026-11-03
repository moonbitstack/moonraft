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
test "a message exposes its route, term and kind" {
  let m = Message::new(
    "a",
    "b",
    Vote({ term: 3, candidate_id: "a", last_log_index: 2, last_log_term: 1, }),
  )
  assert_eq(m.from, "a")
  assert_eq(m.to, "b")
  assert_eq(m.term(), 3)
  assert_eq(m.kind(), "Vote")
  assert_false(m.is_response())
}

///|
test "a heartbeat and an empty-append probe are distinct kinds" {
  let hb = Message::new(
    "L",
    "f",
    Heartbeat({ term: 4, leader_id: "L", commit: 1, context: b"", }),
  )
  assert_eq(hb.kind(), "Heartbeat")
  assert_false(hb.is_response())
  // An empty AppendEntries is now a probe, not a heartbeat.
  let probe = Message::new(
    "L",
    "f",
    Append({
      term: 4,
      leader_id: "L",
      prev_log_index: 1,
      prev_log_term: 1,
      entries: [],
      leader_commit: 1,
    }),
  )
  assert_eq(probe.kind(), "Probe")
  let app = Message::new(
    "L",
    "f",
    Append({
      term: 4,
      leader_id: "L",
      prev_log_index: 1,
      prev_log_term: 1,
      entries: [Entry::normal(4, 2, b"x")],
      leader_commit: 1,
    }),
  )
  assert_eq(app.kind(), "Append")
}

///|
test "a read-index request reports its kind" {
  let m = Message::new("f", "L", ReadIndex(b"ctx"))
  assert_eq(m.kind(), "ReadIndex")
  assert_false(m.is_response())
}

///|
test "a read-index response reports its kind" {
  let m = Message::new(
    "L",
    "f",
    ReadIndexResp({ term: 1, index: 3, context: b"ctx", }),
  )
  assert_eq(m.kind(), "ReadIndexResp")
}

///|
test "responses are recognised" {
  let r = Message::new(
    "f",
    "L",
    AppendResp({
      term: 4,
      success: true,
      match_index: 3,
      conflict_index: 0,
      conflict_term: 0,
      reject_index: 0,
    }),
  )
  assert_true(r.is_response())
  assert_eq(r.term(), 4)
  assert_eq(r.kind(), "AppendResp")
}

///|
/// The name a log line uses for a message: the protobuf type name, so a reader
/// comparing a trace against a script sees the same words in both.
test "type_name names every payload kind" {
  let entry = Entry::normal(1UL, 1UL, b"")
  let vote = {
    term: 1UL,
    candidate_id: "1",
    last_log_index: 0UL,
    last_log_term: 0UL,
  }
  let reply = { term: 1UL, vote_granted: true, }
  let append = {
    term: 1UL,
    leader_id: "1",
    prev_log_index: 0UL,
    prev_log_term: 0UL,
    entries: [],
    leader_commit: 0UL,
  }
  let app_reply = {
    term: 1UL,
    success: true,
    match_index: 0UL,
    conflict_index: 0UL,
    conflict_term: 0UL,
    reject_index: 0UL,
  }
  let beat = { term: 1UL, leader_id: "1", commit: 0UL, context: b"", }
  let beat_reply = { term: 1UL, context: b"", }
  let snap = InstallSnapshotArgs::whole(1UL, "1", {
    last_index: 0UL,
    last_term: 0UL,
    data: b"",
    conf_state: ConfState::empty(),
  })
  assert_eq(Message::new("1", "2", PreVote(vote)).type_name(), "MsgPreVote")
  assert_eq(
    Message::new("1", "2", PreVoteResp(reply)).type_name(),
    "MsgPreVoteResp",
  )
  assert_eq(Message::new("1", "2", Vote(vote)).type_name(), "MsgVote")
  assert_eq(Message::new("1", "2", VoteResp(reply)).type_name(), "MsgVoteResp")
  assert_eq(Message::new("1", "2", Append(append)).type_name(), "MsgApp")
  assert_eq(
    Message::new("1", "2", AppendResp(app_reply)).type_name(),
    "MsgAppResp",
  )
  assert_eq(Message::new("1", "2", Heartbeat(beat)).type_name(), "MsgHeartbeat")
  assert_eq(
    Message::new("1", "2", HeartbeatResp(beat_reply)).type_name(),
    "MsgHeartbeatResp",
  )
  assert_eq(Message::new("1", "2", Snapshot(snap)).type_name(), "MsgSnap")
  assert_eq(
    Message::new("1", "2", TimeoutNow(1UL)).type_name(),
    "MsgTimeoutNow",
  )
  assert_eq(
    Message::new(
      "1",
      "2",
      ReadIndexResp({ term: 1UL, index: 0UL, context: b"", }),
    ).type_name(),
    "MsgReadIndexResp",
  )
  assert_eq(Message::new("1", "2", Propose([entry])).type_name(), "MsgProp")
  assert_eq(Message::new("1", "2", ReadIndex(b"")).type_name(), "MsgReadIndex")
  assert_eq(
    Message::new("1", "2", TransferLeader("2")).type_name(),
    "MsgTransferLeader",
  )
  assert_eq(Message::new("1", "2", ForgetLeader).type_name(), "MsgForgetLeader")
}
