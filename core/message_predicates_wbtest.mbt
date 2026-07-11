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
//
// Ported from etcd-io/raft util_test.go. TestIsLocalMsg / TestIsResponseMsg are
// re-expressed over this port's typed `Payload` (there is no flat MessageType
// enum, and the node-local trigger messages have no Payload here).

///|
test "TestLimitSize" {
  let ents = [
    Entry::normal(4, 4, b""),
    Entry::normal(5, 5, b""),
    Entry::normal(6, 6, b""),
  ]
  let s0 = entry_encoding_size(ents[0])
  let s1 = entry_encoding_size(ents[1])
  let s2 = entry_encoding_size(ents[2])
  // {math.MaxUint64, all entries}
  check_limit(ents, no_limit, 3)
  // {0, first entry only}
  check_limit(ents, 0, 1)
  // limit to 2
  check_limit(ents, s0 + s1, 2)
  check_limit(ents, s0 + s1 + s2 / 2, 2)
  check_limit(ents, s0 + s1 + s2 - 1, 2)
  // all
  check_limit(ents, s0 + s1 + s2, 3)
}

///|
/// Assert `limit_size` returns exactly `want_len` entries and honours the
/// "at least one, else within budget" property.
fn check_limit(
  ents : Array[Entry],
  max_size : UInt64,
  want_len : Int,
) -> Unit raise {
  let got = limit_size(ents[:], max_size)
  assert_eq(got.length(), want_len)
  assert_entries(got, ents[0:want_len].to_owned())
  assert_true(got.length() == 1 || ents_size(got[:]) <= max_size)
}

///|
test "TestIsLocalMsg (equivalent: every transportable Payload is non-local)" {
  // etcd's node-local messages — MsgHup, MsgBeat, MsgSnapStatus, MsgCheckQuorum
  // and the storage-thread messages — have no representation in this port: those
  // triggers are direct method calls (tick / campaign / step), not `Payload`s.
  // So the "is local" predicate is false for every Payload, by construction.
  let vote_args : RequestVoteArgs = {
    term: 1,
    candidate_id: "a",
    last_log_index: 0,
    last_log_term: 0,
  }
  let vote_reply : RequestVoteReply = { term: 1, vote_granted: true }
  let app_args : AppendEntriesArgs = {
    term: 1,
    leader_id: "a",
    prev_log_index: 0,
    prev_log_term: 0,
    entries: [],
    leader_commit: 0,
  }
  let app_reply : AppendEntriesReply = {
    term: 1,
    success: true,
    match_index: 0,
    conflict_index: 0,
    conflict_term: 0,
    reject_index: 0,
  }
  let snap_args : InstallSnapshotArgs = {
    term: 1,
    leader_id: "a",
    last_index: 0,
    last_term: 0,
    offset: 0,
    data: b"",
    done: true,
    conf_state: ConfState::empty(),
  }
  let payloads : Array[Payload] = [
    PreVote(vote_args),
    PreVoteResp(vote_reply),
    Vote(vote_args),
    VoteResp(vote_reply),
    Append(app_args),
    AppendResp(app_reply),
    Snapshot(snap_args),
    TimeoutNow(1),
  ]
  for p in payloads {
    assert_false(p.is_local())
  }
}

///|
test "TestIsResponseMsg" {
  // Requests are not responses; the paired *Resp payloads are.
  let vote_args : RequestVoteArgs = {
    term: 1,
    candidate_id: "a",
    last_log_index: 0,
    last_log_term: 0,
  }
  let vote_reply : RequestVoteReply = { term: 1, vote_granted: true }
  let app_args : AppendEntriesArgs = {
    term: 1,
    leader_id: "a",
    prev_log_index: 0,
    prev_log_term: 0,
    entries: [],
    leader_commit: 0,
  }
  let app_reply : AppendEntriesReply = {
    term: 1,
    success: true,
    match_index: 0,
    conflict_index: 0,
    conflict_term: 0,
    reject_index: 0,
  }
  let snap_args : InstallSnapshotArgs = {
    term: 1,
    leader_id: "a",
    last_index: 0,
    last_term: 0,
    offset: 0,
    data: b"",
    done: true,
    conf_state: ConfState::empty(),
  }
  fn resp(p : Payload) -> Bool {
    Message::new("a", "b", p).is_response()
  }

  assert_false(resp(PreVote(vote_args)))
  assert_true(resp(PreVoteResp(vote_reply)))
  assert_false(resp(Vote(vote_args)))
  assert_true(resp(VoteResp(vote_reply)))
  assert_false(resp(Append(app_args)))
  // A follower's InstallSnapshot reply travels as an AppendResp, covered above.
  assert_true(resp(AppendResp(app_reply)))
  assert_false(resp(Snapshot(snap_args)))
  assert_false(resp(TimeoutNow(1)))
}

///|
test "TestDescribeEntry" {
  // "hello\x00world" with an embedded NUL byte.
  let data = Bytes::from_array([
    b'h', b'e', b'l', b'l', b'o', 0, b'w', b'o', b'r', b'l', b'd',
  ])
  let entry = Entry::normal(1, 2, data)
  assert_eq(describe_entry(entry, None), "1/2 EntryNormal \"hello\\x00world\"")
  let upper : (Bytes) -> String = fn(bs) {
    let buf = StringBuilder::new()
    for i in 0..<bs.length() {
      let b = bs[i].to_int()
      let c = if b >= 0x61 && b <= 0x7a { b - 32 } else { b }
      buf.write_char(c.unsafe_to_char())
    }
    buf.to_string()
  }
  assert_eq(
    describe_entry(entry, Some(upper)),
    "1/2 EntryNormal HELLO\u{0}WORLD",
  )
}

///|
test "TestPayloadSizeOfEmptyEntry" {
  // An empty-payload entry contributes zero to the uncommitted-log quota.
  assert_eq(payload_size(Entry::normal(0, 0, b"")), 0)
}
