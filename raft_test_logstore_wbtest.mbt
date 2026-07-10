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
// raft_test.go rows whose assertions land on the log layer:
//   #30  TestCommit         — RaftLog::maybe_commit term-gate over a quorum index
//   #33  TestHandleMsgApp    — the follower AppendEntries table (log-matching /
//                              append / commit-advance / reject)

///|
/// The quorum-committed index for a set of match indices (etcd's
/// `tracker.Committed()`): the (n/2+1)-th largest match. Isolated here so the
/// test drives `RaftLog::maybe_commit` directly — the quorum arithmetic itself is
/// covered by the quorum tests.
fn quorum_mci(matches : Array[UInt64]) -> UInt64 {
  let s = matches.copy()
  s.sort()
  let n = s.length()
  s[n - 1 - n / 2]
}

///|
fn check_commit(
  matches : Array[UInt64],
  log_terms : Array[UInt64],
  sm_term : UInt64,
  w : UInt64,
) -> Unit raise {
  let storage = MemoryStorage::new()
  storage.append(ents_at(1, log_terms))
  let l = RaftLog::new(storage)
  // etcd's raft.maybeCommit: raftLog.maybeCommit(entryID{term: r.Term, index:
  // quorum-committed}). Commit advances only if that index's entry is from the
  // current term (§5.4.2).
  l.maybe_commit({ term: sm_term, index: quorum_mci(matches) }) |> ignore
  assert_eq(l.committed, w)
}

///|
test "TestCommit" {
  // single
  check_commit([1], [1], 1, 1)
  check_commit([1], [1], 2, 0)
  check_commit([2], [1, 2], 2, 2)
  check_commit([1], [2], 2, 1)
  // odd
  check_commit([2, 1, 1], [1, 2], 1, 1)
  check_commit([2, 1, 1], [1, 1], 2, 0)
  check_commit([2, 1, 2], [1, 2], 2, 2)
  check_commit([2, 1, 2], [1, 1], 2, 0)
  // even
  check_commit([2, 1, 1, 1], [1, 2], 1, 1)
  check_commit([2, 1, 1, 1], [1, 1], 2, 0)
  check_commit([2, 1, 1, 2], [1, 2], 1, 1)
  check_commit([2, 1, 1, 2], [1, 1], 2, 0)
  check_commit([2, 1, 2, 2], [1, 2], 2, 2)
  check_commit([2, 1, 2, 2], [1, 1], 2, 0)
}

///|
/// One TestHandleMsgApp case: a follower with log [1@t1, 2@t2] at term 2 handles
/// an AppendEntries and reports its resulting last index, commit index, and
/// whether it rejected. The message term is fixed at 2 (>= the follower's term):
/// this port folds the Step term-check into `handle_append_entries`, whereas
/// etcd's isolated `handleAppendEntries` ignores the term — and the term does not
/// affect the log-matching/append/commit property under test (etcd's cases that
/// use term 1 assert the same result).
fn check_handle_app(
  prev_index : UInt64,
  prev_term : UInt64,
  commit : UInt64,
  entries : Array[Entry],
  w_index : UInt64,
  w_commit : UInt64,
  w_reject : Bool,
) -> Unit raise {
  let f = Node::new("f")
  f.append(1, b"") |> ignore // 1@t1
  f.append(2, b"") |> ignore // 2@t2
  f.become_follower(2)
  let reply = f.handle_append_entries({
    term: 2,
    leader_id: "L",
    prev_log_index: prev_index,
    prev_log_term: prev_term,
    entries,
    leader_commit: commit,
  })
  assert_eq(f.last_log_index(), w_index)
  assert_eq(f.commit_index, w_commit)
  assert_eq(reply.success, !w_reject)
}

///|
test "TestHandleMsgApp" {
  // Ensure 1: prev-log mismatch / non-existent → reject, log & commit unchanged.
  check_handle_app(2, 3, 3, [], 2, 0, true) // previous log mismatch
  check_handle_app(3, 3, 3, [], 2, 0, true) // previous log non-exist
  // Ensure 2: accept, append, commit up to min(leaderCommit, lastNew).
  check_handle_app(1, 1, 1, [], 2, 1, false)
  check_handle_app(0, 0, 1, [Entry::normal(2, 1, b"")], 1, 1, false)
  check_handle_app(
    2,
    2,
    3,
    [Entry::normal(2, 3, b""), Entry::normal(2, 4, b"")],
    4,
    3,
    false,
  )
  check_handle_app(2, 2, 4, [Entry::normal(2, 3, b"")], 3, 3, false)
  check_handle_app(1, 1, 4, [Entry::normal(2, 2, b"")], 2, 2, false)
  // Ensure 3: commit up to the last new entry, never past the log.
  check_handle_app(1, 1, 3, [], 2, 1, false) // match entry 1, commit to last new 1
  check_handle_app(1, 1, 3, [Entry::normal(2, 2, b"")], 2, 2, false) // commit to last new 2
  check_handle_app(2, 2, 3, [], 2, 2, false) // match entry 2, commit to last new 2
  check_handle_app(2, 2, 4, [], 2, 2, false) // commit up to log.last()
}

///|
/// The follower's reject hint for a probe `(prev_index, prev_term)`, via the
/// actual `handle_append_entries` reject path.
fn fast_reject_hint(
  follower_terms : Array[UInt64],
  compact : UInt64,
  compact_term : UInt64,
  leader_term : UInt64,
  prev_index : UInt64,
  prev_term : UInt64,
) -> (UInt64, UInt64) raise {
  let f = Node::new("2")
  let mut idx : UInt64 = 0
  for t in follower_terms {
    idx = idx + 1
    f.append(t, b"") |> ignore
  }
  f.become_follower(leader_term)
  if compact > 0 {
    f.install_snapshot({
      last_index: compact,
      last_term: compact_term,
      data: b"",
      conf_state: ConfState::empty(),
    })
    // etcd deliberately keeps commit at 0 here (an unrealistic edge case), so the
    // probe is not short-circuited by the accept-committed-prefix path.
    f.commit_index = 0
  }
  let reply = f.handle_append_entries({
    term: leader_term,
    leader_id: "1",
    prev_log_index: prev_index,
    prev_log_term: prev_term,
    entries: [],
    leader_commit: 0,
  })
  assert_false(reply.success)
  (reply.conflict_index, reply.conflict_term)
}

///|
/// The leader's next append `(prev_index, prev_term)` after a reject with hint
/// `(reject_index, reject_term)`, via the actual `handle_append_resp` back-off.
fn fast_next_append(
  leader_terms : Array[UInt64],
  leader_term : UInt64,
  probe_index : UInt64,
  reject_index : UInt64,
  reject_term : UInt64,
) -> (UInt64, UInt64) {
  let r = RaftNode::new("1", ["2", "3"], pre_vote=false)
  for t in leader_terms {
    r.node().append(t, b"") |> ignore
  }
  r
  .node()
  .apply_hard_state({ term: leader_term - 1, vote: None, commit: probe_index })
  r.node().become_candidate()
  r.become_leader()
  r.progress["2"].next_index = probe_index + 1
  let msgs = r.step(
    Message::new(
      "2",
      "1",
      AppendResp({
        term: leader_term,
        success: false,
        match_index: 0,
        conflict_index: reject_index,
        conflict_term: reject_term,
      }),
    ),
  )
  for m in msgs {
    if m.to == "2" {
      match m.payload {
        Append(a) => return (a.prev_log_index, a.prev_log_term)
        _ => ()
      }
    }
  }
  abort("leader did not send an append to the follower")
}

///|
fn check_fast_log_rejection(
  leader_terms : Array[UInt64],
  follower_terms : Array[UInt64],
  compact : UInt64,
  compact_term : UInt64,
  reject_hint_term : UInt64,
  reject_hint_index : UInt64,
  next_append_term : UInt64,
  next_append_index : UInt64,
) -> Unit raise {
  let leader_term = leader_terms[leader_terms.length() - 1]
  let probe_index = leader_terms.length().to_uint64()
  let (ci, ct) = fast_reject_hint(
    follower_terms, compact, compact_term, leader_term, probe_index, leader_term,
  )
  assert_eq(ci, reject_hint_index)
  assert_eq(ct, reject_hint_term)
  let (ni, nt) = fast_next_append(
    leader_terms, leader_term, probe_index, reject_hint_index, reject_hint_term,
  )
  assert_eq(ni, next_append_index)
  assert_eq(nt, next_append_term)
}

///|
test "TestFastLogRejection" {
  check_fast_log_rejection(
    [1, 2, 2, 4, 4, 4, 4],
    [1, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3],
    0,
    0,
    3,
    7,
    2,
    3,
  )
  check_fast_log_rejection(
    [1, 2, 2, 3, 4, 4, 4, 5],
    [1, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3],
    0,
    0,
    3,
    8,
    3,
    4,
  )
  check_fast_log_rejection([1, 1, 1, 1], [1, 2, 2, 4], 0, 0, 1, 1, 1, 1)
  check_fast_log_rejection([1, 1, 1, 1, 1, 1], [1, 2, 2, 4], 0, 0, 1, 1, 1, 1)
  check_fast_log_rejection([1, 1, 1, 1], [1, 2, 2, 4, 4, 4], 0, 0, 1, 1, 1, 1)
  check_fast_log_rejection([1, 1, 1, 4, 5], [1, 1, 1, 4], 0, 0, 4, 4, 4, 4)
  check_fast_log_rejection(
    [2, 5, 5, 5, 5, 5, 5, 5, 5],
    [2, 4, 4, 4, 4, 4],
    0,
    0,
    4,
    6,
    2,
    1,
  )
  check_fast_log_rejection(
    [2, 2, 2, 2, 2],
    [2, 4, 4, 4, 4, 4, 4, 4],
    0,
    0,
    2,
    1,
    2,
    1,
  )
  check_fast_log_rejection([1, 1, 3], [1, 1, 3, 3, 3], 5, 3, 0, 3, 1, 2)
}
