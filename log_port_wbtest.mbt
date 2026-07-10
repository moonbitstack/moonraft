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
// Ported from etcd-io/raft log_test.go. This port has no separate raftLog /
// unstable split — the log lives directly on the Node — so these tests run
// against the Node's log methods (term_at, find_conflict, find_conflict_by_term,
// is_up_to_date, match_term) and its AppendEntries handler (the maybeAppend
// equivalent), reproducing every applicable table case.

///|
/// Build a Node whose log is `ents`, over an optional snapshot baseline `snap`.
/// Entries are fed through the same replay path the recovery code uses.
fn log_node(snap : EntryId, ents : Array[Entry]) -> Node {
  let n = Node::new("n")
  if snap.index > 0 || snap.term > 0 {
    n.install_snapshot({
      last_index: snap.index,
      last_term: snap.term,
      data: b"",
    })
  }
  for e in ents {
    n.accept_entry(e)
  }
  n
}

///|
test "TestFindConflict" {
  let prev = [ent(1, 1), ent(2, 2), ent(3, 3)]
  fn conflict(ents : Array[Entry]) -> UInt64 {
    log_node(eid(0, 0), prev).find_conflict(ents[:])
  }

  // no conflict, empty ent
  assert_eq(conflict([]), 0)
  // no conflict
  assert_eq(conflict([ent(1, 1), ent(2, 2), ent(3, 3)]), 0)
  assert_eq(conflict([ent(2, 2), ent(3, 3)]), 0)
  assert_eq(conflict([ent(3, 3)]), 0)
  // no conflict, but has new entries
  assert_eq(
    conflict([ent(1, 1), ent(2, 2), ent(3, 3), ent(4, 4), ent(5, 4)]),
    4,
  )
  assert_eq(conflict([ent(2, 2), ent(3, 3), ent(4, 4), ent(5, 5)]), 4)
  assert_eq(conflict([ent(3, 3), ent(4, 4), ent(5, 4)]), 4)
  assert_eq(conflict([ent(4, 4), ent(5, 4)]), 4)
  // conflicts with existing entries
  assert_eq(conflict([ent(1, 4), ent(2, 4)]), 1)
  assert_eq(conflict([ent(2, 1), ent(3, 4), ent(4, 4)]), 2)
  assert_eq(conflict([ent(3, 1), ent(4, 2), ent(5, 4), ent(6, 4)]), 3)
}

///|
/// Reproduce one findConflictByTerm case: snapshot baseline is `ents[0]`, the
/// rest are appended. Assert both the returned index and that the returned term
/// equals the log's (zeroed) term at that index, exactly as etcd does.
fn check_fcbt(
  ents : Array[Entry],
  index : UInt64,
  term : UInt64,
  want : UInt64,
) -> Unit raise {
  let snap = eid(ents[0].index, ents[0].term)
  let tail : Array[Entry] = []
  for i in 1..<ents.length() {
    tail.push(ents[i])
  }
  let n = log_node(snap, tail)
  let (ci, ct) = n.find_conflict_by_term(index, term)
  assert_eq(ci, want)
  assert_eq(ct, n.term_at(want))
}

///|
test "TestFindConflictByTerm" {
  // Log starts from index 1 (snapshot at (0,0)).
  let a = [ent(0, 0), ent(1, 2), ent(2, 2), ent(3, 5), ent(4, 5), ent(5, 5)]
  check_fcbt(a, 100, 2, 100) // ErrUnavailable
  check_fcbt(a, 5, 6, 5)
  check_fcbt(a, 5, 5, 5)
  check_fcbt(a, 5, 4, 2)
  check_fcbt(a, 5, 2, 2)
  check_fcbt(a, 5, 1, 0)
  check_fcbt(a, 1, 2, 1)
  check_fcbt(a, 1, 1, 0)
  check_fcbt(a, 0, 0, 0)
  // Log with compacted entries (snapshot at index 10).
  let b = [
    ent(10, 3),
    ent(11, 3),
    ent(12, 3),
    ent(13, 4),
    ent(14, 4),
    ent(15, 4),
  ]
  check_fcbt(b, 30, 3, 30) // ErrUnavailable
  check_fcbt(b, 14, 9, 14)
  check_fcbt(b, 14, 4, 14)
  check_fcbt(b, 14, 3, 12)
  check_fcbt(b, 14, 2, 9)
  check_fcbt(b, 11, 5, 11)
  check_fcbt(b, 10, 5, 10)
  check_fcbt(b, 10, 3, 10)
  check_fcbt(b, 10, 2, 9)
  check_fcbt(b, 9, 2, 9) // ErrCompacted
  check_fcbt(b, 4, 2, 4) // ErrCompacted
  check_fcbt(b, 0, 0, 0) // ErrCompacted
}

///|
test "TestIsUpToDate" {
  let n = log_node(eid(0, 0), [ent(1, 1), ent(2, 2), ent(3, 3)])
  let last = n.last_log_index()
  // greater term, ignore lastIndex
  assert_true(n.is_up_to_date(eid(last - 1, 4)))
  assert_true(n.is_up_to_date(eid(last, 4)))
  assert_true(n.is_up_to_date(eid(last + 1, 4)))
  // smaller term, ignore lastIndex
  assert_false(n.is_up_to_date(eid(last - 1, 2)))
  assert_false(n.is_up_to_date(eid(last, 2)))
  assert_false(n.is_up_to_date(eid(last + 1, 2)))
  // equal term, equal or larger lastIndex wins
  assert_false(n.is_up_to_date(eid(last - 1, 3)))
  assert_true(n.is_up_to_date(eid(last, 3)))
  assert_true(n.is_up_to_date(eid(last + 1, 3)))
}

///|
test "TestMatchTerm" {
  // matchTerm is true iff the log holds the identified entry with that term.
  let n = log_node(eid(0, 0), [ent(1, 1), ent(2, 2), ent(3, 3)])
  assert_true(n.match_term(eid(1, 1)))
  assert_true(n.match_term(eid(2, 2)))
  assert_true(n.match_term(eid(3, 3)))
  // wrong term at a present index
  assert_false(n.match_term(eid(2, 3)))
  // index beyond the log has an unknown (zero) term
  assert_false(n.match_term(eid(4, 4)))
  assert_true(n.match_term(eid(4, 0)))
}

///|
test "TestTerm (log-level term values)" {
  // etcd's raftLog.term over a snapshot baseline plus appended entries. The
  // ErrCompacted/ErrUnavailable distinction lives at the Storage layer (see
  // TestStorageTerm); Node.term_at is the "zero term on out of bounds" view, so
  // here we assert the term values within range and the zero outside it.
  let offset : UInt64 = 100
  let ents : Array[Entry] = []
  // indices 101..199 carry terms 1..99
  let mut t : UInt64 = 1
  while t < 100 {
    ents.push(ent(offset + t, t))
    t = t + 1
  }
  let n = log_node(eid(offset, 1), ents)
  assert_eq(n.term_at(offset), 1) // snapshot baseline term
  assert_eq(n.term_at(offset + 50), 50)
  assert_eq(n.term_at(offset + 99), 99) // last
  assert_eq(n.term_at(offset - 1), 0) // compacted -> zero
  assert_eq(n.term_at(offset + 100), 0) // unavailable -> zero
}

///|
/// The maybeAppend equivalent: run an AppendEntries against a fresh log
/// `index(1).terms(1,2,3)` committed to 1, and report (accepted, match_index,
/// committed).
fn maybe_append(
  prev : EntryId,
  committed : UInt64,
  ents : Array[Entry],
) -> (Bool, UInt64, UInt64) {
  let n = log_node(eid(0, 0), [ent(1, 1), ent(2, 2), ent(3, 3)])
  n.advance_commit(1)
  let reply = n.handle_append_entries({
    term: 100,
    leader_id: "l",
    prev_log_index: prev.index,
    prev_log_term: prev.term,
    entries: ents,
    leader_commit: committed,
  })
  (reply.success, reply.match_index, n.commit_index)
}

///|
fn check_append(
  prev : EntryId,
  committed : UInt64,
  ents : Array[Entry],
  wok : Bool,
  wmatch : UInt64,
  wcommit : UInt64,
) -> Unit raise {
  let (ok, match_index, commit) = maybe_append(prev, committed, ents)
  assert_eq(ok, wok)
  assert_eq(match_index, wmatch)
  assert_eq(commit, wcommit)
}

///|
test "TestLogMaybeAppend" {
  // lastindex=3, lastterm=3, commit=1. Cases that this port's AppendEntries
  // handler shares with etcd's maybeAppend. (Two etcd cases — a probe below the
  // commit index, and a conflict with a committed entry — diverge: this port
  // adopts newer etcd's "accept the already-committed prefix outright" rule
  // instead of panicking; see PORTING.md.)

  // not match: term is different
  check_append(eid(3, 2), 3, [ent(4, 4)], false, 0, 1)
  // not match: index out of bound
  check_append(eid(4, 3), 3, [ent(5, 4)], false, 0, 1)
  // match with the last existing entry, no new entries
  check_append(eid(3, 3), 3, [], true, 3, 3)
  // do not increase commit higher than last new index
  check_append(eid(3, 3), 4, [], true, 3, 3)
  // commit up to the commit in the message
  check_append(eid(3, 3), 2, [], true, 3, 2)
  // commit does not decrease
  check_append(eid(3, 3), 0, [], true, 3, 1)
  // append one new entry
  check_append(eid(3, 3), 3, [ent(4, 4)], true, 4, 3)
  // append and advance commit
  check_append(eid(3, 3), 4, [ent(4, 4)], true, 4, 4)
  // do not increase commit higher than last new index
  check_append(eid(3, 3), 5, [ent(4, 4)], true, 4, 4)
  // append two new entries, commit follows
  check_append(eid(3, 3), 5, [ent(4, 4), ent(5, 4)], true, 5, 5)
  // match in the middle, conflicting tail replaced
  check_append(eid(2, 2), 3, [ent(3, 4)], true, 3, 3)
  check_append(eid(1, 1), 3, [ent(2, 4)], true, 2, 2)
  check_append(eid(1, 1), 3, [ent(2, 4), ent(3, 4)], true, 3, 3)
}

///|
test "TestCommitTo (monotonic advance)" {
  // etcd's commitTo never lowers the commit index (and panics on an index past
  // the log). This port's advance_commit provides the monotonic guarantee; the
  // out-of-range case is defended by being ignored rather than panicking.
  let n = log_node(eid(0, 0), [ent(1, 1), ent(2, 2), ent(3, 3)])
  n.advance_commit(2)
  n.advance_commit(3)
  assert_eq(n.commit_index, 3)
  n.advance_commit(1) // never decreases
  assert_eq(n.commit_index, 3)
}
