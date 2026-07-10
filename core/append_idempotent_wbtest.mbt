///|
test "re-applying the same AppendEntries is idempotent" {
  let f = Node::new("f")
  let args : AppendEntriesArgs = {
    term: 1,
    leader_id: "L",
    prev_log_index: 0,
    prev_log_term: 0,
    entries: [Entry::normal(1, 1, b"a"), Entry::normal(1, 2, b"b")],
    leader_commit: 2,
  }
  let r1 = f.handle_append_entries(args)
  let r2 = f.handle_append_entries(args)
  assert_true(r1.success)
  assert_true(r2.success)
  assert_eq(f.last_log_index(), 2)
  assert_eq(f.term_at(2), 1)
}

///|
test "a vote is granted again to the same candidate in a term" {
  let n = Node::new("n")
  let args : RequestVoteArgs = {
    term: 1,
    candidate_id: "c",
    last_log_index: 0,
    last_log_term: 0,
  }
  assert_true(n.handle_request_vote(args).vote_granted)
  // A retransmitted request from the same candidate is granted again.
  assert_true(n.handle_request_vote(args).vote_granted)
}

///|
test "a leader append steps a candidate back down to follower" {
  let n = Node::new("n")
  n.become_candidate()
  assert_true(n.role() == Candidate)
  let reply = n.handle_append_entries({
    term: n.current_term(),
    leader_id: "L",
    prev_log_index: 0,
    prev_log_term: 0,
    entries: [],
    leader_commit: 0,
  })
  assert_true(reply.success)
  assert_true(n.role() == Follower)
}

///|
test "the write-ahead log preserves a configuration entry's type" {
  let a = Node::new("a")
  let _ = a.append(1, b"x")
  let _ = a.propose_conf_change(1, ConfChange::add("s2"))
  let wal = MemWal::new()
  a.save_to(wal)
  let b = Node::new("a")
  b.recover_from(wal)
  match b.entry_at(2) {
    Some(e) => assert_true(e.is_conf_change())
    None => abort("the configuration entry was lost on replay")
  }
}

///|
test "message term and kind cover snapshot and timeout-now" {
  let snap = Message::new(
    "L",
    "f",
    Snapshot({
      term: 4,
      leader_id: "L",
      last_index: 9,
      last_term: 3,
      offset: 0,
      data: b"",
      done: true,
      conf_state: ConfState::empty(),
    }),
  )
  assert_eq(snap.term(), 4)
  assert_eq(snap.kind(), "Snapshot")
  assert_false(snap.is_response())
  // A follower answers an InstallSnapshot with an AppendResp, so the snapshot
  // reply is classified as a response through that payload.
  let sr = Message::new(
    "f",
    "L",
    AppendResp({
      term: 4,
      success: true,
      match_index: 9,
      conflict_index: 0,
      conflict_term: 0,
    }),
  )
  assert_true(sr.is_response())
  let tn = Message::new("L", "f", TimeoutNow(4))
  assert_eq(tn.term(), 4)
  assert_eq(tn.kind(), "TimeoutNow")
}

///|
test "entering joint consensus twice keeps the latest incoming set" {
  let m = Membership::new(["a", "b", "c"])
  m.enter_joint(["c", "d"])
  m.enter_joint(["d", "e"])
  assert_true(m.is_joint())
  assert_true(m.contains("e"))
  assert_true(m.contains("d"))
}

///|
test "snapshot progress folds into a later acknowledgement" {
  let p = Progress::new(1)
  p.become_snapshot(6)
  assert_true(p.state == Snapshot)
  assert_eq(p.next_index, 7)
  assert_true(p.maybe_update(6))
  assert_eq(p.match_index, 6)
}

///|
test "log matching succeeds against a compacted prev index" {
  let f = Node::new("f")
  let _ = f.append(1, b"a")
  let _ = f.append(1, b"b")
  let _ = f.append(2, b"c")
  f.advance_commit(3)
  let _ = f.compact(2, b"snap")
  // A leader probes at the snapshot baseline (index 2, term 1); it must match.
  let reply = f.handle_append_entries({
    term: 2,
    leader_id: "L",
    prev_log_index: 2,
    prev_log_term: 1,
    entries: [Entry::normal(2, 3, b"c")],
    leader_commit: 3,
  })
  assert_true(reply.success)
  assert_eq(f.last_log_index(), 3)
}

///|
test "entries_after_limited with a zero cap returns nothing" {
  let a = Node::new("a")
  let _ = a.append(1, b"x")
  let _ = a.append(1, b"y")
  assert_eq(a.entries_after_limited(0, 0).length(), 0)
}

///|
test "voters of a simple configuration are just its members" {
  let m = Membership::new(["a", "b", "c"])
  let v = m.voters()
  assert_eq(v.length(), 3)
  assert_true(v.contains("a"))
  assert_false(v.contains("z"))
}

///|
test "the commit index never moves backwards" {
  let n = Node::new("n")
  n.advance_commit(5)
  n.advance_commit(3)
  assert_eq(n.commit_index, 5)
  n.advance_commit(7)
  assert_eq(n.commit_index, 7)
}

///|
test "storage records the snapshot it creates" {
  let s = MemoryStorage::new()
  s.append([Entry::normal(1, 1, b"a"), Entry::normal(2, 2, b"b")])
  let _ = s.create_snapshot(2, b"payload")
  let snap = s.storage_snapshot()
  assert_eq(snap.last_index, 2)
  assert_eq(snap.last_term, 2)
  assert_false(snap.is_empty())
}
