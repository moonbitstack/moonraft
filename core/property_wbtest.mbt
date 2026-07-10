///|
test "a vote is refused below the current term" {
  let n = Node::new("n")
  n.become_follower(5)
  let reply = n.handle_request_vote({
    term: 4,
    candidate_id: "x",
    last_log_index: 9,
    last_log_term: 9,
  })
  assert_false(reply.vote_granted)
  assert_eq(reply.term, 5)
}

///|
test "the election restriction denies a candidate with an older last term" {
  let n = Node::new("n")
  // Local log ends at term 3; a candidate whose last term is 2 is behind even
  // with a longer log, so it must be refused (Raft §5.4.1).
  let _ = n.append(1, b"a")
  let _ = n.append(3, b"b")
  n.become_follower(4)
  let reply = n.handle_request_vote({
    term: 4,
    candidate_id: "x",
    last_log_index: 9,
    last_log_term: 2,
  })
  assert_false(reply.vote_granted)
}

///|
test "a divergent multi-term suffix is replaced wholesale" {
  let f = Node::new("f")
  let _ = f.append(1, b"a")
  let _ = f.append(2, b"b")
  let _ = f.append(2, b"c")
  // A new leader in term 3 overwrites everything after index 1.
  let reply = f.handle_append_entries({
    term: 3,
    leader_id: "L",
    prev_log_index: 1,
    prev_log_term: 1,
    entries: [
      Entry::normal(3, 2, b"x"),
      Entry::normal(3, 3, b"y"),
      Entry::normal(3, 4, b"z"),
    ],
    leader_commit: 0,
  })
  assert_true(reply.success)
  assert_eq(f.last_log_index(), 4)
  assert_eq(f.term_at(2), 3)
  assert_eq(f.term_at(3), 3)
  assert_eq(f.term_at(4), 3)
}

///|
test "a heartbeat carries the commit index forward" {
  let f = Node::new("f")
  let _ = f.handle_append_entries({
    term: 1,
    leader_id: "L",
    prev_log_index: 0,
    prev_log_term: 0,
    entries: [Entry::normal(1, 1, b"a"), Entry::normal(1, 2, b"b")],
    leader_commit: 0,
  })
  assert_eq(f.commit_index, 0)
  // An empty AppendEntries (a heartbeat) advances the commit index.
  let reply = f.handle_append_entries({
    term: 1,
    leader_id: "L",
    prev_log_index: 2,
    prev_log_term: 1,
    entries: [],
    leader_commit: 2,
  })
  assert_true(reply.success)
  assert_eq(f.commit_index, 2)
}

///|
test "storage slice clamps to the stored range" {
  let s = MemoryStorage::new()
  s.append([
    Entry::normal(1, 1, b"a"),
    Entry::normal(1, 2, b"b"),
    Entry::normal(1, 3, b"c"),
  ])
  assert_eq(s.slice(0, 100).length(), 3)
  assert_eq(s.slice(5, 9).length(), 0)
  assert_eq(s.slice(2, 3).length(), 1)
}

///|
test "replaying a write-ahead log twice yields the same state" {
  let a = Node::new("a")
  a.become_candidate()
  let _ = a.append(1, b"x")
  let _ = a.append(1, b"y")
  a.advance_commit(2)
  let wal = MemWal::new()
  a.save_to(wal)
  let records = wal.load()
  let b = Node::new("a")
  b.replay(records)
  b.replay(records)
  assert_eq(b.last_log_index(), 2)
  assert_eq(b.current_term(), 1)
  assert_eq(b.commit_index, 2)
  assert_true(b.voted_for == Some("a"))
}

///|
test "distinct committed values land in the same order everywhere" {
  let c = Cluster::new(["a", "b", "c"], seed=12)
  let _ = c.run_until_leader(200)
  let _ = c.propose(b"v1")
  let _ = c.propose(b"v2")
  let _ = c.propose(b"v3")
  assert_true(c.run_until_committed(4, 300))
  assert_true(c.same_committed_commands())
  let n = c.node("a").node()
  match (n.entry_at(2), n.entry_at(3), n.entry_at(4)) {
    (Some(e2), Some(e3), Some(e4)) => {
      assert_true(e2.command == b"v1")
      assert_true(e3.command == b"v2")
      assert_true(e4.command == b"v3")
    }
    _ => abort("committed entries 2..4 should all be present")
  }
}

///|
test "consensus keeps its committed prefix identical under loss and delay" {
  let c = Cluster::new(["a", "b", "c", "d", "e"], seed=21)
  c.set_drop(150)
  c.set_delay(3)
  let leader = c.run_until_leader(800)
  assert_true(leader != None)
  let _ = c.propose(b"x1")
  let _ = c.propose(b"x2")
  let _ = c.run_until_committed(3, 1500)
  assert_true(c.same_committed_commands())
  assert_true(c.one_leader_per_term())
  assert_true(c.logs_consistent())
}
