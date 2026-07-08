///|
test "pre-vote is granted to an up-to-date higher-term candidate, term intact" {
  let f = RaftNode::new("f", ["a"], seed=1)
  let _ = f.step(
    Message::new(
      "a",
      "f",
      Append({
        term: 2,
        leader_id: "a",
        prev_log_index: 0,
        prev_log_term: 0,
        entries: [Entry::normal(2, 1, b"x")],
        leader_commit: 0,
      }),
    ),
  )
  let out = f.step(
    Message::new(
      "cand",
      "f",
      PreVote({
        term: 3,
        candidate_id: "cand",
        last_log_index: 1,
        last_log_term: 2,
      }),
    ),
  )
  assert_eq(f.term(), 2)
  match out[0].payload {
    PreVoteResp(r) => assert_true(r.vote_granted)
    _ => abort("expected a pre-vote response")
  }
}

///|
test "pre-vote is refused to a candidate whose log is behind" {
  let f = RaftNode::new("f", ["a"], seed=1)
  let _ = f.step(
    Message::new(
      "a",
      "f",
      Append({
        term: 2,
        leader_id: "a",
        prev_log_index: 0,
        prev_log_term: 0,
        entries: [Entry::normal(2, 1, b"x"), Entry::normal(2, 2, b"y")],
        leader_commit: 0,
      }),
    ),
  )
  let out = f.step(
    Message::new(
      "cand",
      "f",
      PreVote({
        term: 3,
        candidate_id: "cand",
        last_log_index: 1,
        last_log_term: 1,
      }),
    ),
  )
  assert_eq(f.term(), 2)
  match out[0].payload {
    PreVoteResp(r) => assert_false(r.vote_granted)
    _ => abort("expected a pre-vote response")
  }
}

///|
test "installing an older snapshot never lowers the commit index" {
  let a = Node::new("a")
  a.install_snapshot({ last_index: 10, last_term: 5, data: b"" })
  assert_eq(a.commit_index, 10)
  let _ = a.handle_install_snapshot({
    term: 5,
    leader_id: "L",
    last_index: 4,
    last_term: 2,
    offset: 0,
    data: b"",
    done: true,
  })
  assert_eq(a.commit_index, 10)
  assert_eq(a.snapshot_index, 10)
}

///|
test "storage append after compaction extends the log" {
  let s = MemoryStorage::new()
  s.append([
    Entry::normal(1, 1, b"a"),
    Entry::normal(1, 2, b"b"),
    Entry::normal(2, 3, b"c"),
    Entry::normal(2, 4, b"d"),
  ])
  assert_true(s.compact(2))
  s.append([Entry::normal(3, 5, b"e")])
  assert_eq(s.first_index(), 3)
  assert_eq(s.last_index(), 5)
  assert_true(s.storage_term(5) == Some(3))
  assert_true(s.storage_term(1) == None)
}

///|
test "an empty grant set is never a majority" {
  let m = Membership::new(["a", "b", "c"])
  assert_false(m.has_majority([]))
  assert_false(m.has_majority(["a"]))
  assert_true(m.has_majority(["a", "b"]))
}

///|
test "after leaving joint consensus the incoming set decides alone" {
  let j = Membership::new(["a", "b", "c"])
  j.enter_joint(["c", "d", "e"])
  j.leave_joint()
  assert_false(j.is_joint())
  assert_true(j.has_majority(["c", "d"]))
  assert_false(j.has_majority(["c"]))
  assert_false(j.contains("a"))
}

///|
test "entries_after works across a snapshot baseline" {
  let a = Node::new("a")
  let _ = a.append(1, b"a")
  let _ = a.append(1, b"b")
  let _ = a.append(2, b"c")
  let _ = a.append(2, b"d")
  a.advance_commit(4)
  let _ = a.compact(2, b"snap")
  let tail = a.entries_after(2)
  assert_eq(tail.length(), 2)
  assert_eq(tail[0].index, 3)
  assert_eq(tail[1].index, 4)
  // Nothing physically remains at or before the baseline.
  assert_eq(a.entries_after(4).length(), 0)
}

///|
test "a configuration change round-trips a multi-character id" {
  let change = ConfChange::add("server-42")
  match ConfChange::decode(change.encode()) {
    Some(cc) => {
      assert_eq(cc.node_id, "server-42")
      assert_true(cc.change_type == AddNode)
    }
    None => abort("multi-character id failed to decode")
  }
}
