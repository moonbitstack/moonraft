///|
test "a successful append reports the follower's match index" {
  let node = Node::new("n1")
  let reply = node.handle_append_entries({
    term: 1,
    leader_id: "L",
    prev_log_index: 0,
    prev_log_term: 0,
    entries: [Entry::normal(1, 1, b"a"), Entry::normal(1, 2, b"b")],
    leader_commit: 0,
  })
  assert_true(reply.success)
  assert_eq(reply.match_index, 2)
  assert_eq(reply.conflict_index, 0)
}

///|
test "a gap past the log hints one past the last index" {
  let node = Node::new("n1")
  let _ = node.append(1, b"a")
  let reply = node.handle_append_entries({
    term: 1,
    leader_id: "L",
    prev_log_index: 5,
    prev_log_term: 1,
    entries: [],
    leader_commit: 0,
  })
  assert_false(reply.success)
  assert_eq(reply.conflict_index, 2)
}

///|
test "a term mismatch hints the start of the conflicting run" {
  let node = Node::new("n1")
  // Three entries of term 2 at indices 1..3, then a leader probes at index 3
  // expecting term 5: the hint should point at the start of the term-2 run.
  let _ = node.append(2, b"a")
  let _ = node.append(2, b"b")
  let _ = node.append(2, b"c")
  node.become_follower(6)
  let reply = node.handle_append_entries({
    term: 6,
    leader_id: "L",
    prev_log_index: 3,
    prev_log_term: 5,
    entries: [],
    leader_commit: 0,
  })
  assert_false(reply.success)
  assert_eq(reply.conflict_index, 1)
}
