///|
test "grants a vote to an up-to-date candidate in a new term" {
  let node = Node::new("n1")
  let reply = node.handle_request_vote({
    term: 1,
    candidate_id: "n2",
    last_log_index: 0,
    last_log_term: 0,
  })
  assert_true(reply.vote_granted)
  assert_eq(reply.term, 1)
  assert_eq(node.current_term(), 1)
}

///|
test "rejects a candidate from a stale term" {
  let node = Node::new("n1")
  node.become_follower(5)
  let reply = node.handle_request_vote({
    term: 3,
    candidate_id: "n2",
    last_log_index: 0,
    last_log_term: 0,
  })
  assert_false(reply.vote_granted)
  assert_eq(reply.term, 5)
}

///|
test "will not vote twice for different candidates in one term" {
  let node = Node::new("n1")
  let _ = node.handle_request_vote({
    term: 1,
    candidate_id: "n2",
    last_log_index: 0,
    last_log_term: 0,
  })
  let reply = node.handle_request_vote({
    term: 1,
    candidate_id: "n3",
    last_log_index: 0,
    last_log_term: 0,
  })
  assert_false(reply.vote_granted)
}

///|
test "denies a vote to a candidate whose log is behind" {
  let node = Node::new("n1")
  let _ = node.append(1, b"a")
  node.become_follower(2)
  let reply = node.handle_request_vote({
    term: 2,
    candidate_id: "n2",
    last_log_index: 0,
    last_log_term: 0,
  })
  assert_false(reply.vote_granted)
}
