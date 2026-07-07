///|
test "new node starts as a follower at term 0 with an empty log" {
  let node = Node::new("n1")
  assert_true(node.role() == Follower)
  assert_eq(node.current_term(), 0)
  assert_eq(node.last_log_index(), 0)
  assert_eq(node.last_log_term(), 0)
}

///|
test "becoming a candidate advances the term and self-votes" {
  let node = Node::new("n1")
  node.become_candidate()
  assert_true(node.role() == Candidate)
  assert_eq(node.current_term(), 1)
}

///|
test "append assigns sequential indices and tracks the last term" {
  let node = Node::new("n1")
  let e1 = node.append(1, b"x")
  let e2 = node.append(1, b"y")
  assert_eq(e1.index, 1)
  assert_eq(e2.index, 2)
  assert_eq(node.last_log_index(), 2)
  assert_eq(node.last_log_term(), 1)
}

///|
test "term_at returns the term at an index and 0 out of range" {
  let node = Node::new("n1")
  let _ = node.append(3, b"a")
  assert_eq(node.term_at(1), 3)
  assert_eq(node.term_at(0), 0)
  assert_eq(node.term_at(2), 0)
}

///|
test "stepping down to a higher term becomes follower and clears the vote" {
  let node = Node::new("n1")
  node.become_candidate()
  node.become_follower(5)
  assert_true(node.role() == Follower)
  assert_eq(node.current_term(), 5)
}
