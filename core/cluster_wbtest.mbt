///|
test "a candidate wins election with a majority of votes" {
  let a = Node::new("a")
  let b = Node::new("b")
  let c = Node::new("c")
  let won = run_election(a, [b, c])
  assert_true(won)
  assert_true(a.role() == Leader)
  assert_true(b.role() == Follower)
  assert_eq(a.current_term(), 1)
}

///|
test "a candidate steps down when a peer reveals a higher term" {
  let a = Node::new("a")
  let b = Node::new("b")
  let c = Node::new("c")
  b.become_follower(5)
  c.become_follower(5)
  let won = run_election(a, [b, c])
  assert_false(won)
  assert_true(a.role() == Follower)
  assert_eq(a.current_term(), 5)
}

///|
test "a two-node cluster needs both votes to elect a leader" {
  let a = Node::new("a")
  let b = Node::new("b")
  b.become_candidate()
  let _ = b.handle_request_vote(b.request_vote_args())
  // b has already voted for itself in term 1; a now stands in the same term
  // and cannot collect b's vote, so it fails to reach the majority of 2.
  let won = run_election(a, [b])
  assert_false(won)
}
