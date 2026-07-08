///|
test "hard state captures term, vote and commit" {
  let a = Node::new("a")
  a.become_candidate()
  let _ = a.append(1, b"x")
  a.advance_commit(1)
  let hs = a.hard_state()
  assert_eq(hs.term, 1)
  assert_true(hs.vote == Some("a"))
  assert_eq(hs.commit, 1)
}

///|
test "applying hard state never lowers the commit index" {
  let a = Node::new("a")
  a.advance_commit(5)
  a.apply_hard_state({ term: 3, vote: Some("b"), commit: 2 })
  assert_eq(a.current_term(), 3)
  assert_true(a.voted_for == Some("b"))
  assert_eq(a.commit_index, 5)
}
