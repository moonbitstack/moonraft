///|
test "leader replicates its entries and commits on a majority" {
  let a = Node::new("a")
  let b = Node::new("b")
  let c = Node::new("c")
  let _ = run_election(a, [b, c])
  let _ = a.append(a.current_term(), b"cmd1")
  let _ = a.append(a.current_term(), b"cmd2")
  let commit = replicate(a, [b, c])
  assert_eq(commit, 2)
  assert_eq(b.last_log_index(), 2)
  assert_eq(c.last_log_index(), 2)
  assert_eq(b.term_at(2), 1)
}

///|
test "a follower with a conflicting tail is repaired by the leader" {
  let a = Node::new("a")
  let b = Node::new("b")
  let c = Node::new("c")
  let _ = run_election(a, [b, c])
  let _ = b.append(0, b"stale")
  let _ = a.append(a.current_term(), b"good")
  let _ = replicate(a, [b, c])
  assert_eq(b.last_log_index(), 1)
  assert_eq(b.term_at(1), 1)
}

///|
test "replication does not commit without a majority in sync" {
  let a = Node::new("a")
  let b = Node::new("b")
  let c = Node::new("c")
  let _ = run_election(a, [b, c])
  let _ = a.append(a.current_term(), b"x")
  // Only push to a single follower; a two-of-three majority is not reached
  // because the other follower never hears from the leader.
  let synced = bring_into_sync(a, b)
  assert_true(synced)
  assert_eq(a.commit_index, 0)
}

///|
test "a follower at a higher term steps the leader down during replication" {
  let a = Node::new("a")
  a.become_candidate()
  let _ = a.append(a.current_term(), b"x")
  let b = Node::new("b")
  b.become_follower(9)
  let synced = bring_into_sync(a, b)
  assert_false(synced)
  assert_eq(a.current_term(), 9)
  assert_true(a.role() == Follower)
}
