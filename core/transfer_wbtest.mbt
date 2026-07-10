///|
test "scenario: leadership is handed to a chosen follower" {
  let c = Cluster::new(["a", "b", "c"], seed=51)
  let first = c.run_until_leader(200).unwrap()
  let _ = c.propose(b"w1")
  let _ = c.run_until_committed(2, 200)

  // Pick a follower and transfer to it. Give a couple of ticks so it is caught
  // up, then hand over.
  let target = other_than(c, first)
  c.run(10)
  assert_true(c.transfer_leadership(target))

  // The target campaigns at once and takes over in a higher term.
  c.run(60)
  assert_true(c.node(target).is_leader())
  assert_true(c.node(target).term() > 1)
  assert_false(c.node(first).is_leader())
  assert_true(c.one_leader_per_term())
  assert_true(c.committed_agrees())
}

///|
test "transfer is a no-op on a follower" {
  let f = RaftNode::new("f", ["a", "b"], seed=1)
  assert_eq(f.transfer_leadership("a").length(), 0)
}
