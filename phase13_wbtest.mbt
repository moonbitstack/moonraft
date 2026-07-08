///|
test "scenario: contended elections still converge on one leader" {
  // Across a spread of seeds the randomized timeouts and pre-vote break any
  // split vote, so the cluster always settles on exactly one leader rather than
  // dueling forever.
  let mut seed : UInt64 = 70
  let mut trial = 0
  while trial < 6 {
    seed = seed + 1
    let c = Cluster::new(["a", "b", "c"], seed~)
    let leader = c.run_until_leader(500)
    assert_true(leader != None)
    assert_eq(c.leaders().length(), 1)
    assert_true(c.one_leader_per_term())
    trial = trial + 1
  }
}

///|
test "scenario: a long stream of proposals commits in order" {
  let c = Cluster::new(["a", "b", "c"], seed=72)
  let _ = c.run_until_leader(200)
  let mut k = 0
  while k < 12 {
    let _ = c.propose(to_wire("op-\{k}"))
    c.run(8)
    k = k + 1
  }
  // no-op(1) plus twelve proposals => index 13.
  assert_true(c.run_until_committed(13, 400))
  assert_true(c.same_committed_commands())
  // The commands appear in exactly the order proposed on node a.
  let n = c.node("a").node()
  let mut i : UInt64 = 2
  let mut expect = 0
  while i <= 13 {
    match n.entry_at(i) {
      Some(e) => assert_true(e.command == to_wire("op-\{expect}"))
      None => abort("missing committed entry")
    }
    expect = expect + 1
    i = i + 1
  }
}

///|
test "hard state round-trips through the storage engine" {
  let s = MemoryStorage::new()
  s.set_hard_state({ term: 9, vote: Some("n3"), commit: 4 })
  let n = Node::new("n")
  n.apply_hard_state(s.storage_hard_state())
  assert_eq(n.current_term(), 9)
  assert_true(n.voted_for == Some("n3"))
  assert_eq(n.commit_index, 4)
}

///|
test "joint consensus rejects a decision missing one half" {
  let m = Membership::new(["a", "b", "c"])
  m.enter_joint(["c", "d", "e"])
  // A full majority of the incoming set but only c from the outgoing set.
  assert_false(m.has_majority(["c", "d", "e"]))
  // A full majority of the outgoing set but only c from the incoming set.
  assert_false(m.has_majority(["a", "b", "c"]))
  // Both halves satisfied.
  assert_true(m.has_majority(["a", "b", "c", "d"]))
}

///|
test "scenario: a recovered follower catches up by log, not snapshot" {
  let c = Cluster::new(["a", "b", "c"], seed=73)
  let leader = c.run_until_leader(200).unwrap()
  let laggard = other_than(c, leader)
  c.crash(laggard)
  let _ = c.propose(b"m1")
  let _ = c.propose(b"m2")
  c.run(120)
  // No compaction, so on return the follower is repaired by ordinary appends.
  c.restart(laggard)
  c.run(300)
  assert_eq(
    c.node(laggard).node().last_log_index(),
    c.node(leader).node().last_log_index(),
  )
  assert_eq(c.node(laggard).node().snapshot_index, 0)
  assert_true(c.committed_agrees())
}
