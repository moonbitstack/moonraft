///|
test "scenario: a lagging follower is caught up by a snapshot" {
  let c = Cluster::new(["a", "b", "c", "d", "e"], seed=8)
  let leader = c.run_until_leader(200).unwrap()

  // One follower drops out and misses a whole batch of commits.
  let laggard = other_than(c, leader)
  c.crash(laggard)
  let mut k = 0
  while k < 8 {
    let _ = c.propose(b"entry")
    c.run(15)
    k = k + 1
  }
  // The batch (plus the election no-op) reaches index 9 on the majority.
  assert_true(c.all_committed(9))

  // The leader compacts past the laggard's position, so the entries it still
  // needs are gone and only a snapshot can catch it up.
  assert_true(c.compact_leader(6, b"state@6"))

  // The follower returns and is brought fully up to date.
  c.restart(laggard)
  c.run(400)
  let lag = c.node(laggard).node()
  assert_true(lag.snapshot_index >= 6)
  assert_eq(lag.last_log_index(), c.node(leader).node().last_log_index())
  assert_true(c.committed_agrees())
  assert_true(c.logs_consistent())
}
