///|
test "scenario: a healthy cluster elects exactly one leader" {
  let c = Cluster::new(["a", "b", "c"], seed=1)
  let leader = c.run_until_leader(200)
  assert_true(leader != None)
  assert_eq(c.leaders().length(), 1)
  assert_true(c.invariants_hold())
}

///|
test "scenario: a leader replicates a batch of proposals to everyone" {
  let c = Cluster::new(["a", "b", "c"], seed=2)
  let _ = c.run_until_leader(200)
  let _ = c.propose(b"cmd-1")
  let _ = c.propose(b"cmd-2")
  let _ = c.propose(b"cmd-3")
  // No-op on election is index 1, so the three proposals reach index 4.
  assert_true(c.run_until_committed(4, 200))
  assert_true(c.committed_agrees())
  assert_true(c.logs_consistent())
}

///|
test "scenario: the cluster re-elects after the leader crashes" {
  let c = Cluster::new(["a", "b", "c", "d", "e"], seed=3)
  let first = c.run_until_leader(200).unwrap()
  let _ = c.propose(b"before-crash")
  let _ = c.run_until_committed(2, 200)
  c.crash(first)
  let second = c.run_until_leader(400)
  assert_true(second != None)
  assert_true(second.unwrap() != first)
  assert_true(c.one_leader_per_term())
  assert_true(c.committed_agrees())
}

///|
test "scenario: a partitioned old leader yields to the majority" {
  let c = Cluster::new(["a", "b", "c", "d", "e"], seed=4)
  let old = c.run_until_leader(200).unwrap()
  let _ = c.propose(b"v1")
  let _ = c.run_until_committed(2, 200)

  // Put the old leader in a two-node minority; the three-node majority elects a
  // new leader and keeps committing.
  let minority = [old, other_than(c, old)]
  let majority : Array[String] = []
  for id in ["a", "b", "c", "d", "e"] {
    if !minority.contains(id) {
      majority.push(id)
    }
  }
  c.partition(minority, majority)

  // The old leader still thinks it leads its minority (nobody has told it of a
  // newer term), so wait specifically for a majority-side leader to appear.
  c.run(400)
  let maj_leaders : Array[String] = []
  for id in majority {
    if c.node(id).is_leader() {
      maj_leaders.push(id)
    }
  }
  assert_true(maj_leaders.length() >= 1)
  // Both leaders coexist, but in different terms — election safety holds.
  assert_true(c.one_leader_per_term())
  let _ = c.propose_on(maj_leaders[0], b"v2-on-majority")
  c.run(200)
  assert_true(c.one_leader_per_term())

  // Heal: the old leader rejoins, adopts the newer term, and logs converge.
  c.heal()
  c.run(400)
  assert_true(c.committed_agrees())
  assert_true(c.logs_consistent())
  assert_eq(c.leaders().length(), 1)
}

///|
/// Any live node id other than `id`, for building a two-node minority.
fn other_than(c : Cluster, id : String) -> String {
  for other in c.ids {
    if other != id {
      return other
    }
  }
  id
}

///|
test "scenario: consensus survives heavy message loss" {
  let c = Cluster::new(["a", "b", "c"], seed=5)
  c.set_drop(300) // drop 30% of messages
  let leader = c.run_until_leader(600)
  assert_true(leader != None)
  let _ = c.propose(b"despite-loss")
  // Give it plenty of ticks to push the entry through a lossy link.
  let _ = c.run_until_committed(2, 1000)
  assert_true(c.committed_agrees())
  assert_true(c.one_leader_per_term())
}

///|
test "scenario: consensus survives delay and reordering" {
  let c = Cluster::new(["a", "b", "c"], seed=6)
  c.set_delay(4) // up to four ticks of jitter, which also reorders
  let leader = c.run_until_leader(400)
  assert_true(leader != None)
  let _ = c.propose(b"reordered-1")
  let _ = c.propose(b"reordered-2")
  assert_true(c.run_until_committed(3, 600))
  assert_true(c.committed_agrees())
  assert_true(c.logs_consistent())
}
