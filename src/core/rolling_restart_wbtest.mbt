///|
test "scenario: the cluster stays available through a rolling restart" {
  let c = Cluster::new(["a", "b", "c", "d", "e"], seed=41)
  let _ = c.run_until_leader(200)
  let _ = c.propose(to_wire("v-start"))
  let _ = c.run_until_committed(2, 200)

  // Take each server down and back up in turn. With at most one node down, a
  // three-of-five majority always remains, so the cluster keeps electing and
  // committing.
  for id in ["a", "b", "c", "d", "e"] {
    c.crash(id)
    let leader = c.run_until_leader(400)
    assert_true(leader != None)
    let _ = c.propose(to_wire("v-" + id))
    c.run(120)
    assert_true(c.one_leader_per_term())
    assert_true(c.logs_consistent())
    c.restart(id)
    c.run(120)
  }
  c.run(300)
  assert_true(c.committed_agrees())
  assert_true(c.same_committed_commands())
}

///|
test "scenario: a partitioned minority leader cannot commit" {
  let c = Cluster::new(["a", "b", "c", "d", "e"], seed=42)
  let leader = c.run_until_leader(200).unwrap()
  let _ = c.propose(to_wire("safe"))
  let _ = c.run_until_committed(2, 200)

  // Trap the leader with one follower in a two-node minority.
  let mate = other_than(c, leader)
  let minority = [leader, mate]
  let majority : Array[String] = []
  for id in ["a", "b", "c", "d", "e"] {
    if !minority.contains(id) {
      majority.push(id)
    }
  }
  c.partition(minority, majority)
  let committed_before = c.node(leader).commit_index()

  // The old leader can still append to its own log, but with only two of five
  // votes it can never reach a majority, so the commit index must not move.
  let _ = c.propose_on(leader, to_wire("doomed"))
  c.run(300)
  assert_eq(c.node(leader).commit_index(), committed_before)
  assert_true(c.node(leader).node().last_log_index() > committed_before)

  // The majority meanwhile elects its own leader (the old one still thinks it
  // leads its minority, so look specifically on the majority side) and keeps
  // making progress.
  c.run(400)
  let maj_leaders : Array[String] = []
  for id in majority {
    if c.node(id).is_leader() {
      maj_leaders.push(id)
    }
  }
  assert_true(maj_leaders.length() >= 1)
  assert_true(c.one_leader_per_term())
  let _ = c.propose_on(maj_leaders[0], to_wire("majority-write"))
  c.run(200)

  // Heal, and every safety invariant still holds as logs reconcile.
  c.heal()
  c.run(600)
  assert_true(c.committed_agrees())
  assert_true(c.same_committed_commands())
  assert_true(c.logs_consistent())
}
