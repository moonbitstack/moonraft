///|
/// Node-failure modelling: a stopped node reports as down and a survivor keeps
/// the cluster live (Leader Completeness across a crash, Raft §5.4).
test "a crashed node reports down until it is restarted" {
  let c = Cluster::new(["a", "b", "c", "d", "e"], seed=71)
  let leader = c.run_until_leader(200).unwrap()
  assert_false(c.is_down(leader))
  c.crash(leader)
  assert_true(c.is_down(leader))
  assert_true(c.run_until_leader(400) != None)
  c.restart(leader)
  assert_false(c.is_down(leader))
}

///|
/// Only a leader accepts client proposals (Raft §5.1); a follower rejects one,
/// so the follower's commit index does not move.
test "a follower refuses a client proposal" {
  let c = Cluster::new(["a", "b", "c"], seed=72)
  let leader = c.run_until_leader(200).unwrap()
  let _ = c.propose(to_wire("committed-by-leader"))
  assert_true(c.run_until_committed(2, 200))
  let follower = other_than(c, leader)
  let before = c.node(follower).commit_index()
  assert_false(c.propose_on(follower, to_wire("rejected")))
  c.run(100)
  assert_eq(c.node(follower).commit_index(), before)
}

///|
/// Compaction, leadership transfer and configuration change are leader-only
/// operations, so a cluster that has not yet elected performs none of them.
test "a leaderless cluster performs no leader-only operation" {
  let c = Cluster::new(["a", "b", "c"], seed=73)
  assert_false(c.compact_leader(1, to_wire("snap")))
  assert_false(c.transfer_leadership("b"))
  assert_false(c.propose_conf(ConfChange::add("z")))
}

///|
/// A leader broadcasts a proposed configuration change, which then commits and
/// folds into every server's committed log.
test "a configuration change is broadcast by the leader" {
  let c = Cluster::new(["a", "b", "c"], seed=74)
  let _ = c.run_until_leader(200)
  // Let the election no-op commit so the followers leave probe state; only then
  // does appending the change immediately produce an outgoing broadcast.
  c.run(30)
  assert_true(c.propose_conf(ConfChange::add("d")))
  assert_true(c.run_until_committed(2, 300))
  assert_true(c.same_committed_commands())
}

///|
/// The safety invariants are well-defined on an empty live set: once every node
/// is stopped there is nothing to compare, so each invariant holds vacuously.
test "the safety invariants hold once every node is stopped" {
  let c = Cluster::new(["a", "b", "c"], seed=75)
  let _ = c.run_until_leader(200)
  let _ = c.propose(to_wire("v1"))
  let _ = c.run_until_committed(2, 200)
  for id in ["a", "b", "c"] {
    c.crash(id)
  }
  assert_true(c.committed_agrees())
  assert_true(c.same_committed_commands())
  assert_true(c.logs_consistent())
  assert_true(c.invariants_hold())
}

///|
/// Log Matching survives compaction: after the leader discards a committed
/// prefix into a snapshot, the surviving committed commands still agree from the
/// snapshot baseline onward.
test "committed commands still agree after the log is compacted" {
  let c = Cluster::new(["a", "b", "c", "d", "e"], seed=76)
  let _ = c.run_until_leader(200).unwrap()
  let mut k = 0
  while k < 6 {
    let _ = c.propose(to_wire("cmd-" + k.to_string()))
    c.run(15)
    k = k + 1
  }
  assert_true(c.all_committed(7))
  assert_true(c.compact_leader(4, to_wire("state@4")))
  c.run(200)
  assert_true(c.same_committed_commands())
  assert_true(c.committed_agrees())
}

///|
/// A network partition diverges the logs — the isolated old leader appends an
/// entry its minority accepts but cannot commit, while the majority elects a new
/// leader and writes a different entry at the same index in a higher term — and
/// the strict log-matching check observes the divergence. Healing overwrites the
/// orphaned tail and restores every safety invariant.
test "a partition diverges the logs and healing reconciles them" {
  let c = Cluster::new(["a", "b", "c", "d", "e"], seed=77)
  let old = c.run_until_leader(200).unwrap()
  let _ = c.propose(to_wire("committed"))
  let _ = c.run_until_committed(2, 200)
  let mate = other_than(c, old)
  let minority = [old, mate]
  let majority : Array[String] = []
  for id in ["a", "b", "c", "d", "e"] {
    if !minority.contains(id) {
      majority.push(id)
    }
  }
  c.partition(minority, majority)
  let _ = c.propose_on(old, to_wire("minority-only"))
  c.run(400)
  let maj_leaders : Array[String] = []
  for id in majority {
    if c.node(id).is_leader() {
      maj_leaders.push(id)
    }
  }
  assert_true(maj_leaders.length() >= 1)
  let _ = c.propose_on(maj_leaders[0], to_wire("majority-only"))
  c.run(200)
  assert_false(c.logs_consistent())
  c.heal()
  c.run(600)
  assert_true(c.logs_consistent())
  assert_true(c.committed_agrees())
  assert_true(c.same_committed_commands())
}
