///|
/// A seeded pseudo-random 64-bit stream, so the chaos run is reproducible.
struct Chaos {
  mut state : UInt64
}

///|
fn Chaos::next(self : Chaos) -> UInt64 {
  self.state = self.state * 6364136223846793005UL + 1442695040888963407UL
  self.state
}

///|
/// Draw a value in [0, n).
fn Chaos::below(self : Chaos, n : UInt64) -> UInt64 {
  (self.next() >> 33) % n
}

///|
test "chaos: random faults never break the safety invariants" {
  let ids = ["a", "b", "c", "d", "e"]
  let c = Cluster::new(ids, seed=77)
  let _ = c.run_until_leader(300)
  let chaos : Chaos = { state: 0x9e3779b97f4a7c15 }
  let mut down : String? = None
  let mut split = false
  let mut step = 0
  while step < 50 {
    match chaos.below(5) {
      0 => {
        // Client write.
        let _ = c.propose(to_wire("op-\{step}"))
      }
      1 =>
        // Crash a node, but never more than one at a time.
        match down {
          None => {
            let victim = ids[chaos.below(5).to_int()]
            c.crash(victim)
            down = Some(victim)
          }
          Some(_) => ()
        }
      2 =>
        // Restart the crashed node.
        match down {
          Some(v) => {
            c.restart(v)
            down = None
          }
          None => ()
        }
      3 =>
        // Heal any partition.
        if split {
          c.heal()
          split = false
        }
      _ =>
        // Split the cluster two against three.
        if !split {
          c.partition([ids[0], ids[1]], [ids[2], ids[3], ids[4]])
          split = true
        }
    }
    c.run(30)
    // These are safety properties: they must hold at every single step, healthy
    // or not, available or not.
    assert_true(c.one_leader_per_term())
    assert_true(c.committed_agrees())
    step = step + 1
  }

  // Recover everything and let the cluster settle, then the strongest checks.
  match down {
    Some(v) => c.restart(v)
    None => ()
  }
  c.heal()
  c.run(1000)
  assert_true(c.one_leader_per_term())
  assert_true(c.committed_agrees())
  assert_true(c.same_committed_commands())
  assert_true(c.logs_consistent())
  // After healing, the cluster is available again.
  assert_true(c.run_until_leader(400) != None)
}
