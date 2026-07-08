///|
test "progress starts probing from the given next index" {
  let p = Progress::new(5)
  assert_eq(p.next_index, 5)
  assert_eq(p.match_index, 0)
  assert_true(p.state == Probe)
}

///|
test "maybe_update advances match and next, only forward" {
  let p = Progress::new(1)
  assert_true(p.maybe_update(3))
  assert_eq(p.match_index, 3)
  assert_eq(p.next_index, 4)
  // A stale, lower ack does not move anything.
  assert_false(p.maybe_update(2))
  assert_eq(p.match_index, 3)
}

///|
test "maybe_decrease jumps to the conflict hint" {
  let p = Progress::new(10)
  assert_true(p.maybe_decrease(4))
  assert_eq(p.next_index, 4)
  // A hint that would not move next_index backwards is ignored.
  assert_false(p.maybe_decrease(9))
  assert_eq(p.next_index, 4)
}

///|
test "maybe_decrease never rewinds below the match point" {
  let p = Progress::new(8)
  let _ = p.maybe_update(5)
  // Even a tiny hint cannot drop next below match+1.
  let _ = p.maybe_decrease(2)
  assert_eq(p.next_index, 6)
}

///|
test "state transitions set the next index" {
  let p = Progress::new(1)
  let _ = p.maybe_update(4)
  p.become_replicate()
  assert_true(p.state == Replicate)
  assert_eq(p.next_index, 5)
  p.optimistic_advance(7)
  assert_eq(p.next_index, 8)
  p.become_snapshot(20)
  assert_true(p.state == Snapshot)
  assert_eq(p.next_index, 21)
  p.become_probe()
  assert_true(p.state == Probe)
}
