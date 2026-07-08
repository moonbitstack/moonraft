///|
test "quorum is a majority of the members" {
  let m = Membership::new(["a", "b", "c"])
  assert_eq(m.quorum(), 2)
  assert_eq(m.size(), 3)
}

///|
test "adding a server is idempotent and grows the quorum" {
  let m = Membership::new(["a", "b", "c"])
  m.add("d")
  m.add("d")
  assert_eq(m.size(), 4)
  assert_eq(m.quorum(), 3)
  assert_true(m.contains("d"))
}

///|
test "removing a server shrinks the cluster" {
  let m = Membership::new(["a", "b", "c"])
  m.remove("a")
  assert_eq(m.size(), 2)
  assert_false(m.contains("a"))
  assert_true(m.contains("b"))
}

///|
test "has_majority counts a simple configuration" {
  let m = Membership::new(["a", "b", "c"])
  assert_true(m.has_majority(["a", "b"]))
  assert_false(m.has_majority(["a"]))
  assert_false(m.is_joint())
}

///|
test "joint consensus needs a majority of both halves" {
  // Move from {a,b,c} to {c,d,e}. During the joint transition a decision needs
  // a majority of the old set AND a majority of the new set.
  let m = Membership::new(["a", "b", "c"])
  m.enter_joint(["c", "d", "e"])
  assert_true(m.is_joint())
  // Majority of new {c,d,e} but only one of old {a,b,c}: not enough.
  assert_false(m.has_majority(["c", "d", "e"]))
  // Majority of both halves: {a,b} carry the old, {c,d} carry the new.
  assert_true(m.has_majority(["a", "b", "c", "d"]))
  assert_true(m.contains("a"))
  assert_true(m.contains("e"))
}

///|
test "leaving joint consensus runs on the incoming set alone" {
  let m = Membership::new(["a", "b", "c"])
  m.enter_joint(["c", "d", "e"])
  m.leave_joint()
  assert_false(m.is_joint())
  assert_false(m.contains("a"))
  assert_true(m.contains("d"))
  assert_true(m.has_majority(["c", "d"]))
}

///|
test "voters de-duplicates across the joint halves" {
  let m = Membership::new(["a", "b", "c"])
  m.enter_joint(["c", "d"])
  let v = m.voters()
  assert_eq(v.length(), 4)
  assert_true(v.contains("a"))
  assert_true(v.contains("d"))
}
