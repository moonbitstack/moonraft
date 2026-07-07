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
