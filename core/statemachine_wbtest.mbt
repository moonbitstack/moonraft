///|
/// A test state machine that records the commands applied to it.
struct Collector {
  applied : Array[Bytes]
}

///|
impl StateMachine for Collector with fn apply(self, command) {
  self.applied.push(command)
}

///|
test "committed entries are applied to the state machine in order" {
  let a = Node::new("a")
  let b = Node::new("b")
  let c = Node::new("c")
  let _ = run_election(a, [b, c])
  let _ = a.append(a.current_term(), b"one")
  let _ = a.append(a.current_term(), b"two")
  let _ = replicate(a, [b, c])
  let sm : Collector = { applied: [] }
  a.apply_committed(sm)
  assert_eq(sm.applied.length(), 2)
  assert_eq(a.last_applied, 2)
}

///|
test "applying again once caught up is a no-op" {
  let a = Node::new("a")
  let _ = a.append(1, b"x")
  a.advance_commit(1)
  let sm : Collector = { applied: [] }
  a.apply_committed(sm)
  a.apply_committed(sm)
  assert_eq(sm.applied.length(), 1)
}
