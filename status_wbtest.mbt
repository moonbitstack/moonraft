///|
test "status reflects a freshly elected leader" {
  let n = RaftNode::new("solo", [])
  let mut ticks = 0
  while !n.is_leader() && ticks < 100 {
    let _ = n.tick()
    ticks = ticks + 1
  }
  let s = n.status()
  assert_true(s.role == Leader)
  assert_eq(s.term, 1)
  assert_true(s.leader == Some("solo"))
  assert_eq(s.commit, 1)
}

///|
test "status describes a follower in one line" {
  let f = RaftNode::new("f", ["a", "b"], seed=1)
  let s = f.status()
  assert_eq(s.describe(), "f follower term=0 leader=- commit=0 last=0")
}

///|
test "status tracks a follower that adopts a leader" {
  let f = RaftNode::new("f", ["a", "b"], seed=2)
  let _ = f.step(
    Message::new(
      "a",
      "f",
      Append({
        term: 3,
        leader_id: "a",
        prev_log_index: 0,
        prev_log_term: 0,
        entries: [Entry::normal(3, 1, b"x")],
        leader_commit: 1,
      }),
    ),
  )
  let s = f.status()
  assert_eq(s.term, 3)
  assert_true(s.leader == Some("a"))
  assert_eq(s.last_index, 1)
  assert_eq(s.commit, 1)
}
