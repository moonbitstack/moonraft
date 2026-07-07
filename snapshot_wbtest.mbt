///|
test "compaction discards the log prefix and keeps the baseline term" {
  let a = Node::new("a")
  let _ = a.append(1, b"x")
  let _ = a.append(1, b"y")
  let _ = a.append(2, b"z")
  a.advance_commit(3)
  let snap = a.compact(2, b"state@2")
  assert_eq(snap.last_index, 2)
  assert_eq(snap.last_term, 1)
  assert_eq(a.last_log_index(), 3)
  assert_eq(a.term_at(3), 2)
  assert_eq(a.term_at(2), 1)
  assert_eq(a.term_at(1), 0)
}

///|
test "compaction refuses to discard uncommitted entries" {
  let a = Node::new("a")
  let _ = a.append(1, b"x")
  let _ = a.append(1, b"y")
  let snap = a.compact(2, b"state@2")
  assert_eq(snap.last_index, 0)
  assert_eq(a.last_log_index(), 2)
}

///|
test "installing a snapshot replaces the log and advances commit" {
  let a = Node::new("a")
  let _ = a.append(1, b"old")
  a.install_snapshot({ last_index: 5, last_term: 3, data: b"snap" })
  assert_eq(a.last_log_index(), 5)
  assert_eq(a.last_log_term(), 3)
  assert_eq(a.commit_index, 5)
}
