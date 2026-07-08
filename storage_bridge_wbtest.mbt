///|
test "a node round-trips through the storage engine" {
  let a = Node::new("a")
  a.become_candidate()
  let _ = a.append(1, b"x")
  let _ = a.append(1, b"y")
  let _ = a.append(2, b"z")
  a.advance_commit(2)
  let storage = MemoryStorage::new()
  a.save_into(storage)

  // Crash, restart blank, and reload from the storage engine alone.
  let b = Node::new("a")
  b.load_from(storage)
  assert_eq(b.current_term(), 1)
  assert_true(b.voted_for == Some("a"))
  assert_eq(b.last_log_index(), 3)
  assert_eq(b.term_at(3), 2)
  assert_eq(b.commit_index, 2)
}

///|
test "storage-backed recovery restores a snapshot baseline" {
  let a = Node::new("a")
  let _ = a.append(1, b"x")
  let _ = a.append(1, b"y")
  let _ = a.append(2, b"z")
  a.advance_commit(3)
  let _ = a.compact(2, b"state@2")
  let storage = MemoryStorage::new()
  a.save_into(storage)
  let b = Node::new("a")
  b.load_from(storage)
  assert_true(b.entry_at(1) == None)
  assert_eq(b.term_at(2), 1)
  assert_eq(b.term_at(3), 2)
  assert_eq(b.last_log_index(), 3)
}

///|
test "reloading twice from storage is idempotent" {
  let a = Node::new("a")
  let _ = a.append(1, b"x")
  a.advance_commit(1)
  let storage = MemoryStorage::new()
  a.save_into(storage)
  let b = Node::new("a")
  b.load_from(storage)
  b.load_from(storage)
  assert_eq(b.last_log_index(), 1)
  assert_eq(b.commit_index, 1)
}
