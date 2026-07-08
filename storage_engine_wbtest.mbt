///|
test "fresh memory storage is empty with a sentinel at index 0" {
  let s = MemoryStorage::new()
  assert_eq(s.first_index(), 1)
  assert_eq(s.last_index(), 0)
  assert_true(s.storage_term(0) == Some(0))
  assert_true(s.storage_term(1) == None)
}

///|
test "appended entries are readable by index, term and slice" {
  let s = MemoryStorage::new()
  s.append([
    Entry::normal(1, 1, b"a"),
    Entry::normal(1, 2, b"b"),
    Entry::normal(2, 3, b"c"),
  ])
  assert_eq(s.first_index(), 1)
  assert_eq(s.last_index(), 3)
  assert_true(s.storage_term(3) == Some(2))
  let mid = s.slice(2, 4)
  assert_eq(mid.length(), 2)
  assert_eq(mid[0].index, 2)
  assert_eq(mid[1].index, 3)
}

///|
test "appending a conflicting index truncates the old suffix" {
  let s = MemoryStorage::new()
  s.append([Entry::normal(1, 1, b"a"), Entry::normal(1, 2, b"b")])
  s.append([Entry::normal(2, 2, b"b2"), Entry::normal(2, 3, b"c2")])
  assert_eq(s.last_index(), 3)
  assert_true(s.storage_term(2) == Some(2))
  assert_true(s.storage_term(3) == Some(2))
}

///|
test "compaction discards the prefix and keeps the boundary term" {
  let s = MemoryStorage::new()
  s.append([
    Entry::normal(1, 1, b"a"),
    Entry::normal(1, 2, b"b"),
    Entry::normal(2, 3, b"c"),
  ])
  assert_true(s.compact(2))
  assert_eq(s.first_index(), 3)
  assert_eq(s.last_index(), 3)
  assert_true(s.storage_term(2) == Some(1))
  assert_true(s.storage_term(1) == None)
}

///|
test "compaction refuses an index outside the log" {
  let s = MemoryStorage::new()
  s.append([Entry::normal(1, 1, b"a")])
  assert_false(s.compact(0))
  assert_false(s.compact(5))
}

///|
test "applying a snapshot resets the baseline and commit" {
  let s = MemoryStorage::new()
  s.append([Entry::normal(1, 1, b"a")])
  s.apply_snapshot({ last_index: 9, last_term: 4, data: b"img" })
  assert_eq(s.first_index(), 10)
  assert_eq(s.last_index(), 9)
  assert_true(s.storage_term(9) == Some(4))
  assert_eq(s.storage_hard_state().commit, 9)
  assert_eq(s.storage_snapshot().last_index, 9)
}

///|
test "create_snapshot captures the term at an in-log index" {
  let s = MemoryStorage::new()
  s.append([Entry::normal(1, 1, b"a"), Entry::normal(3, 2, b"b")])
  match s.create_snapshot(2, b"data") {
    Some(snap) => {
      assert_eq(snap.last_index, 2)
      assert_eq(snap.last_term, 3)
    }
    None => abort("expected a snapshot at index 2")
  }
  assert_true(s.create_snapshot(99, b"x") == None)
}

///|
test "hard state is stored and read back" {
  let s = MemoryStorage::new()
  s.set_hard_state({ term: 7, vote: Some("n2"), commit: 3 })
  let hs = s.storage_hard_state()
  assert_eq(hs.term, 7)
  assert_true(hs.vote == Some("n2"))
  assert_eq(hs.commit, 3)
}
