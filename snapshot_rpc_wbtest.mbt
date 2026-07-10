///|
test "install snapshot rejects a stale leader term" {
  let a = Node::new("a")
  a.become_follower(4)
  let reply = a.handle_install_snapshot({
    term: 3,
    leader_id: "L",
    last_index: 9,
    last_term: 3,
    offset: 0,
    data: b"s",
    done: true,
    conf_state: ConfState::empty(),
  })
  assert_eq(reply.term, 4)
  assert_eq(a.last_log_index(), 0)
}

///|
test "install snapshot adopts the baseline and steps down" {
  let a = Node::new("a")
  a.become_candidate()
  let _ = a.append(1, b"x")
  let reply = a.handle_install_snapshot(
    InstallSnapshotArgs::whole(5, "L", {
      last_index: 8,
      last_term: 4,
      data: b"image",
      conf_state: ConfState::empty(),
    }),
  )
  assert_eq(reply.term, 5)
  assert_true(a.role() == Follower)
  assert_eq(a.last_log_index(), 8)
  assert_eq(a.last_log_term(), 4)
  assert_eq(a.commit_index, 8)
}

///|
test "install snapshot keeps a matching suffix" {
  let a = Node::new("a")
  let _ = a.append(1, b"a")
  let _ = a.append(1, b"b")
  let _ = a.append(2, b"c")
  // Snapshot up to index 2 (term 1) while the follower still holds index 3.
  let _ = a.handle_install_snapshot({
    term: 1,
    leader_id: "L",
    last_index: 2,
    last_term: 1,
    offset: 0,
    data: b"snap",
    done: true,
    conf_state: ConfState::empty(),
  })
  assert_eq(a.last_log_index(), 3)
  assert_eq(a.term_at(3), 2)
  assert_eq(a.term_at(2), 1)
}

///|
test "install snapshot ignores an older snapshot" {
  let a = Node::new("a")
  a.install_snapshot({
    last_index: 10,
    last_term: 5,
    data: b"cur",
    conf_state: ConfState::empty(),
  })
  let reply = a.handle_install_snapshot({
    term: 5,
    leader_id: "L",
    last_index: 4,
    last_term: 2,
    offset: 0,
    data: b"stale",
    done: true,
    conf_state: ConfState::empty(),
  })
  assert_eq(reply.term, 5)
  assert_eq(a.last_log_index(), 10)
}
