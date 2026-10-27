///|
test "a conf change round-trips through its encoding" {
  let add = ConfChange::add("s2")
  let bytes = add.encode()
  match ConfChange::decode(bytes) {
    Some(cc) => {
      assert_true(cc.change_type == AddNode)
      assert_eq(cc.node_id, "s2")
    }
    None => abort("add change failed to decode")
  }
  let rem = ConfChange::remove("s3")
  match ConfChange::decode(rem.encode()) {
    Some(cc) => {
      assert_true(cc.change_type == RemoveNode)
      assert_eq(cc.node_id, "s3")
    }
    None => abort("remove change failed to decode")
  }
}

///|
test "decoding rejects malformed bytes" {
  assert_true(ConfChange::decode(b"") == None)
  assert_true(ConfChange::decode(b"?x") == None)
}

///|
test "applying a change edits the configuration" {
  let m = Membership::new(["a", "b", "c"])
  ConfChange::add("d").apply_to(m)
  assert_true(m.contains("d"))
  ConfChange::remove("a").apply_to(m)
  assert_false(m.contains("a"))
  assert_eq(m.size(), 3)
}

///|
test "propose_conf_change stores a conf entry in the log" {
  let a = Node::new("a")
  let _ = a.append(1, b"cmd")
  let e = a.propose_conf_change(1, ConfChange::add("d"))
  assert_eq(e.index, 2)
  assert_true(e.is_conf_change())
}

///|
test "the conf driver folds committed changes into the configuration" {
  let a = Node::new("a")
  let _ = a.append(1, b"set x")
  let _ = a.propose_conf_change(1, ConfChange::add("d"))
  let _ = a.propose_conf_change(1, ConfChange::remove("a"))
  let driver = ConfDriver::new(["a", "b", "c"])

  // Nothing committed yet: the configuration is untouched.
  assert_eq(driver.drive(a), 0)
  assert_eq(driver.config().size(), 3)

  // Commit through the two conf entries; both fold in, the normal entry skips.
  a.advance_commit(3)
  assert_eq(driver.drive(a), 2)
  let cfg = driver.config()
  assert_true(cfg.contains("d"))
  assert_false(cfg.contains("a"))
  assert_eq(cfg.size(), 3)

  // Driving again is a no-op: applied changes are not re-applied.
  assert_eq(driver.drive(a), 0)
}
