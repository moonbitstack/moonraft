// Tests for the leadership-transfer state machine (Raft §3.10). Adapted from
// etcd-io/raft (Apache-2.0) raft_test.go: TestLeaderTransferToUpToDateNode /
// TestLeaderTransferToSlowFollower / TestLeaderTransferTimeout /
// TestLeaderTransferToSelf / TestLeaderTransferToNonExistingNode /
// TestLeaderTransferIgnoreProposal. (`ack_from` is shared from
// confchange_apply_wbtest.mbt — same white-box test package.)

///|
fn transfer_leader() -> RaftNode {
  let r = RaftNode::new("1", ["2", "3"], pre_vote=false)
  r.node().become_candidate()
  r.become_leader() // no-op @1
  r
}

///|
fn has_timeout_now(msgs : Array[Message], to : String) -> Bool {
  for m in msgs {
    if m.kind() == "TimeoutNow" && m.to == to {
      return true
    }
  }
  false
}

///|
test "transfer to an up-to-date node hands off at once and blocks proposals" {
  let r = transfer_leader()
  // Bring node 2 fully up to date.
  r.step(ack_from("2", 1, r.term())) |> ignore
  let out = r.transfer_leadership("2")
  assert_true(has_timeout_now(out, "2"))
  assert_true(r.lead_transferee == Some("2"))
  // Proposals are refused while the transfer is in flight.
  let before = r.node().last_log_index()
  assert_eq(r.propose(b"x").length(), 0)
  assert_eq(r.node().last_log_index(), before)
}

///|
test "transfer to a slow follower catches it up, then hands off" {
  let r = transfer_leader()
  // Node 2 is behind (match 0 != last 1): the transfer first sends entries.
  let out = r.transfer_leadership("2")
  assert_false(has_timeout_now(out, "2"))
  assert_true(r.lead_transferee == Some("2"))
  // When node 2's ack catches it up to the last index, it is told to time out.
  let out2 = r.step(ack_from("2", 1, r.term()))
  assert_true(has_timeout_now(out2, "2"))
}

///|
test "an incomplete transfer aborts after an election timeout" {
  let r = transfer_leader()
  r.transfer_leadership("2") |> ignore // node 2 slow: transfer pends
  assert_true(r.lead_transferee == Some("2"))
  for _i in 0..<r.election_timeout {
    r.tick() |> ignore
  }
  assert_true(r.lead_transferee == None)
  // Proposals flow again once the transfer is abandoned.
  assert_true(r.propose(b"x").length() > 0)
}

///|
test "transfer to self or a non-member is a no-op" {
  let r = transfer_leader()
  assert_eq(r.transfer_leadership("1").length(), 0)
  assert_true(r.lead_transferee == None)
  assert_eq(r.transfer_leadership("9").length(), 0)
  assert_true(r.lead_transferee == None)
}

///|
test "removing the transfer target aborts the transfer" {
  let r = transfer_leader()
  r.transfer_leadership("2") |> ignore
  assert_true(r.lead_transferee == Some("2"))
  // Committing a removal of node 2 aborts the transfer. Node 3 acks to commit.
  // First the transfer blocks proposals, so drive the conf change directly on
  // the core and let apply run via a commit.
  r.lead_transferee = None // unblock to append the conf entry
  r.propose_conf(ConfChange::remove("2")) |> ignore
  r.lead_transferee = Some("2") // re-arm to observe the abort on removal
  r.step(ack_from("3", r.node().last_log_index(), r.term())) |> ignore
  assert_true(r.lead_transferee == None)
  assert_false(r.config.contains("2"))
}
