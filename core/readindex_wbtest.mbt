///|
test "a follower refuses to serve a read" {
  let f = RaftNode::new("f", ["a", "b"], seed=1)
  assert_true(f.read_index() == None)
  assert_false(f.has_lease())
}

///|
/// A leader with followers holds a read lease only while a quorum has answered
/// within the current liveness window. A fresh leader has heard from no follower,
/// so its window holds only itself — one short of a majority in a three-node
/// group — and the lease is denied. A single acknowledgement lifts the window to
/// a majority and the lease is granted. Mirrors etcd
/// tracker/tracker.go:207 `QuorumActive`, which wins only when the voters whose
/// `RecentActive` flag is set form a majority.
test "has_lease requires an active quorum" {
  let l = RaftNode::new("1", ["2", "3"], pre_vote=false)
  l.node().become_candidate()
  l.become_leader()
  assert_false(l.has_lease())
  l.step(ack_from("2", 1, l.term())) |> ignore
  assert_true(l.has_lease())
}

///|
test "a lone leader serves a read at its commit index" {
  let n = RaftNode::new("solo", [])
  let mut ticks = 0
  while !n.is_leader() && ticks < 100 {
    let _ = n.tick()
    ticks = ticks + 1
  }
  assert_true(n.is_leader())
  // The no-op committed on election is index 1.
  assert_true(n.read_index() == Some(1))
  assert_true(n.has_lease())
}

///|
test "a leader with a live quorum serves reads at the commit index" {
  let c = Cluster::new(["a", "b", "c"], seed=2)
  let leader = c.run_until_leader(200).unwrap()
  let _ = c.propose(b"w1")
  let _ = c.run_until_committed(2, 200)
  // Let a heartbeat round confirm the quorum this window.
  c.run(20)
  let n = c.node(leader)
  match n.read_index() {
    Some(idx) => assert_true(idx >= 2)
    None => abort("leader with a live quorum should serve a read")
  }
}

///|
test "check-quorum makes an isolated leader step down" {
  let c = Cluster::new(["a", "b", "c"], seed=3)
  c.enable_check_quorum()
  let leader = c.run_until_leader(200).unwrap()
  c.isolate(leader)
  // Within a couple of election-timeout windows the cut-off leader gives up.
  c.run(60)
  assert_false(c.node(leader).is_leader())
  // And the connected majority carries a new leader.
  let survivor = c.run_until_leader(300)
  assert_true(survivor != None)
  assert_true(survivor.unwrap() != leader)
}
