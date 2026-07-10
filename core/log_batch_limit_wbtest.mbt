///|
test "entries_after_limited caps the batch size" {
  let a = Node::new("a")
  let mut i = 0
  while i < 5 {
    let _ = a.append(1, b"x")
    i = i + 1
  }
  assert_eq(a.entries_after_limited(0, 2).length(), 2)
  assert_eq(a.entries_after_limited(0, 10).length(), 5)
  assert_eq(a.entries_after_limited(3, 2).length(), 2)
  assert_eq(a.entries_after_limited(4, 9).length(), 1)
}

///|
test "bounded append batches still replicate a large log" {
  // A tight byte cap (etcd's MaxSizePerMsg) forces the leader to split its
  // AppendEntries into several small batches; replication still converges.
  let nodes : Map[String, RaftNode] = {
    "a": RaftNode::new("a", ["b"], seed=1, max_msg_bytes=24),
    "b": RaftNode::new("b", ["a"], seed=2),
  }
  let a = nodes.get("a").unwrap()
  deliver(nodes, a.campaign())
  assert_true(a.is_leader())
  let mut k = 0
  while k < 6 {
    deliver(nodes, a.propose(b"cmd"))
    k = k + 1
  }
  let b = nodes.get("b").unwrap()
  assert_eq(b.node().last_log_index(), a.node().last_log_index())
  assert_eq(a.commit_index(), a.node().last_log_index())
}

///|
test "the randomized election timeout stays within its band" {
  let mut seed : UInt64 = 1
  let mut i = 0
  while i < 20 {
    seed = seed + 1
    let n = RaftNode::new("n", ["a", "b"], seed~, election_timeout=10)
    let d = n.election_deadline()
    assert_true(d >= 10)
    assert_true(d < 20)
    i = i + 1
  }
}

///|
test "a candidate denied a vote remains a candidate" {
  let nodes = three_nodes()
  let a = nodes.get("a").unwrap()
  // Force a into a real election by disabling nothing and campaigning; then feed
  // it a single rejection and confirm it neither wins nor steps down.
  let _ = a.campaign() // pre-vote round begins at would-be term 1
  // Grant the pre-vote from b so a becomes a real candidate in term 1.
  let _ = a.step(
    Message::new("b", "a", PreVoteResp({ term: 1, vote_granted: true })),
  )
  assert_true(a.role() == Candidate)
  assert_eq(a.term(), 1)
  // A rejection in the same term neither wins the election nor steps a down.
  let _ = a.step(
    Message::new("c", "a", VoteResp({ term: 1, vote_granted: false })),
  )
  assert_true(a.role() == Candidate)
}

///|
test "scenario: a seven-node cluster elects and replicates" {
  let c = Cluster::new(["a", "b", "c", "d", "e", "f", "g"], seed=61)
  let leader = c.run_until_leader(300)
  assert_true(leader != None)
  assert_eq(c.leaders().length(), 1)
  let _ = c.propose(b"seven-1")
  let _ = c.propose(b"seven-2")
  assert_true(c.run_until_committed(3, 400))
  assert_true(c.committed_agrees())
  assert_true(c.same_committed_commands())
  assert_true(c.logs_consistent())
}

///|
test "scenario: a seven-node cluster tolerates three crashes" {
  let c = Cluster::new(["a", "b", "c", "d", "e", "f", "g"], seed=62)
  let _ = c.run_until_leader(300)
  let _ = c.propose(b"base")
  let _ = c.run_until_committed(2, 300)
  // Crash three of seven; a four-node majority remains and keeps committing.
  c.crash("e")
  c.crash("f")
  c.crash("g")
  let survivor = c.run_until_leader(500)
  assert_true(survivor != None)
  let _ = c.propose(b"after-crashes")
  c.run(300)
  assert_true(c.committed_agrees())
  assert_true(c.one_leader_per_term())
}
