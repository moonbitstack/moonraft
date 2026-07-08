///|
/// Deliver messages to their recipients in FIFO order, appending each reply to
/// the same queue, until the cluster falls quiet. This is a perfect network:
/// no loss, delay or reordering. The scenario simulator adds those.
fn deliver(nodes : Map[String, RaftNode], initial : Array[Message]) -> Unit {
  let q = initial
  let mut i = 0
  while i < q.length() {
    let m = q[i]
    i = i + 1
    match nodes.get(m.to) {
      Some(n) =>
        for r in n.step(m) {
          q.push(r)
        }
      None => ()
    }
  }
}

///|
fn three_nodes() -> Map[String, RaftNode] {
  {
    "a": RaftNode::new("a", ["b", "c"], seed=1),
    "b": RaftNode::new("b", ["a", "c"], seed=2),
    "c": RaftNode::new("c", ["a", "b"], seed=3),
  }
}

///|
test "a lone server elects itself after its election timeout" {
  let n = RaftNode::new("solo", [])
  let mut ticks = 0
  while !n.is_leader() && ticks < 100 {
    deliver({ "solo": n }, n.tick())
    ticks = ticks + 1
  }
  assert_true(n.is_leader())
  assert_eq(n.term(), 1)
  assert_true(n.leader() == Some("solo"))
}

///|
test "a three-node cluster elects a single leader by messages" {
  let nodes = three_nodes()
  let a = nodes.get("a").unwrap()
  deliver(nodes, a.campaign())
  assert_true(a.is_leader())
  assert_eq(a.term(), 1)
  let b = nodes.get("b").unwrap()
  let c = nodes.get("c").unwrap()
  assert_true(b.role() == Follower)
  assert_true(c.role() == Follower)
  assert_true(b.leader() == Some("a"))
}

///|
test "a leader replicates a proposal to a majority and commits it" {
  let nodes = three_nodes()
  let a = nodes.get("a").unwrap()
  deliver(nodes, a.campaign())
  // The no-op appended on election is index 1; the proposal is index 2.
  deliver(nodes, a.propose(b"set x = 1"))
  assert_eq(a.commit_index(), 2)
  let b = nodes.get("b").unwrap()
  let c = nodes.get("c").unwrap()
  assert_eq(b.node().last_log_index(), 2)
  assert_eq(c.node().last_log_index(), 2)
  assert_eq(b.node().term_at(2), 1)
}

///|
test "pre-vote does not disturb the term of a node with a leader" {
  // b believes a is leader in term 1; a stale candidate d pre-votes. b must not
  // change its term for a hypothetical pre-vote.
  let b = RaftNode::new("b", ["a", "d"], seed=7)
  let _ = b.step(
    Message::new(
      "a",
      "b",
      Append({
        term: 1,
        leader_id: "a",
        prev_log_index: 0,
        prev_log_term: 0,
        entries: [],
        leader_commit: 0,
      }),
    ),
  )
  assert_eq(b.term(), 1)
  let resp = b.step(
    Message::new(
      "d",
      "b",
      PreVote({
        term: 2,
        candidate_id: "d",
        last_log_index: 0,
        last_log_term: 0,
      }),
    ),
  )
  // Its term is untouched by the pre-vote, whatever it answers.
  assert_eq(b.term(), 1)
  assert_eq(resp.length(), 1)
}

///|
test "a stale leader steps down when it learns of a newer term" {
  let nodes = three_nodes()
  let a = nodes.get("a").unwrap()
  deliver(nodes, a.campaign())
  assert_true(a.is_leader())
  // A message from a strictly higher term forces the leader back to follower.
  let _ = a.step(
    Message::new(
      "b",
      "a",
      AppendResp({ term: 9, success: false, match_index: 0, conflict_index: 0 }),
    ),
  )
  assert_true(a.role() == Follower)
  assert_eq(a.term(), 9)
}
