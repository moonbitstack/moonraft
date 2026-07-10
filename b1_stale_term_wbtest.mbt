// Regression tests for bug B1: a message from a superseded term must never be
// acted upon. Ported/adapted from etcd-io/raft (Apache-2.0)
// raft_paper_test.go:TestRejectStaleTermMessage and raft_test.go:
// TestStepIgnoreOldTermMsg, plus a reproduction of the double-leader hazard.

///|
/// Reproduces the Election-Safety hazard B1 guards against. A candidate in a
/// three-node cluster has only its own vote. Before the fix a *stale* granted
/// `VoteResp` (from an earlier term, as message reordering under the simulator
/// can deliver) was counted, giving a false 2-of-3 majority and electing a
/// second leader for the term. With stale responses dropped, the candidate must
/// stay a candidate.
test "B1: stale granted VoteResp is not counted (no double leader)" {
  let n = RaftNode::new("1", ["2", "3"], pre_vote=false)
  n.campaign() |> ignore
  assert_true(n.role() == Candidate)
  assert_eq(n.term(), 1)
  // A vote granted in term 0 arriving now, in term 1. It must be ignored.
  let stale = Message::new("2", "1", VoteResp({ term: 0, vote_granted: true }))
  n.step(stale) |> ignore
  assert_false(n.is_leader())
  assert_true(n.role() == Candidate)
  // The legitimate current-term vote does elect it.
  let ok = Message::new("2", "1", VoteResp({ term: 1, vote_granted: true }))
  n.step(ok) |> ignore
  assert_true(n.is_leader())
}

///|
/// A stale successful `AppendResp` must not advance a follower's progress on the
/// leader, which would let `maybe_commit` over-commit (State-Machine-Safety
/// hazard). The leader here is at term 2; a success ack stamped term 1 is stale.
test "B1: stale AppendResp does not inflate progress or commit" {
  let n = RaftNode::new("1", ["2", "3"], pre_vote=false)
  // Drive to leader at term 2.
  n.campaign() |> ignore // term 1, candidate
  n.step(Message::new("2", "1", VoteResp({ term: 1, vote_granted: true })))
  |> ignore
  assert_true(n.is_leader())
  assert_eq(n.term(), 1)
  // Force a term bump to 2 by observing a higher term, then re-elect.
  n.step(Message::new("2", "1", VoteResp({ term: 2, vote_granted: false })))
  |> ignore
  assert_true(n.role() == Follower)
  n.campaign() |> ignore // term 3 candidate
  n.step(
    Message::new("2", "1", VoteResp({ term: n.term(), vote_granted: true })),
  )
  |> ignore
  assert_true(n.is_leader())
  let _ = n.propose(b"x")
  let commit_before = n.commit_index()
  // A success ack from a stale term claiming a high match index.
  let stale = Message::new(
    "2",
    "1",
    AppendResp({
      term: 1,
      success: true,
      match_index: 99,
      conflict_index: 0,
      conflict_term: 0,
    }),
  )
  n.step(stale) |> ignore
  assert_eq(n.commit_index(), commit_before)
  assert_eq(n.progress["2"].match_index, 0)
}

///|
/// TestStepIgnoreOldTermMsg / TestRejectStaleTermMessage: a stale request is not
/// stepped as current — a follower at term 2 that receives an MsgApp at term 1
/// keeps its term and rejects rather than adopting the stale leader.
test "B1: stale MsgApp is rejected, term preserved" {
  let n = RaftNode::new("1", ["2", "3"], pre_vote=false)
  n.node().become_follower(2)
  let out = n.step(
    Message::new(
      "2",
      "1",
      Append({
        term: 1,
        leader_id: "2",
        prev_log_index: 0,
        prev_log_term: 0,
        entries: [],
        leader_commit: 0,
      }),
    ),
  )
  assert_eq(n.term(), 2)
  assert_true(n.role() == Follower)
  // It replies, revealing its higher term so the stale leader steps down.
  assert_eq(out.length(), 1)
  match out[0].payload {
    AppendResp(r) => {
      assert_false(r.success)
      assert_eq(r.term, 2)
    }
    _ => abort("expected an AppendResp")
  }
}
