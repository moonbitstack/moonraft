// Copyright 2026 Leo Cheng
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Coverage-driven ports: each exercises a step-table branch that maps to a cell
// of etcd's stepLeader/stepCandidate/stepFollower cross-table. See GAP_core.md.

///|
fn cov_leader() -> RaftNode {
  let r = RaftNode::new("1", ["2", "3"], pre_vote=false)
  r.node().become_candidate()
  r.become_leader()
  r
}

///|
/// etcd's stepLeader guards every progress-keyed handler with `if pr == nil {
/// return }` (raft.go:1378) — a response from a node not in the tracker is
/// dropped. Covers the `progress.get None` arms of the resp handlers.
test "cov: responses from an untracked node are ignored (pr == nil)" {
  let r = cov_leader()
  let t = r.term()
  assert_eq(
    r
    .step(Message::new("9", "1", HeartbeatResp({ term: t, context: b"" })))
    .length(),
    0,
  )
  assert_eq(r.step(ack_from("9", 1, t)).length(), 0)
  r.report_snapshot("9", false)
  r.report_unreachable("9")
  assert_true(r.is_leader())
}

///|
/// A pre-vote rejection that reveals a higher term ends the pre-campaign and the
/// node becomes a follower (etcd: a PreVoteResp is not term-adopted in Step, so
/// the rejection reaches the handler which steps down).
test "cov: a higher-term pre-vote rejection steps the pre-candidate down" {
  let r = RaftNode::new("1", ["2", "3"], pre_vote=true)
  r.campaign() |> ignore
  assert_true(r.role() == PreCandidate)
  let out = r.step(
    Message::new(
      "2",
      "1",
      PreVoteResp({ term: r.term() + 5, vote_granted: false }),
    ),
  )
  assert_true(r.role() == Follower)
  assert_eq(out.length(), 0)
}

///|
/// A pre-vote that a majority rejects ends the pre-campaign (VoteLost); a single
/// rejection leaves it pending (VotePending).
test "cov: pre-vote VotePending then VoteLost" {
  let r = RaftNode::new("1", ["2", "3"], pre_vote=true)
  r.campaign() |> ignore
  let t = r.term()
  // One aligned rejection: still pending, still a pre-candidate.
  r.step(Message::new("2", "1", PreVoteResp({ term: t, vote_granted: false })))
  |> ignore
  assert_true(r.role() == PreCandidate)
  // A second rejection is a majority against: the pre-campaign ends.
  r.step(Message::new("3", "1", PreVoteResp({ term: t, vote_granted: false })))
  |> ignore
  assert_true(r.role() == Follower)
}

///|
/// A follower still leased to its leader rejects an aligned-term vote too (the
/// B3 lease check inside handle_vote, not just the higher-term path in step).
test "cov: a leased follower rejects an aligned-term vote" {
  let f = RaftNode::new("2", ["1", "3"], pre_vote=false)
  f.enable_check_quorum()
  f.step(
    Message::new(
      "1",
      "2",
      Heartbeat({ term: 1, leader_id: "1", commit: 0, context: b"" }),
    ),
  )
  |> ignore
  let out = f.step(
    Message::new(
      "3",
      "2",
      Vote({ term: 1, candidate_id: "3", last_log_index: 0, last_log_term: 0 }),
    ),
  )
  match out[0].payload {
    VoteResp(reply) => assert_false(reply.vote_granted)
    _ => abort("expected VoteResp")
  }
}

///|
/// A follower that recognises no leader drops a forwarded ReadIndex and
/// TransferLeader (etcd's stepFollower: "no leader ... dropping").
test "cov: a follower with no leader drops read-index and transfer" {
  let f = RaftNode::new("2", ["1", "3"], pre_vote=false)
  assert_eq(f.step(Message::new("2", "2", ReadIndex(b"x"))).length(), 0)
  assert_eq(f.step(Message::new("2", "2", TransferLeader("1"))).length(), 0)
}

///|
/// A leader ignores a MsgTimeoutNow addressed to it (etcd: only a non-leader
/// campaigns on TimeoutNow).
test "cov: TimeoutNow to a leader is ignored" {
  let r = cov_leader()
  assert_eq(r.step(Message::new("2", "1", TimeoutNow(r.term()))).length(), 0)
  assert_true(r.is_leader())
}

///|
/// A client proposal delivered as a MsgProp carrying a configuration change: the
/// first is accepted and appended, a second (while the first is uncommitted) is
/// refused and replaced with an empty entry (etcd's stepLeader MsgProp).
test "cov: Propose carrying a conf change — accepted then refused" {
  let r = cov_leader()
  let t = r.term()
  let cc1 = Entry::conf(t, 0, ConfChange::add("4").encode())
  r.step(Message::new("1", "1", Propose([cc1]))) |> ignore
  let after1 = r.node().last_log_index()
  let cc2 = Entry::conf(t, 0, ConfChange::add("5").encode())
  r.step(Message::new("1", "1", Propose([cc2]))) |> ignore
  // The refused change still consumes a log slot (empty entry).
  assert_eq(r.node().last_log_index(), after1 + 1)
  // ...but no second membership change is pending beyond the first.
  assert_false(r.config.contains("5"))
}

///|
/// `report_unreachable` and `report_snapshot` mutate follower progress, so they
/// are leader-only; etcd routes MsgUnreachable and MsgSnapStatus through
/// `stepLeader` alone, and a follower ignores them. Prepared progress states
/// survive a report issued after the node has stepped down, confirming the
/// leader guard short-circuits before any mutation.
test "a non-leader ignores unreachability and snapshot reports" {
  let r = RaftNode::new("1", ["2", "3"], pre_vote=false)
  r.node().become_candidate()
  r.become_leader()
  match r.progress.get("2") {
    Some(p) => p.become_replicate()
    None => abort("leader tracks follower 2")
  }
  match r.progress.get("3") {
    Some(p) => p.become_snapshot(5)
    None => abort("leader tracks follower 3")
  }
  r.become_follower(r.term() + 1)
  r.report_unreachable("2")
  r.report_snapshot("3", true)
  assert_true(r.progress.get("2").unwrap().state == Replicate)
  assert_true(r.progress.get("3").unwrap().state == Snapshot)
}
