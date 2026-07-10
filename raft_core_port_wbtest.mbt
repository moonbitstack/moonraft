// Copyright 2015 The etcd Authors
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

// Ported from etcd-io/raft (Apache-2.0): raft_test.go and raft_paper_test.go —
// the property tests that map directly onto a single RaftNode.

///|
/// TestCandidateStartNewElection (raft_paper_test.go): a candidate that fails to
/// win times out and starts a fresh election, incrementing its term.
test "TestCandidateStartNewElection" {
  let r = RaftNode::new("1", ["2", "3"], pre_vote=false, election_timeout=10)
  r.node().become_candidate() // term 1
  assert_eq(r.term(), 1)
  let mut msgs : Array[Message] = []
  for _i in 1..<(2 * 10) {
    let out = r.tick()
    if out.length() > 0 {
      msgs = out
    }
  }
  assert_eq(r.term(), 2)
  assert_true(r.role() == Candidate)
  assert_true(r.votes["1"])
  assert_eq(msgs.length(), 2)
  for m in msgs {
    assert_true(m.kind() == "Vote")
  }
}

///|
/// TestSingleNodeCommit (raft_test.go): a one-node cluster commits every
/// proposal on its own.
test "TestSingleNodeCommit" {
  let r = RaftNode::new("1", [], pre_vote=false)
  r.campaign() |> ignore // no-op @1 commits immediately
  assert_true(r.is_leader())
  r.propose(b"a") |> ignore
  r.propose(b"b") |> ignore
  // no-op(1) + a(2) + b(3), all committed.
  assert_eq(r.commit_index(), 3)
}

///|
/// TestVoteFromAnyState (raft_test.go): from any state, a vote request at a
/// higher term makes the server step down to follower and grant the vote when
/// the candidate's log is at least as up-to-date.
test "TestVoteFromAnyState" {
  for state in ["follower", "candidate", "leader"] {
    let r = RaftNode::new("1", ["2", "3"], pre_vote=false)
    match state {
      "follower" => r.node().become_follower(1)
      "candidate" => r.node().become_candidate()
      _ => {
        r.node().become_candidate()
        r.become_leader()
      }
    }
    let msg_term = r.term() + 1
    let out = r.step(
      Message::new(
        "2",
        "1",
        Vote({
          term: msg_term,
          candidate_id: "2",
          last_log_index: r.node().last_log_index(),
          last_log_term: r.node().last_log_term(),
        }),
      ),
    )
    assert_eq(out.length(), 1)
    match out[0].payload {
      VoteResp(reply) => assert_true(reply.vote_granted)
      _ => abort("expected VoteResp")
    }
    assert_true(r.role() == Follower)
    assert_eq(r.term(), msg_term)
  }
}

///|
/// TestPreVoteFromAnyState (raft_test.go): a pre-vote at a higher term is
/// granted when the candidate is up-to-date, and — being hypothetical — never
/// changes the responder's term or role.
test "TestPreVoteFromAnyState" {
  for state in ["follower", "candidate", "leader"] {
    let r = RaftNode::new("1", ["2", "3"], pre_vote=false)
    match state {
      "follower" => r.node().become_follower(1)
      "candidate" => r.node().become_candidate()
      _ => {
        r.node().become_candidate()
        r.become_leader()
      }
    }
    let term_before = r.term()
    let role_before = r.role()
    let out = r.step(
      Message::new(
        "2",
        "1",
        PreVote({
          term: term_before + 1,
          candidate_id: "2",
          last_log_index: r.node().last_log_index(),
          last_log_term: r.node().last_log_term(),
        }),
      ),
    )
    assert_eq(out.length(), 1)
    match out[0].payload {
      PreVoteResp(reply) => assert_true(reply.vote_granted)
      _ => abort("expected PreVoteResp")
    }
    // Pre-vote is hypothetical: our own term and role are untouched.
    assert_eq(r.term(), term_before)
    assert_true(r.role() == role_before)
  }
}

///|
/// TestLeaderCommitPrecedingEntries (raft_paper_test.go): committing a
/// current-term entry also commits all preceding entries, including those
/// carried over from previous leaders.
test "TestLeaderCommitPrecedingEntries" {
  // (preceding entry terms) — real entries at indices 1..n.
  let cases : Array[Array[UInt64]] = [[], [2], [1, 2], [1]]
  for pre in cases {
    let r = RaftNode::new("1", ["2", "3"], pre_vote=false)
    let mut i = 0
    for t in pre {
      i = i + 1
      let _ = r.node().append(t, b"")
    }
    r.node().become_follower(2)
    r.node().become_candidate() // term 3
    r.become_leader() // no-op at index n+1, term 3
    r.propose(b"some data") |> ignore // data at index n+2
    let last = r.node().last_log_index()
    // A majority (self + node 2) acks the data entry.
    r.step(ack_from("2", last, r.term())) |> ignore
    // Committing the current-term data entry commits everything before it.
    assert_eq(r.commit_index(), last)
    assert_eq(last, (pre.length() + 2).to_uint64())
  }
}
