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

// Port of raft_test.go's testRecvMsgVote (TestRecvMsgVote / TestRecvMsgPreVote),
// which drives one node's (pre-)vote decision across a table of candidate logs
// and prior votes at a shared term — the case that isolates the log comparison
// and the `canVote` clauses from the term check (raft_test.go:1327).

///|
/// Build the receiving node with etcd's fixture: a log of two entries at term 2
/// (`index(0).terms(0, 2, 2)`), the node's term raised to `max(2, log_term)` so
/// the solicitation lands at the same term, and a preset role and prior vote
/// (`""` = no vote).
fn recv_vote_node(
  vote_for : String,
  role : Role,
  log_term : UInt64,
) -> RaftNode {
  let r = RaftNode::new("1", [])
  r.node().append(2, b"") |> ignore
  r.node().append(2, b"") |> ignore
  let last_term = 2UL
  let term = if log_term > last_term { log_term } else { last_term }
  r.node().current_term = term
  r.node().role = role
  r.node().voted_for = if vote_for == "" { None } else { Some(vote_for) }
  r
}

///|
fn recv_vote_reject(
  pre : Bool,
  role : Role,
  index : UInt64,
  log_term : UInt64,
  vote_for : String,
) -> Bool raise {
  let r = recv_vote_node(vote_for, role, log_term)
  let args : RequestVoteArgs = {
    term: r.term(),
    candidate_id: "2",
    last_log_index: index,
    last_log_term: log_term,
  }
  let out = r.step(
    Message::new("2", "1", if pre { PreVote(args) } else { Vote(args) }),
  )
  assert_eq(out.length(), 1)
  match out[0].payload {
    VoteResp(reply) => !reply.vote_granted
    PreVoteResp(reply) => !reply.vote_granted
    _ => {
      assert_true(false)
      false
    }
  }
}

///|
/// The shared table: (role, index, log_term, vote_for, want_reject).
let recv_vote_cases : Array[(Role, UInt64, UInt64, String, Bool)] = [
  (Follower, 0, 0, "", true),
  (Follower, 0, 1, "", true),
  (Follower, 0, 2, "", true),
  (Follower, 0, 3, "", false),
  (Follower, 1, 0, "", true),
  (Follower, 1, 1, "", true),
  (Follower, 1, 2, "", true),
  (Follower, 1, 3, "", false),
  (Follower, 2, 0, "", true),
  (Follower, 2, 1, "", true),
  (Follower, 2, 2, "", false),
  (Follower, 2, 3, "", false),
  (Follower, 3, 0, "", true),
  (Follower, 3, 1, "", true),
  (Follower, 3, 2, "", false),
  (Follower, 3, 3, "", false),
  (Follower, 3, 2, "2", false),
  (Follower, 3, 2, "1", true),
  (Leader, 3, 3, "1", true),
  (PreCandidate, 3, 3, "1", true),
  (Candidate, 3, 3, "1", true),
]

///|
test "TestRecvMsgVote" {
  for tc in recv_vote_cases {
    let (role, index, log_term, vote_for, wreject) = tc
    assert_eq(recv_vote_reject(false, role, index, log_term, vote_for), wreject)
  }
}

///|
test "TestRecvMsgPreVote" {
  for tc in recv_vote_cases {
    let (role, index, log_term, vote_for, wreject) = tc
    assert_eq(recv_vote_reject(true, role, index, log_term, vote_for), wreject)
  }
}
