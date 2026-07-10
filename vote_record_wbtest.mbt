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

///|
fn vote_resp(from : String, term : UInt64, granted : Bool) -> Message {
  Message::new(from, "1", VoteResp({ term, vote_granted: granted }))
}

///|
/// DIVERGENT fix (RecordVote overwrite, ①-path D7): etcd's tracker keeps the
/// *first* vote a node casts in a term and ignores any later differing response
/// (`RecordVote`: `if !ok { Votes[id] = v }`). This node overwrote, so a stale
/// duplicate could flip a vote already counted. Red: node 2 grants, then a
/// duplicate rejection from node 2 arrives; with overwrite the earlier grant is
/// lost and the candidate no longer reaches a majority on node 3's grant.
test "RecordVote keeps the first vote: a duplicate rejection cannot flip it" {
  let c = RaftNode::new("1", ["2", "3", "4", "5"], pre_vote=false)
  c.campaign() |> ignore // term 1, votes itself, still a candidate (needs 3/5)
  let t = c.term()
  c.step(vote_resp("2", t, true)) |> ignore // {1,2} grant — pending
  c.step(vote_resp("2", t, false)) |> ignore // stale duplicate: first vote wins
  assert_false(c.is_leader())
  c.step(vote_resp("3", t, true)) |> ignore // {1,2,3} grant — majority
  assert_true(c.is_leader())
}

///|
/// The symmetric guard: a first rejection is not overwritten by a later grant,
/// so a genuine denial stays denied within the term.
test "RecordVote keeps the first vote: a later grant cannot revive a rejection" {
  let c = RaftNode::new("1", ["2", "3", "4", "5"], pre_vote=false)
  c.campaign() |> ignore
  let t = c.term()
  c.step(vote_resp("2", t, false)) |> ignore
  c.step(vote_resp("3", t, false)) |> ignore
  // Two genuine rejections plus a duplicate "grant" from node 2 must not conjure
  // a majority; the candidate has already lost (3 of 5 cannot be reached).
  c.step(vote_resp("2", t, true)) |> ignore
  assert_false(c.is_leader())
}
