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
test "grants a vote to an up-to-date candidate in a new term" {
  let node = Node::new("n1")
  let reply = node.handle_request_vote({
    term: 1,
    candidate_id: "n2",
    last_log_index: 0,
    last_log_term: 0,
  })
  assert_true(reply.vote_granted)
  assert_eq(reply.term, 1)
  assert_eq(node.current_term(), 1)
}

///|
test "rejects a candidate from a stale term" {
  let node = Node::new("n1")
  node.become_follower(5)
  let reply = node.handle_request_vote({
    term: 3,
    candidate_id: "n2",
    last_log_index: 0,
    last_log_term: 0,
  })
  assert_false(reply.vote_granted)
  assert_eq(reply.term, 5)
}

///|
test "will not vote twice for different candidates in one term" {
  let node = Node::new("n1")
  let _ = node.handle_request_vote({
    term: 1,
    candidate_id: "n2",
    last_log_index: 0,
    last_log_term: 0,
  })
  let reply = node.handle_request_vote({
    term: 1,
    candidate_id: "n3",
    last_log_index: 0,
    last_log_term: 0,
  })
  assert_false(reply.vote_granted)
}

///|
test "denies a vote to a candidate whose log is behind" {
  let node = Node::new("n1")
  let _ = node.append(1, b"a")
  node.become_follower(2)
  let reply = node.handle_request_vote({
    term: 2,
    candidate_id: "n2",
    last_log_index: 0,
    last_log_term: 0,
  })
  assert_false(reply.vote_granted)
}
