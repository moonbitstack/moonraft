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
test "append entries rejects a stale leader term" {
  let node = Node::new("n1")
  node.become_follower(3)
  let reply = node.handle_append_entries({
    term: 2,
    leader_id: "L",
    prev_log_index: 0,
    prev_log_term: 0,
    entries: [],
    leader_commit: 0,
  })
  assert_false(reply.success)
  assert_eq(reply.term, 3)
}

///|
test "append entries stores new entries onto a matching log" {
  let node = Node::new("n1")
  let reply = node.handle_append_entries({
    term: 1,
    leader_id: "L",
    prev_log_index: 0,
    prev_log_term: 0,
    entries: [Entry::normal(1, 1, b"a"), Entry::normal(1, 2, b"b")],
    leader_commit: 0,
  })
  assert_true(reply.success)
  assert_eq(node.last_log_index(), 2)
  assert_true(node.role() == Follower)
}

///|
test "append entries rejects a missing prev-log entry" {
  let node = Node::new("n1")
  node.become_follower(2)
  let reply = node.handle_append_entries({
    term: 2,
    leader_id: "L",
    prev_log_index: 5,
    prev_log_term: 1,
    entries: [],
    leader_commit: 0,
  })
  assert_false(reply.success)
}

///|
test "conflicting entries are overwritten by the leader's" {
  let node = Node::new("n1")
  let _ = node.append(1, b"old")
  let reply = node.handle_append_entries({
    term: 2,
    leader_id: "L",
    prev_log_index: 0,
    prev_log_term: 0,
    entries: [Entry::normal(2, 1, b"new")],
    leader_commit: 0,
  })
  assert_true(reply.success)
  assert_eq(node.last_log_index(), 1)
  assert_eq(node.term_at(1), 2)
}

///|
test "commit index advances with the leader but no further than the log" {
  let node = Node::new("n1")
  let reply = node.handle_append_entries({
    term: 1,
    leader_id: "L",
    prev_log_index: 0,
    prev_log_term: 0,
    entries: [Entry::normal(1, 1, b"a")],
    leader_commit: 9,
  })
  assert_true(reply.success)
  assert_eq(node.commit_index, 1)
}
