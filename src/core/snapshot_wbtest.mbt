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
test "compaction discards the log prefix and keeps the baseline term" {
  let a = Node::new("a")
  let _ = a.append(1, b"x")
  let _ = a.append(1, b"y")
  let _ = a.append(2, b"z")
  a.advance_commit(3)
  let snap = a.compact(2, b"state@2")
  assert_eq(snap.last_index, 2)
  assert_eq(snap.last_term, 1)
  assert_eq(a.last_log_index(), 3)
  assert_eq(a.term_at(3), 2)
  assert_eq(a.term_at(2), 1)
  assert_eq(a.term_at(1), 0)
}

///|
test "compaction refuses to discard uncommitted entries" {
  let a = Node::new("a")
  let _ = a.append(1, b"x")
  let _ = a.append(1, b"y")
  let snap = a.compact(2, b"state@2")
  assert_eq(snap.last_index, 0)
  assert_eq(a.last_log_index(), 2)
}

///|
test "installing a snapshot replaces the log and advances commit" {
  let a = Node::new("a")
  let _ = a.append(1, b"old")
  a.install_snapshot({
    last_index: 5,
    last_term: 3,
    data: b"snap",
    conf_state: ConfState::empty(),
  })
  assert_eq(a.last_log_index(), 5)
  assert_eq(a.last_log_term(), 3)
  assert_eq(a.commit_index, 5)
}
