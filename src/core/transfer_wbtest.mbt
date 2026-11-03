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
test "scenario: leadership is handed to a chosen follower" {
  let c = Cluster::new(["a", "b", "c"], seed=51)
  let first = c.run_until_leader(200).unwrap()
  let _ = c.propose(b"w1")
  let _ = c.run_until_committed(2, 200)

  // Pick a follower and transfer to it. Give a couple of ticks so it is caught
  // up, then hand over.
  let target = other_than(c, first)
  c.run(10)
  assert_true(c.transfer_leadership(target))

  // The target campaigns at once and takes over in a higher term.
  c.run(60)
  assert_true(c.node(target).is_leader())
  assert_true(c.node(target).term() > 1)
  assert_false(c.node(first).is_leader())
  assert_true(c.one_leader_per_term())
  assert_true(c.committed_agrees())
}

///|
test "transfer is a no-op on a follower" {
  let f = RaftNode::new("f", ["a", "b"], seed=1)
  assert_eq(f.transfer_leadership("a").length(), 0)
}
