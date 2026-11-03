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
test "scenario: a configuration change replicates and every node folds it in" {
  let c = Cluster::new(["a", "b", "c"], seed=11)
  let _ = c.run_until_leader(200)
  let _ = c.propose(b"normal-1")
  let _ = c.propose_conf(ConfChange::add("f"))
  let _ = c.propose(b"normal-2")
  // no-op(1) + normal(2) + conf(3) + normal(4)
  assert_true(c.run_until_committed(4, 300))

  // Every node, folding its own committed log through a configuration state
  // machine, arrives at the same membership — including the added server.
  for id in ["a", "b", "c"] {
    let driver = ConfDriver::new(["a", "b", "c"])
    let _ = driver.drive(c.node(id).node())
    assert_true(driver.config().contains("f"))
    assert_eq(driver.config().size(), 4)
  }
  assert_true(c.same_committed_commands())
}

///|
test "scenario: a removal change is folded in consistently" {
  let c = Cluster::new(["a", "b", "c"], seed=13)
  let _ = c.run_until_leader(200)
  let _ = c.propose_conf(ConfChange::remove("c"))
  assert_true(c.run_until_committed(2, 300))
  let driver = ConfDriver::new(["a", "b", "c"])
  let _ = driver.drive(c.node("a").node())
  assert_false(driver.config().contains("c"))
  assert_eq(driver.config().size(), 2)
}
