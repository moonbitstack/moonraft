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

// Network-orchestration safety tests, driven through the deterministic cluster
// simulator. Adapted from etcd-io/raft (Apache-2.0) raft_test.go:
// TestDuelingCandidates / TestLeaderElectionOverwriteNewerLogs (the safety
// properties, expressed over partitions rather than etcd's message plumbing).

///|
fn five() -> Array[String] {
  ["1", "2", "3", "4", "5"]
}

///|
/// A minority partition cannot elect a leader; only the majority side can. This
/// is Election Safety under a split (a "dueling candidates" outcome).
test "a minority partition cannot elect a leader" {
  let c = Cluster::new(five(), seed=7)
  c.enable_check_quorum()
  let first = c.run_until_leader(300)
  assert_true(first != None)
  // Split into a majority {3,4,5} and a minority {1,2}.
  c.partition(["1", "2"], ["3", "4", "5"])
  c.run(400)
  // Every server that still thinks it leads is in the majority partition, and
  // no term ever has two leaders.
  for l in c.leaders() {
    assert_true(["3", "4", "5"].contains(l))
  }
  assert_true(c.one_leader_per_term())
  // The majority can still make progress.
  assert_true(c.leaders().length() >= 1)
}

///|
/// After the partition heals, the cluster reconverges to a single leader and a
/// fresh proposal commits on every node — no split-brain survived.
test "a healed partition reconverges and commits everywhere" {
  let c = Cluster::new(five(), seed=13)
  c.enable_check_quorum()
  c.run_until_leader(300) |> ignore
  c.partition(["1", "2"], ["3", "4", "5"])
  c.run(400)
  c.heal()
  let leader = c.run_until_leader(400)
  assert_true(leader != None)
  assert_true(c.one_leader_per_term())
  // A proposal now replicates to all five nodes.
  assert_true(c.propose(b"after-heal"))
  let idx = c.node(leader.unwrap()).commit_index()
  assert_true(c.run_until_committed(idx, 400))
  assert_true(c.committed_agrees())
  assert_true(c.logs_consistent())
}

///|
/// Isolating the leader forces the surviving majority to elect a new one at a
/// higher term; when the old leader rejoins it discovers the newer term and
/// steps down, so its stale log never overwrites the majority's.
test "an isolated leader is superseded and steps down on rejoin" {
  let c = Cluster::new(five(), seed=21)
  c.enable_check_quorum()
  let old = c.run_until_leader(300).unwrap()
  let old_term = c.node(old).term()
  c.isolate(old)
  // Check-quorum demotes the isolated leader; the majority elects a new one at
  // a higher term.
  c.run(600)
  assert_false(c.node(old).is_leader())
  assert_true(c.one_leader_per_term())
  let ls = c.leaders()
  assert_true(ls.length() >= 1)
  for l in ls {
    assert_true(c.node(l).term() > old_term)
  }
  // Rejoin: the old node's stale log never overwrites the majority's.
  c.heal()
  c.run(400)
  assert_true(c.one_leader_per_term())
  assert_false(c.node(old).is_leader())
  assert_true(c.logs_consistent())
}
