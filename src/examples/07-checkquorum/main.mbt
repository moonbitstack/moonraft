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
/// Check-quorum: a leader that can no longer reach a majority of its cluster
/// steps down on its own, instead of going on to serve reads and writes the
/// rest of the cluster has moved past. Elect a leader, turn check-quorum on,
/// isolate the leader, and watch it relinquish leadership.
///
///     moon run src/examples/07-checkquorum

///|
fn main {
  let ids = ["a", "b", "c"]
  let cluster = @raft.Cluster::new(ids, seed=5)
  let leader = cluster.run_until_leader(200).unwrap()
  cluster.enable_check_quorum()

  println("leader before isolate: \{leader}")
  println("  \{leader} is leader: \{cluster.node(leader).is_leader()}")

  // The leader is cut off from the other two nodes.
  cluster.isolate(leader)
  cluster.run(400)

  println("after isolating \{leader} with check-quorum on:")
  println("  \{leader} still leader:  \{cluster.node(leader).is_leader()}")
  println("  a leader still serves:   \{cluster.leader() is Some(_)}")

  // Heal the partition; the cluster must converge back to one leader and its
  // safety invariants must hold again.
  cluster.heal()
  cluster.run(400)
  println("after healing the partition:")
  println("  single leader again:     \{cluster.leader() is Some(_)}")
  println("  invariants hold:         \{cluster.invariants_hold()}")
}
