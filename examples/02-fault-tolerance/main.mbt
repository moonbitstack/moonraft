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
/// Chaos without contradiction. Crash the leader, split the network, isolate a
/// node, then drop and delay packets - and check that Raft's safety invariants
/// held through all of it.
///
///     moon run examples/02-fault-tolerance
#coverage.skip
fn main {
  let ids = ["a", "b", "c", "d", "e"]
  let cluster = @raft.Cluster::new(ids, seed=5)
  let first = cluster.run_until_leader(200).unwrap()
  let _ = cluster.propose(b"cmd-1")
  let _ = cluster.run_until_committed(2, 200)
  println("initial leader: \{first}")

  // 1) Crash the leader; a survivor must take over.
  cluster.crash(first)
  let second = cluster.run_until_leader(400).unwrap()
  println("crashed \{first} -> new leader \{second}")
  let _ = cluster.propose(b"cmd-2")
  cluster.run(200)

  // 2) Restart the old leader; it rejoins as a follower and catches up.
  cluster.restart(first)
  cluster.run(300)
  println("\{first} rejoined (down = \{cluster.is_down(first)})")

  // 3) Isolate the current leader; the majority side elects another.
  match cluster.leader() {
    Some(cur) => {
      cluster.isolate(cur)
      cluster.run(400)
      cluster.heal()
      cluster.run(300)
      println("isolated \{cur}, then healed; live leaders now = \{cluster.leaders().length()}")
    }
    None => println("no stable leader to isolate")
  }

  // 4) A lossy, delayed link still converges once the storm passes.
  cluster.set_drop(150) // 15% packet loss
  cluster.set_delay(3)
  let _ = cluster.propose(b"cmd-3")
  cluster.run(600)
  cluster.set_drop(0)
  cluster.set_delay(0)
  cluster.run(300)

  println("--- safety invariants after all of that ---")
  println("at most one leader per term:  \{cluster.one_leader_per_term()}")
  println("committed prefixes agree:     \{cluster.committed_agrees()}")
  println("logs are consistent:          \{cluster.logs_consistent()}")
}
