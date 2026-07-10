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
/// The smallest useful tour of the library: elect a leader, replicate a
/// command, kill the leader, and confirm the survivors still agree.
///
/// Run it with `moon run cmd/example`.
fn main {
  let ids = ["a", "b", "c", "d", "e"]
  let cluster = @raft.Cluster::new(ids, seed=1)
  println("cluster of \{ids.length()} nodes, seed 1")

  // Elections are randomized, so the simulator ticks until one node wins.
  let leader = match cluster.run_until_leader(200) {
    Some(id) => id
    None => {
      println("no leader within 200 ticks")
      return
    }
  }
  println("elected leader: \{leader}")

  if !cluster.propose(b"set x = 1") {
    println("leader refused the proposal")
    return
  }
  if !cluster.run_until_committed(1, 200) {
    println("command did not commit within 200 ticks")
    return
  }
  println("committed 'set x = 1' on a majority")

  // Kill the leader. A survivor must take over without ever admitting a
  // second leader in the same term, and the committed prefix must survive.
  cluster.crash(leader)
  println("crashed the leader")
  match cluster.run_until_leader(400) {
    Some(next) => println("new leader: \{next}")
    None => {
      println("no leader re-elected within 400 ticks")
      return
    }
  }

  println("one leader per term : \{cluster.one_leader_per_term()}")
  println("committed prefixes agree : \{cluster.committed_agrees()}")
  println("safety invariants hold : \{cluster.invariants_hold()}")
}
