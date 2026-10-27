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
/// Log compaction (§7). The leader folds the prefix of its log into a snapshot
/// so the live log stops growing without bound. Here the leader compacts, then a
/// follower that was down while the leader kept appending restarts and catches
/// up from the compacted log - the snapshot is exactly the point a lagging node
/// is served from by the leader's automatic InstallSnapshot fallback.
///
///     moon run examples/08-snapshot
fn commit_to(cluster : @raft.Cluster, max_ticks : Int32) -> Unit {
  match cluster.leader() {
    Some(l) => {
      let target = cluster.node(l).node().last_log_index()
      let _ = cluster.run_until_committed(target, max_ticks)
    }
    None => ()
  }
}

///|
fn main {
  let ids = ["a", "b", "c"]
  let cluster = @raft.Cluster::new(ids, seed=9)
  let lead = cluster.run_until_leader(200).unwrap()

  // Grow the log.
  for i in 0..<8 {
    let _ = cluster.propose(b"cmd-\{i}")
    commit_to(cluster, 100)
  }

  // Fold everything at or below index 5 into a snapshot.
  let _ = cluster.compact_leader(5, b"snapshot-payload")

  // While c is down, the leader keeps appending to its compacted log.
  cluster.crash("c")
  for i in 0..<4 {
    let _ = cluster.propose(b"more-\{i}")
    commit_to(cluster, 100)
  }
  let leader_committed = cluster.node(lead).commit_index()

  // c restarts and must catch up from the compacted log.
  cluster.restart("c")
  cluster.run(400)
  println(
    "c caught up to leader:   \{cluster.node("c").commit_index() == leader_committed}",
  )
  println("committed agree:         \{cluster.committed_agrees()}")
  println("invariants hold:         \{cluster.invariants_hold()}")
}
