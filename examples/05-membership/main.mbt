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
/// Membership changes, one server at a time. Add a fourth voter, watch the
/// quorum grow, then remove one and watch it shrink - the cluster keeps
/// agreeing and the safety invariants keep holding the whole way through.
///
///     moon run examples/05-membership
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
fn show(title : String, cluster : @raft.Cluster) -> Unit {
  let vs = cluster.node("a").voter_nodes()
  vs.sort()
  println("\{title}: \{vs.join(",")}")
}

///|
fn main {
  let ids = ["a", "b", "c"]
  let cluster = @raft.Cluster::new(ids, seed=7)
  let _ = cluster.run_until_leader(200)
  let _ = cluster.propose(b"init")
  commit_to(cluster, 200)

  show("voters at start      ", cluster)

  // Add a fourth voter. The change is an ordinary log entry, so every node
  // folds it in at the same position once it commits (§6).
  let _ = cluster.propose_conf(@raft.ConfChange::add("d"))
  commit_to(cluster, 200)
  show("after adding d        ", cluster)

  // Remove a voter. Quorum math shifts from 2/3 to 2/4.
  let _ = cluster.propose_conf(@raft.ConfChange::remove("c"))
  commit_to(cluster, 200)
  show("after removing c      ", cluster)

  // The cluster is still a working cluster after reconfiguration.
  let _ = cluster.propose(b"after-reconfig")
  commit_to(cluster, 200)
  println("still agrees after reconfig: \{cluster.committed_agrees()}")
  println("safety invariants hold:       \{cluster.invariants_hold()}")
}
