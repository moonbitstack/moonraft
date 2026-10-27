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
/// Learners: non-voting members that receive the log but never campaign and
/// never count toward a quorum. Add `d` as a learner, show it joins the
/// configuration without joining the voter set, prove it cannot elect a leader
/// on its own, then promote it to a full voter.
///
///     moon run src/examples/06-learner
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
fn voters(cluster : @raft.Cluster) -> String {
  let vs = cluster.node("a").voter_nodes()
  vs.sort()
  vs.join(",")
}

///|
fn learners(cluster : @raft.Cluster) -> String {
  let ls = cluster.node("a").conf_state().learners
  ls.sort()
  ls.join(",")
}

///|
fn main {
  let ids = ["a", "b", "c"]
  let cluster = @raft.Cluster::new(ids, seed=3)
  let _ = cluster.run_until_leader(200)
  let _ = cluster.propose(b"v1")
  commit_to(cluster, 200)

  // Add d as a learner (non-voting).
  let _ = cluster.propose_conf(@raft.ConfChange::add_learner("d"))
  commit_to(cluster, 200)

  println("voters:  \{voters(cluster)}")
  println("learners: \{learners(cluster)}")

  // A learner cannot vote or campaign: with every voter down, no leader appears.
  cluster.crash("a")
  cluster.crash("b")
  cluster.crash("c")
  let elected = cluster.run_until_leader(400)
  println("only a learner up -> leader elected: \{elected is Some(_)}")

  // Bring the voters back and promote the learner to a full voter.
  cluster.restart("a")
  cluster.restart("b")
  cluster.restart("c")
  let _ = cluster.run_until_leader(200)
  let _ = cluster.propose_conf(@raft.ConfChange::add("d"))
  commit_to(cluster, 200)
  println("after promoting d -> voters:  \{voters(cluster)}")
  println("after promoting d -> learners: \{learners(cluster)}")
}
