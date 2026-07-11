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
/// Day-two operations on a running cluster: inspect each follower's replication
/// progress, hand leadership to a chosen peer, compact the log behind a
/// snapshot, and turn on check-quorum.
///
///     moon run examples/03-operations
#coverage.skip
fn main {
  let ids = ["a", "b", "c"]
  let cluster = @raft.Cluster::new(ids, seed=9)
  let leader = cluster.run_until_leader(200).unwrap()
  println("leader: \{leader}")

  // Commit a batch of writes (indices 2..6, after the leader's empty entry).
  for _ in 0..<5 {
    let _ = cluster.propose(b"op")
  }
  let _ = cluster.run_until_committed(6, 300)

  // The leader's view of every follower's replication progress.
  println("replication progress seen by \{leader}:")
  for p in cluster.node(leader).progress_status() {
    println(
      "  \{p.id}: match=\{p.match_index} next=\{p.next_index} learner=\{p.is_learner}",
    )
  }

  // Hand leadership to a specific peer and let it settle.
  let target = if leader == "a" { "b" } else { "a" }
  println(
    "request transfer to \{target}: \{cluster.transfer_leadership(target)}",
  )
  cluster.run(200)
  match cluster.leader() {
    Some(l) => println("leader after transfer: \{l}")
    None => println("leader after transfer: none")
  }

  // Compact the log up to index 4 behind a snapshot; older entries are dropped.
  println("compact up to index 4: \{cluster.compact_leader(4, b"snapshot@4")}")

  // Check-quorum: a leader that cannot reach a majority steps down on its own.
  cluster.enable_check_quorum()
  cluster.run(150)
  println("committed prefixes still agree: \{cluster.committed_agrees()}")
}
