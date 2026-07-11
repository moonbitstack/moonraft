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
/// Replicate a few commands through the log and confirm every node commits the
/// same prefix - the one guarantee a replicated log exists to provide.
///
///     moon run examples/01-replicate
fn main {
  let ids = ["a", "b", "c"]
  let cluster = @raft.Cluster::new(ids, seed=11)
  let leader = cluster.run_until_leader(200).unwrap()
  println("leader: \{leader}")

  // On election the leader appends one empty entry (index 1); our three
  // commands follow it, landing at indices 2, 3 and 4.
  let commands = [b"set x = 1", b"set y = 2", b"set x = 3"]
  let mut n = 0
  for cmd in commands {
    n = n + 1
    println("propose #\{n}: accepted by leader = \{cluster.propose(cmd)}")
  }
  let last : UInt64 = 4
  let ok = cluster.run_until_committed(last, 300)
  println("every node committed through index \{last}: \{ok}")

  for id in ids {
    println("  \{id}: commit_index = \{cluster.node(id).commit_index()}")
  }
  println("committed logs agree across the cluster: \{cluster.committed_agrees()}")
}
