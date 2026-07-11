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
/// Hello, Raft. Bring up a five-node cluster and let it elect a leader - the
/// quiet, agreed-upon state every later example departs from.
///
///     moon run examples/00-helloworld
fn main {
  let ids = ["n1", "n2", "n3", "n4", "n5"]
  let cluster = @raft.Cluster::new(ids, seed=7)
  println("started a 5-node cluster - no leader yet")
  match cluster.run_until_leader(300) {
    None => println("no leader emerged within 300 ticks")
    Some(id) => {
      let leader = cluster.node(id)
      println("elected \{id} as leader in term \{leader.term()}")
      for peer in ids {
        if peer != id {
          let n = cluster.node(peer)
          let seen = match n.leader() {
            Some(l) => l
            None => "?"
          }
          println("  \{peer}: follows \{seen}, term \{n.term()}")
        }
      }
    }
  }
}
