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
/// Persistence and crash recovery. The same `RawNode` Ready / Advance loop runs
/// a cluster; then one node's durable state is copied into a `MemoryStorage`,
/// and a fresh node rebuilt from that storage comes back with the same term,
/// commit index and log - the restart path a deployment drives from a write-ahead
/// log.
///
///     moon run src/examples/10-storage
struct Net {
  nodes : Map[String, @raft.RawNode]
}

///|
fn Net::new(ids : Array[String]) -> Net {
  let seeds = [1UL, 2UL, 3UL, 4UL, 5UL, 6UL, 7UL, 8UL]
  let nodes : Map[String, @raft.RawNode] = Map([])
  for i = 0; i < ids.length(); i = i + 1 {
    let id = ids[i]
    let peers = ids.filter(fn(x) { x != id })
    let raft = @raft.RaftNode::new(id, peers, seed=seeds[i])
    nodes.set(id, raft.raw())
  }
  { nodes, }
}

///|
fn Net::round(self : Net, ids : Array[String]) -> Unit {
  for id in ids {
    self.nodes[id].tick()
  }
  let mut queue : Array[@raft.Message] = []
  for id in ids {
    for m in self.nodes[id].stabilize() {
      queue.push(m)
    }
  }
  for _ in 0..<10000 {
    if queue.is_empty() {
      break
    }
    let next : Array[@raft.Message] = []
    for m in queue {
      if self.nodes.contains(m.to) {
        self.nodes[m.to].step(m)
        for mm in self.nodes[m.to].stabilize() {
          next.push(mm)
        }
      }
    }
    queue = next
  }
}

///|
fn Net::leader(self : Net, ids : Array[String]) -> String? {
  for id in ids {
    if self.nodes[id].node().is_leader() {
      return Some(id)
    }
  }
  None
}

///|
fn Net::propose(self : Net, id : String, data : Bytes) -> Unit {
  self.nodes[id].propose(data)
}

///|
fn main {
  let ids = ["a", "b", "c"]
  let net = Net::new(ids)

  for _ in 0..<40 {
    net.round(ids)
    if net.leader(ids) is Some(_) {
      break
    }
  }
  let leader = net.leader(ids)
  match leader {
    Some(l) =>
      for i in 0..<3 {
        net.propose(l, b"cmd-\{i}")
        for _ in 0..<8 {
          net.round(ids)
        }
      }
    None => ()
  }

  // Snapshot node a's durable state into an external storage.
  let store = @raft.MemoryStorage::new()
  net.nodes["a"].node().node().save_into(store)

  let before_term = net.nodes["a"].node().term()
  let before_commit = net.nodes["a"].node().commit_index()
  let before_last = net.nodes["a"].node().node().last_log_index()
  println(
    "before restart: term=\{before_term}, commit=\{before_commit}, last_log=\{before_last}",
  )

  // A fresh node rebuilt from that storage.
  let recovered = @raft.RaftNode::new("a", ["b", "c"], seed=1UL)
  recovered.node().load_from(store)

  let after_term = recovered.term()
  let after_commit = recovered.commit_index()
  let after_last = recovered.node().last_log_index()
  println(
    "after restart:  term=\{after_term}, commit=\{after_commit}, last_log=\{after_last}",
  )
  println(
    "recovered matches: \{after_term == before_term && after_commit == before_commit && after_last == before_last}",
  )
}
