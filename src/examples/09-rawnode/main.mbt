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
/// The message-driven deployment path. The `Cluster` simulator drives the core
/// for you; here we drive it ourselves with `RawNode` and the Ready / Advance
/// cycle a real transport runs: tick every node, take each `Ready`, persist its
/// entries and hard state, send its messages, apply its committed entries, then
/// `advance`. No goroutines, no async - a `Ready` is a plain value.
///
///     moon run src/examples/09-rawnode

///|
/// A tiny in-process network: a `RawNode` per id and the messages in flight.
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
    // A distinct seed per node desynchronises the election timers, so one
    // node campaigns first and wins - the same trick the Cluster simulator uses.
    let raft = @raft.RaftNode::new(id, peers, seed=seeds[i])
    nodes.set(id, raft.raw())
  }
  { nodes, }
}

///|
fn Net::tick_all(self : Net, ids : Array[String]) -> Unit {
  for id in ids {
    self.nodes[id].tick()
  }
}

///|
/// One tick of the whole cluster, then deliver messages until the network is
/// quiet: take each node's `Ready`, persist + apply + `advance`, send its
/// messages, and repeat for whatever those messages provoke.
fn Net::round(self : Net, ids : Array[String]) -> Unit {
  self.tick_all(ids)
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
fn Net::commit_index(self : Net, id : String) -> UInt64 {
  self.nodes[id].node().commit_index()
}

///|
fn main {
  let ids = ["a", "b", "c"]
  let net = Net::new(ids)

  // Elect a leader.
  for _ in 0..<40 {
    net.round(ids)
    if net.leader(ids) is Some(_) {
      break
    }
  }
  let leader = net.leader(ids)
  println("leader elected:  \{leader is Some(_)}")

  match leader {
    Some(l) => {
      net.propose(l, b"hello raft")
      // Replicate and commit the proposal.
      for _ in 0..<10 {
        net.round(ids)
      }
      println("leader commit_index:   \{net.commit_index(l)}")
    }
    None => ()
  }
  println("b commit_index:        \{net.commit_index("b")}")
  println("c commit_index:        \{net.commit_index("c")}")
}
