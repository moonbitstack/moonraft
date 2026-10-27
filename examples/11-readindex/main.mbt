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
/// A linearizable read (ReadOnlySafe, §6.3). Instead of serving a read from a
/// stale lease, the leader confirms its commit index with a fresh quorum round
/// trip; only once a quorum acknowledges does the read become safe. We ask for a
/// read, drive the Ready / Advance loop by hand so we can see the confirming
/// `ReadState` surface on the leader's `Ready`, and report that the read is safe.
///
///     moon run examples/11-readindex
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
/// Drive the loop by hand (tick, Ready, store, advance, deliver) and count the
/// `ReadState`s that surface on `leader`'s Ready - each one is a read the quorum
/// has confirmed safe.
fn Net::collect_reads(
  self : Net,
  ids : Array[String],
  leader : String,
) -> Int32 {
  let mut total = 0
  for _ in 0..<300 {
    for id in ids {
      self.nodes[id].tick()
    }
    let queue : Array[@raft.Message] = []
    for id in ids {
      if self.nodes[id].has_ready() {
        let rd = self.nodes[id].ready()
        self.nodes[id].store(rd)
        self.nodes[id].advance(rd)
        if id == leader {
          total = total + rd.read_states.length()
        }
        for m in rd.messages {
          queue.push(m)
        }
      }
    }
    for m in queue {
      if self.nodes.contains(m.to) {
        self.nodes[m.to].step(m)
      }
    }
    if total > 0 {
      break
    }
  }
  total
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
  println("leader elected:  \{leader is Some(_)}")

  match leader {
    Some(l) => {
      // Get something committed so there is a commit index to read at.
      net.propose(l, b"v1")
      for _ in 0..<8 {
        net.round(ids)
      }
      let committed = net.nodes[l].node().commit_index()
      println("leader commit_index before read: \{committed}")

      // Ask for a linearizable read.
      net.nodes[l].read_index(b"client-ctx-1")
      let confirmed = net.collect_reads(ids, l)
      println("read confirmed by quorum:        \{confirmed > 0}")
      println("safe to read at commit index:   \{committed}")
    }
    None => ()
  }
}
