///|
/// One message in flight across the simulated network: the tick it is due to be
/// delivered, a monotonic sequence number for deterministic tie-breaking, and
/// the message itself.
struct InFlight {
  deliver_at : Int
  msg : Message
}

///|
/// A deterministic, discrete-time cluster simulator. It owns a set of RaftNodes
/// and a network that can be told to drop, delay, reorder and partition traffic,
/// then advanced tick by tick. Because every random choice comes from a single
/// seeded PRNG, a whole run — elections, replication, failures and all — replays
/// identically, which is what makes it useful for finding consensus bugs and
/// pinning them down (the deterministic-simulation approach the task recommends).
pub struct Cluster {
  nodes : Map[String, RaftNode]
  ids : Array[String]
  mut inflight : Array[InFlight]
  mut now : Int
  part : Map[String, Int]
  down : Map[String, Bool]
  mut drop_permil : Int
  mut max_delay : Int
  mut next_group : Int
  mut rng : UInt64
}

///|
/// Build a cluster of the given server ids. Every node knows every other as a
/// peer and starts in one network partition (fully connected). `seed` fixes the
/// network's PRNG; per-node election jitter is seeded from each id so a run is
/// fully reproducible.
pub fn Cluster::new(ids : Array[String], seed? : UInt64 = 42) -> Cluster {
  let nodes : Map[String, RaftNode] = {}
  let part : Map[String, Int] = {}
  let down : Map[String, Bool] = {}
  let mut s = seed
  for id in ids {
    let peers : Array[String] = []
    for other in ids {
      if other != id {
        peers.push(other)
      }
    }
    s = s * 2862933555777941757UL + 3037000493UL
    nodes[id] = RaftNode::new(id, peers, seed=s)
    part[id] = 0
    down[id] = false
  }
  {
    nodes,
    ids: ids.copy(),
    inflight: [],
    now: 0,
    part,
    down,
    drop_permil: 0,
    max_delay: 0,
    next_group: 100,
    rng: seed,
  }
}

///|
/// The next value of the network PRNG (a 64-bit LCG).
fn Cluster::rand(self : Cluster) -> UInt64 {
  self.rng = self.rng * 6364136223846793005UL + 1442695040888963407UL
  self.rng
}

///|
/// The server with the given id.
pub fn Cluster::node(self : Cluster, id : String) -> RaftNode {
  self.nodes[id]
}

///|
/// Set the per-message drop probability, in parts per thousand.
pub fn Cluster::set_drop(self : Cluster, permil : Int) -> Unit {
  self.drop_permil = permil
}

///|
/// Set the maximum extra delivery delay, in ticks. Any value above zero also
/// reorders traffic, since messages sent together can arrive apart.
pub fn Cluster::set_delay(self : Cluster, max_delay : Int) -> Unit {
  self.max_delay = max_delay
}

///|
/// Split the cluster so that the two id groups cannot exchange messages. Nodes
/// inside a group still reach each other; nodes not listed keep their group.
pub fn Cluster::partition(
  self : Cluster,
  group_a : Array[String],
  group_b : Array[String],
) -> Unit {
  let ga = self.next_group
  let gb = self.next_group + 1
  self.next_group = self.next_group + 2
  for id in group_a {
    self.part[id] = ga
  }
  for id in group_b {
    self.part[id] = gb
  }
}

///|
/// Cut one node off from every other node.
pub fn Cluster::isolate(self : Cluster, id : String) -> Unit {
  self.part[id] = self.next_group
  self.next_group = self.next_group + 1
}

///|
/// Heal all partitions: every node shares one network again.
pub fn Cluster::heal(self : Cluster) -> Unit {
  for id in self.ids {
    self.part[id] = 0
  }
}

///|
/// Stop a node: it no longer ticks and all traffic to or from it is dropped,
/// modelling a crash. Its state is retained, so `restart` brings it back as it
/// would return after reloading from stable storage.
pub fn Cluster::crash(self : Cluster, id : String) -> Unit {
  self.down[id] = true
}

///|
/// Bring a stopped node back.
pub fn Cluster::restart(self : Cluster, id : String) -> Unit {
  self.down[id] = false
}

///|
/// Whether `id` is currently stopped.
pub fn Cluster::is_down(self : Cluster, id : String) -> Bool {
  self.down.get(id) == Some(true)
}

///|
/// Whether a message from `from` to `to` can be delivered: both endpoints up
/// and in the same network partition.
fn Cluster::reachable(self : Cluster, from : String, to : String) -> Bool {
  if self.down.get(from) == Some(true) || self.down.get(to) == Some(true) {
    return false
  }
  let ga = match self.part.get(from) {
    Some(g) => g
    None => 0
  }
  let gb = match self.part.get(to) {
    Some(g) => g
    None => 0
  }
  ga == gb
}

///|
/// Queue a message for delivery after the network's latency (one tick, plus a
/// random jitter up to `max_delay`).
fn Cluster::schedule(self : Cluster, msg : Message) -> Unit {
  let delay = if self.max_delay > 0 {
    (self.rand() % (self.max_delay + 1).to_uint64()).to_int()
  } else {
    0
  }
  self.inflight.push({ deliver_at: self.now + 1 + delay, msg })
}

///|
/// Deliver every message now due, dropping those cut off by a partition, a
/// stopped endpoint, or the random loss rate, and queueing whatever the
/// recipients send in reply.
fn Cluster::deliver_due(self : Cluster) -> Unit {
  let keep : Array[InFlight] = []
  let due : Array[InFlight] = []
  for f in self.inflight {
    if f.deliver_at <= self.now {
      due.push(f)
    } else {
      keep.push(f)
    }
  }
  self.inflight = keep
  for f in due {
    let m = f.msg
    let dropped = !self.reachable(m.from, m.to) ||
      (self.drop_permil > 0 && (self.rand() % 1000).to_int() < self.drop_permil)
    if !dropped {
      match self.nodes.get(m.to) {
        Some(n) =>
          for r in n.step(m) {
            self.schedule(r)
          }
        None => ()
      }
    }
  }
}

///|
/// Advance the whole cluster by one tick: every running node ticks (which may
/// start elections or emit heartbeats), then all due messages are delivered.
pub fn Cluster::tick(self : Cluster) -> Unit {
  self.now = self.now + 1
  for id in self.ids {
    if self.down.get(id) != Some(true) {
      match self.nodes.get(id) {
        Some(n) =>
          for msg in n.tick() {
            self.schedule(msg)
          }
        None => ()
      }
    }
  }
  self.deliver_due()
}

///|
/// Advance the cluster by `ticks` ticks.
pub fn Cluster::run(self : Cluster, ticks : Int) -> Unit {
  let mut i = 0
  while i < ticks {
    self.tick()
    i = i + 1
  }
}

///|
/// Propose a command on the current leader, if there is one. Returns whether a
/// leader accepted it.
pub fn Cluster::propose(self : Cluster, command : Bytes) -> Bool {
  match self.leader() {
    Some(id) => {
      for msg in self.nodes[id].propose(command) {
        self.schedule(msg)
      }
      true
    }
    None => false
  }
}

///|
/// Propose a command on a specific server. Useful when several nodes believe
/// they lead — for example a partitioned old leader alongside a fresh one — and
/// the test wants the proposal to go to a chosen side. Returns whether that
/// node accepted it as leader.
pub fn Cluster::propose_on(
  self : Cluster,
  id : String,
  command : Bytes,
) -> Bool {
  let n = self.nodes[id]
  if !n.is_leader() {
    return false
  }
  for msg in n.propose(command) {
    self.schedule(msg)
  }
  true
}

///|
/// Compact the current leader's log up to `upto`, standing the discarded prefix
/// in for a snapshot with payload `data`. A lagging follower that later needs an
/// entry from the discarded prefix will be caught up by InstallSnapshot. Returns
/// whether a leader performed the compaction.
pub fn Cluster::compact_leader(
  self : Cluster,
  upto : UInt64,
  data : Bytes,
) -> Bool {
  match self.leader() {
    Some(id) => {
      let _ = self.nodes[id].node().compact(upto, data)
      true
    }
    None => false
  }
}

///|
/// Enable check-quorum (and lease reads) on every server.
pub fn Cluster::enable_check_quorum(self : Cluster) -> Unit {
  for id in self.ids {
    self.nodes[id].enable_check_quorum()
  }
}

///|
/// The ids of every server that currently believes it is leader.
pub fn Cluster::leaders(self : Cluster) -> Array[String] {
  let out : Array[String] = []
  for id in self.ids {
    if self.down.get(id) != Some(true) && self.nodes[id].is_leader() {
      out.push(id)
    }
  }
  out
}

///|
/// The id of a current leader, if exactly the usual single one is running.
pub fn Cluster::leader(self : Cluster) -> String? {
  let ls = self.leaders()
  if ls.length() >= 1 {
    Some(ls[0])
  } else {
    None
  }
}

///|
/// Tick until a leader emerges or `max_ticks` elapse; returns the leader id.
pub fn Cluster::run_until_leader(self : Cluster, max_ticks : Int) -> String? {
  let mut i = 0
  while i < max_ticks {
    self.tick()
    let ls = self.leaders()
    if ls.length() > 0 {
      return Some(ls[0])
    }
    i = i + 1
  }
  None
}

///|
/// Tick until every running node has committed at least `index`, or `max_ticks`
/// elapse. Returns whether the target was reached.
pub fn Cluster::run_until_committed(
  self : Cluster,
  index : UInt64,
  max_ticks : Int,
) -> Bool {
  let mut i = 0
  while i < max_ticks {
    self.tick()
    if self.all_committed(index) {
      return true
    }
    i = i + 1
  }
  false
}

///|
/// Whether every running node has committed up through `index`.
pub fn Cluster::all_committed(self : Cluster, index : UInt64) -> Bool {
  for id in self.ids {
    if self.down.get(id) != Some(true) && self.nodes[id].commit_index() < index {
      return false
    }
  }
  true
}
