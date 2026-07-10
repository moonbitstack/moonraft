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
  // Only simulated nodes emit messages, and every simulated node is registered
  // in `part`, so the sender's group is always present. The recipient may be a
  // peer added by a configuration change that this cluster never instantiated;
  // such an id defaults to group 0 (its messages are later dropped when the node
  // lookup fails in `deliver_due`).
  let ga = self.part[from]
  let gb = self.part.get(to).unwrap_or(0)
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
    if !dropped && self.nodes.get(m.to) is Some(n) {
      for r in n.step(m) {
        self.schedule(r)
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
      for msg in self.nodes[id].tick() {
        self.schedule(msg)
      }
    }
  }
  self.deliver_due()
}

///|
/// Advance the cluster by `ticks` ticks.
pub fn Cluster::run(self : Cluster, ticks : Int) -> Unit {
  for _ in 0..<ticks {
    self.tick()
  }
}

///|
/// Propose a command on the current leader, if there is one. Returns whether a
/// leader accepted it.
pub fn Cluster::propose(self : Cluster, command : Bytes) -> Bool {
  guard self.leader() is Some(id) else { return false }
  for msg in self.nodes[id].propose(command) {
    self.schedule(msg)
  }
  true
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
  guard self.leader() is Some(id) else { return false }
  let _ = self.nodes[id].node().compact(upto, data)
  true
}

///|
/// Ask the current leader to transfer leadership to `target`. Returns whether a
/// leader started the transfer.
pub fn Cluster::transfer_leadership(self : Cluster, target : String) -> Bool {
  guard self.leader() is Some(id) else { return false }
  for msg in self.nodes[id].transfer_leadership(target) {
    self.schedule(msg)
  }
  true
}

///|
/// Enable check-quorum (and lease reads) on every server.
pub fn Cluster::enable_check_quorum(self : Cluster) -> Unit {
  for id in self.ids {
    self.nodes[id].enable_check_quorum()
  }
}

///|
/// Propose a configuration change on the current leader. Returns whether a
/// leader accepted it.
pub fn Cluster::propose_conf(self : Cluster, change : ConfChange) -> Bool {
  guard self.leader() is Some(id) else { return false }
  for msg in self.nodes[id].propose_conf(change) {
    self.schedule(msg)
  }
  true
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
  self.leaders().get(0)
}

///|
/// Tick until a leader emerges or `max_ticks` elapse; returns the leader id.
pub fn Cluster::run_until_leader(self : Cluster, max_ticks : Int) -> String? {
  for _ in 0..<max_ticks {
    self.tick()
    let ls = self.leaders()
    if ls.length() > 0 {
      return Some(ls[0])
    }
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
  for _ in 0..<max_ticks {
    self.tick()
    if self.all_committed(index) {
      return true
    }
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

///|
/// The outcome of the introductory demonstration `cmd/example` runs: a leader is
/// elected, one command is replicated, the leader is crashed, and a survivor
/// takes over. Kept as data rather than printed inline so the very run a reader
/// watches is the run a test asserts against.
pub struct DemoReport {
  seed : UInt64
  node_count : Int
  first_leader : String?
  proposal_accepted : Bool
  committed : Bool
  second_leader : String?
  one_leader_per_term : Bool
  committed_agrees : Bool
  invariants_hold : Bool
}

///|
/// Run the introductory simulation and record what happened at each step.
///
/// Parameters:
/// - `ids` : the servers to simulate.
/// - `seed` : fixes the deterministic run.
/// - `elect_ticks` : tick budget for the first election.
/// - `commit_ticks` : tick budget for committing the first command.
/// - `reelect_ticks` : tick budget for the re-election after the crash.
///
/// Returns a `DemoReport` naming the elected leader, whether the command
/// committed, the successor once the leader is crashed, and the safety
/// invariants that must survive the succession.
pub fn demo_run(
  ids : Array[String],
  seed : UInt64,
  elect_ticks : Int,
  commit_ticks : Int,
  reelect_ticks : Int,
) -> DemoReport {
  let c = Cluster::new(ids, seed~)
  let first = c.run_until_leader(elect_ticks)
  let accepted = first is Some(_) && c.propose(b"set x = 1")
  let committed = accepted && c.run_until_committed(1, commit_ticks)
  if first is Some(leader) {
    c.crash(leader)
  }
  let second = if committed { c.run_until_leader(reelect_ticks) } else { None }
  {
    seed,
    node_count: ids.length(),
    first_leader: first,
    proposal_accepted: accepted,
    committed,
    second_leader: second,
    one_leader_per_term: c.one_leader_per_term(),
    committed_agrees: c.committed_agrees(),
    invariants_hold: c.invariants_hold(),
  }
}

///|
/// Render a [DemoReport] as the exact lines `cmd/example` prints, stopping at
/// whichever step the run failed to reach.
///
/// Parameters:
/// - `report` : the outcome to render.
///
/// Returns the human-readable transcript, one entry per line.
pub fn demo_report_lines(report : DemoReport) -> Array[String] {
  let out : Array[String] = [
    "cluster of \{report.node_count} nodes, seed \{report.seed}",
  ]
  guard report.first_leader is Some(leader) else {
    out.push("no leader within 200 ticks")
    return out
  }
  out.push("elected leader: \{leader}")
  guard report.proposal_accepted else {
    out.push("leader refused the proposal")
    return out
  }
  guard report.committed else {
    out.push("command did not commit within 200 ticks")
    return out
  }
  out.push("committed 'set x = 1' on a majority")
  out.push("crashed the leader")
  guard report.second_leader is Some(next) else {
    out.push("no leader re-elected within 400 ticks")
    return out
  }
  out.push("new leader: \{next}")
  out.push("one leader per term : \{report.one_leader_per_term}")
  out.push("committed prefixes agree : \{report.committed_agrees}")
  out.push("safety invariants hold : \{report.invariants_hold}")
  out
}
