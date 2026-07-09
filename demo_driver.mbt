///|
/// Browser-facing demo driver. It wraps the deterministic `Cluster` simulator
/// behind a flat string/int/JSON surface so a JavaScript front end (compiled with
/// `moon build --target js`) can drive *real* Raft consensus — leader election,
/// log replication, network partitions, crashes and restarts — and read back a
/// full snapshot every tick: each node's role, term and log, the messages in
/// flight across the simulated network, and the safety invariants that must hold
/// no matter what chaos is injected. Only plain scalars and JSON strings cross
/// the FFI boundary, keeping the JS interop trivial and stable.

///|
priv struct DemoState {
  mut cluster : Cluster?
  mut seq : Int
}

///|
let demo_ctx : DemoState = { cluster: None, seq: 0 }

///|
/// Build a fresh cluster of `n` servers ("S1".."Sn") seeded by `seed`. Because
/// the simulator is fully deterministic, the same (n, seed) replays an identical
/// run — the "reset by seed" the UI exposes.
pub fn demo_new(n : Int, seed : Int) -> Unit {
  let count = if n < 1 { 1 } else if n > 9 { 9 } else { n }
  let ids : Array[String] = []
  for i in 0..<count {
    ids.push("S" + (i + 1).to_string())
  }
  demo_ctx.cluster = Some(Cluster::new(ids, seed=seed.to_uint64()))
  demo_ctx.seq = 0
}

///|
/// Advance the whole cluster by one discrete tick (nodes tick, then due messages
/// are delivered).
pub fn demo_tick() -> Unit {
  match demo_ctx.cluster {
    Some(c) => c.tick()
    None => ()
  }
}

///|
/// Advance the cluster by several ticks at once (for the "fast" speed setting).
pub fn demo_run(ticks : Int) -> Unit {
  match demo_ctx.cluster {
    Some(c) => c.run(ticks)
    None => ()
  }
}

///|
/// Propose a command on whichever server currently leads. Returns whether a
/// leader accepted it.
pub fn demo_propose(cmd : String) -> Bool {
  match demo_ctx.cluster {
    Some(c) => {
      demo_ctx.seq = demo_ctx.seq + 1
      c.propose(cmd_bytes(cmd))
    }
    None => false
  }
}

///|
/// Propose a command on a specific server — useful to hand a proposal to a
/// partitioned would-be leader and watch it fail to commit.
pub fn demo_propose_on(id : String, cmd : String) -> Bool {
  match demo_ctx.cluster {
    Some(c) => c.propose_on(id, cmd_bytes(cmd))
    None => false
  }
}

///|
/// Split the network into two groups that cannot exchange messages, given two
/// comma-separated id lists (e.g. "S1,S2" and "S3,S4,S5").
pub fn demo_partition(group_a : String, group_b : String) -> Unit {
  match demo_ctx.cluster {
    Some(c) => c.partition(split_commas(group_a), split_commas(group_b))
    None => ()
  }
}

///|
/// Cut a single server off from every other.
pub fn demo_isolate(id : String) -> Unit {
  match demo_ctx.cluster {
    Some(c) => c.isolate(id)
    None => ()
  }
}

///|
/// Heal all partitions: one shared network again.
pub fn demo_heal() -> Unit {
  match demo_ctx.cluster {
    Some(c) => c.heal()
    None => ()
  }
}

///|
/// Crash a server: it stops ticking and all traffic to or from it is dropped.
/// Its durable state is retained for a later restart.
pub fn demo_crash(id : String) -> Unit {
  match demo_ctx.cluster {
    Some(c) => c.crash(id)
    None => ()
  }
}

///|
/// Bring a crashed server back, as if it reloaded from stable storage.
pub fn demo_restart(id : String) -> Unit {
  match demo_ctx.cluster {
    Some(c) => c.restart(id)
    None => ()
  }
}

///|
/// Set the per-message drop probability, in parts per thousand.
pub fn demo_set_drop(permil : Int) -> Unit {
  match demo_ctx.cluster {
    Some(c) => c.set_drop(permil)
    None => ()
  }
}

///|
/// Set the maximum extra delivery delay in ticks (any value above zero also
/// reorders traffic).
pub fn demo_set_delay(ticks : Int) -> Unit {
  match demo_ctx.cluster {
    Some(c) => c.set_delay(ticks)
    None => ()
  }
}

///|
/// Ask the current leader to hand leadership to `id`. Returns whether a transfer
/// started.
pub fn demo_transfer(id : String) -> Bool {
  match demo_ctx.cluster {
    Some(c) => c.transfer_leadership(id)
    None => false
  }
}

///|
/// A full JSON snapshot of the cluster: network clock, current leader(s), the
/// four Raft safety invariants, every node (role/term/log/timers/partition), and
/// the messages currently crossing the simulated network. The front end polls
/// this each frame to render.
pub fn demo_state() -> String {
  match demo_ctx.cluster {
    None => "{\"ready\":false}"
    Some(c) => build_state_json(c)
  }
}

///|
fn build_state_json(c : Cluster) -> String {
  let b = StringBuilder::new()
  b.write_string("{\"ready\":true,\"tick\":")
  b.write_string(c.now.to_string())
  b.write_string(",\"n\":")
  b.write_string(c.ids.length().to_string())
  b.write_string(",\"dropPermil\":")
  b.write_string(c.drop_permil.to_string())
  b.write_string(",\"maxDelay\":")
  b.write_string(c.max_delay.to_string())
  b.write_string(",\"leaders\":[")
  let ls = c.leaders()
  for i in 0..<ls.length() {
    if i > 0 {
      b.write_string(",")
    }
    b.write_string(quote(ls[i]))
  }
  b.write_string("],\"leader\":")
  match c.leader() {
    Some(id) => b.write_string(quote(id))
    None => b.write_string("null")
  }
  b.write_string(",\"invariants\":{\"electionSafety\":")
  b.write_string(bool_json(c.one_leader_per_term()))
  b.write_string(",\"logMatching\":")
  b.write_string(bool_json(c.logs_consistent()))
  b.write_string(",\"stateMachineSafety\":")
  b.write_string(bool_json(c.committed_agrees()))
  b.write_string(",\"commandsAgree\":")
  b.write_string(bool_json(c.same_committed_commands()))
  b.write_string(",\"allHold\":")
  b.write_string(bool_json(c.invariants_hold()))
  b.write_string("},\"nodes\":[")
  for i in 0..<c.ids.length() {
    if i > 0 {
      b.write_string(",")
    }
    let id = c.ids[i]
    let rn = c.node(id)
    let st = rn.status()
    let core = rn.node()
    b.write_string("{\"id\":")
    b.write_string(quote(id))
    b.write_string(",\"role\":")
    b.write_string(quote(role_json(st.role)))
    b.write_string(",\"term\":")
    b.write_string(st.term.to_string())
    b.write_string(",\"commit\":")
    b.write_string(st.commit.to_string())
    b.write_string(",\"lastIndex\":")
    b.write_string(st.last_index.to_string())
    b.write_string(",\"applied\":")
    b.write_string(st.applied.to_string())
    b.write_string(",\"down\":")
    b.write_string(bool_json(c.is_down(id)))
    b.write_string(",\"group\":")
    let g = match c.part.get(id) {
      Some(x) => x
      None => 0
    }
    b.write_string(g.to_string())
    b.write_string(",\"leaderId\":")
    match rn.leader() {
      Some(l) => b.write_string(quote(l))
      None => b.write_string("null")
    }
    b.write_string(",\"votedFor\":")
    match core.voted_for {
      Some(v) => b.write_string(quote(v))
      None => b.write_string("null")
    }
    b.write_string(",\"electionElapsed\":")
    b.write_string(rn.election_elapsed.to_string())
    b.write_string(",\"randTimeout\":")
    b.write_string(rn.randomized_election_timeout.to_string())
    b.write_string(",\"electionTimeout\":")
    b.write_string(rn.election_timeout.to_string())
    b.write_string(",\"heartbeatElapsed\":")
    b.write_string(rn.heartbeat_elapsed.to_string())
    b.write_string(",\"heartbeatTimeout\":")
    b.write_string(rn.heartbeat_timeout.to_string())
    b.write_string(",\"log\":[")
    let log = core.log
    for j in 0..<log.length() {
      if j > 0 {
        b.write_string(",")
      }
      let e = log[j]
      b.write_string("{\"i\":")
      b.write_string(e.index.to_string())
      b.write_string(",\"t\":")
      b.write_string(e.term.to_string())
      b.write_string(",\"c\":")
      b.write_string(bool_json(e.is_conf_change()))
      b.write_string("}")
    }
    b.write_string("]}")
  }
  b.write_string("],\"messages\":[")
  for i in 0..<c.inflight.length() {
    if i > 0 {
      b.write_string(",")
    }
    let f = c.inflight[i]
    let m = f.msg
    b.write_string("{\"from\":")
    b.write_string(quote(m.from))
    b.write_string(",\"to\":")
    b.write_string(quote(m.to))
    b.write_string(",\"kind\":")
    b.write_string(quote(m.kind()))
    b.write_string(",\"term\":")
    b.write_string(m.term().to_string())
    b.write_string(",\"resp\":")
    b.write_string(bool_json(m.is_response()))
    b.write_string(",\"deliverAt\":")
    b.write_string(f.deliver_at.to_string())
    b.write_string("}")
  }
  b.write_string("]}")
  b.to_string()
}

///|
fn role_json(r : Role) -> String {
  match r {
    Follower => "Follower"
    Candidate => "Candidate"
    Leader => "Leader"
  }
}

///|
fn bool_json(v : Bool) -> String {
  if v {
    "true"
  } else {
    "false"
  }
}

///|
fn quote(s : String) -> String {
  let b = StringBuilder::new()
  b.write_char('"')
  for c in s {
    if c == '"' {
      b.write_string("\\\"")
    } else if c == '\\' {
      b.write_string("\\\\")
    } else if c.to_int() < 0x20 {
      b.write_char(' ')
    } else {
      b.write_char(c)
    }
  }
  b.write_char('"')
  b.to_string()
}

///|
/// Split a comma-separated id list into its parts, dropping empties.
fn split_commas(s : String) -> Array[String] {
  let out : Array[String] = []
  let mut cur = ""
  for c in s {
    if c == ',' {
      if cur.length() > 0 {
        out.push(cur)
      }
      cur = ""
    } else {
      cur = cur + c.to_string()
    }
  }
  if cur.length() > 0 {
    out.push(cur)
  }
  out
}

///|
/// Encode a command string as wire bytes (UTF-16 little-endian, two bytes per
/// code unit) so distinct proposals replicate as distinct committed entries.
fn cmd_bytes(s : String) -> Bytes {
  let arr : Array[Byte] = []
  for c in s {
    let code = c.to_int()
    arr.push((code & 0xff).to_byte())
    arr.push(((code >> 8) & 0xff).to_byte())
  }
  Bytes::from_array(arr)
}
