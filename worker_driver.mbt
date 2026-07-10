///|
/// Per-node driver for the Web Worker demo. Each browser Worker owns one wasm
/// instance holding a single server's `RawNode`; the main thread is only a
/// message router. To keep the FFI trivial we never pass a string *into* wasm:
/// a message crosses the boundary as a flat array of small integers, pushed one
/// at a time (`wnode_recv_push`) and decoded back into a `Message`. Outbound
/// messages leave as a JSON array of those same int arrays, so the router can
/// read `[kind, from, to, term, ...]` to route and visualise, then replay the
/// whole array verbatim into the destination worker. Node ids are "S1".."Sn";
/// on the wire a node is just the integer after the 'S'.

///|
priv struct WCtx {
  mut raw : RawNode?
  out : Array[Array[Int]]
  inbuf : Array[Int]
}

///|
let wctx : WCtx = { raw: None, out: [], inbuf: [] }

///|
/// Create this worker's node: server `self_idx` (0-based) of `n`, with the other
/// n-1 as peers. `seed` should differ per node so randomized election timeouts
/// spread out instead of colliding into split votes.
pub fn wnode_new(n : Int, self_idx : Int, seed : Int) -> Unit {
  let count = if n < 1 { 1 } else if n > 9 { 9 } else { n }
  let si = if self_idx < 0 {
    0
  } else if self_idx >= count {
    count - 1
  } else {
    self_idx
  }
  let self_id = "S" + (si + 1).to_string()
  let peers : Array[String] = []
  for k in 0..<count {
    if k != si {
      peers.push("S" + (k + 1).to_string())
    }
  }
  wctx.raw = Some(RaftNode::new(self_id, peers, seed=seed.to_uint64()).raw())
  wctx.out.clear()
  wctx.inbuf.clear()
}

///|
/// Drive the Ready/Advance loop to quiescence, appending every outbound message
/// to the pending queue. Called after any input so a single `wnode_drain`
/// returns all traffic the step produced.
fn pump() -> Unit {
  match wctx.raw {
    Some(raw) =>
      for m in raw.stabilize() {
        wctx.out.push(enc_msg(m))
      }
    None => ()
  }
}

///|
/// Advance this node's logical clock by one tick (heartbeats on a leader, the
/// election countdown on a follower).
pub fn wnode_tick() -> Unit {
  match wctx.raw {
    Some(raw) => raw.tick()
    None => ()
  }
  pump()
}

///|
/// Force this node to stand for election now (a manual kick; nodes also campaign
/// on their own when the election timer fires during `wnode_tick`).
pub fn wnode_campaign() -> Unit {
  match wctx.raw {
    Some(raw) => raw.campaign()
    None => ()
  }
  pump()
}

///|
/// Propose command `cmd` (a small integer) on this node. It only takes effect
/// on a leader; elsewhere the core drops it.
pub fn wnode_propose(cmd : Int) -> Unit {
  match wctx.raw {
    Some(raw) => raw.propose(int_bytes(cmd))
    None => ()
  }
  pump()
}

///|
/// Begin decoding an inbound message: clear the int buffer.
pub fn wnode_recv_reset() -> Unit {
  wctx.inbuf.clear()
}

///|
/// Push one integer of the inbound message's flat encoding.
pub fn wnode_recv_push(v : Int) -> Unit {
  wctx.inbuf.push(v)
}

///|
/// Decode the buffered integers into a `Message` and step the core with it.
pub fn wnode_recv_apply() -> Unit {
  match (wctx.raw, dec_msg(wctx.inbuf)) {
    (Some(raw), Some(m)) => raw.step(m)
    _ => ()
  }
  wctx.inbuf.clear()
  pump()
}

///|
/// Take all pending outbound messages as a JSON array of int arrays and clear
/// the queue. Called once per animation frame by the router.
pub fn wnode_drain() -> String {
  let b = StringBuilder::new()
  b.write_char('[')
  for i in 0..<wctx.out.length() {
    if i > 0 {
      b.write_char(',')
    }
    let arr = wctx.out[i]
    b.write_char('[')
    for j in 0..<arr.length() {
      if j > 0 {
        b.write_char(',')
      }
      b.write_string(arr[j].to_string())
    }
    b.write_char(']')
  }
  b.write_char(']')
  wctx.out.clear()
  b.to_string()
}

///|
/// This node's observable state as compact JSON: role, term, believed leader,
/// commit/last/applied indices, votedFor, election timers and its log terms.
/// Kept small and polled at a throttled rate to bound wasm string allocation.
pub fn wnode_state() -> String {
  match wctx.raw {
    None => "{\"ready\":false}"
    Some(raw) => {
      let node = raw.node()
      let core = node.node()
      let st = raw.status()
      let b = StringBuilder::new()
      b.write_string("{\"ready\":true,\"id\":")
      b.write_string(id_num(st.id).to_string())
      b.write_string(",\"role\":\"")
      b.write_string(wrole(st.role))
      b.write_string("\",\"term\":")
      b.write_string(u2i(st.term).to_string())
      b.write_string(",\"leader\":")
      match st.leader {
        Some(l) => b.write_string(id_num(l).to_string())
        None => b.write_string("-1")
      }
      b.write_string(",\"commit\":")
      b.write_string(u2i(st.commit).to_string())
      b.write_string(",\"lastIndex\":")
      b.write_string(u2i(st.last_index).to_string())
      b.write_string(",\"applied\":")
      b.write_string(u2i(st.applied).to_string())
      b.write_string(",\"votedFor\":")
      match core.voted_for {
        Some(v) => b.write_string(id_num(v).to_string())
        None => b.write_string("-1")
      }
      b.write_string(",\"electionElapsed\":")
      b.write_string(node.election_elapsed.to_string())
      b.write_string(",\"randTimeout\":")
      b.write_string(node.randomized_election_timeout.to_string())
      b.write_string(",\"log\":[")
      let log = core.log
      for j in 0..<log.length() {
        if j > 0 {
          b.write_char(',')
        }
        let e = log[j]
        b.write_string("{\"i\":")
        b.write_string(u2i(e.index).to_string())
        b.write_string(",\"t\":")
        b.write_string(u2i(e.term).to_string())
        b.write_string(",\"c\":")
        b.write_string(if e.is_conf_change() { "true" } else { "false" })
        b.write_string("}")
      }
      b.write_string("]}")
      b.to_string()
    }
  }
}

// ---- flat-int message codec -------------------------------------------------

///|
/// A UInt64 protocol value narrowed to Int for the wire. Demo terms and indices
/// stay well inside the positive i32 range.
fn u2i(v : UInt64) -> Int {
  v.to_int()
}

///|
/// The integer after the 'S' in a node id ("S3" -> 3), 0 on a malformed id.
fn id_num(s : String) -> Int {
  let mut acc = 0
  let mut seen = false
  for c in s {
    let d = c.to_int()
    if d >= 0x30 && d <= 0x39 {
      acc = acc * 10 + (d - 0x30)
      seen = true
    }
  }
  if seen {
    acc
  } else {
    0
  }
}

///|
/// Encode a command int as 4 big-endian bytes, matching `dec_bytes`.
fn int_bytes(v : Int) -> Bytes {
  Bytes::from_array([
    ((v >> 24) & 0xff).to_byte(),
    ((v >> 16) & 0xff).to_byte(),
    ((v >> 8) & 0xff).to_byte(),
    (v & 0xff).to_byte(),
  ])
}

///|
fn wrole(r : Role) -> String {
  match r {
    Follower => "Follower"
    PreCandidate => "PreCandidate"
    Candidate => "Candidate"
    Leader => "Leader"
  }
}

///|
/// Append a command's bytes to the buffer, length-prefixed, so any command
/// (including a leader's empty no-op entry) round-trips.
fn enc_bytes(out : Array[Int], data : Bytes) -> Unit {
  out.push(data.length())
  for i in 0..<data.length() {
    out.push(data[i].to_int())
  }
}

///|
/// Read a length-prefixed byte run starting at `pos`; return the bytes and the
/// index just past them.
fn dec_bytes(buf : Array[Int], pos : Int) -> (Bytes, Int) {
  let len = buf[pos]
  let arr : Array[Byte] = []
  for i in 0..<len {
    arr.push((buf[pos + 1 + i] & 0xff).to_byte())
  }
  (Bytes::from_array(arr), pos + 1 + len)
}

///|
/// Flatten one message to `[kind, from, to, term, ...payload]`. Snapshot traffic
/// (kinds that would need a full state image on the wire) never arises in this
/// demo — it has no log compaction — so it is dropped rather than encoded.
fn enc_msg(m : Message) -> Array[Int] {
  let from = id_num(m.from)
  let to = id_num(m.to)
  let out : Array[Int] = []
  match m.payload {
    PreVote(a) => {
      out.push(1)
      out.push(from)
      out.push(to)
      out.push(u2i(a.term))
      out.push(u2i(a.last_log_index))
      out.push(u2i(a.last_log_term))
    }
    PreVoteResp(r) => {
      out.push(2)
      out.push(from)
      out.push(to)
      out.push(u2i(r.term))
      out.push(if r.vote_granted { 1 } else { 0 })
    }
    Vote(a) => {
      out.push(3)
      out.push(from)
      out.push(to)
      out.push(u2i(a.term))
      out.push(u2i(a.last_log_index))
      out.push(u2i(a.last_log_term))
    }
    VoteResp(r) => {
      out.push(4)
      out.push(from)
      out.push(to)
      out.push(u2i(r.term))
      out.push(if r.vote_granted { 1 } else { 0 })
    }
    Append(a) => {
      out.push(5)
      out.push(from)
      out.push(to)
      out.push(u2i(a.term))
      out.push(u2i(a.prev_log_index))
      out.push(u2i(a.prev_log_term))
      out.push(u2i(a.leader_commit))
      out.push(a.entries.length())
      for e in a.entries {
        out.push(u2i(e.term))
        out.push(u2i(e.index))
        out.push(if e.is_conf_change() { 1 } else { 0 })
        enc_bytes(out, e.command)
      }
    }
    AppendResp(r) => {
      out.push(6)
      out.push(from)
      out.push(to)
      out.push(u2i(r.term))
      out.push(if r.success { 1 } else { 0 })
      out.push(u2i(r.match_index))
      out.push(u2i(r.conflict_index))
      out.push(u2i(r.conflict_term))
    }
    Heartbeat(a) => {
      out.push(7)
      out.push(from)
      out.push(to)
      out.push(u2i(a.term))
      out.push(u2i(a.commit))
    }
    HeartbeatResp(r) => {
      out.push(8)
      out.push(from)
      out.push(to)
      out.push(u2i(r.term))
    }
    TimeoutNow(t) => {
      out.push(9)
      out.push(from)
      out.push(to)
      out.push(u2i(t))
    }
    Snapshot(_) | SnapshotResp(_) => ()
    // This demo drives a fixed leader and never forwards proposals, requests a
    // read index, transfers leadership, or forgets a leader, so these routable
    // payloads never reach the wire here.
    Propose(_)
    | ReadIndex(_)
    | ReadIndexResp(_)
    | TransferLeader(_)
    | ForgetLeader => ()
  }
  out
}

///|
/// Rebuild a `Message` from its flat encoding, or `None` for a kind this demo
/// does not carry.
fn dec_msg(buf : Array[Int]) -> Message? {
  if buf.length() < 4 {
    return None
  }
  let kind = buf[0]
  let from = "S" + buf[1].to_string()
  let to = "S" + buf[2].to_string()
  let term = buf[3].to_uint64()
  let payload : Payload? = match kind {
    1 =>
      Some(
        PreVote({
          term,
          candidate_id: from,
          last_log_index: buf[4].to_uint64(),
          last_log_term: buf[5].to_uint64(),
        }),
      )
    2 => Some(PreVoteResp({ term, vote_granted: buf[4] != 0 }))
    3 =>
      Some(
        Vote({
          term,
          candidate_id: from,
          last_log_index: buf[4].to_uint64(),
          last_log_term: buf[5].to_uint64(),
        }),
      )
    4 => Some(VoteResp({ term, vote_granted: buf[4] != 0 }))
    5 => {
      let entries : Array[Entry] = []
      let count = buf[7]
      let mut p = 8
      for _ in 0..<count {
        let et = buf[p].to_uint64()
        let ei = buf[p + 1].to_uint64()
        let is_conf = buf[p + 2] != 0
        let (cmd, np) = dec_bytes(buf, p + 3)
        entries.push(
          if is_conf {
            Entry::conf(et, ei, cmd)
          } else {
            Entry::normal(et, ei, cmd)
          },
        )
        p = np
      }
      Some(
        Append({
          term,
          leader_id: from,
          prev_log_index: buf[4].to_uint64(),
          prev_log_term: buf[5].to_uint64(),
          entries,
          leader_commit: buf[6].to_uint64(),
        }),
      )
    }
    6 =>
      Some(
        AppendResp({
          term,
          success: buf[4] != 0,
          match_index: buf[5].to_uint64(),
          conflict_index: buf[6].to_uint64(),
          conflict_term: buf[7].to_uint64(),
        }),
      )
    // The codec carries only scalars, and the demo never issues a linearizable
    // read, so the ReadIndex context a heartbeat would echo is always empty.
    7 =>
      Some(
        Heartbeat({
          term,
          leader_id: from,
          commit: buf[4].to_uint64(),
          context: b"",
        }),
      )
    8 => Some(HeartbeatResp({ term, context: b"" }))
    9 => Some(TimeoutNow(term))
    _ => None
  }
  match payload {
    Some(p) => Some(Message::new(from, to, p))
    None => None
  }
}
