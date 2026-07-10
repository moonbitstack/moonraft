// Differential-testing harness (MoonBit side).
//
// Mirrors go-harness/main.go: same DSL, same normalized JSON frame schema, so
// the two stdout streams diff line by line. The script text is injected as the
// generated `script` constant (see script_gen.mbt), which the diff driver
// rewrites per scenario -- this avoids any cross-target file/stdin dependency.

///|
struct Node {
  id : String
  raw : @raft.RawNode
  mut alive : Bool
}

///|
struct BM {
  from : String
  to : String
  msg : @raft.Message
}

///|
struct Cfg {
  mut election_tick : Int
  mut heartbeat_tick : Int
  mut pre_vote : Bool
  mut check_quorum : Bool
  mut lease_based : Bool
  mut max_msg_bytes : UInt64
  mut max_inflight : Int
  mut max_inflight_bytes : UInt64
  mut max_uncommitted : UInt64
}

///|
struct Env {
  mut step : Int
  nodes : Map[String, Node]
  order : Array[String]
  mut bus : Array[BM]
  blocked : Map[String, Bool]
  cfg : Cfg
}

///|
fn Env::new() -> Env {
  {
    step: 0,
    nodes: {},
    order: [],
    bus: [],
    blocked: {},
    cfg: {
      election_tick: 10,
      heartbeat_tick: 1,
      pre_vote: false,
      check_quorum: false,
      lease_based: false,
      max_msg_bytes: 18446744073709551615UL,
      max_inflight: 256,
      max_inflight_bytes: 0,
      max_uncommitted: 0,
    },
  }
}

///|
fn atoi(s : String) -> Int {
  let mut n = 0
  for c in s.iter() {
    if c >= '0' && c <= '9' {
      n = n * 10 + (c.to_int() - '0'.to_int())
    }
  }
  n
}

///|
fn split_on(s : String, sep : Char) -> Array[String] {
  let out = []
  let cur = StringBuilder::new()
  for c in s.iter() {
    if c == sep {
      out.push(cur.to_string())
      cur.reset()
    } else {
      cur.write_char(c)
    }
  }
  out.push(cur.to_string())
  out
}

///|
fn fields(s : String) -> Array[String] {
  let out = []
  for tok in split_on(s, ' ') {
    if tok.length() > 0 {
      out.push(tok)
    }
  }
  out
}

///|
fn trim(s : String) -> String {
  let cs = []
  for c in s.iter() {
    cs.push(c)
  }
  let mut i = 0
  let mut j = cs.length()
  while i < j && (cs[i] == ' ' || cs[i] == '\t' || cs[i] == '\r') {
    i = i + 1
  }
  while j > i && (cs[j - 1] == ' ' || cs[j - 1] == '\t' || cs[j - 1] == '\r') {
    j = j - 1
  }
  let b = StringBuilder::new()
  let mut k = i
  while k < j {
    b.write_char(cs[k])
    k = k + 1
  }
  b.to_string()
}

///|
fn sort_ids(ids : Array[String]) -> Array[String] {
  let a = ids.copy()
  a.sort_by(fn(x, y) { atoi(x) - atoi(y) })
  a
}

///|
fn jstr(s : String) -> String {
  let b = StringBuilder::new()
  b.write_char('"')
  for c in s.iter() {
    if c == '"' {
      b.write_string("\\\"")
    } else if c == '\\' {
      b.write_string("\\\\")
    } else {
      b.write_char(c)
    }
  }
  b.write_char('"')
  b.to_string()
}

///|
fn ids_json(ids : Array[String]) -> String {
  let b = StringBuilder::new()
  b.write_char('[')
  let sorted = sort_ids(ids)
  for i, id in sorted {
    if i > 0 {
      b.write_char(',')
    }
    b.write_string(jstr(id))
  }
  b.write_char(']')
  b.to_string()
}

///|
fn opt(s : String?) -> String {
  match s {
    Some(v) => v
    None => ""
  }
}

///|
fn role_name(r : @raft.Role) -> String {
  match r {
    Follower => "Follower"
    PreCandidate => "PreCandidate"
    Candidate => "Candidate"
    Leader => "Leader"
  }
}

///|
struct MsgRow {
  mtype : String
  from : String
  to : String
  term : UInt64
  index : UInt64
  logterm : UInt64
  entries : Int
  commit : UInt64
  reject : Bool
  rejecthint : UInt64
}

///|
fn msg_row(m : @raft.Message) -> MsgRow {
  let mut mtype = "?"
  let mut term = 0UL
  let mut index = 0UL
  let mut logterm = 0UL
  let mut entries = 0
  let mut commit = 0UL
  let mut reject = false
  let mut rejecthint = 0UL
  match m.payload {
    Vote(a) => {
      mtype = "Vote"
      term = a.term
      index = a.last_log_index
      logterm = a.last_log_term
    }
    VoteResp(a) => {
      mtype = "VoteResp"
      term = a.term
      reject = !(a.vote_granted)
    }
    PreVote(a) => {
      mtype = "PreVote"
      term = a.term
      index = a.last_log_index
      logterm = a.last_log_term
    }
    PreVoteResp(a) => {
      mtype = "PreVoteResp"
      term = a.term
      reject = !(a.vote_granted)
    }
    Append(a) => {
      mtype = "Append"
      term = a.term
      index = a.prev_log_index
      logterm = a.prev_log_term
      entries = a.entries.length()
      commit = a.leader_commit
    }
    AppendResp(a) => {
      mtype = "AppendResp"
      term = a.term
      reject = !(a.success)
      index = a.match_index
      logterm = a.conflict_term
      rejecthint = a.conflict_index
    }
    Heartbeat(a) => {
      mtype = "Heartbeat"
      term = a.term
      commit = a.commit
    }
    HeartbeatResp(a) => {
      mtype = "HeartbeatResp"
      term = a.term
    }
    Snapshot(a) => {
      mtype = "Snapshot"
      term = a.term
      index = a.last_index
      logterm = a.last_term
    }
    // 0.4.0: follower answers `Snapshot` with `AppendResp`, no dedicated reply.
    TimeoutNow(t) => {
      mtype = "TimeoutNow"
      term = t
    }
    ReadIndexResp(a) => {
      mtype = "ReadIndexResp"
      term = a.term
      index = a.index
    }
    ReadIndex(_) => mtype = "ReadIndex"
    Propose(_) => mtype = "Propose"
    TransferLeader(_) => mtype = "TransferLeader"
    ForgetLeader => mtype = "ForgetLeader"
  }
  {
    mtype,
    from: m.from,
    to: m.to,
    term,
    index,
    logterm,
    entries,
    commit,
    reject,
    rejecthint,
  }
}

///|
fn msg_json(r : MsgRow) -> String {
  let b = StringBuilder::new()
  b.write_string("{\"type\":")
  b.write_string(jstr(r.mtype))
  b.write_string(",\"from\":")
  b.write_string(jstr(r.from))
  b.write_string(",\"to\":")
  b.write_string(jstr(r.to))
  b.write_string(",\"term\":")
  b.write_string(r.term.to_string())
  b.write_string(",\"index\":")
  b.write_string(r.index.to_string())
  b.write_string(",\"logterm\":")
  b.write_string(r.logterm.to_string())
  b.write_string(",\"entries\":")
  b.write_string(r.entries.to_string())
  b.write_string(",\"commit\":")
  b.write_string(r.commit.to_string())
  b.write_string(",\"reject\":")
  b.write_string(if r.reject { "true" } else { "false" })
  b.write_string(",\"rejecthint\":")
  b.write_string(r.rejecthint.to_string())
  b.write_char('}')
  b.to_string()
}

///|
// Fixed cross-impl type ordering, identical to the Go side's typeRank.
fn type_rank(t : String) -> Int {
  match t {
    "Append" => 0
    "AppendResp" => 1
    "ForgetLeader" => 2
    "Heartbeat" => 3
    "HeartbeatResp" => 4
    "PreVote" => 5
    "PreVoteResp" => 6
    "Propose" => 7
    "ReadIndex" => 8
    "ReadIndexResp" => 9
    "Snapshot" => 10
    "SnapshotResp" => 11
    "TimeoutNow" => 12
    "TransferLeader" => 13
    "Vote" => 14
    "VoteResp" => 15
    _ => 99
  }
}

///|
fn ucmp(a : UInt64, b : UInt64) -> Int {
  if a < b {
    -1
  } else if a > b {
    1
  } else {
    0
  }
}

///|
fn cmp_row(x : MsgRow, y : MsgRow) -> Int {
  let r = type_rank(x.mtype) - type_rank(y.mtype)
  if r != 0 {
    return r
  }
  let r2 = atoi(x.to) - atoi(y.to)
  if r2 != 0 {
    return r2
  }
  let r3 = atoi(x.from) - atoi(y.from)
  if r3 != 0 {
    return r3
  }
  let r4 = ucmp(x.term, y.term)
  if r4 != 0 {
    return r4
  }
  let r5 = ucmp(x.index, y.index)
  if r5 != 0 {
    return r5
  }
  let r6 = ucmp(x.logterm, y.logterm)
  if r6 != 0 {
    return r6
  }
  ucmp(x.rejecthint, y.rejecthint)
}

///|
fn node_json(n : Node) -> String {
  let st = n.raw.status()
  let rf = n.raw.node()
  let core = rf.node()
  let vote = opt(core.hard_state().vote)
  let lead = opt(st.leader)
  let transferee = opt(rf.lead_transferee)
  let conf = n.raw.conf_state()
  let b = StringBuilder::new()
  b.write_string("{\"id\":")
  b.write_string(jstr(n.id))
  b.write_string(",\"role\":")
  b.write_string(jstr(role_name(st.role)))
  b.write_string(",\"term\":")
  b.write_string(st.term.to_string())
  b.write_string(",\"vote\":")
  b.write_string(jstr(vote))
  b.write_string(",\"lead\":")
  b.write_string(jstr(lead))
  b.write_string(",\"commit\":")
  b.write_string(st.commit.to_string())
  b.write_string(",\"applied\":")
  b.write_string(st.applied.to_string())
  b.write_string(",\"last_index\":")
  b.write_string(st.last_index.to_string())
  b.write_string(",\"last_term\":")
  b.write_string(core.last_log_term().to_string())
  b.write_string(",\"lead_transferee\":")
  b.write_string(jstr(transferee))
  b.write_string(",\"pending_conf_index\":")
  b.write_string(rf.pending_conf_index.to_string())
  b.write_string(",\"uncommitted\":")
  b.write_string(rf.uncommitted_size().to_string())
  b.write_string(",\"voters\":")
  b.write_string(ids_json(conf.voters))
  b.write_string(",\"voters_outgoing\":")
  b.write_string(ids_json(conf.voters_outgoing))
  b.write_string(",\"learners\":")
  b.write_string(ids_json(conf.learners))
  b.write_string(",\"learners_next\":")
  b.write_string(ids_json(conf.learners_next))
  b.write_char('}')
  b.to_string()
}

///|
fn Env::build_config(self : Env, id : String, peers : Array[String]) -> @raft.RaftNode {
  // RaftNode::new defaults read_only_option to LeaseBased and the enum is a
  // read-only type we cannot construct from here, so we express the choice via
  // the public setters: enable_read_only_safe() for Safe, leave default for
  // lease-based.
  let rf = @raft.RaftNode::new(
    id,
    peers,
    election_timeout=self.cfg.election_tick,
    heartbeat_timeout=self.cfg.heartbeat_tick,
    pre_vote=self.cfg.pre_vote,
    max_msg_bytes=self.cfg.max_msg_bytes,
    max_inflight=self.cfg.max_inflight,
    max_inflight_bytes=self.cfg.max_inflight_bytes,
    max_uncommitted_size=self.cfg.max_uncommitted,
  )
  if self.cfg.lease_based {

  } else {
    rf.enable_read_only_safe()
  }
  if self.cfg.check_quorum {
    rf.enable_check_quorum()
  }
  rf
}

///|
fn Env::add_cluster(self : Env, ids : Array[String]) -> Unit {
  for id in ids {
    let peers = []
    for o in ids {
      if o != id {
        peers.push(o)
      }
    }
    let rf = self.build_config(id, peers)
    let raw = rf.raw()
    self.nodes.set(id, { id, raw, alive: true })
    self.order.push(id)
  }
  self.order.sort_by(fn(x, y) { atoi(x) - atoi(y) })
}

///|
fn Env::route(self : Env, from : String, m : @raft.Message) -> Unit {
  let to = m.to
  if self.blocked.get(from + "|" + to) is Some(_) ||
    self.blocked.get(to + "|" + from) is Some(_) {
    return
  }
  self.bus.push({ from, to, msg: m })
}

///|
fn Env::pump(self : Env) -> Array[@raft.Message] {
  let produced = []
  let mut progress = true
  while progress {
    progress = false
    for id in self.order {
      let n = self.nodes.get(id).unwrap()
      if !(n.alive) {
        continue
      }
      if !(n.raw.has_ready()) {
        continue
      }
      progress = true
      let rd = n.raw.ready()
      for e in rd.committed_entries {        if e.is_conf_change() {
          match @raft.ConfChange::decode(e.command) {
            Some(cc) => n.raw.apply_conf_change(cc) |> ignore
            None => ()
          }
        }
      }
      for m in rd.messages {
        produced.push(m)
        self.route(id, m)
      }
      n.raw.store(rd)
      n.raw.advance(rd)
    }
  }
  produced
}

///|
fn Env::deliver(self : Env, from : String, to : String) -> Unit {
  let rest = []
  for bm in self.bus {
    if bm.from == from && bm.to == to {
      match self.nodes.get(bm.to) {
        Some(n) => if n.alive { n.raw.step(bm.msg) }
        None => ()
      }
    } else {
      rest.push(bm)
    }
  }
  self.bus = rest
}

///|
fn Env::deliver_all(self : Env) -> Unit {
  let pending = self.bus
  self.bus = []
  for bm in pending {
    match self.nodes.get(bm.to) {
      Some(n) => if n.alive { n.raw.step(bm.msg) }
      None => ()
    }
  }
}

///|
fn Env::drop_msgs(self : Env, from : String, to : String) -> Unit {
  let rest = []
  for bm in self.bus {
    if !(bm.from == from && bm.to == to) {
      rest.push(bm)
    }
  }
  self.bus = rest
}

///|
fn Env::partition(self : Env, g1 : Array[String], g2 : Array[String]) -> Unit {
  for a in g1 {
    for b in g2 {
      self.blocked.set(a + "|" + b, true)
      self.blocked.set(b + "|" + a, true)
    }
  }
  let rest = []
  for bm in self.bus {
    if self.blocked.get(bm.from + "|" + bm.to) is Some(_) {
      continue
    }
    rest.push(bm)
  }
  self.bus = rest
}

///|
fn Env::stabilize(self : Env) -> Array[@raft.Message] {
  let all = []
  let mut i = 0
  while i < 2000 {
    for m in self.pump() {
      all.push(m)
    }
    if self.bus.length() == 0 {
      break
    }
    self.deliver_all()
    i = i + 1
  }
  for m in self.pump() {
    all.push(m)
  }
  all
}

///|
fn Env::emit(self : Env, cmd : String, msgs : Array[@raft.Message]) -> Unit {
  self.step = self.step + 1
  let rows = []
  for m in msgs {
    rows.push(msg_row(m))
  }
  rows.sort_by(cmp_row)
  let b = StringBuilder::new()
  b.write_string("{\"step\":")
  b.write_string(self.step.to_string())
  b.write_string(",\"cmd\":")
  b.write_string(jstr(cmd))
  b.write_string(",\"nodes\":[")
  for i, id in self.order {
    if i > 0 {
      b.write_char(',')
    }
    b.write_string(node_json(self.nodes.get(id).unwrap()))
  }
  b.write_string("],\"msgs\":[")
  for i, r in rows {
    if i > 0 {
      b.write_char(',')
    }
    b.write_string(msg_json(r))
  }
  b.write_string("]}")
  println(b.to_string())
}

///|
fn Env::conf_change(self : Env, id : String, op : String, target : String) -> Unit {
  let cc = match op {
    "addnode" => @raft.ConfChange::add(target)
    "removenode" => @raft.ConfChange::remove(target)
    "addlearner" => @raft.ConfChange::add_learner(target)
    _ => @raft.ConfChange::add(target)
  }
  self.nodes.get(id).unwrap().raw.propose_conf(cc)
}

///|
fn Env::apply_config(self : Env, args : Array[String]) -> Unit {
  for a in args {
    let kv = split_on(a, '=')
    let k = kv[0]
    let v = if kv.length() > 1 { kv[1] } else { "" }
    match k {
      "prevote" => self.cfg.pre_vote = v == "true"
      "checkquorum" => self.cfg.check_quorum = v == "true"
      "leasebased" => self.cfg.lease_based = v == "true"
      "electiontick" => self.cfg.election_tick = atoi(v)
      "heartbeattick" => self.cfg.heartbeat_tick = atoi(v)
      "maxsizepermsg" => self.cfg.max_msg_bytes = atoi(v).to_uint64()
      "maxinflight" => self.cfg.max_inflight = atoi(v)
      "maxinflightbytes" => self.cfg.max_inflight_bytes = atoi(v).to_uint64()
      "maxuncommitted" => self.cfg.max_uncommitted = atoi(v).to_uint64()
      _ => ()
    }
  }
}

///|
fn Env::tick(self : Env, who : String, cnt : Int) -> Unit {
  let mut i = 0
  while i < cnt {
    if who == "all" {
      for id in self.order {
        let n = self.nodes.get(id).unwrap()
        if n.alive {
          n.raw.tick()
        }
      }
    } else {
      self.nodes.get(who).unwrap().raw.tick()
    }
    i = i + 1
  }
}

///|
fn Env::run(self : Env, text : String) -> Unit {
  for rawline in split_on(text, '\n') {
    let line = trim(rawline)
    if line.length() == 0 || is_comment(line) {
      continue
    }
    let f = fields(line)
    let cmd = f[0]
    let args = f[1:].to_owned()
    match cmd {
      "config" => self.apply_config(args)
      "cluster" => {
        self.add_cluster(args)
        self.emit(line, self.pump())
      }
      "campaign" => {
        self.nodes.get(args[0]).unwrap().raw.campaign()
        self.emit(line, self.pump())
      }
      "propose" => {
        let data = ascii_bytes(join_from(args, 1))
        self.nodes.get(args[0]).unwrap().raw.propose(data)
        self.emit(line, self.pump())
      }
      "conf_change" => {
        self.conf_change(args[0], args[1], args[2])
        self.emit(line, self.pump())
      }
      "tick" => {
        let cnt = if args.length() > 1 { atoi(args[1]) } else { 1 }
        self.tick(args[0], cnt)
        self.emit(line, self.pump())
      }
      "deliver" => {
        self.deliver(args[0], args[1])
        self.emit(line, self.pump())
      }
      "deliver_all" => {
        self.deliver_all()
        self.emit(line, self.pump())
      }
      "stabilize" => self.emit(line, self.stabilize())
      "drop" => {
        self.drop_msgs(args[0], args[1])
        self.emit(line, [])
      }
      "partition" => {
        self.partition(split_on(args[0], ','), split_on(args[1], ','))
        self.emit(line, [])
      }
      "heal" => {
        self.blocked.clear()
        self.emit(line, self.pump())
      }
      "crash" => {
        self.nodes.get(args[0]).unwrap().alive = false
        self.emit(line, [])
      }
      "transfer" => {
        self.nodes.get(args[0]).unwrap().raw.transfer_leader(args[1])
        self.emit(line, self.pump())
      }
      "read_index" => {
        self.nodes.get(args[0]).unwrap().raw.read_index(ascii_bytes(join_from(args, 1)))
        self.emit(line, self.pump())
      }
      "report_unreachable" => {
        self.nodes.get(args[0]).unwrap().raw.report_unreachable(args[1])
        self.emit(line, self.pump())
      }
      "report_snapshot" => {
        let ok = !(args.length() > 2 && args[2] == "fail")
        self.nodes.get(args[0]).unwrap().raw.report_snapshot(args[1], ok)
        self.emit(line, self.pump())
      }
      _ => println("unknown cmd: " + cmd)
    }
  }
}

///|
fn ascii_bytes(s : String) -> Bytes {
  // Build UTF-8/ASCII bytes (1 per char) so proposal payloads are byte-identical
  // to the Go side; String::to_bytes would emit UTF-16LE and skew payload sizes.
  let arr = []
  for c in s.iter() {
    arr.push((c.to_int() & 0xFF).to_byte())
  }
  Bytes::from_array(arr)
}

///|
fn is_comment(s : String) -> Bool {
  for c in s.iter() {
    return c == '#'
  }
  false
}

///|
fn join_from(args : Array[String], start : Int) -> String {
  let b = StringBuilder::new()
  let mut i = start
  while i < args.length() {
    if i > start {
      b.write_char(' ')
    }
    b.write_string(args[i])
    i = i + 1
  }
  b.to_string()
}

///|
fn main {
  let env = Env::new()
  env.run(script)
}
