// Copyright 2019 The etcd Authors
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
//
// etcd-io/raft's `rafttest.InteractionEnv`: a cluster driven through the
// Ready/Advance contract by the command vocabulary of testdata/*.txt. Every
// command upstream understands is executed here, and the output is rendered in
// upstream's own format (see rafttest_format_wbtest.mbt), so a script's expected
// block can be compared byte for byte.

///|
/// The output buffer the handlers and the raft nodes share, with upstream's log
/// level filter: at level `NONE` nothing is written at all, not even the
/// handlers' own dumps. `stack` is a stack of buffers so a handler can indent a
/// block it produces (`> 1 handling Ready` and its indented body), which is what
/// upstream's `withIndent` does.
pub struct RtOut {
  stack : Array[StringBuilder]
  level : Ref[Int32]
}

///|
/// Upstream's `RedirectLogger.printf`: a level name, the message, and a newline
/// unless the message already ends with one.
impl Logger for RtOut with fn log(self, level : LogLevel, msg : String) {
  let lvl = match level {
    Debug => 0
    Info => 1
    Warning => 2
    Error => 3
    Fatal | Panic => 4
  }
  if lvl < self.level.val || self.level.val >= 5 {
    return
  }
  let name = match lvl {
    0 => "DEBUG"
    1 => "INFO"
    2 => "WARN"
    3 => "ERROR"
    _ => "FATAL"
  }
  self.write("\{name} \{msg}\n")
}

///|
fn RtOut::new() -> RtOut {
  // Upstream's harness logs everything by default; the scripts that want quiet
  // open with `log-level none`.
  { stack: [StringBuilder()], level: Ref(0), }
}

///|
fn RtOut::write(self : RtOut, s : String) -> Unit {
  if self.level.val >= 5 {
    return
  }
  self.stack[self.stack.length() - 1].write_string(s)
}

///|
fn RtOut::quiet(self : RtOut) -> Bool {
  self.level.val >= 5
}

///|
/// Run `f`, capturing what it writes and re-emitting every line with the
/// indent upstream's `withIndent` applies. A line that is empty stays empty: the
/// indent is prefix text, not padding.
fn RtOut::with_indent(self : RtOut, f : () -> Unit) -> Unit {
  self.stack.push(StringBuilder())
  f()
  let inner = self.stack.pop().unwrap()
  let text = inner.to_string()
  let lines = text.split("\n").to_array()
  let out = StringBuilder()
  let n = lines.length()
  let mut i = 0
  while i < n {
    let line = lines[i].to_owned()
    let last = i == n - 1
    if !(last && line == "") {
      out.write_string("  ")
      out.write_string(line)
      out.write_string("\n")
    }
    i = i + 1
  }
  self.write(out.to_string())
}

///|
/// Set the log level by upstream's name. `NONE` silences the handlers too.
fn RtOut::set_level(self : RtOut, name : String) -> Unit {
  let n = name.to_lower()
  self.level.val = match n {
    "debug" => 0
    "info" => 1
    "warn" | "warning" => 2
    "error" => 3
    "fatal" => 4
    _ => 5
  }
}

///|
/// One member of the cluster under test.
pub struct RtNode {
  id : String
  rn : RawNode
  // AsyncStorageWrites: the Ready hands its persistence to local threads, which
  // this harness models as queues of pending directives.
  async_mode : Bool
  appends : Array[StorageAppend]
  applies : Array[StorageApply]
}

///|
/// A cluster driven through the Ready/Advance contract.
pub struct RaftTest {
  nodes : Array[RtNode]
  // In-flight messages, in the order they were produced.
  messages : Array[Message]
  out : RtOut
}

///|
/// A parsed datadriven directive line: the command, positional args, and
/// `key=value` args (parentheses stripped from values like `(1,2,3)`).
priv struct Args {
  cmd : String
  positional : Array[String]
  kv : Map[String, String]
}

///|
/// Split on `sep`, returning an owned `Array[String]`.
fn split_str(s : String, sep : String) -> Array[String] {
  s.split(sep).map(fn(x : StringView) { x.to_owned() }).to_array()
}

///|
/// Positional arg at `i`, or `default` if out of range.
fn pos_or(a : Array[String], i : Int32, default : String) -> String {
  if i >= 0 && i < a.length() {
    a[i]
  } else {
    default
  }
}

///|
/// A decimal count, as a directive's first argument spells it.
fn parse_count(s : String) -> Int32 {
  let mut n = 0
  let mut any = false
  for c in s {
    if c >= '0' && c <= '9' {
      n = n * 10 + (c.to_int() - '0'.to_int())
      any = true
    } else if any {
      break
    }
  }
  n
}

///|
pub fn RaftTest::new() -> RaftTest {
  { nodes: [], messages: [], out: RtOut::new(), }
}

///|
/// Parse a directive line into its command and arguments.
fn parse_args(line : String) -> Args {
  let toks = arg_tokens(line)
  let cmd = if toks.length() > 0 { toks[0] } else { "" }
  let positional : Array[String] = []
  let kv : Map[String, String] = Map([], capacity=8)
  for i = 1; i < toks.length(); i = i + 1 {
    let t = toks[i]
    if t.contains("=") {
      let eq = t.find("=").unwrap()
      let key = t[:eq].to_owned()
      let value = t[eq + 1:].to_owned()
      kv[key] = strip_parens(value)
    } else {
      positional.push(strip_parens(t))
    }
  }
  { cmd, positional, kv, }
}

///|
/// The words of a directive line. A parenthesised list is one word even with
/// spaces inside it, as in datadriven's `cfg=(1, 2)`.
fn arg_tokens(line : String) -> Array[String] {
  let out : Array[String] = []
  let word = StringBuilder()
  let mut depth = 0
  for ch in line {
    match ch {
      '(' => {
        depth += 1
        word.write_char(ch)
      }
      ')' => {
        depth -= 1
        word.write_char(ch)
      }
      ' ' | '\t' if depth == 0 =>
        if !word.is_empty() {
          out.push(word.to_string())
          word.reset()
        }
      ' ' => ()
      _ => word.write_char(ch)
    }
  }
  if !word.is_empty() {
    out.push(word.to_string())
  }
  out
}

///|
fn strip_parens(s : String) -> String {
  s.trim().to_owned().replace(old="(", new="").replace(old=")", new="")
}

///|
fn kv_get(kv : Map[String, String], key : String, default : String) -> String {
  match kv.get(key) {
    Some(v) => v
    None => default
  }
}

///|
fn kv_flag(kv : Map[String, String], key : String) -> Bool {
  kv_get(kv, key, "false") == "true"
}

///|
/// A comma or space separated id list, as `voters=(1,2,3)` or `voters=(1 2 3)`
/// spells it.
fn id_list(s : String) -> Array[String] {
  let out : Array[String] = []
  for part in s.replace(old=" ", new=",").split(",") {
    let p = part.to_owned().trim().to_owned()
    if p != "" {
      out.push(p)
    }
  }
  out
}

///|
/// Encode a string's code units to bytes. The datadriven payloads are ASCII.
fn str_to_bytes(s : String) -> Bytes {
  let arr : Array[Byte] = []
  for c in s.to_array() {
    arr.push(c.to_int().to_byte())
  }
  Bytes::from_array(arr)
}

///|
/// The index of the node with this id, or `-1`.
fn RaftTest::index_of(self : RaftTest, id : String) -> Int32 {
  for i = 0; i < self.nodes.length(); i = i + 1 {
    if self.nodes[i].id == id {
      return i
    }
  }
  -1
}

// ---------------------------------------------------------------------------
// Command dispatch
// ---------------------------------------------------------------------------

///|
pub fn RaftTest::run(
  self : RaftTest,
  cmd_line : String,
  input : String,
) -> String {
  let a = parse_args(cmd_line)
  match a.cmd {
    "log-level" => self.out.set_level(pos_or(a.positional, 0, "info"))
    "add-nodes" => self.add_nodes(a) |> ignore
    "campaign" => {
      let i = self.index_of(pos_or(a.positional, 0, "1"))
      if i >= 0 {
        self.nodes[i].rn.campaign()
      }
    }
    "stabilize" => {
      // `stabilize log-level=none` silences the run and restores the level after
      // (upstream defers the restore inside the handler).
      let level_arg = a.kv.get("log-level")
      if level_arg is Some(level) {
        let saved = self.out.level.val
        self.out.set_level(level)
        self.stabilize(a.positional) |> ignore
        self.out.level.val = saved
      } else {
        self.stabilize(a.positional) |> ignore
      }
    }
    "propose" => {
      let i = self.index_of(pos_or(a.positional, 0, "1"))
      if i >= 0 {
        self.nodes[i].rn.propose(str_to_bytes(pos_or(a.positional, 1, "")))
      }
    }
    "process-ready" => self.process_ready_many(a.positional)
    "deliver-msgs" => self.deliver(a) |> ignore
    "raft-log" => self.raft_log(a.positional) |> ignore
    "raft-state" => self.raft_state() |> ignore
    "status" => self.status(a.positional) |> ignore
    "tick" => {
      let i = self.index_of(pos_or(a.positional, 0, "1"))
      if i >= 0 {
        self.nodes[i].rn.tick()
      }
    }
    "tick-heartbeat" => {
      // One heartbeat interval: HeartbeatTick is 1 in the config stub.
      let i = self.index_of(pos_or(a.positional, 0, "1"))
      if i >= 0 {
        self.nodes[i].rn.tick()
      }
    }
    "tick-election" => {
      // One election timeout worth of ticks, which is what upstream's handler
      // does: the tick counter is what a leader's check-quorum window measures.
      let i = self.index_of(pos_or(a.positional, 0, "1"))
      if i >= 0 {
        for _t = 0; _t < 3; _t = _t + 1 {
          self.nodes[i].rn.tick()
        }
      }
    }
    "transfer-leadership" => {
      let from = kv_get(a.kv, "from", pos_or(a.positional, 0, "1"))
      let to = kv_get(a.kv, "to", pos_or(a.positional, 1, ""))
      let i = self.index_of(from)
      if i >= 0 && to != "" {
        self.nodes[i].rn.transfer_leader(to)
      }
    }
    "read-index" => {
      let i = self.index_of(pos_or(a.positional, 0, "1"))
      if i >= 0 {
        self.nodes[i].rn.read_index(str_to_bytes(pos_or(a.positional, 1, "")))
      }
    }
    "forget-leader" => {
      let i = self.index_of(pos_or(a.positional, 0, "1"))
      if i >= 0 {
        // A lease-based read makes the leader's word authoritative, so a forget
        // is refused outright (etcd's stepFollower returns an error here).
        if self.nodes[i].rn.node().read_only_option() is LeaseBased {
          self.out.write(
            "ERROR ignoring MsgForgetLeader due to ReadOnlyLeaseBased\n",
          )
        } else {
          self.nodes[i].rn.forget_leader()
        }
      }
    }
    "report-unreachable" => {
      let i = self.index_of(pos_or(a.positional, 0, "1"))
      if i >= 0 {
        self.nodes[i].rn.report_unreachable(pos_or(a.positional, 1, ""))
      }
    }
    "set-randomized-election-timeout" => {
      let i = self.index_of(pos_or(a.positional, 0, "1"))
      if i >= 0 {
        let ticks = parse_count(kv_get(a.kv, "timeout", "10"))
        self.nodes[i].rn.node().set_randomized_election_timeout(ticks)
      }
    }
    "compact" => {
      let i = self.index_of(pos_or(a.positional, 0, "1"))
      if i >= 0 {
        let upto = parse_count(pos_or(a.positional, 1, "0")).to_uint64()
        self.nodes[i].rn.node().node().compact(upto, b"") |> ignore
      }
    }
    "snapshot" => self.snapshot(a) |> ignore
    "send-snapshot" => self.send_snapshot(a) |> ignore
    "propose-conf-change" => self.propose_conf_change(a, input) |> ignore
    "remove-nodes" => self.remove_nodes(a) |> ignore
    "add-learner" => self.add_learner(a) |> ignore
    "request-snapshot" => {
      let i = self.index_of(pos_or(a.positional, 0, "1"))
      if i >= 0 {
        if self.nodes[i].rn.next_unstable_snapshot() is Some(s) {
          self.out.write("Snapshot Index:\{s.last_index} Term:\{s.last_term}\n")
        }
      }
    }
    "process-append-thread" =>
      // Each named node's append thread handles its next directive, under the
      // header the stabilize loop also prints.
      for id in a.positional {
        let i = self.index_of(id)
        if i >= 0 {
          self.out.write("> \{id} processing append thread\n")
          self.out.with_indent(fn() { self.process_append_thread(i) |> ignore })
        }
      }
    "process-apply-thread" =>
      for id in a.positional {
        let i = self.index_of(id)
        if i >= 0 {
          self.out.write("> \{id} processing apply thread\n")
          self.out.with_indent(fn() { self.process_apply_thread(i) |> ignore })
        }
      }
    _ => self.out.write("unknown command: \{a.cmd}\n")
  }
  if self.out.quiet() {
    return "ok"
  }
  let text = self.out.stack[0].to_string()
  self.out.stack[0].reset()
  if text == "" {
    "ok"
  } else {
    text.trim_end().to_owned()
  }
}

// ---------------------------------------------------------------------------
// add-nodes
// ---------------------------------------------------------------------------

///|
/// `add-nodes N [voters=(...)] [learners=(...)] [index=k] [content=c]
/// [prevote=true] [checkquorum=true] [read-only=safe|lease-based]
/// [step-down-on-removal=true] [disable-conf-change-validation=true]
/// [async-storage-writes=true] [max-committed-size-per-ready=n] [inflight=n]`
///
/// A non-zero `index` bootstraps every new node from a snapshot at that index,
/// as upstream does: the log starts there, the configuration comes from the
/// snapshot's ConfState, and commit and applied both equal the index.
fn RaftTest::add_nodes(self : RaftTest, a : Args) -> Unit {
  let count = parse_count(pos_or(a.positional, 0, "0"))
  let voters = id_list(kv_get(a.kv, "voters", ""))
  let learners = id_list(kv_get(a.kv, "learners", ""))
  let index = parse_count(kv_get(a.kv, "index", "0")).to_uint64()
  let content = kv_get(a.kv, "content", "")
  let prevote = kv_flag(a.kv, "prevote")
  let checkquorum = kv_flag(a.kv, "checkquorum")
  let step_down = kv_flag(a.kv, "step-down-on-removal")
  let no_validation = kv_flag(a.kv, "disable-conf-change-validation")
  let async_mode = kv_flag(a.kv, "async-storage-writes")
  let max_committed = parse_count(
    kv_get(a.kv, "max-committed-size-per-ready", "0"),
  ).to_uint64()
  let inflight = {
    let n = parse_count(kv_get(a.kv, "inflight", "0"))
    if n > 0 {
      n
    } else {
      2147483647
    }
  }
  let read_only = if kv_get(a.kv, "read-only", "safe") == "lease-based" {
    LeaseBased
  } else {
    Safe
  }
  let base = self.nodes.length()
  for k = 0; k < count; k = k + 1 {
    let id = (base + k + 1).to_string()
    let peers : Array[String] = []
    for v in voters {
      if v != id {
        peers.push(v)
      }
    }
    for l in learners {
      if l != id {
        peers.push(l)
      }
    }
    // Every node is bootstrapped from a snapshot, as upstream does: a directive
    // that names no voters leaves the configuration empty (a node pulled out of
    // thin air for a later conf change), and one with an index starts the log
    // there.
    let snap : Snapshot = {
      last_index: index,
      last_term: if index > 0UL {
        1UL
      } else {
        0UL
      },
      data: str_to_bytes(content),
      conf_state: {
        voters,
        learners,
        voters_outgoing: [],
        learners_next: [],
        auto_leave: false,
      },
    }
    let r = RaftNode::new(
      id,
      peers,
      pre_vote=prevote,
      check_quorum=checkquorum,
      election_timeout=3,
      heartbeat_timeout=1,
      max_inflight=inflight,
      read_only_option=read_only,
      step_down_on_removal=step_down,
      disable_conf_change_validation=no_validation,
      logger=self.out,
      snapshot=Some(snap),
    )
    let rn = if async_mode {
      if max_committed > 0UL {
        r.raw_async_sized(max_committed)
      } else {
        r.raw_async()
      }
    } else if max_committed > 0UL {
      r.raw_with_max_committed_size(max_committed)
    } else {
      r.raw()
    }
    self.nodes.push({ id, rn, async_mode, appends: [], applies: [], })
  }
}

// ---------------------------------------------------------------------------
// Ready handling and stabilization
// ---------------------------------------------------------------------------

///|
/// Take one Ready from node `i`, print it, hand its persistence to storage (or
/// queue it, under async writes), and queue its messages.
fn RaftTest::process_ready(self : RaftTest, i : Int32) -> Unit {
  let n = self.nodes[i]
  let rd = n.rn.ready()
  // Under async writes the Ready also carries the two local directives, which
  // upstream appends to `Messages` after the outbound ones.
  let extra : Array[String] = []
  if n.async_mode {
    if rd.storage_append is Some(sa) {
      extra.push(rt_describe_storage_append(n.id, sa, true))
    }
    if rd.storage_apply is Some(sa) {
      extra.push(rt_describe_storage_apply(n.id, sa, true))
    }
  }
  self.out.write(rt_describe_ready(rd, extra))
  if n.async_mode {
    if rd.storage_append is Some(sa) {
      n.appends.push(sa)
    }
    if rd.storage_apply is Some(sa) {
      n.applies.push(sa)
    }
  } else {
    n.rn.store(rd)
    n.rn.advance(rd)
  }
  for m in rd.messages {
    self.messages.push(m)
  }
}

///|
fn RaftTest::process_ready_many(self : RaftTest, ids : Array[String]) -> Unit {
  let targets : Array[String] = if ids.is_empty() {
    let all : Array[String] = []
    for n in self.nodes {
      all.push(n.id)
    }
    all
  } else {
    ids
  }
  for id in targets {
    let i = self.index_of(id)
    if i >= 0 {
      if targets.length() > 1 {
        self.out.write("> \{id} handling Ready\n")
        self.out.with_indent(fn() { self.process_ready(i) })
      } else {
        self.process_ready(i)
      }
    }
  }
}

///|
/// Deliver every in-flight message addressed to `id`, printing each one and
/// letting the node step it.
fn RaftTest::deliver_to(self : RaftTest, id : String) -> Int32 {
  let keep : Array[Message] = []
  let pass : Array[Message] = []
  for m in self.messages {
    if m.to == id {
      pass.push(m)
    } else {
      keep.push(m)
    }
  }
  self.messages.clear()
  for m in keep {
    self.messages.push(m)
  }
  for m in pass {
    self.out.write(rt_describe_message(m))
    self.out.write("\n")
    let i = self.index_of(id)
    if i >= 0 {
      self.nodes[i].rn.step(m)
    }
  }
  pass.length()
}

///|
/// Run Ready handling and message delivery on the given nodes (all of them when
/// none are named) until nothing is left to do, printing upstream's headers.
fn RaftTest::stabilize(self : RaftTest, ids : Array[String]) -> Unit {
  let targets : Array[String] = if ids.is_empty() {
    let all : Array[String] = []
    for n in self.nodes {
      all.push(n.id)
    }
    all
  } else {
    ids
  }
  let mut done = false
  // A fixed point is reached in a handful of rounds; the bound only stops a
  // harness-level bug (a node that keeps producing work) from hanging a run.
  let mut rounds = 0
  while !done && rounds < 500 {
    rounds = rounds + 1
    done = true
    for id in targets {
      let i = self.index_of(id)
      if i >= 0 && self.nodes[i].rn.has_ready() {
        self.out.write("> \{id} handling Ready\n")
        self.out.with_indent(fn() { self.process_ready(i) })
        done = false
      }
    }
    for id in targets {
      let mut pending = false
      for m in self.messages {
        if m.to == id {
          pending = true
        }
      }
      if pending {
        self.out.write("> \{id} receiving messages\n")
        self.out.with_indent(fn() { self.deliver_to(id) |> ignore })
        done = false
      }
    }
    for id in targets {
      let i = self.index_of(id)
      if i >= 0 && !self.nodes[i].appends.is_empty() {
        self.out.write("> \{id} processing append thread\n")
        while !self.nodes[i].appends.is_empty() {
          self.out.with_indent(fn() { self.process_append_thread(i) |> ignore })
        }
        done = false
      }
    }
    for id in targets {
      let i = self.index_of(id)
      if i >= 0 && !self.nodes[i].applies.is_empty() {
        self.out.write("> \{id} processing apply thread\n")
        while !self.nodes[i].applies.is_empty() {
          self.out.with_indent(fn() { self.process_apply_thread(i) |> ignore })
        }
        done = false
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Async storage threads
// ---------------------------------------------------------------------------

///|
fn RaftTest::process_append_thread(self : RaftTest, i : Int32) -> Unit {
  let n = self.nodes[i]
  if n.appends.is_empty() {
    return
  }
  let sa = n.appends.remove(0)
  self.out.write("Processing:\n")
  self.out.write(rt_describe_storage_append(n.id, sa, false))
  self.out.write("\n")
  // The write is durable: put the waiting responses on the wire, then the
  // acknowledgement itself. All of them are delivered like any other message, so
  // the order is the one upstream's scripts record — a self-addressed vote
  // response is printed and stepped when the recipient is served.
  for r in sa.responses {
    self.messages.push(r)
  }
  if sa.resp.index != 0UL || sa.snapshot is Some(_) {
    self.messages.push(
      Message::new("AppendThread", n.id, StorageAppendResp(sa.resp)),
    )
  }
}

///|
fn RaftTest::process_apply_thread(self : RaftTest, i : Int32) -> Unit {
  let n = self.nodes[i]
  if n.applies.is_empty() {
    return
  }
  let sa = n.applies.remove(0)
  self.out.write("Processing:\n")
  self.out.write(rt_describe_storage_apply(n.id, sa, false))
  self.out.write("\n")
  // The batch is applied: the acknowledgement goes on the wire like any other
  // message, and the node advances its applied cursor when it arrives.
  self.messages.push(
    Message::new("ApplyThread", n.id, StorageApplyResp(sa.resp)),
  )
}

// ---------------------------------------------------------------------------
// Query commands
// ---------------------------------------------------------------------------

///|
/// `deliver-msgs <id>... [type=MsgX] [drop=(id,...)]`
fn RaftTest::deliver(self : RaftTest, a : Args) -> Unit {
  let typ = kv_get(a.kv, "type", "")
  let drop = id_list(kv_get(a.kv, "drop", ""))
  let recipients : Array[(String, Bool)] = []
  for id in a.positional {
    recipients.push((id, false))
  }
  for id in drop {
    recipients.push((id, true))
  }
  let keep : Array[Message] = []
  let mut handled = 0
  for m in self.messages {
    let mut matched = false
    for r in recipients {
      let (id, is_drop) = r
      let type_ok = if typ == "" { true } else { rt_msg_type(m) == typ }
      if m.to == id && type_ok {
        matched = true
        handled = handled + 1
        if is_drop {
          self.out.write("dropped: ")
        }
        self.out.write(rt_describe_message(m))
        self.out.write("\n")
        if !is_drop {
          let i = self.index_of(id)
          if i >= 0 {
            self.nodes[i].rn.step(m)
          }
        }
      }
    }
    if !matched {
      keep.push(m)
    }
  }
  self.messages.clear()
  for m in keep {
    self.messages.push(m)
  }
  if handled == 0 {
    self.out.write("no messages\n")
  }
}

///|
fn RaftTest::raft_log(self : RaftTest, ids : Array[String]) -> Unit {
  for id in ids {
    let i = self.index_of(id)
    if i < 0 {
      continue
    }
    let node = self.nodes[i].rn.node().node()
    let first = node.snapshot_index + 1UL
    let last = node.last_log_index()
    if last < first {
      self.out.write("log is empty: first index=\{first}, last index=\{last}")
      self.out.write("\n")
      continue
    }
    let mut ix = first
    while ix <= last {
      if node.entry_at(ix) is Some(e) {
        self.out.write(@log.describe_entry(e, None))
        self.out.write("\n")
      }
      ix = ix + 1UL
    }
  }
}

///|
fn RaftTest::raft_state(self : RaftTest) -> Unit {
  for n in self.nodes {
    let st = n.rn.status()
    let full = n.rn.node().full_status()
    let is_voter = full.config.voters.contains(n.id)
    let kind = if is_voter { "(Voter)" } else { "(Non-Voter)" }
    self.out.write(
      "\{st.id}: \{rt_state_name(st.role)} \{kind} Term:\{st.term} Lead:\{rt_lead(st.leader)}\n",
    )
  }
}

///|
fn RaftTest::status(self : RaftTest, ids : Array[String]) -> Unit {
  for id in ids {
    let i = self.index_of(id)
    if i < 0 {
      continue
    }
    let progress = self.nodes[i].rn.node().progress_status()
    let ids_sorted : Array[String] = []
    for p in progress {
      ids_sorted.push(p.id)
    }
    ids_sorted.sort()
    for pid in ids_sorted {
      for p in progress {
        if p.id == pid {
          let state = match p.state {
            Probe => "StateProbe"
            Replicate => "StateReplicate"
            Snapshot => "StateSnapshot"
          }
          self.out.write(
            "\{p.id}: \{state} match=\{p.match_index} next=\{p.next_index}",
          )
          if p.is_learner {
            self.out.write(" learner")
          }
          if p.paused {
            self.out.write(" paused")
          }
          if p.pending_snapshot > 0UL {
            self.out.write(" pendingSnap=\{p.pending_snapshot}")
          }
          if !p.recent_active {
            self.out.write(" inactive")
          }
          if p.inflight > 0 {
            self.out.write(" inflight=\{p.inflight}")
            if p.inflight_full {
              self.out.write("[full]")
            }
          }
          self.out.write("\n")
        }
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Snapshot, membership and proposal commands
// ---------------------------------------------------------------------------

///|
fn RaftTest::snapshot(self : RaftTest, a : Args) -> Unit {
  let i = self.index_of(pos_or(a.positional, 0, "1"))
  if i >= 0 {
    if self.nodes[i].rn.next_unstable_snapshot() is Some(s) {
      self.out.write("Snapshot Index:\{s.last_index} Term:\{s.last_term}\n")
    }
  }
}

///|
/// `send-snapshot <from> <to>`: queue a snapshot message from one node to
/// another, as upstream's handler does, without delivering it.
fn RaftTest::send_snapshot(self : RaftTest, a : Args) -> Unit {
  let from = pos_or(a.positional, 0, "1")
  let to = pos_or(a.positional, 1, "")
  if to == "" {
    return
  }
  let i = self.index_of(from)
  if i < 0 {
    return
  }
  // Hand the follower the snapshot the leader's storage holds: the state it has
  // applied, which is at or ahead of its last compaction (upstream reaches
  // through `raftLog.snapshot()`, whose index is the applied one).
  let node = self.nodes[i].rn.node()
  let snap_index = if node.node().last_applied > node.node().snapshot_index {
    node.node().last_applied
  } else {
    node.node().snapshot_index
  }
  let snap : Snapshot = {
    last_index: snap_index,
    last_term: node.node().term_at(snap_index),
    data: b"",
    conf_state: node.conf_state(),
  }
  let msg = Message::new(
    from,
    to,
    Snapshot(
      InstallSnapshotArgs::whole(self.nodes[i].rn.status().term, from, snap),
    ),
  )
  // The handler reports the message it queued; delivering it is a later step.
  self.out.write(rt_describe_message(msg))
  self.out.write("\n")
  self.messages.push(msg)
}

///|
/// The block input of a conf-change directive: `v1 v3 l4 r5` — add voter, add
/// learner, remove.
fn rt_parse_changes(input : String) -> Array[(String, String)] {
  let out : Array[(String, String)] = []
  for word in input.split("\n") {
    for part in word.to_owned().split(" ") {
      let t = part.to_owned().trim().to_owned()
      if t.length() >= 2 {
        let kind = t[:1].to_owned()
        let target = t[1:].to_owned()
        if target != "" && (kind == "v" || kind == "r" || kind == "l") {
          out.push((kind, target))
        }
      }
    }
  }
  out
}

///|
fn conf_change(kind : String, target : String) -> ConfChange {
  match kind {
    "r" => ConfChange::remove(target)
    "l" => ConfChange::add_learner(target)
    _ => ConfChange::add(target)
  }
}

///|
fn RaftTest::propose_conf_change(
  self : RaftTest,
  a : Args,
  input : String,
) -> Unit {
  let i = self.index_of(pos_or(a.positional, 0, "1"))
  if i < 0 {
    return
  }
  let is_v1 = kv_flag(a.kv, "v1")
  let changes = rt_parse_changes(input)
  if changes.is_empty() {
    return
  }
  if is_v1 {
    let (kind, target) = changes[0]
    self.nodes[i].rn.propose_conf(conf_change(kind, target))
  } else {
    let ccs : Array[ConfChange] = []
    for ch in changes {
      let (kind, target) = ch
      ccs.push(conf_change(kind, target))
    }
    // `transition=auto` applies the batch as one change; `explicit` enters the
    // joint configuration and waits for a leave; `implicit` enters it with
    // auto-leave off and leaves on the next committed change.
    let transition = kv_get(a.kv, "transition", "auto")
    let cc = match transition {
      "explicit" => ConfChangeV2::enter_joint(ccs, auto_leave=false)
      "implicit" => ConfChangeV2::enter_joint(ccs, auto_leave=true)
      _ => ConfChangeV2::auto(ccs)
    }
    self.nodes[i].rn.propose_conf_v2(cc)
  }
}

///|
fn RaftTest::remove_nodes(self : RaftTest, a : Args) -> Unit {
  let i = self.index_of(pos_or(a.positional, 0, "1"))
  let target = pos_or(a.positional, 1, "")
  if i >= 0 && target != "" {
    self.nodes[i].rn.propose_conf(ConfChange::remove(target))
  }
}

///|
fn RaftTest::add_learner(self : RaftTest, a : Args) -> Unit {
  let i = self.index_of(pos_or(a.positional, 0, "1"))
  let target = pos_or(a.positional, 1, "")
  if i >= 0 && target != "" {
    self.nodes[i].rn.propose_conf(ConfChange::add_learner(target))
  }
}
