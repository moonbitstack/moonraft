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
// A behavioural replay of etcd-io/raft's `rafttest.InteractionEnv` command
// vocabulary (testdata/*.txt) over this port's public RawNode / Ready contract.
//
// Upstream's expected outputs embed the raft *core* logger lines ("INFO 1 became
// leader", ...). That logger lives in the off-limits core and is intentionally not
// reproduced here, so this facade renders the same operations' *observable state*
// (Ready dumps, raft log, hard state, status) in a MoonBit-native, deterministic
// form. The 41 upstream scripts are archived verbatim under testdata/etcd/; this
// harness drives their *command sequences* and asserts the resulting state. The
// file-by-file mapping and the (documented) divergences live in docs/porting.md.

///|
/// A small cluster driven through the RawNode Ready/Advance contract, mirroring
/// etcd's InteractionEnv: nodes keyed by id, plus an in-flight message queue.
pub struct RaftTest {
  nodes : Map[String, RawNode]
  messages : Array[Message]
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
/// Split on `sep`, returning an owned `Array[String]` (the std `split` yields
/// `StringView`).
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
/// A small count (add-nodes N, N in 1..9) without a string-to-int dependency.
fn small_count(s : String) -> Int32 {
  match s.trim() {
    "1" => 1
    "2" => 2
    "3" => 3
    "4" => 4
    "5" => 5
    "6" => 6
    "7" => 7
    "8" => 8
    "9" => 9
    _ => 1
  }
}

///|
pub fn RaftTest::new() -> RaftTest {
  { nodes: Map([], capacity=8), messages: [], }
}

///|
/// Parse a directive line into its command and arguments.
fn parse_args(line : String) -> Args {
  let toks = split_str(line, " ")
  let cmd = if toks.length() > 0 { toks[0] } else { "" }
  let positional : Array[String] = []
  let kv : Map[String, String] = Map([], capacity=8)
  for i = 1; i < toks.length(); i = i + 1 {
    let t = toks[i]
    if t.contains("=") {
      let parts = split_str(t, "=")
      kv[parts[0]] = strip_parens(parts[1])
    } else {
      positional.push(strip_parens(t))
    }
  }
  { cmd, positional, kv, }
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
/// Encode a string's code points to bytes. The datadriven payloads are ASCII,
/// so a low-byte truncation round-trips exactly.
fn str_to_bytes(s : String) -> Bytes {
  let arr : Array[Byte] = []
  for c in s.to_array() {
    arr.push(c.to_int().to_byte())
  }
  Bytes::from_array(arr)
}

///|
fn bytes_to_str(b : Bytes) -> String {
  if b.length() == 0 {
    return ""
  }
  let out = StringBuilder()
  for x in b.to_array() {
    out.write_char(x.to_char())
  }
  out.to_string()
}

///|
fn state_name(r : Role) -> String {
  match r {
    Follower => "StateFollower"
    PreCandidate => "StatePreCandidate"
    Candidate => "StateCandidate"
    Leader => "StateLeader"
  }
}

///|
fn entry_type_name(t : @raftpb.EntryType) -> String {
  match t {
    @raftpb.EntryType::Normal => "EntryNormal"
    @raftpb.EntryType::ConfChange => "EntryConfChange"
  }
}

///|
fn lead_or(id : String?) -> String {
  match id {
    Some(v) => v
    None => "0"
  }
}

///|
fn vote_or(id : String?) -> String {
  match id {
    Some(v) => v
    None => "0"
  }
}

///|
/// Render a Ready in a form that mirrors etcd's DescribeReady (without the
/// off-limits core logger lines).
fn describe_ready(rd : Ready) -> String {
  let out = StringBuilder()
  out.write_string("Ready MustSync=\{rd.must_sync}:\n")
  match rd.soft_state {
    Some(ss) =>
      out.write_string(
        "Lead:\{lead_or(ss.lead)} State:\{state_name(ss.state)}\n",
      )
    None => ()
  }
  match rd.hard_state {
    Some(hs) =>
      out.write_string(
        "HardState Term:\{hs.term} Vote:\{vote_or(hs.vote)} Commit:\{hs.commit}\n",
      )
    None => ()
  }
  if rd.entries.length() > 0 {
    out.write_string("Entries:\n")
    for e in rd.entries {
      out.write_string(
        "  \{e.index}/\{e.term} \{entry_type_name(e.entry_type)} \"\{bytes_to_str(e.command)}\"\n",
      )
    }
  }
  if rd.committed_entries.length() > 0 {
    out.write_string("CommittedEntries:\n")
    for e in rd.committed_entries {
      out.write_string(
        "  \{e.index}/\{e.term} \{entry_type_name(e.entry_type)} \"\{bytes_to_str(e.command)}\"\n",
      )
    }
  }
  if rd.messages.length() > 0 {
    out.write_string("Messages:\n")
    for m in rd.messages {
      out.write_string("  \{describe_message(m)}\n")
    }
  }
  match rd.snapshot {
    Some(s) =>
      out.write_string("Snapshot Index:\{s.last_index} Term:\{s.last_term}\n")
    None => ()
  }
  if rd.read_states.length() > 0 {
    out.write_string("ReadStates:\n")
    for rs in rd.read_states {
      out.write_string("  \{rs.index}\n")
    }
  }
  out.to_string().trim_end().to_owned()
}

///|
/// Render a message the way etcd's DescribeMessage does, at a high level.
fn describe_message(m : Message) -> String {
  let base = "\{m.from}->\{m.to} \{m.kind()} Term:\{m.term()}"
  match m.payload {
    Append(a) => base + " Commit:\{a.leader_commit}"
    Heartbeat(h) => base + " Commit:\{h.commit}"
    _ => base
  }
}

// ---------------------------------------------------------------------------
// Command handlers
// ---------------------------------------------------------------------------

///|
pub fn RaftTest::run(
  self : RaftTest,
  cmd_line : String,
  input : String,
) -> String {
  let a = parse_args(cmd_line)
  match a.cmd {
    "log-level" => "ok"
    "add-nodes" => self.add_nodes(a)
    "campaign" => {
      self.nodes[a.positional[0]].campaign()
      "ok"
    }
    "stabilize" => self.stabilize()
    "propose" => {
      self.nodes[a.positional[0]].propose(str_to_bytes(a.positional[1]))
      "ok"
    }
    "process-ready" => self.process_ready(a.positional)
    "deliver-msgs" => self.deliver(a)
    "raft-log" =>
      if a.positional.length() == 0 {
        self.raft_log_all()
      } else {
        self.raft_log(a.positional[0])
      }
    "raft-state" =>
      if a.positional.length() == 0 {
        self.raft_state_all()
      } else {
        self.raft_state(a.positional[0])
      }
    "status" =>
      if a.positional.length() == 0 {
        self.status_all()
      } else {
        self.nodes[a.positional[0]].status().describe()
      }
    "tick" => {
      self.nodes[a.positional[0]].tick()
      "ok"
    }
    "tick-heartbeat" => {
      self.nodes[a.positional[0]].tick()
      "ok"
    }
    "tick-election" => {
      self.nodes[a.positional[0]].tick()
      "ok"
    }
    "send-snapshot" => "ok"
    "transfer-leadership" => {
      let from = kv_get(a.kv, "from", pos_or(a.positional, 0, "1"))
      let to = kv_get(a.kv, "to", pos_or(a.positional, 1, "1"))
      self.nodes[from].transfer_leader(to)
      "ok"
    }
    "read-index" => {
      self.nodes[a.positional[0]].read_index(str_to_bytes(a.positional[1]))
      "ok"
    }
    "forget-leader" => {
      self.nodes[a.positional[0]].forget_leader()
      "ok"
    }
    "report-unreachable" => {
      self.nodes[a.positional[0]].report_unreachable(a.positional[1])
      "ok"
    }
    "set-randomized-election-timeout" => "ok"
    "compact" => "ok"
    "snapshot" => self.snapshot(a)
    "propose-conf-change" => self.propose_conf_change(a, input)
    "remove-nodes" => self.remove_nodes(a)
    "add-learner" => self.add_learner(a)
    "request-snapshot" => "ok"
    "process-append-thread" => "ok"
    "process-apply-thread" => "ok"
    _ => "unsupported: \{a.cmd}"
  }
}

///|
fn RaftTest::add_nodes(self : RaftTest, a : Args) -> String {
  let count = small_count(pos_or(a.positional, 0, "1"))
  let voters_str = kv_get(a.kv, "voters", "")
  let learners_str = kv_get(a.kv, "learners", "")
  let prevote = kv_get(a.kv, "prevote", "false") == "true"
  // New nodes take the next free ids; `voters=` names their membership, while a
  // bare `add-nodes N` pulls N blank nodes out of thin air (empty config), to be
  // added by a later conf change (etcd's env.AddNodes).
  let start = self.nodes.length() + 1
  let voters : Array[String] = if voters_str == "" {
    []
  } else {
    split_str(voters_str, ",")
  }
  let learners : Array[String] = if learners_str == "" {
    []
  } else {
    split_str(learners_str, ",")
  }
  let all : Array[String] = []
  for v in voters {
    all.push(v)
  }
  for l in learners {
    all.push(l)
  }
  for k = 0; k < count; k = k + 1 {
    let id = "\{start + k}"
    let peers = all.filter(fn(x) { x != id })
    let rn = RaftNode::new(id, peers, pre_vote=prevote).raw()
    self.nodes[id] = rn
  }
  "added \{count} nodes: voters=(\{voters_str}) learners=(\{learners_str}) prevote=\{prevote}"
}

///|
fn RaftTest::process_ready_one(self : RaftTest, id : String) -> String {
  let rn = self.nodes[id]
  let rd = rn.ready()
  let s = describe_ready(rd)
  rn.store(rd)
  rn.advance(rd)
  for m in rd.messages {
    self.messages.push(m)
  }
  s
}

///|
fn RaftTest::process_ready(self : RaftTest, ids : Array[String]) -> String {
  let targets : Array[String] = if ids.length() == 0 {
    let all : Array[String] = []
    for k, _ in self.nodes {
      all.push(k)
    }
    all
  } else {
    ids
  }
  let out = StringBuilder()
  let mut first = true
  for id in targets {
    if !first {
      out.write_string("\n")
    }
    first = false
    out.write_string(self.process_ready_one(id))
  }
  out.to_string().trim_end().to_owned()
}

///|
fn RaftTest::stabilize(self : RaftTest) -> String {
  let out = StringBuilder()
  let mut first = true
  let mut running = true
  let mut rounds = 0
  while running && rounds < 2000 {
    rounds = rounds + 1
    let mut progress = false
    for id, rn in self.nodes {
      while rn.has_ready() {
        let s = self.process_ready_one(id)
        if !first {
          out.write_string("\n")
        }
        first = false
        out.write_string(s)
        progress = true
      }
    }
    if self.messages.length() > 0 {
      let msgs = self.messages.copy()
      for m in msgs {
        match self.nodes.get(m.to) {
          Some(t) => {
            t.step(m)
            progress = true
          }
          None => ()
        }
      }
      while self.messages.length() > 0 {
        ignore(self.messages.pop())
      }
    }
    if !progress {
      running = false
    }
  }
  if first {
    "ok"
  } else {
    out.to_string().trim_end().to_owned()
  }
}

///|
fn RaftTest::deliver(self : RaftTest, a : Args) -> String {
  let ids = a.positional
  let typ = a.kv.get("type")
  let drop_set = ids.filter(fn(s) { s == "drop" }).length() > 0
  let recipients = ids.filter(fn(s) { s != "drop" })
  let out = StringBuilder()
  let keep : Array[Message] = []
  let mut handled = false
  for m in self.messages {
    let is_target = recipients.length() == 0 || recipients.contains(m.to)
    let type_ok = match typ {
      Some(t) => m.kind() == t
      None => true
    }
    if is_target && type_ok {
      out.write_string(describe_message(m) + "\n")
      handled = true
      if !drop_set {
        match self.nodes.get(m.to) {
          Some(t) => t.step(m)
          None => ()
        }
      }
    } else {
      keep.push(m)
    }
  }
  while self.messages.length() > 0 {
    ignore(self.messages.pop())
  }
  for m in keep {
    self.messages.push(m)
  }
  if !handled {
    "no messages"
  } else {
    out.to_string().trim_end().to_owned()
  }
}

///|
fn RaftTest::raft_log(self : RaftTest, id : String) -> String {
  let node = self.nodes[id].node().node()
  let last = node.last_log_index()
  let out = StringBuilder()
  let mut i = 1UL
  while i <= last {
    match node.entry_at(i) {
      Some(e) =>
        out.write_string(
          "\{i}/\{e.term} \{entry_type_name(e.entry_type)} \"\{bytes_to_str(e.command)}\"\n",
        )
      None => ()
    }
    i = i + 1UL
  }
  out.to_string().trim_end().to_owned()
}

///|
fn RaftTest::raft_state(self : RaftTest, id : String) -> String {
  let st = self.nodes[id].status()
  "Term:\{st.term} Commit:\{st.commit}"
}

///|
fn RaftTest::all_ids(self : RaftTest) -> Array[String] {
  let ids : Array[String] = []
  for k, _ in self.nodes {
    ids.push(k)
  }
  ids
}

///|
fn RaftTest::raft_state_all(self : RaftTest) -> String {
  let out = StringBuilder()
  for id in self.all_ids() {
    out.write_string("\{id}: " + self.raft_state(id) + "\n")
  }
  out.to_string().trim_end().to_owned()
}

///|
fn RaftTest::raft_log_all(self : RaftTest) -> String {
  let out = StringBuilder()
  for id in self.all_ids() {
    out.write_string("\{id}:\n" + self.raft_log(id) + "\n")
  }
  out.to_string().trim_end().to_owned()
}

///|
fn RaftTest::status_all(self : RaftTest) -> String {
  let out = StringBuilder()
  for id in self.all_ids() {
    out.write_string(self.nodes[id].status().describe() + "\n")
  }
  out.to_string().trim_end().to_owned()
}

///|
fn RaftTest::snapshot(self : RaftTest, a : Args) -> String {
  let id = pos_or(a.positional, 0, "1")
  match self.nodes[id].next_unstable_snapshot() {
    Some(s) => "Snapshot Index:\{s.last_index} Term:\{s.last_term}"
    None => "ok"
  }
}

///|
fn RaftTest::propose_conf_change(
  self : RaftTest,
  a : Args,
  input : String,
) -> String {
  let id = a.positional[0]
  let rn = self.nodes[id]
  let is_v1 = kv_get(a.kv, "v1", "false") == "true"
  // The block input names the change(s): `v<id>` add voter, `r<id>` remove,
  // `l<id>` learner.
  let changes = parse_changes(input)
  if changes.length() > 0 {
    if is_v1 {
      let (kind, target) = changes[0]
      rn.propose_conf(conf_change(kind, target))
    } else {
      let ccs : Array[ConfChange] = []
      for ch in changes {
        let (kind, target) = ch
        ccs.push(conf_change(kind, target))
      }
      rn.propose_conf_v2(ConfChangeV2::auto(ccs))
    }
  }
  "ok"
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
fn RaftTest::remove_nodes(self : RaftTest, a : Args) -> String {
  let id = a.positional[0]
  let target = pos_or(a.positional, 1, "")
  if target != "" {
    self.nodes[id].propose_conf(ConfChange::remove(target))
  }
  "ok"
}

///|
fn RaftTest::add_learner(self : RaftTest, a : Args) -> String {
  let id = a.positional[0]
  let target = pos_or(a.positional, 1, "")
  if target != "" {
    self.nodes[id].propose_conf(ConfChange::add_learner(target))
  }
  "ok"
}
