// Copyright 2015 The etcd Authors
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
// The rendering the etcd datadriven scripts expect, ported from upstream's
// `util.go` (DescribeReady / DescribeMessage / DescribeEntry and friends). The
// expected blocks in testdata/ are byte comparisons, so every separator, the
// order of the fields, and which fields are omitted when zero have to match.
// Payload fields are read out of this port's typed `Payload` in the same order
// upstream reads them out of `pb.Message`.

///|
/// One entry, rendered as upstream's `DescribeEntry` does: a normal entry shows
/// its payload, a configuration change shows the changes themselves (`r1`,
/// `v2 l4`) rather than the serialized bytes. A `ConfChange` entry whose payload
/// is a batch is upstream's `EntryConfChangeV2`, and is named accordingly.
fn rt_describe_entry(e : Entry) -> String {
  if e.entry_type == Normal {
    @log.describe_entry(e, None)
  } else if @confchange.ConfChangeV2::decode(e.command) is Some(v2) {
    let ch = @confchange.describe_changes(v2.changes)
    // etcd's DescribeEntry appends the payload only when it is non-empty, so an
    // empty ConfChangeV2 (the auto-leave no-op) has no trailing space.
    "\{e.term}/\{e.index} EntryConfChangeV2" +
    (if ch == "" { "" } else { " " + ch })
  } else if @confchange.ConfChange::decode(e.command) is Some(cc) {
    let ch = @confchange.describe_changes([cc])
    "\{e.term}/\{e.index} EntryConfChange" +
    (if ch == "" { "" } else { " " + ch })
  } else {
    @log.describe_entry(e, None)
  }
}

///|
/// Each entry on its own line, rendered by `rt_describe_entry`.
fn rt_describe_entries(entries : ArrayView[Entry]) -> String {
  let buf = StringBuilder()
  for e in entries {
    buf.write_string(rt_describe_entry(e))
    buf.write_char('\n')
  }
  buf.to_string()
}

///|
/// The upstream name of a message type, as `%v` prints the protobuf enum.
fn rt_msg_type(m : Message) -> String {
  match m.payload {
    PreVote(_) => "MsgPreVote"
    PreVoteResp(_) => "MsgPreVoteResp"
    Vote(_) => "MsgVote"
    VoteResp(_) => "MsgVoteResp"
    // Upstream has no separate heartbeat payload: a heartbeat is MsgHeartbeat.
    Heartbeat(_) => "MsgHeartbeat"
    HeartbeatResp(_) => "MsgHeartbeatResp"
    Append(_) => "MsgApp"
    AppendResp(_) => "MsgAppResp"
    Snapshot(_) => "MsgSnap"
    TimeoutNow(_) => "MsgTimeoutNow"
    ReadIndexResp(_) => "MsgReadIndexResp"
    Propose(_) => "MsgProp"
    ReadIndex(_) => "MsgReadIndex"
    TransferLeader(_) => "MsgTransferLeader"
    ForgetLeader => "MsgForgetLeader"
    StorageAppendResp(_) => "MsgStorageAppendResp"
    StorageApplyResp(_) => "MsgStorageApplyResp"
  }
}

///|
/// The (log term, index) pair a message carries, in the fields upstream prints
/// as `Log:%d/%d`. A response carries the position it answers; a vote carries
/// the candidate's log tail; a heartbeat carries 0/0.
fn rt_log_position(m : Message) -> (UInt64, UInt64) {
  match m.payload {
    PreVote(a) | Vote(a) => (a.last_log_term, a.last_log_index)
    PreVoteResp(_) | VoteResp(_) | HeartbeatResp(_) => (0UL, 0UL)
    Append(a) => (a.prev_log_term, a.prev_log_index)
    AppendResp(r) => {
      let term = if r.success { 0UL } else { r.conflict_term }
      let index = if r.success { r.match_index } else { r.reject_index }
      (term, index)
    }
    // A storage acknowledgement names the prefix it made durable; an apply
    // acknowledgement names no position (committed entries are term-free).
    StorageAppendResp(r) => (r.log_term, r.index)
    _ => (0UL, 0UL)
  }
}

///|
/// Render one message the way upstream's `DescribeMessage` does:
/// `from->to Type Term:t Log:lt/i`, then, only when set, ` Rejected (Hint: n)`,
/// ` Commit:n`, ` Vote:n`, the entries, and the snapshot on its own line.
fn rt_describe_message(m : Message) -> String {
  let buf = StringBuilder()
  let (log_term, index) = rt_log_position(m)
  buf.write_string(
    "\{m.from}->\{m.to} \{rt_msg_type(m)} Term:\{m.term()} Log:\{log_term}/\{index}",
  )
  match m.payload {
    AppendResp(r) =>
      // A rejection carries the index the leader should retry from: the last
      // entry the follower could match (etcd's reject hint).
      if !r.success {
        buf.write_string(" Rejected (Hint: \{r.conflict_index})")
      }
    VoteResp(r) =>
      if !r.vote_granted {
        buf.write_string(" Rejected (Hint: 0)")
      }
    PreVoteResp(r) =>
      if !r.vote_granted {
        buf.write_string(" Rejected (Hint: 0)")
      }
    _ => ()
  }
  let commit = match m.payload {
    Append(a) => a.leader_commit
    Heartbeat(a) => a.commit
    _ => 0UL
  }
  if commit != 0 {
    buf.write_string(" Commit:\{commit}")
  }
  let vote = match m.payload {
    VoteResp(r) => if r.vote_granted { 0UL } else { 0UL }
    _ => 0UL
  }
  if vote != 0 {
    buf.write_string(" Vote:\{vote}")
  }
  // Any message that carries entries shows them, as upstream's `DescribeMessage`
  // reads the entry list off the message rather than off a type check — a
  // follower forwarding a proposal still prints what it is forwarding.
  let carried : Array[Entry] = match m.payload {
    Append(a) => a.entries
    Propose(es) => es
    StorageApplyResp(r) => r.entries
    _ => []
  }
  if carried.length() == 1 {
    buf.write_string(" Entries:[\{rt_describe_entry(carried[0])}]")
  } else if carried.length() > 1 {
    buf.write_string(" Entries:[")
    for e in carried {
      buf.write_string("\n  \{rt_describe_entry(e)}")
    }
    buf.write_string("\n]")
  }
  match m.payload {
    Snapshot(s) =>
      buf.write_string(
        "\n  Snapshot: Index:\{s.last_index} Term:\{s.last_term} ConfState:\{@log.describe_conf_state(s.conf_state)}",
      )
    _ => ()
  }
  buf.to_string()
}

///|
/// The append directive as upstream's `MsgStorageAppend` prints it, with the
/// responses that travel with it. `nested` chooses upstream's `Responses:[…]`
/// form (a message inside a Ready); the plain form (a `Responses:` header) is
/// what the append thread dumps when it processes the message.
fn rt_describe_storage_append(
  id : String,
  sa : StorageAppend,
  nested : Bool,
) -> String {
  let buf = StringBuilder()
  // A MsgStorageAppend carries no term of its own: the fields are assigned from
  // the HardState only when there is one to write, and `Index`/`LogTerm` are left
  // unset (they describe entries, which travel in `Entries`).
  let term = match sa.hard_state {
    Some(hs) => hs.term
    None => 0UL
  }
  buf.write_string("\{id}->AppendThread MsgStorageAppend Term:\{term} Log:0/0")
  match sa.hard_state {
    Some(hs) => {
      if hs.commit != 0UL {
        buf.write_string(" Commit:\{hs.commit}")
      }
      match hs.vote {
        Some(v) => buf.write_string(" Vote:\{v}")
        None => ()
      }
    }
    None => ()
  }
  buf.write_string(rt_describe_entries_attr(sa.entries))
  // The append thread acknowledges the write when there are entries or a snapshot
  // to make durable (etcd's `needStorageAppendRespMsg`), and that acknowledgement
  // is the last response in the batch.
  let needs_ack = sa.resp.index != 0UL || sa.snapshot is Some(_)
  let ack_term = sa.resp.term
  let ack_log = sa.resp.log_term
  let ack_index = sa.resp.index
  let ack = "AppendThread->\{id} MsgStorageAppendResp Term:\{ack_term} Log:\{ack_log}/\{ack_index}"
  if nested {
    // Inside a Ready the responses are a bracketed list, printed only when there
    // is at least one (an append that writes nothing but a commit says nothing).
    if !sa.responses.is_empty() || needs_ack {
      buf.write_string(" Responses:[")
      for r in sa.responses {
        buf.write_string("\n  \{rt_describe_message(r)}")
      }
      if needs_ack {
        buf.write_string("\n  " + ack)
      }
      buf.write_string("\n]")
    }
  } else {
    // The append thread's own dump always names the section, even when empty.
    buf.write_string("\nResponses:")
    for r in sa.responses {
      buf.write_string("\n\{rt_describe_message(r)}")
    }
    if needs_ack {
      buf.write_string("\n" + ack)
    }
  }
  buf.to_string()
}

///|
/// The apply directive as upstream's `MsgStorageApply` prints it.
fn rt_describe_storage_apply(
  id : String,
  sa : StorageApply,
  nested : Bool,
) -> String {
  let buf = StringBuilder()
  buf.write_string("\{id}->ApplyThread MsgStorageApply Term:0 Log:0/0")
  buf.write_string(rt_describe_entries_attr(sa.entries))
  let ack = "ApplyThread->\{id} MsgStorageApplyResp Term:0 Log:0/0" +
    rt_describe_entries_attr(sa.resp.entries)
  if nested {
    buf.write_string(" Responses:[")
    buf.write_string("\n  " + ack)
    buf.write_string("\n]")
  } else {
    buf.write_string("\nResponses:")
    buf.write_string("\n" + ack)
  }
  buf.to_string()
}

///|
/// ` Entries:[…]` in upstream's message layout: one entry inline, several one per
/// line.
fn rt_describe_entries_attr(entries : Array[Entry]) -> String {
  if entries.is_empty() {
    return ""
  }
  let buf = StringBuilder()
  if entries.length() == 1 {
    buf.write_string(" Entries:[\{rt_describe_entry(entries[0])}]")
  } else {
    buf.write_string(" Entries:[")
    for e in entries {
      buf.write_string("\n  \{rt_describe_entry(e)}")
    }
    buf.write_string("\n]")
  }
  buf.to_string()
}

///|
/// The raft state name upstream prints (`StateFollower`, ...).
fn rt_state_name(r : Role) -> String {
  match r {
    Follower => "StateFollower"
    PreCandidate => "StatePreCandidate"
    Candidate => "StateCandidate"
    Leader => "StateLeader"
  }
}

///|
/// Render a Ready exactly as upstream's `DescribeReady` does: the soft state,
/// the hard state (only when not empty), read states, entries, snapshot,
/// committed entries and messages, in that order, each section only when it has
/// content, all under one `Ready MustSync=...:` header.
fn rt_describe_ready(rd : Ready, extra : Array[String]) -> String {
  let buf = StringBuilder()
  match rd.soft_state {
    Some(ss) =>
      buf.write_string(
        "Lead:\{rt_lead(ss.lead)} State:\{rt_state_name(ss.state)}\n",
      )
    None => ()
  }
  match rd.hard_state {
    Some(hs) =>
      if !rt_hard_state_is_empty(hs) {
        buf.write_string("HardState \{@log.describe_hard_state(hs)}\n")
      }
    None => ()
  }
  if rd.entries.length() > 0 {
    buf.write_string("Entries:\n")
    buf.write_string(rt_describe_entries(rd.entries[:]))
  }
  match rd.snapshot {
    Some(s) => buf.write_string("Snapshot \{@log.describe_snapshot(s)}\n")
    None => ()
  }
  if rd.committed_entries.length() > 0 {
    buf.write_string("CommittedEntries:\n")
    buf.write_string(rt_describe_entries(rd.committed_entries[:]))
  }
  if rd.messages.length() > 0 || !extra.is_empty() {
    buf.write_string("Messages:\n")
    for m in rd.messages {
      buf.write_string(rt_describe_message(m))
      buf.write_char('\n')
    }
    for line in extra {
      buf.write_string(line)
      buf.write_char('\n')
    }
  }
  if buf.is_empty() {
    "<empty Ready>"
  } else {
    "Ready MustSync=\{rd.must_sync}:\n" + buf.to_string()
  }
}

///|
fn rt_lead(lead : String?) -> String {
  match lead {
    Some(v) => v
    None => "0"
  }
}

///|
/// A hard state is empty when no term was ever adopted, no vote was cast and
/// nothing is committed (upstream's `IsEmptyHardState`).
fn rt_hard_state_is_empty(hs : HardState) -> Bool {
  hs.term == 0UL && hs.vote is None && hs.commit == 0UL
}
