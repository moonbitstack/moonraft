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
      if !r.success {
        buf.write_string(" Rejected (Hint: \{r.match_index})")
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
  match m.payload {
    Append(a) => {
      let ents = a.entries
      if ents.length() == 1 {
        buf.write_string(" Entries:[\{@log.describe_entry(ents[0], None)}]")
      } else if ents.length() > 1 {
        buf.write_string(" Entries:[")
        for e in ents {
          buf.write_string("\n  \{@log.describe_entry(e, None)}")
        }
        buf.write_string("\n]")
      }
    }
    Snapshot(s) =>
      buf.write_string(
        "\n  Snapshot: Index:\{s.last_index} Term:\{s.last_term} ConfState:\{@log.describe_conf_state(s.conf_state)}",
      )
    _ => ()
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
fn rt_describe_ready(rd : Ready) -> String {
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
    buf.write_string(@log.describe_entries(rd.entries[:], None))
  }
  match rd.snapshot {
    Some(s) => buf.write_string("Snapshot \{@log.describe_snapshot(s)}\n")
    None => ()
  }
  if rd.committed_entries.length() > 0 {
    buf.write_string("CommittedEntries:\n")
    buf.write_string(@log.describe_entries(rd.committed_entries[:], None))
  }
  if rd.messages.length() > 0 {
    buf.write_string("Messages:\n")
    for m in rd.messages {
      buf.write_string(rt_describe_message(m))
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
