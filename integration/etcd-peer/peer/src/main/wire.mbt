// The wire the two peers share, written twice: here and in the Go harness that
// drives this process. One message is one line of space-separated fields, so a
// reader of either side can see the whole exchange in a log:
//
//   msg <type> <to> <from> <term> <logterm> <index> <commit> <reject> <hint> <force> <context> <n> [<type> <term> <index> <data>]...
//
// `context` and `data` are hex, `-` for empty. Bytes on the line are bytes on
// the wire: a transport is free to encode them however it likes, and this one
// chooses text so a failed run can be read.

///|
/// A malformed line. The harness feeds this process from a program, so anything
/// unexpected means the two sides disagree about the format and the run should
/// stop rather than guess.
suberror Wire {
  Wire(String)
}

///|
/// Space-separated fields with the empty runs dropped.
fn fields(line : StringView) -> Array[String] {
  let out : Array[String] = []
  for field in line.split(" ") {
    if field.length() > 0 {
      out.push(field.to_owned())
    }
  }
  out
}

///|
fn parse_u64(text : StringView) -> UInt64 raise Wire {
  guard text.length() > 0 else { raise Wire("empty number") }
  let mut n = 0UL
  for i in 0..<text.length() {
    let c = text[i]
    guard c >= '0' && c <= '9' else { raise Wire("not a number: \{text}") }
    n = n * 10UL + (c.to_int() - 48).to_uint64()
  }
  n
}

///|
fn parse_bool(text : StringView) -> Bool raise Wire {
  match text {
    "1" => true
    "0" => false
    _ => raise Wire("not a flag: \{text}")
  }
}

///|
fn parse_hex(text : StringView) -> Bytes raise Wire {
  if text == "-" || text.length() == 0 {
    return b""
  }
  @base16.decode(text) catch {
    _ => raise Wire("not hex: \{text}")
  }
}

///|
fn show_hex(bytes : BytesView) -> String {
  if bytes.length() == 0 {
    "-"
  } else {
    @base16.encode(bytes)
  }
}

///|
fn encode_message(m : @raft.Message) -> String raise Wire {
  let parts : Array[String] = ["msg"]
  let mut term = 0UL
  let mut logterm = 0UL
  let mut index = 0UL
  let mut commit = 0UL
  let mut reject = false
  let mut hint = 0UL
  let mut context : Bytes = b""
  let mut entries : Array[@raft.Entry] = []
  let kind = match m.payload {
    PreVote(a) => {
      term = a.term
      logterm = a.last_log_term
      index = a.last_log_index
      "PreVote"
    }
    Vote(a) => {
      term = a.term
      logterm = a.last_log_term
      index = a.last_log_index
      "Vote"
    }
    PreVoteResp(r) => {
      term = r.term
      reject = !r.vote_granted
      "PreVoteResp"
    }
    VoteResp(r) => {
      term = r.term
      reject = !r.vote_granted
      "VoteResp"
    }
    Append(a) => {
      term = a.term
      logterm = a.prev_log_term
      index = a.prev_log_index
      commit = a.leader_commit
      entries = a.entries
      "App"
    }
    AppendResp(r) => {
      term = r.term
      reject = !r.success
      if r.success {
        index = r.match_index
      } else {
        index = r.reject_index
        hint = r.conflict_index
        logterm = r.conflict_term
      }
      "AppResp"
    }
    Heartbeat(a) => {
      term = a.term
      commit = a.commit
      context = a.context
      "Heartbeat"
    }
    HeartbeatResp(r) => {
      term = r.term
      context = r.context
      "HeartbeatResp"
    }
    Snapshot(a) => {
      term = a.term
      logterm = a.last_term
      index = a.last_index
      "Snap"
    }
    _ =>
      raise Wire(
        "the harness only carries the replication and election messages",
      )
  }
  let snapshot = match m.payload {
    Snapshot(a) => Some(a)
    _ => None
  }
  parts.push(kind)
  parts.push(m.to)
  parts.push(m.from)
  parts.push(term.to_string())
  parts.push(logterm.to_string())
  parts.push(index.to_string())
  parts.push(commit.to_string())
  parts.push(if reject { "1" } else { "0" })
  parts.push(hint.to_string())
  parts.push(if m.force { "1" } else { "0" })
  parts.push(show_hex(context))
  parts.push(entries.length().to_string())
  for e in entries {
    parts.push(
      match e.entry_type {
        @raft.EntryType::Normal => "0"
        @raft.EntryType::ConfChange => "1"
      },
    )
    parts.push(e.term.to_string())
    parts.push(e.index.to_string())
    let command : BytesView = e.command
    parts.push(show_hex(command))
  }
  match snapshot {
    Some(a) => {
      // A snapshot travels whole, so the message needs the membership it
      // records and the state it carries: the follower rebuilds its voter and
      // learner sets from these, not from the entries it never had.
      parts.push(a.conf_state.voters.length().to_string())
      for voter in a.conf_state.voters {
        parts.push(voter)
      }
      parts.push(a.conf_state.learners.length().to_string())
      for learner in a.conf_state.learners {
        parts.push(learner)
      }
      let data : BytesView = a.data
      parts.push(show_hex(data))
    }
    None => ()
  }
  parts.join(" ")
}

///|
fn field(f : Array[String], i : Int32) -> String raise Wire {
  guard i < f.length() else {
    raise Wire("the message ends early: \{f.join(" ")}")
  }
  f[i]
}

///|
fn decode_message(f : Array[String]) -> @raft.Message raise Wire {
  // f[0] is "msg".
  let kind = field(f, 1)
  let to = field(f, 2)
  let from = field(f, 3)
  let term = parse_u64(field(f, 4))
  let logterm = parse_u64(field(f, 5))
  let index = parse_u64(field(f, 6))
  let commit = parse_u64(field(f, 7))
  let reject = parse_bool(field(f, 8))
  let hint = parse_u64(field(f, 9))
  let force = parse_bool(field(f, 10))
  let context = parse_hex(field(f, 11))
  let count = parse_u64(field(f, 12)).to_int()
  let entries : Array[@raft.Entry] = []
  for i in 0..<count {
    let at = 13 + i * 4
    let entry_type = match field(f, at) {
      "0" => @raft.EntryType::Normal
      "1" => @raft.EntryType::ConfChange
      other => raise Wire("unknown entry type \{other}")
    }
    entries.push({
      term: parse_u64(field(f, at + 1)),
      index: parse_u64(field(f, at + 2)),
      entry_type,
      command: parse_hex(field(f, at + 3)),
    })
  }
  let payload = match kind {
    "Vote" =>
      @raft.Payload::Vote({
        term,
        candidate_id: from,
        last_log_index: index,
        last_log_term: logterm,
      })
    "VoteResp" => @raft.Payload::VoteResp({ term, vote_granted: !reject, })
    "PreVote" =>
      @raft.Payload::PreVote({
        term,
        candidate_id: from,
        last_log_index: index,
        last_log_term: logterm,
      })
    "PreVoteResp" =>
      @raft.Payload::PreVoteResp({ term, vote_granted: !reject, })
    "App" =>
      @raft.Payload::Append({
        term,
        leader_id: from,
        prev_log_index: index,
        prev_log_term: logterm,
        entries,
        leader_commit: commit,
      })
    "AppResp" =>
      @raft.Payload::AppendResp({
        term,
        success: !reject,
        match_index: if reject {
          0UL
        } else {
          index
        },
        conflict_index: hint,
        conflict_term: logterm,
        reject_index: if reject {
          index
        } else {
          0UL
        },
      })
    "Heartbeat" =>
      @raft.Payload::Heartbeat({ term, leader_id: from, commit, context, })
    "HeartbeatResp" => @raft.Payload::HeartbeatResp({ term, context, })
    "Snap" => {
      let mut at = 13
      let voters = read_ids(f, at)
      at = at + 1 + voters.length()
      let learners = read_ids(f, at)
      at = at + 1 + learners.length()
      @raft.Payload::Snapshot({
        term,
        leader_id: from,
        last_index: index,
        last_term: logterm,
        offset: 0UL,
        data: parse_hex(field(f, at)),
        done: true,
        conf_state: {
          voters,
          learners,
          voters_outgoing: [],
          learners_next: [],
          auto_leave: false,
        },
      })
    }
    other => raise Wire("unknown message type \{other}")
  }
  @raft.Message::new(from, to, payload, force~)
}

///|
/// Read a count and that many ids, starting at `at`.
fn read_ids(f : Array[String], at : Int32) -> Array[String] raise Wire {
  let count = parse_u64(field(f, at)).to_int()
  let out : Array[String] = []
  for i in 0..<count {
    out.push(field(f, at + 1 + i))
  }
  out
}
