// One node of a cluster whose other nodes are a different Raft implementation.
// The harness owns the clock and the network: it ticks this process and hands it
// the messages the Go nodes sent, so everything Raft decides for this node is
// decided here, in this library, and everything the Go nodes decide is decided
// there. Neither side is a simulation of the other.

///|
/// What the run needs to know about this node. Defaults are the library's, so a
/// run that passes only an id and the peer list ticks like a stock node.
struct Options {
  id : String
  peers : Array[String]
  election_tick : Int32
  heartbeat_tick : Int32
  seed : UInt64
}

///|
/// The peer list arrives as one comma-separated argument.
fn comma_fields(text : StringView) -> Array[String] {
  let out : Array[String] = []
  for field in text.split(",") {
    if field.length() > 0 {
      out.push(field.to_owned())
    }
  }
  out
}

///|
fn parse_options(args : Array[String]) -> Options raise Wire {
  let mut id = ""
  let mut peers : Array[String] = []
  let mut election_tick = 10
  let mut heartbeat_tick = 1
  let mut seed = 1UL
  let mut i = 1
  while i < args.length() {
    let flag = args[i]
    guard i + 1 < args.length() else { raise Wire("missing value for \{flag}") }
    let value = args[i + 1]
    match flag {
      "--id" => id = value
      "--peers" => peers = comma_fields(value[:])
      "--election-tick" => election_tick = parse_u64(value[:]).to_int()
      "--heartbeat-tick" => heartbeat_tick = parse_u64(value[:]).to_int()
      "--seed" => seed = parse_u64(value[:])
      _ => raise Wire("unknown argument \{flag}")
    }
    i += 2
  }
  guard id.length() > 0 else { raise Wire("--id is required") }
  guard peers.length() > 0 else { raise Wire("--peers is required") }
  { id, peers, election_tick, heartbeat_tick, seed, }
}

///|
/// The node as this process sees it: the driver over the consensus core, the
/// committed entries it has applied, and the last snapshot it installed. The
/// committed view is what the harness compares against the Go nodes' logs — a
/// follower that has caught up is one whose applied entries agree byte for byte.
struct Peer {
  id : String
  raw : @raft.RawNode
  committed : Array[@raft.Entry]
  mut snapshot_index : UInt64
  mut snapshot_term : UInt64
  mut snapshot_data : Bytes
}

///|
fn Peer::new(options : Options) -> Peer {
  let node = @raft.RaftNode::new(
    options.id,
    options.peers,
    seed=options.seed,
    election_timeout=options.election_tick,
    heartbeat_timeout=options.heartbeat_tick,
  )
  {
    id: options.id,
    raw: @raft.RawNode::new(node),
    committed: [],
    snapshot_index: 0UL,
    snapshot_term: 0UL,
    snapshot_data: b"",
  }
}

///|
/// Take every Ready the driver has, keep what the harness will ask about, and
/// hand back the messages that have to reach another node. A message addressed
/// to this node is stepped straight back in, the way a transport would deliver
/// it: a leader's own append acknowledgement travels that path.
fn Peer::drain(self : Peer) -> Array[@raft.Message] {
  let out : Array[@raft.Message] = []
  let queue : Array[@raft.Message] = []
  let take = fn() {
    while self.raw.has_ready() {
      let ready = self.raw.ready()
      self.raw.store(ready)
      match ready.snapshot {
        Some(s) => {
          self.snapshot_index = s.last_index
          self.snapshot_term = s.last_term
          self.snapshot_data = s.data
        }
        None => ()
      }
      for entry in ready.committed_entries {
        self.committed.push(entry)
      }
      for message in ready.messages {
        queue.push(message)
      }
      self.raw.advance(ready)
    }
  }
  take()
  let mut i = 0
  while i < queue.length() {
    let message = queue[i]
    i += 1
    if message.to == self.id {
      self.raw.step(message)
      take()
    } else {
      out.push(message)
    }
  }
  out
}

///|
async fn say(line : String) -> Unit {
  @stdio.stdout.write(@utf8.encode(line + "\n"))
}

///|
fn show_entry(entry : @raft.Entry) -> String {
  let kind = match entry.entry_type {
    @raft.EntryType::Normal => "0"
    @raft.EntryType::ConfChange => "1"
  }
  "\{kind} \{entry.term} \{entry.index} \{show_hex(entry.command)}"
}

///|
async fn main {
  let options = parse_options(@env.args()) catch {
    Wire(text) => {
      say("error \{text}")
      return
    }
  }
  let peer = Peer::new(options)
  let mut running = true
  while running {
    let line = @stdio.stdin.read_until("\n")
    let line = match line {
      None => break
      Some(text) => text.trim().to_owned()
    }
    if line.length() == 0 {
      continue
    }
    let words = fields(line[:])
    if words.length() == 0 {
      continue
    }
    try {
      match words[0] {
        "tick" => {
          let n = if words.length() > 1 {
            parse_u64(words[1][:]).to_int()
          } else {
            1
          }
          for _ in 0..<n {
            peer.raw.tick()
            for message in peer.drain() {
              say(encode_message(message))
            }
          }
        }
        "msg" => {
          let message = decode_message(words)
          peer.raw.step(message)
          for reply in peer.drain() {
            say(encode_message(reply))
          }
        }
        "propose" => {
          guard words.length() > 1 else { raise Wire("propose needs data") }
          peer.raw.propose(parse_hex(words[1][:]))
          for message in peer.drain() {
            say(encode_message(message))
          }
        }
        "status" => {
          let status = peer.raw.status()
          let role = match status.role {
            @raft.Role::Follower => "follower"
            @raft.Role::PreCandidate => "precandidate"
            @raft.Role::Candidate => "candidate"
            @raft.Role::Leader => "leader"
          }
          let leader = match status.leader {
            None => "-"
            Some(id) => id
          }
          say(
            "status \{role} \{status.term} \{status.commit} \{leader} \{status.last_index}",
          )
        }
        "log" => {
          let mut text = "log \{peer.snapshot_index} \{peer.snapshot_term} \{peer.raw.status().commit} \{show_hex(peer.snapshot_data)} \{peer.committed.length()}"
          for entry in peer.committed {
            text = text + " " + show_entry(entry)
          }
          say(text)
        }
        "quit" => {
          say("done")
          running = false
          break
        }
        other => raise Wire("unknown command \{other}")
      }
    } catch {
      Wire(text) => say("error \{text}")
      _ => say("error unknown")
    }
    if running {
      say("done")
    }
  }
}
