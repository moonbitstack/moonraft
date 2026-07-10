// White-box tests for the Web Worker driver. They exercise the flat wasm-facing
// API against the one global `wctx` the browser build owns (one worker = one
// wasm instance = one node), and drive the flat-int message codec directly so
// every `Payload` variant's encode/decode arm is hit.

///|
/// Encode then decode a message, returning the payload the wire preserved.
fn rt(m : Message) -> Payload {
  match dec_msg(enc_msg(m)) {
    Some(d) => d.payload
    None => abort("codec dropped a message it should carry")
  }
}

///|
/// A node in the demo is identified as "S<n>"; the codec keys everything off the
/// integer after the 'S', so all wire actors here use that form.
test "the flat API is inert until a node exists" {
  wctx.raw = None
  wctx.out.clear()
  wctx.inbuf.clear()
  // None of these may touch a node that isn't there.
  wnode_tick()
  wnode_campaign()
  wnode_propose(1)
  wnode_recv_reset()
  wnode_recv_push(7)
  wnode_recv_apply()
  inspect(wnode_state(), content="{\"ready\":false}")
  inspect(wnode_drain(), content="[]")
}

///|
test "wnode_new clamps node count and self index" {
  // n < 1 collapses to a single node; self_idx past the end clamps to the last
  // (here to S1, the only node).
  wnode_new(0, 5, 1)
  inspect(state_id(), content="1")
  // n > 9 caps at 9; self_idx 100 clamps to the last, S9.
  wnode_new(50, 100, 1)
  inspect(state_id(), content="9")
  // Negative self_idx clamps to the first node, S1.
  wnode_new(3, -1, 1)
  inspect(state_id(), content="1")
  // An in-range self_idx is honoured verbatim: S2.
  wnode_new(3, 1, 1)
  inspect(state_id(), content="2")
}

///|
/// The node id the driver assigned itself, read back through its own status so
/// the clamping in `wnode_new` is observed rather than recomputed.
fn state_id() -> Int {
  match wctx.raw {
    Some(raw) => id_num(raw.status().id)
    None => -1
  }
}

///|
test "a lone node self-elects, and its leader state JSON is complete" {
  wnode_new(1, 0, 1)
  wnode_campaign()
  // A solo node reaches quorum with its own vote: leader of term 1, having
  // appended the no-op entry at index 1.
  inspect(
    wnode_state(),
    content=(
      #|{"ready":true,"id":1,"role":"Leader","term":1,"leader":1,"commit":1,"lastIndex":1,"applied":1,"votedFor":1,"electionElapsed":0,"randTimeout":16,"log":[{"i":1,"t":1,"c":false}]}
    ),
  )
  // A second entry exercises the multi-entry log loop; it commits immediately.
  wnode_propose(42)
  inspect(
    wnode_state(),
    content=(
      #|{"ready":true,"id":1,"role":"Leader","term":1,"leader":1,"commit":2,"lastIndex":2,"applied":2,"votedFor":1,"electionElapsed":0,"randTimeout":16,"log":[{"i":1,"t":1,"c":false},{"i":2,"t":1,"c":false}]}
    ),
  )
}

///|
test "ticking a node past its election timeout drives an election" {
  wnode_new(1, 0, 1)
  // A lone node's clock advances on tick; once the randomized election timeout
  // fires it campaigns and, reaching quorum alone, becomes leader.
  for _ in 0..<30 {
    wnode_tick()
  }
  match wctx.raw {
    Some(raw) => assert_true(raw.status().role == Leader)
    None => abort("node vanished")
  }
}

///|
test "a fresh follower reports no leader and no vote" {
  wnode_new(3, 1, 7)
  inspect(
    wnode_state(),
    content="{\"ready\":true,\"id\":2,\"role\":\"Follower\",\"term\":0,\"leader\":-1,\"commit\":0,\"lastIndex\":0,\"applied\":0,\"votedFor\":-1,\"electionElapsed\":0,\"randTimeout\":18,\"log\":[]}",
  )
}

///|
test "a campaigning node becomes a candidate and drains its vote requests" {
  wnode_new(3, 0, 3)
  wnode_campaign()
  // Pre-vote is off in the demo, so the node jumps straight to Candidate, votes
  // for itself, and has no believed leader.
  inspect(
    wnode_state(),
    content=(
      #|{"ready":true,"id":1,"role":"Candidate","term":1,"leader":-1,"commit":0,"lastIndex":0,"applied":0,"votedFor":1,"electionElapsed":0,"randTimeout":13,"log":[]}
    ),
  )
  // Two peers, so two Vote messages, each a six-int array: exercises both the
  // between-message and between-int comma paths of wnode_drain.
  inspect(wnode_drain(), content="[[3,1,2,1,0,0],[3,1,3,1,0,0]]")
  // Draining consumes the queue.
  inspect(wnode_drain(), content="[]")
}

///|
test "an inbound append is decoded, stored, and shows in the log" {
  wnode_new(3, 1, 5) // S2, peers S1 and S3
  // A leader S1 replicates two entries into the empty follower: a normal command
  // and a configuration change, driving the codec's entry loop, its
  // length-prefixed byte runs, and the is_conf_change split.
  let append = Message::new(
    "S1",
    "S2",
    Append({
      term: 1,
      leader_id: "S1",
      prev_log_index: 0,
      prev_log_term: 0,
      entries: [Entry::normal(1, 1, b"hi"), Entry::conf(1, 2, b"")],
      leader_commit: 1,
    }),
  )
  feed(enc_msg(append))
  inspect(
    wnode_state(),
    content=(
      #|{"ready":true,"id":2,"role":"Follower","term":1,"leader":1,"commit":1,"lastIndex":2,"applied":1,"votedFor":-1,"electionElapsed":0,"randTimeout":13,"log":[{"i":1,"t":1,"c":false},{"i":2,"t":1,"c":true}]}
    ),
  )
  // The follower's AppendResp is queued for the router.
  inspect(wnode_drain(), content="[[6,2,1,1,1,2,0,0]]")
}

///|
/// Push an encoded message through the receive buffer and apply it, the way the
/// router feeds one integer at a time across the wasm boundary.
fn feed(arr : Array[Int]) -> Unit {
  wnode_recv_reset()
  for v in arr {
    wnode_recv_push(v)
  }
  wnode_recv_apply()
}

// ---- codec round-trips ------------------------------------------------------

///|
test "codec round-trips a PreVote request and reply" {
  let a : RequestVoteArgs = {
    term: 5,
    candidate_id: "S1",
    last_log_index: 3,
    last_log_term: 2,
  }
  match rt(Message::new("S1", "S2", PreVote(a))) {
    PreVote(got) => assert_true(got == a)
    _ => abort("expected PreVote")
  }
  let r : RequestVoteReply = { term: 5, vote_granted: true }
  match rt(Message::new("S1", "S2", PreVoteResp(r))) {
    PreVoteResp(got) => assert_true(got == r)
    _ => abort("expected PreVoteResp")
  }
  // A denied pre-vote drives the other branch of the reply's boolean encoding.
  let denied : RequestVoteReply = { term: 5, vote_granted: false }
  match rt(Message::new("S2", "S1", PreVoteResp(denied))) {
    PreVoteResp(got) => assert_true(got == denied)
    _ => abort("expected PreVoteResp")
  }
}

///|
test "codec round-trips a Vote request and reply" {
  let a : RequestVoteArgs = {
    term: 4,
    candidate_id: "S3",
    last_log_index: 9,
    last_log_term: 4,
  }
  match rt(Message::new("S3", "S1", Vote(a))) {
    Vote(got) => assert_true(got == a)
    _ => abort("expected Vote")
  }
  // vote_granted false drives the other branch of the boolean encoding.
  let r : RequestVoteReply = { term: 4, vote_granted: false }
  match rt(Message::new("S1", "S3", VoteResp(r))) {
    VoteResp(got) => assert_true(got == r)
    _ => abort("expected VoteResp")
  }
}

///|
test "codec round-trips an Append with mixed entries and an empty one" {
  let a : AppendEntriesArgs = {
    term: 2,
    leader_id: "S1",
    prev_log_index: 1,
    prev_log_term: 1,
    entries: [Entry::normal(2, 2, b"cmd"), Entry::conf(2, 3, b"\x01\x02")],
    leader_commit: 1,
  }
  match rt(Message::new("S1", "S2", Append(a))) {
    Append(got) => assert_true(got == a)
    _ => abort("expected Append")
  }
  // An entry-less Append is the heartbeat-shaped replication message.
  let empty : AppendEntriesArgs = {
    term: 2,
    leader_id: "S1",
    prev_log_index: 5,
    prev_log_term: 2,
    entries: [],
    leader_commit: 4,
  }
  match rt(Message::new("S1", "S2", Append(empty))) {
    Append(got) => assert_true(got == empty)
    _ => abort("expected empty Append")
  }
}

///|
test "codec round-trips an AppendResp in both success states" {
  let ok : AppendEntriesReply = {
    term: 2,
    success: true,
    match_index: 7,
    conflict_index: 0,
    conflict_term: 0,
  }
  match rt(Message::new("S2", "S1", AppendResp(ok))) {
    AppendResp(got) => assert_true(got == ok)
    _ => abort("expected AppendResp")
  }
  let rej : AppendEntriesReply = {
    term: 3,
    success: false,
    match_index: 0,
    conflict_index: 4,
    conflict_term: 2,
  }
  match rt(Message::new("S2", "S1", AppendResp(rej))) {
    AppendResp(got) => assert_true(got == rej)
    _ => abort("expected AppendResp")
  }
}

///|
test "codec round-trips a Heartbeat, its reply, and TimeoutNow" {
  // The wire carries no read-index context, so both heartbeat forms round-trip
  // only with an empty context (see the behavioural note in the report).
  let hb : HeartbeatArgs = { term: 6, leader_id: "S1", commit: 3, context: b"" }
  match rt(Message::new("S1", "S2", Heartbeat(hb))) {
    Heartbeat(got) => assert_true(got == hb)
    _ => abort("expected Heartbeat")
  }
  let hr : HeartbeatReply = { term: 6, context: b"" }
  match rt(Message::new("S2", "S1", HeartbeatResp(hr))) {
    HeartbeatResp(got) => assert_true(got == hr)
    _ => abort("expected HeartbeatResp")
  }
  match rt(Message::new("S1", "S2", TimeoutNow(6))) {
    TimeoutNow(got) => assert_true(got == 6)
    _ => abort("expected TimeoutNow")
  }
}

///|
test "codec drops the payloads the demo never puts on the wire" {
  let dropped : Array[Payload] = [
    Snapshot(InstallSnapshotArgs::whole(1, "S1", Snapshot::empty())),
    SnapshotResp({ term: 1 }),
    Propose([Entry::normal(1, 1, b"x")]),
    ReadIndex(b"rctx"),
    ReadIndexResp({ term: 1, index: 1, context: b"" }),
    TransferLeader("S2"),
    ForgetLeader,
  ]
  for p in dropped {
    // A dropped payload encodes to nothing and never reaches a decoder.
    assert_eq(enc_msg(Message::new("S1", "S2", p)).length(), 0)
  }
}

///|
test "the decoder rejects malformed input" {
  // A buffer too short to hold even a header.
  match dec_msg([1, 2]) {
    None => ()
    Some(_) => abort("short buffer must not decode")
  }
  // A kind tag the codec does not know.
  match dec_msg([99, 1, 2, 3]) {
    None => ()
    Some(_) => abort("unknown kind must not decode")
  }
}

///|
test "id_num and wrole cover their edge cases" {
  // The digits after the 'S', with malformed ids folding to 0.
  inspect(id_num("S1"), content="1")
  inspect(id_num("S12"), content="12")
  inspect(id_num("x9y"), content="9")
  inspect(id_num("S"), content="0")
  inspect(id_num(""), content="0")
  // A character below '0' takes the short-circuit's low side before the upper
  // digit bound is even tested.
  inspect(id_num("!7"), content="7")
  // Every role name, including PreCandidate which the demo's off-by-default
  // pre-vote path never produces but the mapping must still handle.
  inspect(wrole(Follower), content="Follower")
  inspect(wrole(PreCandidate), content="PreCandidate")
  inspect(wrole(Candidate), content="Candidate")
  inspect(wrole(Leader), content="Leader")
}

///|
/// FIFO delivery through the flat-int codec: every message is encoded exactly as
/// the browser router would serialise it, then decoded before the recipient
/// steps on it. A cluster that elects a leader and commits a proposal this way
/// proves the codec is faithful on real traffic.
test "a three-node cluster elects and commits through the wire codec" {
  let nodes : Map[String, RaftNode] = {
    "S1": RaftNode::new("S1", ["S2", "S3"], seed=1),
    "S2": RaftNode::new("S2", ["S1", "S3"], seed=2),
    "S3": RaftNode::new("S3", ["S1", "S2"], seed=3),
  }
  let leader = nodes.get("S1").unwrap()
  route(nodes, leader.campaign())
  assert_true(leader.is_leader())
  route(nodes, leader.propose(b"set x = 1"))
  // The no-op at index 1 and the proposal at index 2 are committed everywhere.
  assert_eq(leader.commit_index(), 2)
  for id in ["S1", "S2", "S3"] {
    let n = nodes.get(id).unwrap()
    assert_eq(n.node().last_log_index(), 2)
    assert_eq(n.node().term_at(2), 1)
  }
}

///|
fn route(nodes : Map[String, RaftNode], initial : Array[Message]) -> Unit {
  let q = initial
  let mut i = 0
  while i < q.length() {
    let m = q[i]
    i = i + 1
    let wire = enc_msg(m)
    if wire.is_empty() {
      continue
    }
    match dec_msg(wire) {
      Some(decoded) =>
        match nodes.get(decoded.to) {
          Some(n) =>
            for r in n.step(decoded) {
              q.push(r)
            }
          None => ()
        }
      None => ()
    }
  }
}
