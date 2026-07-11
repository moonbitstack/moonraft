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

// Ported from etcd-io/raft (Apache-2.0): raft_paper_test.go.
// Each test pins a property from the Raft paper (https://raft.github.io/raft.pdf).
// Our server returns messages from `step`/`tick` rather than accumulating them
// in a mailbox, so message assertions inspect the returned batch; where etcd
// checks a wire field we do not carry (e.g. a reply echoing the probe index) we
// assert the semantically equivalent state and note it.

///|
fn paper_node(peers : Array[String]) -> RaftNode {
  RaftNode::new("1", peers, pre_vote=false, election_timeout=10)
}

///|
fn empty_append(
  from : String,
  term : UInt64,
  prev_index : UInt64,
  prev_term : UInt64,
) -> Message {
  Message::new(
    from,
    "1",
    Append({
      term,
      leader_id: from,
      prev_log_index: prev_index,
      prev_log_term: prev_term,
      entries: [],
      leader_commit: 0,
    }),
  )
}

// --- §5.1: a server updates its term from a larger one and reverts to follower.

///|
test "TestFollowerUpdateTermFromMessage" {
  let r = paper_node(["2", "3"])
  r.node().become_follower(1)
  r.step(empty_append("2", 2, 0, 0)) |> ignore
  assert_eq(r.term(), 2)
  assert_true(r.role() == Follower)
}

///|
test "TestCandidateUpdateTermFromMessage" {
  let r = paper_node(["2", "3"])
  r.node().become_candidate()
  r.step(empty_append("2", 2, 0, 0)) |> ignore
  assert_eq(r.term(), 2)
  assert_true(r.role() == Follower)
}

///|
test "TestLeaderUpdateTermFromMessage" {
  let r = paper_node(["2", "3"])
  r.node().become_candidate()
  r.become_leader()
  r.step(empty_append("2", 2, 0, 0)) |> ignore
  assert_eq(r.term(), 2)
  assert_true(r.role() == Follower)
}

// --- §5.2: servers start as followers.

///|
test "TestStartAsFollower" {
  let r = paper_node(["2", "3"])
  assert_true(r.role() == Follower)
}

// --- §5.2: a non-leader that hears nothing for an election timeout campaigns,
// incrementing its term, voting for itself and requesting votes from all peers.

///|
test "testNonleaderStartElection (follower)" {
  let r = paper_node(["2", "3"])
  r.node().become_follower(1)
  let mut msgs : Array[Message] = []
  for _i in 1..<(2 * 10) {
    let out = r.tick()
    if out.length() > 0 {
      msgs = out
    }
  }
  assert_eq(r.term(), 2)
  assert_true(r.role() == Candidate)
  assert_true(r.votes["1"])
  assert_eq(msgs.length(), 2)
  for m in msgs {
    assert_true(m.kind() == "Vote")
    assert_eq(m.term(), 2)
  }
}

// --- §5.2: all outcomes of one round of RequestVote.

///|
test "TestLeaderElectionInOneRoundRPC" {
  // (size, granting voter ids, expected state) — every etcd table case.
  let cases : Array[(Int, Array[String], String)] = [
    (1, [], "leader"),
    (3, ["2", "3"], "leader"),
    (3, ["2"], "leader"),
    (5, ["2", "3", "4", "5"], "leader"),
    (5, ["2", "3", "4"], "leader"),
    (5, ["2", "3"], "leader"),
    (3, [], "candidate"),
    (5, ["2"], "candidate"),
    (5, [], "candidate"),
  ]
  for tc in cases {
    let (size, granters, want) = tc
    let peers = Array::new()
    for i in 2..<(size + 1) {
      peers.push(i.to_string())
    }
    let r = paper_node(peers)
    r.campaign() |> ignore
    for g in granters {
      r.step(Message::new(g, "1", VoteResp({ term: 1, vote_granted: true })))
      |> ignore
    }
    let got = if r.is_leader() {
      "leader"
    } else if r.role() == Candidate {
      "candidate"
    } else {
      "follower"
    }
    assert_eq(got, want)
    assert_eq(r.term(), 1)
  }
  // Denials by a majority send the candidate back to follower.
  let denials : Array[(Int, Array[String])] = [
    (3, ["2", "3"]),
    (5, ["2", "3", "4", "5"]),
  ]
  for tc in denials {
    let (size, deniers) = tc
    let peers = Array::new()
    for i in 2..<(size + 1) {
      peers.push(i.to_string())
    }
    let r = paper_node(peers)
    r.campaign() |> ignore
    for d in deniers {
      r.step(Message::new(d, "1", VoteResp({ term: 1, vote_granted: false })))
      |> ignore
    }
    assert_true(r.role() == Follower)
  }
}

// --- §5.2: a follower grants at most one vote per term, first-come-first-served.

///|
test "TestFollowerVote" {
  // (already voted for, candidate asking, expect reject)
  let cases : Array[(String, String, Bool)] = [
    ("", "2", false),
    ("", "3", false),
    ("2", "2", false),
    ("3", "3", false),
    ("2", "3", true),
    ("3", "2", true),
  ]
  for tc in cases {
    let (voted, asker, wreject) = tc
    let r = paper_node(["2", "3"])
    r.node().become_follower(1)
    if voted != "" {
      r.node().voted_for = Some(voted)
    }
    let out = r.step(
      Message::new(
        asker,
        "1",
        Vote({
          term: 1,
          candidate_id: asker,
          last_log_index: 0,
          last_log_term: 0,
        }),
      ),
    )
    assert_eq(out.length(), 1)
    match out[0].payload {
      VoteResp(reply) => assert_eq(reply.vote_granted, !wreject)
      _ => abort("expected VoteResp")
    }
  }
}

// --- §5.2: a candidate that hears a leader at >= its term becomes a follower.

///|
test "TestCandidateFallback" {
  let terms : Array[UInt64] = [1, 2]
  for at in terms {
    let r = paper_node(["2", "3"])
    r.campaign() |> ignore
    assert_true(r.role() == Candidate)
    r.step(empty_append("2", at, 0, 0)) |> ignore
    assert_true(r.role() == Follower)
    assert_eq(r.term(), at)
  }
}

// --- §5.3: a leader appends a client proposal and replicates it to all peers,
// carrying the index/term of the immediately preceding entry.

///|
test "TestLeaderStartReplication" {
  let r = paper_node(["2", "3"])
  r.node().become_candidate()
  r.become_leader()
  // etcd's commitNoopEntry: replicate and commit the leader's initial no-op so
  // progress anchors past it (prev = li) rather than re-sending it.
  for peer in ["2", "3"] {
    r.step(
      Message::new(
        peer,
        "1",
        AppendResp({
          term: 1,
          success: true,
          match_index: 1,
          conflict_index: 0,
          conflict_term: 0,
          reject_index: 0,
        }),
      ),
    )
    |> ignore
  }
  let li = r.node().last_log_index()
  let msgs = r.propose(b"some data")
  assert_eq(r.node().last_log_index(), li + 1)
  assert_eq(r.commit_index(), li)
  assert_eq(msgs.length(), 2)
  for m in msgs {
    assert_true(m.kind() == "Append")
    match m.payload {
      Append(a) => {
        assert_eq(a.prev_log_index, li)
        assert_eq(a.entries[a.entries.length() - 1].index, li + 1)
        assert_eq(a.entries[a.entries.length() - 1].command, b"some data")
      }
      _ => abort("expected Append")
    }
  }
}

// --- §5.3: an entry commits once stored on a majority, and the leader's commit
// index is carried to followers.

///|
test "TestLeaderCommitEntry" {
  let r = paper_node(["2", "3"])
  r.node().become_candidate()
  r.become_leader()
  let li = r.node().last_log_index()
  r.propose(b"some data") |> ignore
  // A majority (self + node 2) acks the new entry.
  r.step(
    Message::new(
      "2",
      "1",
      AppendResp({
        term: 1,
        success: true,
        match_index: li + 1,
        conflict_index: 0,
        conflict_term: 0,
        reject_index: 0,
      }),
    ),
  )
  |> ignore
  assert_eq(r.commit_index(), li + 1)
}

// --- §5.3: an entry is acknowledged as committed once a majority stores it.

///|
test "TestLeaderAcknowledgeCommit" {
  // (size, acceptor ids, expect commit advanced)
  let cases : Array[(Int, Array[String], Bool)] = [
    (1, [], true),
    (3, [], false),
    (3, ["2"], true),
    (3, ["2", "3"], true),
    (5, [], false),
    (5, ["2"], false),
    (5, ["2", "3"], true),
    (5, ["2", "3", "4"], true),
    (5, ["2", "3", "4", "5"], true),
  ]
  for tc in cases {
    let (size, acceptors, wack) = tc
    let peers = Array::new()
    for i in 2..<(size + 1) {
      peers.push(i.to_string())
    }
    let r = paper_node(peers)
    r.node().become_candidate()
    r.become_leader()
    let li = r.node().last_log_index()
    r.propose(b"some data") |> ignore
    for a in acceptors {
      r.step(
        Message::new(
          a,
          "1",
          AppendResp({
            term: 1,
            success: true,
            match_index: li + 1,
            conflict_index: 0,
            conflict_term: 0,
            reject_index: 0,
          }),
        ),
      )
      |> ignore
    }
    assert_eq(r.commit_index() > li, wack)
  }
}

// --- §5.3: a follower stores committed entries in log order.

///|
test "TestFollowerCommitEntry" {
  let r = paper_node(["2", "3"])
  r.node().become_follower(1)
  let entries = [Entry::normal(1, 1, b"a"), Entry::normal(1, 2, b"b")]
  r.step(
    Message::new(
      "2",
      "1",
      Append({
        term: 1,
        leader_id: "2",
        prev_log_index: 0,
        prev_log_term: 0,
        entries,
        leader_commit: 2,
      }),
    ),
  )
  |> ignore
  assert_eq(r.commit_index(), 2)
  assert_eq(r.node().last_log_index(), 2)
}

// --- §5.3: a follower rejects an AppendEntries whose (prevIndex, prevTerm) does
// not match its log, and hints where the logs diverge (findConflictByTerm).

///|
test "TestFollowerCheckMsgApp" {
  let build = fn() -> RaftNode {
    let r = paper_node(["2", "3"])
    let _ = r.node().append(1, b"")
    let _ = r.node().append(2, b"")
    r.node().advance_commit(1)
    r.node().become_follower(2)
    r
  }
  // (prev_term, prev_index, wreject, wrejectHint, wlogterm)
  let cases : Array[(UInt64, UInt64, Bool, UInt64, UInt64)] = [
    (0, 0, false, 0, 0),
    (1, 1, false, 0, 0),
    (2, 2, false, 0, 0),
    (1, 2, true, 1, 1),
    (3, 3, true, 2, 2),
  ]
  for tc in cases {
    let (pterm, pindex, wreject, whint, wlogterm) = tc
    let r = build()
    let out = r.step(empty_append("2", 2, pindex, pterm))
    assert_eq(out.length(), 1)
    match out[0].payload {
      AppendResp(reply) => {
        assert_eq(reply.success, !wreject)
        if wreject {
          assert_eq(reply.conflict_index, whint)
          assert_eq(reply.conflict_term, wlogterm)
        }
      }
      _ => abort("expected AppendResp")
    }
  }
}

// --- §5.3: a valid AppendEntries deletes conflicting entries and appends new ones.

///|
test "TestFollowerAppendEntries" {
  // (prev_index, prev_term, new-entry terms, expected full-log terms)
  let cases : Array[(UInt64, UInt64, Array[UInt64], Array[UInt64])] = [
    (2, 2, [3], [1, 2, 3]),
    (1, 1, [3, 4], [1, 3, 4]),
    (0, 0, [1], [1, 2]),
    (0, 0, [3], [3]),
  ]
  for tc in cases {
    let (pindex, pterm, new_terms, want_terms) = tc
    let r = paper_node(["2", "3"])
    let _ = r.node().append(1, b"")
    let _ = r.node().append(2, b"")
    r.node().become_follower(2)
    let entries = Array::new()
    let mut idx = pindex
    for t in new_terms {
      idx = idx + 1
      entries.push(Entry::normal(t, idx, b""))
    }
    r.step(
      Message::new(
        "2",
        "1",
        Append({
          term: 2,
          leader_id: "2",
          prev_log_index: pindex,
          prev_log_term: pterm,
          entries,
          leader_commit: 0,
        }),
      ),
    )
    |> ignore
    assert_eq(r.node().last_log_index(), want_terms.length().to_uint64())
    for i in 0..<want_terms.length() {
      assert_eq(r.node().term_at((i + 1).to_uint64()), want_terms[i])
    }
  }
}

// --- §5.4.1: a vote request carries the candidate's last log index and term.

///|
test "TestVoteRequest" {
  // (given entries' terms, expected campaign term)
  let cases : Array[(Array[UInt64], UInt64)] = [([1], 2), ([1, 2], 3)]
  for tc in cases {
    let (ent_terms, wterm) = tc
    let r = paper_node(["2", "3"])
    let entries = Array::new()
    let mut i = 0
    for t in ent_terms {
      i = i + 1
      entries.push(Entry::normal(t, i.to_uint64(), b""))
    }
    r.step(
      Message::new(
        "2",
        "1",
        Append({
          term: wterm - 1,
          leader_id: "2",
          prev_log_index: 0,
          prev_log_term: 0,
          entries,
          leader_commit: 0,
        }),
      ),
    )
    |> ignore
    let mut msgs : Array[Message] = []
    for _t in 1..<(10 * 2) {
      let out = r.tick()
      if out.length() > 0 {
        msgs = out
      }
    }
    assert_eq(msgs.length(), 2)
    let last_i = ent_terms.length().to_uint64()
    let last_t = ent_terms[ent_terms.length() - 1]
    for m in msgs {
      assert_true(m.kind() == "Vote")
      assert_eq(m.term(), wterm)
      match m.payload {
        Vote(a) => {
          assert_eq(a.last_log_index, last_i)
          assert_eq(a.last_log_term, last_t)
        }
        _ => abort("expected Vote")
      }
    }
  }
}

// --- §5.4.1: a voter denies its vote if its own log is more up-to-date.

///|
test "TestVoter" {
  // (own entry terms, candidate logterm, candidate index, expect reject)
  let cases : Array[(Array[UInt64], UInt64, UInt64, Bool)] = [
    ([1], 1, 1, false),
    ([1], 1, 2, false),
    ([1, 1], 1, 1, true),
    ([1], 2, 1, false),
    ([1], 2, 2, false),
    ([1, 1], 2, 1, false),
    ([2], 1, 1, true),
    ([2], 1, 2, true),
    ([2, 2], 1, 1, true),
  ]
  for tc in cases {
    let (own_terms, clogterm, cindex, wreject) = tc
    let r = paper_node(["2"])
    let mut i = 0
    for t in own_terms {
      i = i + 1
      let _ = r.node().append(t, b"")
    }
    let out = r.step(
      Message::new(
        "2",
        "1",
        Vote({
          term: 3,
          candidate_id: "2",
          last_log_index: cindex,
          last_log_term: clogterm,
        }),
      ),
    )
    assert_eq(out.length(), 1)
    match out[0].payload {
      VoteResp(reply) => assert_eq(!reply.vote_granted, wreject)
      _ => abort("expected VoteResp")
    }
  }
}

// --- §5.4.2 (Figure 8): a leader only commits an entry from its current term by
// counting replicas; earlier-term entries commit only on the back of one.

///|
test "TestLeaderOnlyCommitsLogFromCurrentTerm" {
  // (ack index, expected commit)
  let cases : Array[(UInt64, UInt64)] = [(1, 0), (2, 0), (3, 3)]
  for tc in cases {
    let (ack_index, wcommit) = tc
    let r = paper_node(["2"])
    // Two entries carried over from earlier terms (1 and 2).
    let _ = r.node().append(1, b"")
    let _ = r.node().append(2, b"")
    r.node().become_follower(2)
    // Become leader at term 3, which appends a no-op at index 3.
    r.node().become_candidate()
    r.become_leader()
    // A follower acks up through `ack_index`.
    r.step(
      Message::new(
        "2",
        "1",
        AppendResp({
          term: r.term(),
          success: true,
          match_index: ack_index,
          conflict_index: 0,
          conflict_term: 0,
          reject_index: 0,
        }),
      ),
    )
    |> ignore
    assert_eq(r.commit_index(), wcommit)
  }
}

// --- §5.3 / Figure 7: the leader brings every follower's log into agreement
// with its own, overwriting divergent suffixes, using the conflict backoff.

///|
fn node_from_terms(id : String, terms : Array[UInt64]) -> Node {
  // terms[0] is the index-0 dummy (term 0); real entries start at index 1.
  let n = Node::new(id)
  let mut i = 1
  while i < terms.length() {
    let _ = n.append(terms[i], b"")
    i = i + 1
  }
  n
}

///|
fn sync_follower(leader : Node, follower : Node) -> Unit {
  // Probe from the leader's tail and back off on rejection via the two-sided
  // findConflictByTerm, exactly as the leader does on an MsgAppResp reject.
  let mut next = leader.last_log_index() + 1
  let mut guard_iters = 0
  while guard_iters < 1000 {
    guard_iters = guard_iters + 1
    let prev = next - 1
    let reply = follower.handle_append_entries({
      term: leader.current_term(),
      leader_id: leader.id,
      prev_log_index: prev,
      prev_log_term: leader.term_at(prev),
      entries: leader.entries_after(prev),
      leader_commit: leader.commit_index,
    })
    if reply.success {
      return
    }
    let np = if reply.conflict_term > 0 {
      let (idx, _) = leader.find_conflict_by_term(
        reply.conflict_index,
        reply.conflict_term,
      )
      idx
    } else {
      reply.conflict_index
    }
    next = if np > 0 { np } else { 1 }
  }
}

///|
test "TestLeaderSyncFollowerLog (Figure 7)" {
  let lead_terms : Array[UInt64] = [0, 1, 1, 1, 4, 4, 5, 5, 6, 6, 6]
  let cases : Array[Array[UInt64]] = [
    [0, 1, 1, 1, 4, 4, 5, 5, 6, 6],
    [0, 1, 1, 1, 4, 4],
    [0, 1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6],
    [0, 1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7],
    [0, 1, 1, 1, 4, 4, 4, 4],
    [0, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3],
  ]
  for follower_terms in cases {
    let leader = node_from_terms("1", lead_terms)
    // Leader is at term 8 and appends a fresh entry, as after winning election.
    leader.become_follower(8)
    leader.become_candidate() // term 9
    leader.become_leader()
    let _ = leader.append(leader.current_term(), b"")
    let follower = node_from_terms("2", follower_terms)
    follower.become_follower(7)
    sync_follower(leader, follower)
    // Log Matching: the follower's log now equals the leader's, index for index.
    assert_eq(follower.last_log_index(), leader.last_log_index())
    let mut i = 1UL
    while i <= leader.last_log_index() {
      assert_eq(follower.term_at(i), leader.term_at(i))
      i = i + 1
    }
  }
}
