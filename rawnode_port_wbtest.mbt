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

///|
/// Whether an entry has the given term, index, and command. Entry does not
/// derive Show, so tests compare fields rather than the whole value.
fn entry_is(e : Entry, term : UInt64, index : UInt64, cmd : Bytes) -> Bool {
  e.term == term && e.index == index && e.command == cmd
}

///|
/// A fresh single-voter server viewed through the Ready/Advance contract.
fn fresh_raw() -> RawNode {
  RaftNode::new("1", []).raw()
}

///|
/// A single-voter server driven to leadership and quiesced, so its no-op is
/// appended, committed, and applied — the usual precondition for proposing.
fn single_leader() -> RawNode {
  let rn = fresh_raw()
  rn.campaign()
  rn.stabilize() |> ignore
  rn
}

///|
/// Rebuild a server from a recovered log and hard state, as on restart. `log`
/// gives each entry's (term, command); indices run sequentially from 1.
fn build_restart(
  log : Array[(UInt64, Bytes)],
  term : UInt64,
  commit : UInt64,
) -> RawNode {
  let rn = RaftNode::new("1", [])
  let node = rn.node()
  for e in log {
    let (t, c) = e
    node.append(t, c) |> ignore
  }
  node.apply_hard_state({ term, vote: None, commit })
  rn.raw()
}

// TestRawNodeStep: RawNode.Step handles every network message. In etcd this
// test also asserts that *local* message types (MsgHup/MsgBeat/MsgProp/...) are
// rejected. Here that rejection is structural: local operations are distinct
// methods (campaign/tick/propose/read_index), and the `Payload` enum admits
// only the nine network messages — a local message cannot even be constructed
// as a `Message`, a stronger guarantee than a runtime check. This test walks all
// nine network payloads through Step and confirms each is accepted.

///|
test "TestRawNodeStep every network payload routes through Step" {
  let args : RequestVoteArgs = {
    term: 1,
    candidate_id: "2",
    last_log_index: 0,
    last_log_term: 0,
  }
  let vreply : RequestVoteReply = { term: 1, vote_granted: false }
  let aargs : AppendEntriesArgs = {
    term: 1,
    leader_id: "2",
    prev_log_index: 0,
    prev_log_term: 0,
    entries: [],
    leader_commit: 0,
  }
  let areply : AppendEntriesReply = {
    term: 1,
    success: false,
    match_index: 0,
    conflict_index: 0,
    conflict_term: 0,
  }
  let snap : Snapshot = {
    last_index: 1,
    last_term: 1,
    data: b"",
    conf_state: ConfState::empty(),
  }
  let sargs = InstallSnapshotArgs::whole(1, "2", snap)
  let sreply : InstallSnapshotReply = { term: 1 }
  let payloads : Array[Payload] = [
    PreVote(args),
    PreVoteResp(vreply),
    Vote(args),
    VoteResp(vreply),
    Append(aargs),
    AppendResp(areply),
    Snapshot(sargs),
    SnapshotResp(sreply),
    TimeoutNow(1),
  ]
  for p in payloads {
    let rn = RaftNode::new("1", ["2"]).raw()
    // Step must accept every network payload without panicking.
    rn.step(Message::new("2", "1", p))
  }
  assert_eq(payloads.length(), 9)
}

// TestRawNodeProposeAndConfChange (V1 case): propose a command and a simple
// AddNode change, drive until it commits, and check the resulting voter set and
// that the log holds the command followed by the conf change. The joint /
// ConfChangeV2 / learner cases are registered N/A in PORTING.md — they depend on
// membership.mbt / confchange.mbt machinery owned by another agent.

///|
test "TestRawNodeProposeAndConfChange add a voter" {
  let rn = fresh_raw()
  rn.campaign()
  let mut proposed = false
  let mut voters : Array[String]? = None
  while voters is None {
    let rd = rn.ready()
    for e in rd.committed_entries {
      if e.is_conf_change() {
        match ConfChange::decode(e.command) {
          Some(cc) => voters = Some(rn.apply_conf_change(cc))
          None => ()
        }
      }
    }
    rn.advance(rd)
    if !proposed && rn.node().is_leader() {
      rn.propose(b"somedata")
      rn.propose_conf(ConfChange::add("2"))
      proposed = true
    }
  }
  assert_true(voters == Some(["1", "2"]))
  let node = rn.node().node()
  // index 1 is the election no-op; 2 is the command; 3 is the conf change.
  assert_true(entry_is(node.entry_at(2).unwrap(), 1, 2, b"somedata"))
  let cc = node.entry_at(3).unwrap()
  assert_true(cc.is_conf_change())
  assert_true(ConfChange::decode(cc.command) == Some(ConfChange::add("2")))
}

// TestRawNodeProposeAddDuplicateNode: two proposals to add the same node must
// not stop a later proposal to add a different node — all three conf changes
// are appended to the log in order.

///|
test "TestRawNodeProposeAddDuplicateNode" {
  let rn = single_leader()
  let apply_committed = fn(node : RawNode) {
    while node.has_ready() {
      let rd = node.ready()
      for e in rd.committed_entries {
        if e.is_conf_change() {
          match ConfChange::decode(e.command) {
            Some(cc) => node.apply_conf_change(cc) |> ignore
            None => ()
          }
        }
      }
      node.advance(rd)
    }
  }
  let cc1 = ConfChange::add("1")
  rn.propose_conf(cc1)
  apply_committed(rn)
  // add the same node again
  rn.propose_conf(cc1)
  apply_committed(rn)
  let cc2 = ConfChange::add("2")
  rn.propose_conf(cc2)
  apply_committed(rn)
  let node = rn.node().node()
  let last = node.last_log_index()
  // the last three entries are the conf changes cc1, cc1, cc2.
  assert_true(
    ConfChange::decode(node.entry_at(last - 2).unwrap().command) == Some(cc1),
  )
  assert_true(
    ConfChange::decode(node.entry_at(last - 1).unwrap().command) == Some(cc1),
  )
  assert_true(
    ConfChange::decode(node.entry_at(last).unwrap().command) == Some(cc2),
  )
}

// TestRawNodeReadIndex: a recorded ReadState is surfaced by Ready and reset
// after it is accepted; and ReadIndex on a leader records a ReadState carrying
// the caller's context at the leader's commit index.

///|
test "TestRawNodeReadIndex surfaces and resets read states" {
  let rn = fresh_raw()
  rn.read_states.push({ index: 1, request_ctx: b"somedata" })
  assert_true(rn.has_ready())
  let rd = rn.ready()
  assert_eq(rd.read_states.length(), 1)
  assert_eq(rd.read_states[0].index, 1)
  assert_eq(rd.read_states[0].request_ctx, b"somedata")
  rn.advance(rd)
  // read states are reset once accepted.
  assert_eq(rn.read_states.length(), 0)
}

///|
test "TestRawNodeReadIndex records a read on the leader" {
  let rn = single_leader()
  assert_true(rn.node().is_leader())
  rn.read_index(b"somedata2")
  let rd = rn.ready()
  assert_eq(rd.read_states.length(), 1)
  assert_eq(rd.read_states[0].request_ctx, b"somedata2")
  // the read index is the leader's commit index (the committed no-op).
  assert_eq(rd.read_states[0].index, rn.node().commit_index())
}

// TestRawNodeStart: the Ready lifecycle of a fresh single node. Adapted from the
// snapshot-bootstrapped etcd test (which needs MemoryStorage FirstIndex >= 2);
// the invariants are the same — an entry is emitted for persistence before it is
// emitted for application, MustSync is set only while there is a durable write,
// and HasReady falls to false once everything is applied. Indices differ by one
// (no snapshot at index 1).

///|
test "TestRawNodeStart" {
  let rn = fresh_raw()
  assert_true(!rn.has_ready())

  // Campaign: persist the vote and the election no-op.
  rn.campaign()
  let rd1 = rn.ready()
  assert_true(rd1.soft_state == Some({ lead: Some("1"), state: Leader }))
  assert_true(rd1.hard_state == Some({ term: 1, vote: Some("1"), commit: 1 }))
  assert_true(entry_is(rd1.entries[0], 1, 1, b""))
  assert_true(rd1.must_sync)
  rn.advance(rd1)

  // Propose a command; it must be appended (persisted) before it is applied.
  rn.propose(b"foo")
  assert_true(rn.has_ready())
  let rd2 = rn.ready()
  assert_eq(rd2.entries.length(), 1)
  assert_true(entry_is(rd2.entries[0], 1, 2, b"foo"))
  assert_true(rd2.must_sync)
  rn.advance(rd2)

  // Now the command comes back to be applied; nothing left to persist.
  assert_true(rn.has_ready())
  let rd3 = rn.ready()
  assert_true(rd3.entries.is_empty())
  assert_true(!rd3.must_sync)
  assert_eq(rd3.committed_entries.length(), 1)
  assert_true(entry_is(rd3.committed_entries[0], 1, 2, b"foo"))
  rn.advance(rd3)
  assert_true(!rn.has_ready())
}

// TestRawNodeRestart: a recovered node with a committed prefix emits only the
// committed-but-unapplied entries, no HardState (nothing changed), and does not
// require a sync.

///|
test "TestRawNodeRestart" {
  let rn = build_restart([(1, b""), (1, b"foo")], 1, 1)
  let rd = rn.ready()
  assert_true(rd.hard_state == None)
  assert_true(rd.soft_state == None)
  assert_true(rd.entries.is_empty())
  assert_eq(rd.committed_entries.length(), 1)
  assert_true(entry_is(rd.committed_entries[0], 1, 1, b""))
  assert_true(!rd.must_sync)
  rn.advance(rd)
  assert_true(!rn.has_ready())
}

// TestRawNodeRestartFromSnapshot: a node recovered from a snapshot at index 2
// with one committed entry past it emits that entry to apply, no HardState, no
// sync.

///|
test "TestRawNodeRestartFromSnapshot" {
  let rn = RaftNode::new("1", [])
  let node = rn.node()
  node.install_snapshot({
    last_index: 2,
    last_term: 1,
    data: b"",
    conf_state: ConfState::empty(),
  })
  node.append(1, b"foo") |> ignore
  node.apply_hard_state({ term: 1, vote: None, commit: 3 })
  let raw = rn.raw()
  let rd = raw.ready()
  assert_true(rd.hard_state == None)
  assert_true(rd.entries.is_empty())
  assert_eq(rd.committed_entries.length(), 1)
  assert_true(entry_is(rd.committed_entries[0], 1, 3, b"foo"))
  assert_true(!rd.must_sync)
  raw.advance(rd)
  assert_true(!raw.has_ready())
}

// TestRawNodeStatus: before campaigning the node is a follower with no leader;
// after it wins and the Ready is drained it reports itself leader at term 1.
// (etcd additionally asserts on the Progress map and the tracker.Config; those
// live behind membership.mbt and are registered N/A in PORTING.md.)

///|
test "TestRawNodeStatus" {
  let rn = fresh_raw()
  let s0 = rn.status()
  assert_true(s0.role == Follower)
  assert_true(s0.leader == None)
  rn.campaign()
  rn.stabilize() |> ignore
  let s1 = rn.status()
  assert_true(s1.leader == Some("1"))
  assert_true(s1.role == Leader)
  assert_eq(s1.term, 1)
}

// TestRawNodeConsumeReady: readyWithoutAccept must not drain the message buffer,
// but Ready must; and Advance must not drop a message enqueued after Ready.

///|
test "TestRawNodeConsumeReady" {
  let rn = fresh_raw()
  let m1 = Message::new("foo", "1", TimeoutNow(0))
  let m2 = Message::new("bar", "1", TimeoutNow(0))
  rn.msgs.push(m1)
  let rd0 = rn.ready_without_accept()
  assert_eq(rd0.messages.length(), 1)
  assert_eq(rd0.messages[0].from, "foo")
  // still present in the buffer — readyWithoutAccept did not consume it.
  assert_eq(rn.msgs.length(), 1)
  assert_eq(rn.msgs[0].from, "foo")
  let rd = rn.ready()
  assert_eq(rn.msgs.length(), 0)
  assert_eq(rd.messages.length(), 1)
  assert_eq(rd.messages[0].from, "foo")
  // a message enqueued after Ready survives Advance.
  rn.msgs.push(m2)
  rn.advance(rd)
  assert_eq(rn.msgs.length(), 1)
  assert_eq(rn.msgs[0].from, "bar")
}

// End-to-end proof that the non-test driver drives a real RawNode: three
// proposals on a single-node group are all committed and applied in order.

///|
test "run_single_node applies every proposal in order" {
  let applied = run_single_node("1", [b"a", b"b", b"c"])
  assert_eq(applied.length(), 3)
  assert_eq(applied[0], b"a")
  assert_eq(applied[1], b"b")
  assert_eq(applied[2], b"c")
}
