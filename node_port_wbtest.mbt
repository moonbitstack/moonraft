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
// These map node_test.go onto RawNode. etcd's Node is a goroutine loop over Go
// channels; RawNode is its documented goroutine-free equivalent, so the tests
// that assert protocol behaviour (rather than channel/goroutine plumbing) port
// directly. The ones that test channel blocking, context cancellation, or the
// run() goroutine are registered N/A in PORTING.md with per-function reasons.

// TestSoftStateEqual: a soft state differs from the zero value when either the
// leader or the role differs.

///|
test "TestSoftStateEqual" {
  let zero : SoftState = { lead: None, state: Follower }
  assert_true(({ lead: None, state: Follower } : SoftState).equal(zero))
  assert_true(!({ lead: Some("1"), state: Follower } : SoftState).equal(zero))
  assert_true(!({ lead: None, state: Leader } : SoftState).equal(zero))
}

// TestIsHardStateEqual: the zero hard state equals itself; any set field makes
// it differ.

///|
test "TestIsHardStateEqual" {
  let zero = HardState::initial()
  assert_true(hard_state_equal(zero, zero))
  assert_true(!hard_state_equal({ term: 0, vote: Some("1"), commit: 0 }, zero))
  assert_true(!hard_state_equal({ term: 0, vote: None, commit: 1 }, zero))
  assert_true(!hard_state_equal({ term: 1, vote: None, commit: 0 }, zero))
}

// TestNodeStep: proposals and received messages take distinct routes. In etcd a
// proposal is steered to propc and every other network message to recvc; here a
// proposal is the Propose method and a received message is Step. This confirms a
// network message stepped into a follower produces a reply, while proposals are
// a separate entry point.

///|
test "TestNodeStep routes network messages through Step" {
  let rn = RaftNode::new("1", ["2"]).raw()
  rn.step(
    Message::new(
      "2",
      "1",
      Vote({ term: 1, candidate_id: "2", last_log_index: 0, last_log_term: 0 }),
    ),
  )
  assert_true(rn.msgs.length() >= 1)
  assert_true(rn.msgs[0].is_response())
}

// TestNodePropose: a proposal on the leader is appended to the log and surfaced
// by the next Ready.

///|
test "TestNodePropose" {
  let rn = single_leader()
  rn.propose(b"somedata")
  let rd = rn.ready()
  assert_eq(rd.entries.length(), 1)
  assert_true(entry_is(rd.entries[0], 1, 2, b"somedata"))
}

// TestNodeProposeConfig: a proposed configuration change is appended as a
// ConfChange entry carrying the encoded change.

///|
test "TestNodeProposeConfig" {
  let rn = single_leader()
  rn.propose_conf(ConfChange::add("1"))
  let rd = rn.ready()
  assert_eq(rd.entries.length(), 1)
  assert_true(rd.entries[0].is_conf_change())
  assert_true(
    ConfChange::decode(rd.entries[0].command) == Some(ConfChange::add("1")),
  )
}

// TestNodeProposeAddDuplicateNode (Node flavour): the goroutine-driven duplicate
// -add scenario mapped to RawNode. Committing two adds of node 1 and then an add
// of node 2 yields four committed entries — the election no-op plus the three
// conf changes — with the conf-change payloads in order.

///|
test "TestNodeProposeAddDuplicateNode" {
  let rn = fresh_raw()
  rn.campaign()
  let committed : Array[Entry] = []
  let drive = fn(node : RawNode) {
    while node.has_ready() {
      let rd = node.ready()
      for e in rd.committed_entries {
        committed.push(e)
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
  drive(rn)
  let cc1 = ConfChange::add("1")
  rn.propose_conf(cc1)
  drive(rn)
  rn.propose_conf(cc1)
  drive(rn)
  rn.propose_conf(ConfChange::add("2"))
  drive(rn)
  // no-op, cc1, cc1, cc2
  assert_eq(committed.length(), 4)
  assert_true(ConfChange::decode(committed[1].command) == Some(cc1))
  assert_true(ConfChange::decode(committed[2].command) == Some(cc1))
  assert_true(
    ConfChange::decode(committed[3].command) == Some(ConfChange::add("2")),
  )
}

// TestBlockProposal: a proposal made before a leader is known is dropped (the
// node has nothing to append), and one made after the node becomes leader is
// appended. (etcd blocks the caller until a leader exists; without goroutines
// the observable equivalent is that the pre-leader proposal produces no entry.)

///|
test "TestBlockProposal" {
  let rn = fresh_raw()
  rn.propose(b"somedata")
  assert_eq(rn.node().node().last_log_index(), 0)
  assert_true(!rn.has_ready())
  rn.campaign()
  rn.stabilize() |> ignore
  rn.propose(b"somedata")
  let node = rn.node().node()
  assert_eq(node.last_log_index(), 2)
  assert_true(entry_is(node.entry_at(2).unwrap(), 1, 2, b"somedata"))
}

// TestNodeTick: one tick advances the underlying election clock by one.

///|
test "TestNodeTick" {
  let rn = RaftNode::new("1", ["2"]).raw()
  let before = rn.node().election_elapsed
  rn.tick()
  assert_eq(rn.node().election_elapsed, before + 1)
}

// TestNodeAdvance: after a proposal is persisted and Advance is called, the now
// -committed entry is available to apply in the next Ready.

///|
test "TestNodeAdvance" {
  let rn = single_leader()
  rn.propose(b"foo")
  let rd = rn.ready()
  assert_true(!rd.entries.is_empty())
  rn.advance(rd)
  assert_true(rn.has_ready())
  let rd2 = rn.ready()
  assert_eq(rd2.committed_entries.length(), 1)
  assert_true(entry_is(rd2.committed_entries[0], 1, 2, b"foo"))
}

// TestNodeStart: the HardState progression of a starting node — vote and first
// commit on election, commit bump on a proposal, and a final apply-only Ready
// that needs no sync. Adapted from the StartNode/faux-ConfChange bootstrap (see
// PORTING.md); the durable-state transitions are the invariant being checked.

///|
test "TestNodeStart" {
  let rn = fresh_raw()
  rn.campaign()
  let rd1 = rn.ready()
  assert_true(rd1.hard_state == Some({ term: 1, vote: Some("1"), commit: 1 }))
  assert_true(rd1.must_sync)
  rn.advance(rd1)
  rn.propose(b"foo")
  let rd2 = rn.ready()
  assert_true(rd2.hard_state == Some({ term: 1, vote: Some("1"), commit: 2 }))
  rn.advance(rd2)
  let rd3 = rn.ready()
  assert_eq(rd3.committed_entries.length(), 1)
  assert_true(entry_is(rd3.committed_entries[0], 1, 2, b"foo"))
  assert_true(!rd3.must_sync)
  rn.advance(rd3)
  assert_true(!rn.has_ready())
}

// TestNodeRestart (Node flavour): a recovered node emits its committed-but-
// unapplied prefix with no HardState change and no sync.

///|
test "TestNodeRestart" {
  let rn = build_restart([(1, b""), (1, b"foo")], 1, 1)
  let rd = rn.ready()
  assert_true(rd.hard_state == None)
  assert_eq(rd.committed_entries.length(), 1)
  assert_true(entry_is(rd.committed_entries[0], 1, 1, b""))
  assert_true(!rd.must_sync)
  rn.advance(rd)
  assert_true(!rn.has_ready())
}

// TestNodeRestartFromSnapshot (Node flavour): recovered from a snapshot at index
// 2 with one entry past it, the node emits that entry to apply, no HardState, no
// sync.

///|
test "TestNodeRestartFromSnapshot" {
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
  assert_eq(rd.committed_entries.length(), 1)
  assert_true(entry_is(rd.committed_entries[0], 1, 3, b"foo"))
  assert_true(!rd.must_sync)
  raw.advance(rd)
  assert_true(!raw.has_ready())
}
