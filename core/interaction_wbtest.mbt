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
// Behavioural ports of etcd-io/raft interaction_test.go (testdata/*.txt). The
// upstream corpus is a `datadriven` string-snapshot harness whose output is
// dominated by the raft core's logger lines ("became leader", "cast MsgVote",
// ...) and numeric node ids — neither reproducible from this port (the logger
// lives in the off-limits core; ids here are Strings). What IS this layer's is
// the per-node Ready/Advance contract and the raftLog it drives, so each
// driveable script is reproduced as an explicit behavioural test over the same
// RawNode API the harness uses, asserting the Ready contents and end state the
// script encodes. Scripts needing the off-limits core, membership, snapshots, a
// multi-node network beyond the Ready contract, or async storage are mapped,
// file by file, in PORTING.md.

///|
/// A cluster of `RawNode`s addressed by id, driven purely through the public
/// Ready/Advance contract — a minimal `InteractionEnv` over what this layer owns.
/// (sim.mbt drives the raw `RaftNode.step` instead; this exercises the Ready
/// pipeline, which sim does not.)
struct InteractionCluster {
  nodes : Map[String, RawNode]
}

///|
/// Build a fresh cluster of `ids`, each a voter that knows all the others.
fn InteractionCluster::new(ids : Array[String]) -> InteractionCluster {
  let nodes : Map[String, RawNode] = {}
  for id in ids {
    let peers : Array[String] = []
    for other in ids {
      if other != id {
        peers.push(other)
      }
    }
    nodes[id] = RaftNode::new(id, peers, pre_vote=false).raw()
  }
  { nodes, }
}

///|
/// This node.
fn InteractionCluster::at(self : InteractionCluster, id : String) -> RawNode {
  self.nodes[id]
}

///|
/// Drive every node's Ready and deliver every message it emits, until the
/// cluster is quiescent (etcd's `stabilize`). Returns the number of rounds.
fn InteractionCluster::stabilize(self : InteractionCluster) -> Int {
  let mut rounds = 0
  let mut active = true
  while active && rounds < 1000 {
    active = false
    rounds = rounds + 1
    let pending : Array[Message] = []
    for _id, n in self.nodes {
      while n.has_ready() {
        active = true
        let rd = n.ready()
        for m in rd.messages {
          pending.push(m)
        }
        n.store(rd) // persist before advancing (the Ready contract)
        n.advance(rd)
      }
    }
    for m in pending {
      match self.nodes.get(m.to) {
        Some(target) => {
          target.step(m)
          active = true
        }
        None => ()
      }
    }
  }
  rounds
}

///|
test "interaction/single_node: a single voter campaigns and commits its no-op" {
  // testdata/single_node.txt. Bootstrap-at-index-3 is a raftLog-restore detail
  // (covered by TestLogRestore / TestRawNodeRestartFromSnapshot); the Ready
  // contract this script pins down — become leader, persist the no-op, then
  // apply it — is asserted here from a fresh log.
  let rn = RaftNode::new("1", [], pre_vote=false).raw()
  assert_true(!rn.has_ready())
  rn.campaign()
  // A single voter reaches leadership; the no-op is persisted then applied within
  // one Ready (the caller stores before applying, so committed entries are not
  // held back to the next Ready).
  let rd1 = rn.ready()
  assert_true(rd1.soft_state == Some({ lead: Some("1"), state: Leader }))
  assert_eq(rd1.entries.length(), 1)
  assert_true(rd1.entries[0].entry_type == Normal)
  assert_eq(rd1.entries[0].command.length(), 0)
  assert_eq(rd1.committed_entries.length(), 1)
  assert_eq(rd1.committed_entries[0].index, 1)
  assert_true(rd1.must_sync)
  rn.store(rd1)
  rn.advance(rd1)
  assert_true(!rn.has_ready())
  assert_true(rn.node().is_leader())
  assert_eq(rn.node().term(), 1)
  assert_eq(rn.node().commit_index(), 1)
}

///|
test "interaction/campaign: one candidate wins a three-voter election" {
  // testdata/campaign.txt. Node 1 campaigns; 2 and 3 grant their votes; 1 becomes
  // leader and replicates the term's no-op to a committed majority. (Core logger
  // lines and the bootstrap-at-index-2 offset are not reproduced; the election
  // outcome and no-op replication are.)
  let c = InteractionCluster::new(["1", "2", "3"])
  c.at("1").campaign()
  c.stabilize() |> ignore
  // Exactly one leader, at term 1, and it is node 1.
  assert_true(c.at("1").node().is_leader())
  assert_eq(c.at("1").node().term(), 1)
  for id in ["2", "3"] {
    assert_true(c.at(id).node().role() == Follower)
    assert_eq(c.at(id).node().term(), 1)
    assert_true(c.at(id).node().leader() == Some("1"))
  }
  // The leader commits its no-op at once; the followers have it replicated to
  // their logs and learn the commit on the next heartbeat (etcd's leader sends a
  // trailing `MsgApp Commit:3` to the same effect).
  assert_eq(c.at("1").node().commit_index(), 1)
  c.at("1").tick()
  c.stabilize() |> ignore
  // The no-op (index 1) is now committed on every node, and the logs agree.
  for id in ["1", "2", "3"] {
    assert_eq(c.at(id).node().commit_index(), 1)
    let e = c.at(id).node().node().entry_at(1).unwrap()
    assert_true(e.entry_type == Normal)
    assert_eq(e.command.length(), 0)
  }
}

///|
test "interaction/replicate: a proposal commits across the cluster" {
  // The propose -> replicate -> commit -> apply flow the corpus exercises after
  // an election (e.g. lagging_commit.txt, probe_and_replicate.txt), driven
  // through the Ready contract and asserted at the log level.
  let c = InteractionCluster::new(["1", "2", "3"])
  c.at("1").campaign()
  c.stabilize() |> ignore
  let applied : Array[Bytes] = []
  // Capture what the leader applies once the proposal commits.
  c.at("1").propose(b"hello")
  // Drive to quiescence, collecting the leader's applied entries.
  let mut rounds = 0
  let mut active = true
  while active && rounds < 1000 {
    active = false
    rounds = rounds + 1
    let pending : Array[Message] = []
    for id, n in c.nodes {
      while n.has_ready() {
        active = true
        let rd = n.ready()
        if id == "1" {
          for e in rd.committed_entries {
            if e.command == b"hello" {
              applied.push(e.command)
            }
          }
        }
        for m in rd.messages {
          pending.push(m)
        }
        n.store(rd) // persist, then apply (above), then advance
        n.advance(rd)
      }
    }
    for m in pending {
      match c.nodes.get(m.to) {
        Some(t) => {
          t.step(m)
          active = true
        }
        None => ()
      }
    }
  }
  // The proposal (index 2, after the no-op at 1) commits on every node.
  for id in ["1", "2", "3"] {
    assert_eq(c.at(id).node().commit_index(), 2)
    let e = c.at(id).node().node().entry_at(2).unwrap()
    assert_eq(e.command, b"hello")
  }
  // And the leader applied it exactly once.
  assert_eq(applied.length(), 1)
}
