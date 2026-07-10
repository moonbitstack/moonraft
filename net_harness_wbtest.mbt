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

// A message-delivery harness with partitions and drops, modelled on etcd's
// `newNetwork` (cut / isolate / recover / ignore), so raft_test.go's
// network-driven cases can be ported. Perfect FIFO delivery otherwise.

///|
struct Net {
  nodes : Map[String, RaftNode]
  // Unordered pair "a|b" is cut when present and true.
  cuts : Map[String, Bool]
  // Payload kinds (Message::kind) that are dropped in flight.
  ignored : Map[String, Bool]
}

///|
fn Net::new(ids : Array[String], pre_vote? : Bool = false) -> Net {
  let nodes : Map[String, RaftNode] = {}
  for i, id in ids {
    let peers = ids.filter(fn(x) { x != id })
    nodes[id] = RaftNode::new(id, peers, seed=(i + 1).to_uint64(), pre_vote~)
  }
  { nodes, cuts: {}, ignored: {} }
}

///|
fn Net::node(self : Net, id : String) -> RaftNode {
  self.nodes[id]
}

///|
fn pair_key(a : String, b : String) -> String {
  if a < b {
    a + "|" + b
  } else {
    b + "|" + a
  }
}

///|
/// Sever the link between `a` and `b` in both directions.
fn Net::cut(self : Net, a : String, b : String) -> Unit {
  self.cuts[pair_key(a, b)] = true
}

///|
/// Heal all partitions and stop dropping any message kind (etcd's recover resets
/// both the cut set and the ignore set).
fn Net::recover(self : Net) -> Unit {
  self.cuts.clear()
  self.ignored.clear()
}

///|
/// Drop every message of the given payload kind in flight.
fn Net::ignore(self : Net, kind : String) -> Unit {
  self.ignored[kind] = true
}

///|
fn Net::blocked(self : Net, m : Message) -> Bool {
  match self.cuts.get(pair_key(m.from, m.to)) {
    Some(true) => return true
    _ => ()
  }
  match self.ignored.get(m.kind()) {
    Some(true) => true
    _ => false
  }
}

///|
/// Deliver `initial` and every reply in FIFO order, dropping messages across a
/// cut link or of an ignored kind, until the network falls quiet.
fn Net::deliver(self : Net, initial : Array[Message]) -> Unit {
  let q = initial
  let mut i = 0
  while i < q.length() {
    let m = q[i]
    i = i + 1
    if self.blocked(m) {
      continue
    }
    match self.nodes.get(m.to) {
      Some(n) =>
        for r in n.step(m) {
          q.push(r)
        }
      None => ()
    }
  }
}

///|
/// `id` starts an election, and the resulting traffic is delivered.
fn Net::campaign(self : Net, id : String) -> Unit {
  self.deliver(self.nodes[id].campaign())
}

///|
/// `id` proposes `data`, and the resulting traffic is delivered.
fn Net::propose(self : Net, id : String, data : Bytes) -> Unit {
  self.deliver(self.nodes[id].propose(data))
}

///|
/// Tick `id` once and deliver whatever it emits (a leader's heartbeat round,
/// which clears follower flow-control pauses — etcd's MsgBeat).
fn Net::beat(self : Net, id : String) -> Unit {
  self.deliver(self.nodes[id].tick())
}

///|
/// TestLeaderCycle: each node in turn can campaign and be elected, even when the
/// cluster is not starting from a clean slate (raft_test.go:455).
test "TestLeaderCycle" {
  let net = Net::new(["1", "2", "3"])
  for cid in ["1", "2", "3"] {
    net.campaign(cid)
    for id in ["1", "2", "3"] {
      if id == cid {
        assert_true(net.node(id).is_leader())
      } else {
        assert_true(net.node(id).role() == Follower)
      }
    }
  }
}

///|
/// TestLeaderCyclePreVote: the same, with pre-vote enabled.
test "TestLeaderCyclePreVote" {
  let net = Net::new(["1", "2", "3"], pre_vote=true)
  for cid in ["1", "2", "3"] {
    net.campaign(cid)
    assert_true(net.node(cid).is_leader())
  }
}

///|
/// TestCommitWithoutNewTermEntry: when a leader dies with entries replicated to a
/// minority, a new leader that commits its own term's entry also carries the old
/// entries to commitment (raft_test.go:756). Five nodes; node 1 replicates two
/// entries only to node 2 (3,4,5 cut), then a new leader (node 2) is elected and
/// its no-op commit brings the earlier entries along.
test "TestCommitWithoutNewTermEntry" {
  let net = Net::new(["1", "2", "3", "4", "5"])
  net.campaign("1")
  net.cut("1", "3")
  net.cut("1", "4")
  net.cut("1", "5")
  net.propose("1", b"some data")
  net.propose("1", b"some data")
  // Only nodes 1 and 2 hold the entries; a 5-node quorum is not met, so nothing
  // past the election no-op commits.
  assert_eq(net.node("1").commit_index(), 1)
  net.recover()
  // Node 2 is elected leader at a new term; committing its no-op commits the
  // inherited entries too (§5.4.2 once a current-term entry commits).
  net.campaign("2")
  assert_eq(net.node("2").commit_index(), 4)
}

///|
/// TestCannotCommitWithoutNewTermEntry: entries from a previous term are NOT
/// committed by replica count alone; only once the new leader commits an entry
/// from its own term (raft_test.go:718).
test "TestCannotCommitWithoutNewTermEntry" {
  let net = Net::new(["1", "2", "3", "4", "5"])
  net.campaign("1")
  net.cut("1", "3")
  net.cut("1", "4")
  net.cut("1", "5")
  net.propose("1", b"some data")
  net.propose("1", b"some data")
  assert_eq(net.node("1").commit_index(), 1)
  net.recover()
  // Elect node 2, but drop its AppendEntries so the ChangeTerm entry does not
  // commit yet: the inherited entries stay uncommitted (§5.4.2).
  net.ignore("Append")
  net.ignore("Probe")
  net.campaign("2")
  assert_eq(net.node("2").commit_index(), 1)
  // Once appends flow again — a heartbeat clears the follower wait state — and
  // node 2 commits an entry in its own term, the inherited entries commit too.
  net.recover()
  net.beat("2")
  net.propose("2", b"some data")
  assert_eq(net.node("2").commit_index(), 5)
}
