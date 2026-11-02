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
/// A configuration change rendered the way etcd's `%v` prints the protobuf: the
/// transition then one `changes:{…}` per change, so a log line quoting a refused
/// change names exactly what the client asked for.
test "configuration changes render as the protobuf does" {
  assert_eq(
    ConfChange::add("4").describe_proto(),
    "type:ConfChangeAddNode node_id:4",
  )
  assert_eq(
    ConfChange::add_learner("5").describe_proto(),
    "type:ConfChangeAddLearnerNode node_id:5",
  )
  assert_eq(
    ConfChange::remove("6").describe_proto(),
    "type:ConfChangeRemoveNode node_id:6",
  )
  assert_eq(
    ConfChangeV2::auto([ConfChange::add("4")]).describe(),
    "transition:ConfChangeTransitionAuto changes:{type:ConfChangeAddNode node_id:4}",
  )
  assert_eq(
    ConfChangeV2::enter_joint([ConfChange::add("4")], auto_leave=true).describe(),
    "transition:ConfChangeTransitionJointImplicit changes:{type:ConfChangeAddNode node_id:4}",
  )
  assert_eq(
    ConfChangeV2::enter_joint([ConfChange::add("4")], auto_leave=false).describe(),
    "transition:ConfChangeTransitionJointExplicit changes:{type:ConfChangeAddNode node_id:4}",
  )
}

///|
/// A change proposed while an earlier one is still unapplied is refused, and the
/// refusal says which change and why: the proposal silently becomes an empty
/// entry, so the line is the only explanation the client gets. Both the v1 and
/// the v2 spelling are covered, because they render differently.
test "a refused configuration change is announced with its reason" {
  let log = CapturingLogger::new()
  let rn = RaftNode::new("1", [], pre_vote=false, logger=log).raw()
  rn.campaign()
  let rd = rn.ready()
  rn.store(rd)
  rn.advance(rd)
  // The no-op is applied, so the log below this point belongs to proposals.
  rn.propose_conf(ConfChange::add_learner("2"))
  rn.propose_conf(ConfChange::remove("2"))
  rn.propose_conf_v2(ConfChangeV2::auto([ConfChange::add("3")]))
  let mut v1 = false
  let mut v2 = false
  for line in log.messages() {
    if line.contains("ignoring conf change type:ConfChangeRemoveNode node_id:2") {
      v1 = true
    }
    if line.contains(
        "ignoring conf change transition:ConfChangeTransitionAuto changes:{type:ConfChangeAddNode node_id:3}",
      ) {
      v2 = true
    }
  }
  assert_true(v1)
  assert_true(v2)
}

///|
/// A follower whose next entry has been compacted away is sent a snapshot rather
/// than an append it could not resolve (Raft §7).
test "a follower behind the compaction point is sent a snapshot" {
  let rn = RaftNode::new("1", [], pre_vote=false).raw()
  rn.campaign()
  let rd = rn.ready()
  rn.store(rd)
  rn.advance(rd)
  rn.propose(b"x")
  let rd1 = rn.ready()
  rn.store(rd1)
  rn.advance(rd1)
  let rd2 = rn.ready()
  rn.store(rd2)
  rn.advance(rd2)
  // A learner keeps the quorum at one, so the change commits on this node alone.
  rn.propose_conf(ConfChange::add_learner("2"))
  rn.stabilize() |> ignore
  rn.node().node().compact(2UL, b"") |> ignore
  // The learner's next entry is now behind the compaction point: the leader can
  // only ship it a snapshot.
  rn.node().progress["2"].next_index = 1UL
  // A snapshot is only shipped to a follower that has been heard from, so the
  // leader does not waste it on a server that is probably gone.
  rn.node().progress["2"].recent_active = true
  rn.node().progress["2"].msg_app_flow_paused = false
  rn.propose(b"y")
  let mut snap = false
  for m in rn.msgs {
    if m.kind() == "Snapshot" {
      snap = true
    }
  }
  assert_true(snap)
}

///|
/// A configuration entry whose payload does not decode is refused like any other
/// unusable change — with an empty description, since there is nothing to name —
/// rather than crashing the leader.
test "an undecodable configuration entry is refused" {
  let log = CapturingLogger::new()
  let rn = RaftNode::new("1", [], pre_vote=false, logger=log).raw()
  rn.campaign()
  let rd = rn.ready()
  rn.store(rd)
  rn.advance(rd)
  // One change is in flight, so the next one is refused on that ground.
  rn.propose_conf(ConfChange::add_learner("2"))
  rn.step(
    Message::new(
      "1",
      "1",
      Propose([Entry::conf(1UL, 0UL, b"not-a-conf-change")]),
    ),
  )
  let mut refused = false
  for line in log.messages() {
    if line.contains("ignoring conf change  at config") {
      refused = true
    }
  }
  assert_true(refused)
}
