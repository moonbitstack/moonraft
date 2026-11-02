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
/// A server announces, at construction, the three facts etcd's `newRaft` logs:
/// the configuration it restored, the state it settled into, and where its log
/// begins. A restart is one event, so the peer list, term, commit, applied and
/// log tail share a line.
test "startup: a fresh server logs its configuration, state and log position" {
  let log = CapturingLogger::new()
  let _r = RaftNode::new("1", ["2", "3"], pre_vote=false, logger=log)
  assert_eq(log.messages(), [
    "1 switched to configuration voters=(1 2 3)", "1 became follower at term 0",
    "newRaft 1 [peers: [1,2,3], term: 0, commit: 0, applied: 0, lastindex: 0, lastterm: 0]",
  ])
}

///|
/// A server restarted on a compacted log reports the snapshot it resumes from:
/// commit and applied both sit at the snapshot index, the log tail is that
/// index and term, and the configuration comes from the snapshot's `ConfState`
/// rather than from the constructor's peer list.
test "startup: a server bootstrapped from a snapshot resumes at its index" {
  let log = CapturingLogger::new()
  let snap : Snapshot = {
    last_index: 3,
    last_term: 1,
    data: b"",
    conf_state: {
      voters: ["1"],
      learners: [],
      voters_outgoing: [],
      learners_next: [],
      auto_leave: false,
    },
  }
  let r = RaftNode::new(
    "1",
    [],
    pre_vote=false,
    logger=log,
    snapshot=Some(snap),
  )
  assert_eq(log.messages(), [
    "1 switched to configuration voters=(1)", "1 became follower at term 0", "newRaft 1 [peers: [1], term: 0, commit: 3, applied: 3, lastindex: 3, lastterm: 1]",
  ])
  assert_eq(r.node().last_log_index(), 3UL)
  assert_eq(r.status().commit, 3UL)
  assert_eq(r.status().applied, 3UL)
}

///|
/// The pinned timeout is what a tick loop counts against: one tick short of it
/// nothing happens, the tick that reaches it starts the election. That is the
/// knob a reproducible replay of a script needs.
test "startup: a pinned randomized election timeout drives the tick loop" {
  let r = RaftNode::new("1", ["2", "3"], pre_vote=false)
  r.set_randomized_election_timeout(5)
  for i = 0; i < 4; i = i + 1 {
    r.tick() |> ignore
  }
  assert_true(r.role() == Follower)
  r.tick() |> ignore
  assert_true(r.role() == Candidate)
  assert_eq(r.term(), 1UL)
}
