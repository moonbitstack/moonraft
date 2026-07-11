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

// Ported from etcd-io/raft (Apache-2.0) raft_test.go:TestReadOnlyWithLearner.
// A single-voter leader with a learner serves linearizable reads without waiting
// on the learner: the learner is never counted toward the read quorum, and the
// sole voter (the leader itself) confirms the read at the current commit index.

///|
/// A single voter plus a learner is a *singleton* for read purposes (etcd's
/// `trk.IsSingleton`, which counts voters only): the read is answered at once at
/// the commit index, in ReadOnlySafe mode, with no heartbeat round-trip and no
/// dependence on the learner acknowledging anything.
test "ReadOnlyWithLearner: a lone voter answers reads without the learner" {
  let r = RaftNode::new("1", [], pre_vote=false) // Safe by default
  r.campaign() |> ignore // sole voter -> leader, no-op committed @1
  r.propose_conf(ConfChange::add_learner("2")) |> ignore // learner committed @2
  assert_true(r.config.is_learner("2"))
  assert_false(r.config.contains("2")) // not a voter
  assert_eq(r.config.size(), 1) // quorum is still the single voter
  assert_true(r.peers.contains("2")) // learner is replicated to
  r.enable_read_only_safe() // etcd's default; assert it explicitly

  // The learner has acked nothing, yet the read is served at the commit index.
  r.request_read_index(b"ctx1") |> ignore
  let rs = r.take_read_states()
  assert_eq(rs.length(), 1)
  assert_eq(rs[0].request_ctx, b"ctx1")
  assert_eq(rs[0].index, r.commit_index())
}

///|
/// The served read index tracks the leader's commit index as it advances under
/// the single voter — proposals commit without the learner, and each read
/// reflects the latest committed index (upstream drives ten proposals before
/// each read and checks the returned index climbs).
test "ReadOnlyWithLearner: read index tracks the single voter's commit" {
  let r = RaftNode::new("1", [], pre_vote=false)
  r.campaign() |> ignore
  r.propose_conf(ConfChange::add_learner("2")) |> ignore
  r.enable_read_only_safe()
  let mut last = r.commit_index()
  for _round in 0..<4 {
    for _j in 0..<3 {
      r.propose(b"x") |> ignore // commits under the sole voter
    }
    let ci = r.commit_index()
    assert_true(ci > last)
    r.request_read_index(b"ctx") |> ignore
    let rs = r.take_read_states()
    assert_eq(rs.length(), 1)
    assert_eq(rs[0].index, ci)
    last = ci
  }
}
