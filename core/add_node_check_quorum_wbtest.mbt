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

// Ported from etcd-io/raft (Apache-2.0): raft_test.go TestAddNodeCheckQuorum.

///|
/// FINDINGS_LEDGER #16: a check-quorum leader that adds a voter must survive the
/// election window that immediately follows, because the newcomer's progress
/// starts recently-active (etcd's `initProgress` sets `RecentActive: true`). It
/// only steps down after a further full election timeout without hearing from the
/// newcomer.
test "TestAddNodeCheckQuorum" {
  let et = 10
  let r = RaftNode::new(
    "1",
    [],
    pre_vote=false,
    check_quorum=true,
    election_timeout=et,
  )
  r.node().become_candidate()
  r.become_leader()
  for _i in 0..<(et - 1) {
    let _ = r.tick()
  }
  r.propose_conf(ConfChange::add("2")) |> ignore
  assert_true(r.config.contains("2"))
  // The election-timeout tick runs the quorum check; the newcomer's fresh
  // recent-active flag keeps node 1 in the lead.
  let _ = r.tick()
  assert_true(r.is_leader())
  // Without hearing from node 2, a further election timeout forces the step-down.
  for _i in 0..<et {
    let _ = r.tick()
  }
  assert_true(r.role() == Follower)
}
