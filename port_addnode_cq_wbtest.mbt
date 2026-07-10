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

// Port of raft_test.go:TestAddNodeCheckQuorum. See GAP_core.md census.
//
// SKIPPED pending FINDINGS_LEDGER.md #16: `reconcile_peers` (raftnode.mbt,
// ③-path, off-limits to this test agent) builds a newly-added voter's Progress
// with `recent_active: false`, whereas etcd's `initProgress` sets it true. As a
// result a check-quorum leader that adds a voter steps down on the very first
// quorum-check tick instead of surviving one election window as etcd does. The
// assertions below are the etcd-faithful ones — restore the `test` wrapper once
// reconcile_peers seeds recent_active=true.
//
// test "TestAddNodeCheckQuorum" {
//   let et = 10
//   let r = RaftNode::new(
//     "1", [], pre_vote=false, check_quorum=true, election_timeout=et,
//   )
//   r.node().become_candidate()
//   r.become_leader()
//   for _i in 0..<(et - 1) {
//     r.tick() |> ignore
//   }
//   // Add node 2; it commits at once on the single-voter quorum and its progress
//   // must start recently-active (etcd initProgress RecentActive=true).
//   r.propose_conf(ConfChange::add("2")) |> ignore
//   assert_true(r.config.contains("2"))
//   // This tick reaches the election timeout and runs the quorum check; node 2's
//   // fresh recent-active flag keeps node 1 in the lead.
//   r.tick() |> ignore
//   assert_true(r.is_leader())
//   // Without hearing from node 2, another election timeout forces a step-down.
//   for _i in 0..<et {
//     r.tick() |> ignore
//   }
//   assert_true(r.role() == Follower)
// }
