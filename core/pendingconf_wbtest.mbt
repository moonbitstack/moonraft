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
/// MISSING fix (M-pendingconf): etcd allows at most one configuration change in
/// flight, gating a proposal on `pendingConfIndex` vs the applied index
/// (raft.go:1326). Red: with the gate absent a leader could append two
/// overlapping, uncommitted conf changes; with it the second is refused (dropped)
/// until the first commits and is folded in.
test "pendingConfIndex: a second conf change is refused while the first is uncommitted" {
  let r = RaftNode::new("1", ["2", "3"], pre_vote=false)
  r.node().become_candidate()
  r.become_leader() // no-op @1; pending_conf_index = 0 (fresh log)
  let after_noop = r.node().last_log_index()
  // First change: allowed, appended.
  r.propose_conf(ConfChange::add("4")) |> ignore
  let after_first = r.node().last_log_index()
  assert_eq(after_first, after_noop + 1)
  // Second change before the first commits: refused. etcd replaces a refused
  // conf change with an empty entry (so a log slot is consumed) but applies no
  // membership change.
  r.propose_conf(ConfChange::add("5")) |> ignore
  assert_eq(r.node().last_log_index(), after_first + 1)
  assert_false(r.config.contains("5"))
}

///|
/// Once the first change commits (and is folded into the configuration) the next
/// change is allowed, so the gate only blocks genuinely overlapping changes.
test "pendingConfIndex: the next conf change is allowed after the first commits" {
  let r = RaftNode::new("1", ["2", "3"], pre_vote=false)
  r.node().become_candidate()
  r.become_leader()
  r.propose_conf(ConfChange::add("4")) |> ignore // @2
  let after_first = r.node().last_log_index()
  // Commit @2: a majority (self + node 2) acks, folding the change in.
  r.step(ack_from("2", after_first, r.term())) |> ignore
  // Now a further change is accepted.
  r.propose_conf(ConfChange::remove("4")) |> ignore
  assert_eq(r.node().last_log_index(), after_first + 1)
}

///|
/// DisableConfChangeValidation turns the gate off, so overlapping changes append.
test "DisableConfChangeValidation lets overlapping conf changes through" {
  let r = RaftNode::new(
    "1",
    ["2", "3"],
    pre_vote=false,
    disable_conf_change_validation=true,
  )
  r.node().become_candidate()
  r.become_leader()
  let after_noop = r.node().last_log_index()
  r.propose_conf(ConfChange::add("4")) |> ignore
  r.propose_conf(ConfChange::add("5")) |> ignore
  assert_eq(r.node().last_log_index(), after_noop + 2)
}
