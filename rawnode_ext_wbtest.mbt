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
// Exercises the RawNode wrappers added to complete rawnode.go coverage:
// ForgetLeader / ReportUnreachable / ReportSnapshot / BasicStatus / Status
// (full) / WithProgress.

///|
test "RawNode status / with_progress wrappers" {
  let c = InteractionCluster::new(["1", "2", "3"])
  c.at("1").campaign()
  c.stabilize() |> ignore
  let leader = c.at("1")
  // BasicStatus reports the elected leader; full Status carries the progress map.
  assert_true(leader.basic_status().role == Leader)
  let st = leader.full_status()
  assert_eq(st.progress.length(), 3)
  // WithProgress visits every follower's progress entry.
  let visited : Array[String] = []
  leader.with_progress(fn(id, _p) { visited.push(id) })
  assert_eq(visited.length(), 3)
}

///|
test "RawNode report_unreachable / report_snapshot / forget_leader wrappers" {
  let c = InteractionCluster::new(["1", "2", "3"])
  c.at("1").campaign()
  c.stabilize() |> ignore
  let leader = c.at("1")
  // These drive the leader's follower bookkeeping; they must not fault.
  leader.report_unreachable("2")
  leader.report_snapshot("3", false)
  leader.report_snapshot("3", true)
  // A follower forgetting the leader clears its known-leader (under safe
  // read-index, per etcd's ForgetLeader contract).
  let follower = c.at("2")
  follower.node().enable_read_only_safe()
  assert_true(follower.node().leader() == Some("1"))
  follower.forget_leader()
  assert_true(follower.node().leader() == None)
}
