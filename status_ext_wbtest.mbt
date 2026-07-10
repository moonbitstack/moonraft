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

// Ported from etcd-io/raft (Apache-2.0): the progress-map and config half of
// rawnode_test.go:TestRawNodeStatus / status_test.go — the part earlier left
// out because RaftStatus exposed only role/term/commit. `full_status` now
// carries the per-follower Progress view and the current configuration.

///|
test "full_status exposes per-follower progress and the configuration" {
  let r = RaftNode::new("1", ["2", "3"], pre_vote=false)
  r.node().become_candidate()
  r.become_leader()
  r.step(ack_from("2", 1, r.term())) |> ignore // node 2 catches up

  let st = r.full_status()
  // Basic status still there.
  assert_true(st.basic.role == Leader)
  assert_eq(st.basic.term, 1)
  // Progress covers the leader itself plus both followers.
  assert_eq(st.progress.length(), 3)
  // The leader's own entry is fully caught up.
  match r.progress_of("1") {
    Some(p) => {
      assert_eq(p.match_index, r.node().last_log_index())
      assert_true(p.state == Replicate)
      assert_false(p.is_learner)
    }
    None => abort("expected the leader's own progress")
  }
  // A follower's entry reflects its ack.
  match r.progress_of("2") {
    Some(p) => assert_eq(p.match_index, 1)
    None => abort("expected node 2's progress")
  }
  assert_true(r.progress_of("9") == None)
  // The config carries the voters.
  assert_true(st.config.voters.contains("1"))
  assert_true(st.config.voters.contains("2"))
  assert_true(st.config.voters.contains("3"))
}

///|
test "with_progress visits every tracked follower" {
  let r = RaftNode::new("1", ["2", "3"], pre_vote=false)
  r.node().become_candidate()
  r.become_leader()
  let seen : Array[String] = []
  r.with_progress(fn(id, _p) { seen.push(id) })
  seen.sort()
  assert_eq(seen.length(), 3)
  assert_true(seen.contains("1") && seen.contains("2") && seen.contains("3"))
}

///|
test "status reflects learners and joint configuration" {
  let r = RaftNode::new("1", [], pre_vote=false)
  r.campaign() |> ignore
  r.propose_conf(ConfChange::add_learner("2")) |> ignore
  let st = r.full_status()
  // The learner shows up in the config and is flagged in its progress.
  assert_true(st.config.learners.contains("2"))
  match r.progress_of("2") {
    Some(p) => assert_true(p.is_learner)
    None => abort("expected the learner's progress")
  }
}

///|
test "a non-leader tracks no progress" {
  let r = RaftNode::new("2", ["1"], pre_vote=false)
  r.node().become_follower(3)
  let st = r.full_status()
  assert_eq(st.progress.length(), 0)
  assert_true(st.basic.role == Follower)
}
