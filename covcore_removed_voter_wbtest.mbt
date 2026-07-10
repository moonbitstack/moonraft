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

// FINDINGS_LEDGER #08 removed-voter-not-notified. `ack_from` is shared from
// confchange_apply_wbtest.mbt.

///|
/// A follower removed by a committed configuration change is told about the very
/// commit that removes it, rather than being dropped from the leader's view
/// before that commit is broadcast. Eager apply-on-commit reconciled the peer set
/// the instant the removal committed, so the departing node otherwise never saw
/// the final commit index and stalled on the stale one. etcd keeps the progress
/// until the application applies the change, so the removed node is still sent the
/// append; this port sends it explicitly before reconciling it away.
test "a removed follower is notified of the commit that removes it" {
  let r = RaftNode::new("1", ["2", "3"], pre_vote=false)
  r.node().become_candidate()
  r.become_leader() // no-op at index 1
  r.propose_conf(ConfChange::remove("3")) |> ignore // conf entry at index 2
  // A majority (node 1 and node 2) acks index 2, committing the removal.
  let out = r.step(ack_from("2", 2, r.term()))
  // The removal committed and applied: node 3 is gone from the leader's view.
  assert_false(r.progress.contains("3"))
  // Yet the leader sent node 3 a final append carrying the commit that removes it.
  let mut told = false
  for m in out {
    if m.to == "3" {
      if m.payload is Append(a) {
        if a.leader_commit >= 2 {
          told = true
        }
      }
    }
  }
  assert_true(told)
}
