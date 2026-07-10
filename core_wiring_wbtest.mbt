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
/// Propose three ~250-byte commands to a two-node cluster whose byte budget is
/// `budget`, returning how many entries actually reach the follower. Wires the
/// ③-path config knob `max_inflight_bytes` end to end into each follower's
/// `Progress`/`Inflights`.
fn replicate_three_big(budget : UInt64) -> Int {
  let r = RaftNode::new(
    "1",
    ["2"],
    pre_vote=false,
    max_inflight=100,
    // etcd requires max_inflight_bytes >= max_msg_bytes; keep the per-message
    // byte cap at one 250-byte entry so the in-flight byte budget is what binds.
    max_msg_bytes=250,
    max_inflight_bytes=budget,
  )
  r.node().become_candidate()
  r.become_leader()
  // Move follower 2 into StateReplicate and commit the no-op.
  r.step(ack_from("2", 1, r.term())) |> ignore
  let big = Bytes::make(250, b'x')
  let mut sent = 0
  for _i in 0..<3 {
    for m in r.propose(big) {
      if m.payload is Append(a) {
        sent = sent + a.entries.length()
      }
    }
  }
  sent
}

///|
/// End-to-end wiring proof for `MaxInflightBytes`: with a 400-byte in-flight
/// budget the third large entry is held back (the byte window fills after two
/// 250-byte entries), whereas with the budget disabled all three flow. If the
/// knob were an unwired orphan both runs would be equal.
test "MaxInflightBytes is wired: a byte budget throttles replication" {
  let throttled = replicate_three_big(400)
  let unlimited = replicate_three_big(0)
  assert_true(throttled < unlimited)
}

///|
/// End-to-end wiring proof for `sentCommit`: after the leader replicates to a
/// follower, that follower's progress records the commit index put in flight
/// (etcd's `SentCommit`), so a later commit-only send can be skipped. If
/// `note_commit_sent` were never called `sent_commit` would stay 0.
test "sentCommit is wired: a send records the commit put in flight" {
  let r = RaftNode::new("1", ["2"], pre_vote=false)
  r.node().become_candidate()
  r.become_leader()
  r.step(ack_from("2", 1, r.term())) |> ignore
  let p = r.progress["2"]
  assert_eq(p.sent_commit, r.commit_index())
  assert_true(p.sent_commit > 0)
  // Fully caught up: there is no further commit to bump, so can_bump_commit is
  // false — the optimization that avoids a redundant commit-only MsgApp.
  assert_false(p.can_bump_commit(r.commit_index()))
}
