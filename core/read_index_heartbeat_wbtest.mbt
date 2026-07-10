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
/// A three-voter leader that has committed its no-op in the current term, viewed
/// through the Ready/Advance contract. Built through the core so the messages the
/// election produced are discarded before the RawNode is wrapped.
fn safe_read_leader() -> RawNode {
  let l = RaftNode::new("1", ["2", "3"], pre_vote=false)
  l.node().become_candidate()
  l.become_leader()
  l.step(ack_from("2", 1, l.term())) |> ignore
  l.enable_read_only_safe()
  l.raw()
}

///|
/// Under `ReadOnlySafe` a `ReadIndex` must not be answered from the leader's lease;
/// it is confirmed by a fresh heartbeat quorum, so the `Ready` following the
/// request carries the confirming heartbeats. Regression: `read_index` previously
/// called the synchronous lease API `RaftNode::read_index`, which returns an index
/// without emitting the confirmation round, so the read was recorded unconfirmed
/// and no heartbeat ever reached the followers.
test "RawNode read_index under ReadOnlySafe broadcasts confirmation heartbeats" {
  let rn = safe_read_leader()
  rn.read_index(b"rctx")
  let rd = rn.ready()
  let heartbeats = rd.messages.filter(fn(m) {
    match m.payload {
      Heartbeat(_) => true
      _ => false
    }
  })
  // Two followers, so two confirming heartbeats, and the read is not yet recorded.
  assert_eq(heartbeats.length(), 2)
  assert_eq(rd.read_states.length(), 0)
}

///|
/// The confirmation completes end to end: once a heartbeat quorum acknowledges the
/// read position, the confirmed read surfaces in the next `Ready` at the leader's
/// commit index, carrying the caller's context.
test "RawNode read_index surfaces the confirmed read after a heartbeat quorum" {
  let rn = safe_read_leader()
  rn.read_index(b"rctx")
  let rd = rn.ready()
  rn.store(rd)
  rn.advance(rd)
  // A follower acknowledges the heartbeat echoing the read position it carried.
  for m in rd.messages {
    match m.payload {
      Heartbeat(a) =>
        rn.step(
          Message::new(
            m.to,
            "1",
            HeartbeatResp({ term: rn.node().term(), context: a.context }),
          ),
        )
      _ => ()
    }
  }
  let rd2 = rn.ready()
  assert_eq(rd2.read_states.length(), 1)
  assert_eq(rd2.read_states[0].request_ctx, b"rctx")
  assert_eq(rd2.read_states[0].index, rn.node().commit_index())
}
