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

// Ported from etcd-io/raft (Apache-2.0) raft_test.go:TestReadOnlyDuplicateRequest.
// A read-index request that is duplicated and whose confirming heartbeats are
// delayed across a leadership change must never be confirmed at an index smaller
// than the commit index it was issued against (linearizability). etcd's current
// position-counter `readOnly` (which this port mirrors) is the design that closed
// the earlier context-keyed collision; this drives the exact duplication + delay
// + stale-leader scenario and checks no confirmed read index is too small.

///|
test "TestReadOnlyDuplicateRequest: a duplicated, delayed read is never stale" {
  let net = Net::new(["1", "2", "3"]) // ReadOnlySafe by default

  // A hook that delays (saves, does not re-deliver) only heartbeat responses, so
  // a read's confirmation can be held back and replayed after a leader change.
  let delayed : Array[Message] = []
  let delay_heartbeat_resps = fn(m : Message) -> Bool {
    if m.kind() == "HeartbeatResp" {
      delayed.push(m)
      false
    } else {
      true
    }
  }

  // Elect node 1 leader; its no-op commits so it can serve reads.
  net.campaign("1")
  assert_true(net.node("1").is_leader())

  // readIndexMsgA, forwarded by follower 2; its confirmed index must be at least
  // the leader's committed index now.
  let read_index_a = Message::new("2", "1", ReadIndex(b"A"))
  let read_index_minimum_a = net.node("1").commit_index()

  // Explicitly duplicate readIndexMsgA for later replay — as the network, or a
  // client retrying ReadIndex(), would.
  delayed.push(read_index_a)

  // Process the read, but hold its confirming heartbeat responses back.
  net.msg_hook = Some(delay_heartbeat_resps)
  net.inject(read_index_a)
  net.msg_hook = None

  // A heartbeat round now finishes processing readIndexA at the correct index.
  net.beat("1")

  net.isolate("1")

  // Elect node 2 leader and commit a new entry, advancing its committed index.
  net.campaign("2")
  net.propose("2", b"someOp")
  let read_index_minimum_b = net.node("2").commit_index()

  // Read from the disconnected, stale leader r1.
  let read_index_b = Message::new("1", "1", ReadIndex(b"B"))
  net.inject(read_index_b)

  net.recover()
  // Replay the delayed readIndexA duplicate and the delayed heartbeats; this must
  // not confirm B against the stale leader.
  net.deliver(delayed)

  // No confirmed read index may be smaller than the commit index it was issued
  // against — the linearizability property.
  let mut confirmed_a = 0
  for id in ["1", "2", "3"] {
    for rs in net.node(id).take_read_states() {
      if rs.request_ctx == b"A" {
        confirmed_a = confirmed_a + 1
        assert_true(rs.index >= read_index_minimum_a)
      } else if rs.request_ctx == b"B" {
        assert_true(rs.index >= read_index_minimum_b)
      }
    }
  }
  // Non-vacuity: read A is actually confirmed (its heartbeat round completed);
  // the linearizability check above is therefore exercised, not merely empty.
  assert_true(confirmed_a >= 1)
}
