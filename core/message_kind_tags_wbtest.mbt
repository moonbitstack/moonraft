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

// Further coverage-driven tests for reachable branches.

///|
/// Message::kind produces a stable tag for every payload variant.
test "cov: message kind tags for the remaining payloads" {
  assert_eq(
    Message::new("a", "b", SnapshotResp({ term: 1 })).kind(),
    "SnapshotResp",
  )
  assert_eq(Message::new("a", "b", Propose([])).kind(), "Propose")
  assert_eq(Message::new("a", "b", ForgetLeader).kind(), "ForgetLeader")
}

///|
/// A rejected AppendEntries whose implied index is at or below the follower's
/// match point in StateReplicate cannot decrement `next` (etcd's MaybeDecrTo
/// returns false), so no probe is sent.
test "cov: a reject that cannot decrement sends nothing" {
  let r = cov_leader()
  let p = r.progress["2"]
  p.maybe_update(1) |> ignore // match = 1, next = 2
  p.become_replicate() // next = match + 1 = 2
  let out = r.step(
    Message::new(
      "2",
      "1",
      AppendResp({
        term: r.term(),
        success: false,
        match_index: 0,
        conflict_index: 1,
        conflict_term: 0,
      }),
    ),
  )
  assert_eq(out.length(), 0)
}

///|
/// A quorum ack for an earlier read position confirms that read but leaves a
/// later one pending, which is then confirmed by a further ack (the maybe_advance
/// partial-release path).
test "cov: partial read confirmation leaves later reads pending" {
  let r = cov_leader()
  r.set_read_only_option(Safe)
  r.step(ack_from("2", 1, r.term())) |> ignore // commit @1
  r.request_read_index(b"r1") |> ignore // position 1
  r.request_read_index(b"r2") |> ignore // position 2
  // Node 2 acknowledges position 1 (an earlier heartbeat): confirms only r1.
  r.step(
    Message::new(
      "2",
      "1",
      HeartbeatResp({ term: r.term(), context: encode_u64_le(1) }),
    ),
  )
  |> ignore
  assert_eq(r.take_read_states().length(), 1)
  // A later ack for position 2 confirms r2.
  r.step(
    Message::new(
      "2",
      "1",
      HeartbeatResp({ term: r.term(), context: encode_u64_le(2) }),
    ),
  )
  |> ignore
  assert_eq(r.take_read_states().length(), 1)
}
