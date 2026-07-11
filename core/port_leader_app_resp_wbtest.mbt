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

// Ports of the two raft_test.go cases that pin the leader's handling of an
// AppendEntries rejection to the probe point the follower echoed back
// (etcd's `MsgAppResp.Index`), which the reply now carries as `reject_index`.
// Both depend on `maybe_decr_to`'s staleness guard comparing the *carried*
// rejected index against `next_index - 1`, so a reordered reject for an entry
// no longer in flight is discarded rather than driving a spurious back-off.

///|
/// The `(prev_log_index, leader_commit)` a message advertises — etcd's
/// `msg.GetIndex()` / `msg.GetCommit()` for an outbound MsgApp.
fn app_index_commit(m : Message) -> (UInt64, UInt64) {
  match m.payload {
    Append(a) => (a.prev_log_index, a.leader_commit)
    Heartbeat(a) => (0, a.commit)
    _ => (0, 0)
  }
}

///|
/// TestLeaderAppResp (raft_test.go:2227). A leader over {1,2,3} at term 1 with
/// log terms [_,1,1] plus its election no-op at index 3 steps a single
/// MsgAppResp from follower 2 and the table pins the resulting progress and the
/// messages emitted. The first row is the case the carried rejected index
/// fixes: a reject for index 3 while the follower is probed at index 2 is stale
/// (`next_index - 1 = 2 != 3`) and must move nothing — deriving `rejected` from
/// `next_index` instead would wrongly decrement and probe.
test "TestLeaderAppResp" {
  // (index, reject, wmatch, wnext, wmsg_num, windex, wcommitted)
  let cases : Array[(UInt64, Bool, UInt64, UInt64, Int, UInt64, UInt64)] = [
    (3, true, 0, 3, 0, 0, 0), // stale resp; no replies
    (2, true, 0, 2, 1, 1, 0), // denied; decrease next and send a probe
    (2, false, 2, 4, 2, 2, 2), // accepted; commit and broadcast with the commit
    (0, false, 0, 4, 1, 0, 0), // accepted at match; promote to replicate and fill
  ]
  for c in cases {
    let (index, reject, wmatch, wnext, wmsg_num, windex, wcommitted) = c
    let r = RaftNode::new("1", ["2", "3"], pre_vote=false)
    r.node().append(1, b"") |> ignore // index 1, term 1
    r.node().append(1, b"") |> ignore // index 2, term 1
    r.node().become_candidate() // term -> 1
    r.become_leader() // election no-op at index 3, term 1
    let msgs = r.step(
      Message::new(
        "2",
        "1",
        AppendResp({
          term: r.term(),
          success: !reject,
          match_index: if reject {
            0
          } else {
            index
          },
          conflict_index: index, // etcd RejectHint = m.Index in this test
          conflict_term: 0,
          reject_index: index, // etcd m.Index
        }),
      ),
    )
    let p = r.progress["2"]
    assert_eq(p.match_index, wmatch)
    assert_eq(p.next_index, wnext)
    assert_eq(msgs.length(), wmsg_num)
    for m in msgs {
      let (idx, commit) = app_index_commit(m)
      assert_eq(idx, windex)
      assert_eq(commit, wcommitted)
    }
  }
}

///|
/// TestLogReplicationWithReorderedMessage (raft_test.go:3976). A leader ships
/// two MsgApp to follower 2, which receives them out of order. The reject for
/// the second (higher) append arrives at the leader only after the follower has
/// caught up and the leader has dropped it back to probing. The delayed reject
/// carries `reject_index = 2` with a reject hint of 0; the leader must resend
/// from the match index, not from the stale hint.
test "TestLogReplicationWithReorderedMessage" {
  let r1 = RaftNode::new("1", ["2"], pre_vote=false)
  r1.node().become_candidate()
  r1.become_leader() // election no-op at index 1
  r1.progress["2"].become_replicate()

  // r1 ships two appends. The first, sent from next = 1, carries the no-op and
  // the first payload (prev_log_index 0); the second carries the next payload
  // (prev_log_index 2).
  let req1 = one(
    r1.step(Message::new("1", "1", Propose([Entry::normal(0, 0, b"d1")]))),
  )
  let req2 = one(
    r1.step(Message::new("1", "1", Propose([Entry::normal(0, 0, b"d2")]))),
  )

  let r2 = RaftNode::new("2", ["1"], pre_vote=false)

  // The follower sees the second append first: prev_log_index 2 is past its
  // empty log, so it rejects, echoing the rejected index (2) with a zero hint.
  let resp2 = one(r2.step(req2))
  match resp2.payload {
    AppendResp(reply) => {
      assert_false(reply.success)
      assert_eq(reply.conflict_index, 0)
      assert_eq(reply.reject_index, 2)
    }
    _ => abort("expected AppendResp")
  }

  // The first append lands next and is accepted, advancing the follower to
  // index 2; the leader records the match and broadcasts the new commit.
  let resp1 = one(r2.step(req1))
  match resp1.payload {
    AppendResp(reply) => {
      assert_true(reply.success)
      assert_eq(reply.match_index, 2)
    }
    _ => abort("expected AppendResp")
  }
  one(r1.step(resp1)) |> ignore
  assert_eq(r1.progress["2"].match_index, 2)

  // A transient network fault drops the streaming follower back to probing.
  r1.report_unreachable("2")
  assert_true(r1.progress["2"].state == Probe)

  // The delayed reject finally arrives. Its hint (0) is below the match index,
  // yet the resent append must anchor at the match index rather than regress.
  let resent = one(r1.step(resp2))
  let (idx, _) = app_index_commit(resent)
  assert_eq(idx, r1.progress["2"].match_index)
  assert_eq(idx, 2)
}

///|
/// Assert a step produced exactly one message and return it (etcd's
/// `expectOneMessage`).
fn one(msgs : Array[Message]) -> Message raise {
  assert_eq(msgs.length(), 1)
  msgs[0]
}
