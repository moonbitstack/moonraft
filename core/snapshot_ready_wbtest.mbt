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

// Ported from etcd-io/raft (Apache-2.0): raft_test.go (TestSlowNodeRestore) and
// the snapshot-through-Ready contract that raft_snap_test.go / raft_test.go's
// TestLeaderTransferAfterSnapshot rely on (`raftLog.nextUnstableSnapshot`). Our
// core installs a leader-sent snapshot synchronously; the `RawNode` mirror
// surfaces it once through `Ready.snapshot` (etcd's `rawnode.go:156`) and clears
// it when the consumer persists it (`stableSnapTo`).

///|
/// A snapshot the core installed on receipt now flows out through a single
/// `Ready`, and is withdrawn once persisted. This is the direct witness of the
/// fix: before it, `rd.snapshot` was always `None`.
test "snapshot flows through Ready then is stabilized" {
  let raw = RaftNode::new("1", ["2"]).raw()
  let snap : Snapshot = {
    last_index: 11,
    last_term: 11,
    data: b"image",
    conf_state: {
      voters: ["1", "2"],
      learners: [],
      voters_outgoing: [],
      learners_next: [],
      auto_leave: false,
    },
  }
  raw.step(
    Message::new("2", "1", Snapshot(InstallSnapshotArgs::whole(1, "2", snap))),
  )
  assert_true(raw.has_ready())
  let rd = raw.ready()
  assert_true(rd.snapshot is Some(_))
  assert_eq(rd.snapshot.unwrap().last_index, 11)
  assert_eq(rd.snapshot.unwrap().last_term, 11)
  // Persisting the snapshot (etcd's ApplySnapshot + appliedSnap) drops it from the
  // unstable tail, so the next Ready no longer offers it.
  raw.store(rd)
  raw.advance(rd)
  assert_true(raw.ready_without_accept().snapshot is None)
}

///|
/// `has_ready` reports a pending snapshot even when neither the soft nor the hard
/// state changed: a follower already caught up to the snapshot's index receives a
/// same-term snapshot covering entries it holds, so only the mirror's unstable
/// snapshot is new.
test "has_ready reports a pending snapshot without a state change" {
  let cs : ConfState = {
    voters: ["1", "2"],
    learners: [],
    voters_outgoing: [],
    learners_next: [],
    auto_leave: false,
  }
  let n = RaftNode::new("2", ["1"])
  n.step(
    Message::new(
      "1",
      "2",
      Append({
        term: 1,
        leader_id: "1",
        prev_log_index: 0,
        prev_log_term: 0,
        entries: [Entry::normal(1, 1, b""), Entry::normal(1, 2, b"")],
        leader_commit: 2,
      }),
    ),
  )
  |> ignore
  let raw = n.raw()
  // A same-term snapshot from the same leader, at an index already held: commit,
  // term, vote and leader are all unchanged.
  n.step(
    Message::new(
      "1",
      "2",
      Snapshot(
        InstallSnapshotArgs::whole(1, "1", {
          last_index: 2,
          last_term: 1,
          data: b"",
          conf_state: cs,
        }),
      ),
    ),
  )
  |> ignore
  assert_true(raw.has_ready())
  assert_true(raw.next_unstable_snapshot() is Some(_))
}

///|
/// Under AsyncStorageWrites the snapshot rides the `StorageAppend` directive and
/// is acknowledged by the paired append response (etcd attaches the snapshot to
/// `MsgStorageAppend`/`MsgStorageAppendResp`). Persisting it drops it from the
/// unstable tail, mirroring the synchronous path above.
test "snapshot flows through an async StorageAppend" {
  let raw = RaftNode::new("1", ["2"]).raw_async()
  let snap : Snapshot = {
    last_index: 11,
    last_term: 11,
    data: b"image",
    conf_state: {
      voters: ["1", "2"],
      learners: [],
      voters_outgoing: [],
      learners_next: [],
      auto_leave: false,
    },
  }
  raw.step(
    Message::new("2", "1", Snapshot(InstallSnapshotArgs::whole(1, "2", snap))),
  )
  let rd = raw.ready()
  let directive = rd.storage_append.unwrap()
  assert_true(directive.snapshot is Some(_))
  assert_eq(directive.snapshot.unwrap().last_index, 11)
  // The append response acknowledges the durable snapshot; it is then withdrawn.
  raw.step_append_resp(directive.resp)
  assert_true(raw.next_unstable_snapshot() is None)
}

///|
/// TestSlowNodeRestore (raft_test.go:2756): a node isolated while the leader
/// races ahead, then compacts past the entries the node is missing, is brought
/// back up to date by a snapshot and rejoins the commit quorum. The assertion is
/// that the recovered follower's commit index catches the leader's.
test "TestSlowNodeRestore" {
  let net = Net::new(["1", "2", "3"])
  net.campaign("1")
  net.isolate("3")
  for _j in 0..<101 {
    net.propose("1", b"")
  }
  let lead = net.node("1")
  // etcd's nextEnts drains the leader's Ready and advances applied before the
  // snapshot is created at `raftLog.applied`.
  lead.advance_applied(lead.commit_index())
  let applied = lead.node().last_applied
  lead.node().compact(applied, b"", conf_state=lead.conf_state()) |> ignore
  net.recover()
  // Beat until the leader has heard from node 3 this window; only then will it
  // ship a snapshot (etcd's RecentActive tries on maybeSendSnapshot).
  let mut tries = 0
  while !lead.progress["3"].recent_active && tries < 100 {
    net.beat("1")
    tries = tries + 1
  }
  assert_true(lead.progress["3"].recent_active)
  // A proposal now probes node 3, finds it needs the compacted prefix, and ships
  // the snapshot; node 3 installs it and rejoins replication.
  net.propose("1", b"")
  let follower = net.node("3")
  // A further proposal commits on the full quorum, including the restored node.
  net.propose("1", b"")
  assert_eq(lead.commit_index(), follower.commit_index())
}

///|
/// TestLeaderTransferAfterSnapshot (raft_test.go): leadership is transferred to a
/// node that must first be caught up by a snapshot. The transfer stalls while the
/// transferee's snapshot acknowledgement is withheld, and completes once the
/// snapshot is applied and the acknowledgement is released. The snapshot reaches
/// the transferee through its `Ready` (`nextUnstableSnapshot`), the property this
/// batch adds.
test "TestLeaderTransferAfterSnapshot" {
  let net = Net::new(["1", "2", "3"])
  net.campaign("1")
  net.isolate("3")
  net.propose("1", b"")
  let lead = net.node("1")
  // etcd's nextEnts + CreateSnapshot(applied) + Compact(applied) on the leader.
  lead.advance_applied(lead.commit_index())
  let applied = lead.node().last_applied
  lead.node().compact(applied, b"", conf_state=lead.conf_state()) |> ignore
  // A driver over node 3, built before it receives the snapshot, so the install
  // registers as a fresh unstable snapshot in its mirror.
  let raw3 = net.node("3").raw()
  net.recover()
  assert_eq(lead.progress["3"].match_index, 1)
  // Hold node 3's non-rejecting AppendResp — the snapshot acknowledgement — as
  // etcd's msgHook does, so the leader does not learn the transferee is caught up.
  let held : Array[Message] = []
  net.msg_hook = Some(m => {
    match m.payload {
      AppendResp(r) =>
        if m.from == "3" && r.success {
          held.push(m)
          false
        } else {
          true
        }
      _ => true
    }
  })
  // Transfer to node 3 while it still lacks the snapshot: the leader ships the
  // snapshot but, with the acknowledgement withheld, stays leader.
  net.transfer("3", "1")
  assert_true(lead.is_leader())
  assert_true(held.length() > 0)
  // The snapshot reached node 3 and is pending in its Ready (etcd's
  // `follower.raftLog.nextUnstableSnapshot()`).
  let snap = raw3.next_unstable_snapshot()
  assert_true(snap is Some(_))
  assert_eq(snap.unwrap().last_index, applied)
  // Apply it (etcd's ApplySnapshot + appliedSnap) and release the held ack.
  let rd = raw3.ready()
  assert_true(rd.snapshot is Some(_))
  raw3.store(rd)
  raw3.advance(rd)
  net.msg_hook = None
  for m in held {
    net.inject(m)
  }
  // The transfer now completes: the old leader steps down to follower under 3.
  assert_true(lead.role() == Follower)
  assert_true(lead.leader() == Some("3"))
}
