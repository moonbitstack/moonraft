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
// Exercises reachable branches that the ported etcd tests do not hit directly:
// the debug renderer's escape/hex paths, the raftLog snapshot-delivery API, and
// the storage/unstable/wal boundary conditions. Every assertion checks a real
// property (no number-padding).

///|
test "coverage: describe_entry quoting, hex, ConfChange, empty format" {
  // All the %q escape branches plus a high byte (hex a–f) in one payload.
  let data = Bytes::from_array([b'"', b'\\', b'\n', b'\t', b'\r', (0xAA).to_byte()])
  inspect(describe_entry(Entry::normal(1, 1, data), None), content=(
    #|1/1 EntryNormal "\"\\\n\t\r\xaa"
  ))
  // A ConfChange entry renders its type name.
  inspect(describe_entry(Entry::conf(1, 2, b""), None), content=(
    #|1/2 EntryConfChange ""
  ))
  // A formatter that renders nothing leaves just the header (no trailing space).
  inspect(
    describe_entry(Entry::normal(1, 3, b"x"), Some(fn(_bytes) { "" })),
    content=(
      #|1/3 EntryNormal
    ),
  )
}

///|
test "coverage: raftLog empty/async/snapshot API and storage-compacted slice" {
  let l = RaftLog::new(MemoryStorage::new())
  // commit_stable with no entries is a no-op.
  l.commit_stable([])
  // async_stabilize with index 0, and with an index the unstable log does not
  // hold, are both no-ops (the ABA guards).
  l.async_stabilize(0, 1)
  l.async_stabilize(99, 1)
  assert_false(l.has_next_unstable_ents())
  // snapshot() falls through to storage when the unstable log has none.
  let empty = l.snapshot() catch {
    _ => Snapshot::empty()
  }
  assert_true(empty.is_empty())
  // Restoring a snapshot exposes it through the unstable snapshot API.
  l.restore({
    last_index: 5,
    last_term: 2,
    data: b"snap",
    conf_state: ConfState::empty(),
  })
  assert_true(l.has_next_unstable_snapshot())
  match l.next_unstable_snapshot() {
    Some(s) => assert_eq(s.last_index, 5)
    None => abort("expected an unstable snapshot")
  }
  let snap = l.snapshot() catch { _ => Snapshot::empty() }
  assert_eq(snap.last_index, 5)
  l.stable_snap_to(5)
  assert_false(l.has_next_unstable_snapshot())
}

///|
test "coverage: storage boundary — Unavailable, snapshot pending" {
  // storage_entries on a storage holding only the sentinel raises Unavailable.
  let s = MemoryStorage::new()
  assert_raises(() => s.storage_entries(1, 1, no_limit) |> ignore, Unavailable)
  // A pending snapshot is reported as temporarily unavailable.
  s.set_snapshot_pending(true)
  assert_raises(() => s.storage_snapshot() |> ignore, SnapshotTemporarilyUnavailable)
  s.set_snapshot_pending(false)
  s.storage_snapshot() |> ignore
}

///|
test "coverage: unstable stable_snap_to matched / unmatched / none" {
  let u = us(ents_at(6, [1]), 6, Some(mk_snap(5, 1)), 6, false)
  // A non-matching index leaves the snapshot in place.
  u.stable_snap_to(4)
  assert_true(u.snapshot is Some(_))
  // The matching index clears it.
  u.stable_snap_to(5)
  assert_true(u.snapshot is None)
  // With no snapshot, it is a no-op.
  u.stable_snap_to(5)
  assert_true(u.snapshot is None)
}

///|
test "coverage: wal replay skips an entry at or below the snapshot baseline" {
  let n = Node::new("n")
  n.install_snapshot({
    last_index: 5,
    last_term: 2,
    data: b"",
    conf_state: ConfState::empty(),
  })
  // Replaying a stale entry (index <= snapshot baseline) is ignored.
  n.replay([WalEntry(Entry::normal(2, 3, b"stale"))])
  assert_true(n.entry_at(3) == None)
  assert_eq(n.last_log_index(), 5)
}

///|
test "coverage: entries_after skips indices swallowed by a snapshot" {
  // A snapshot at 5 compacts 1..5; the two live entries are 6 and 7. Asking for
  // the suffix past index 3 walks i = 3,4 first, where entry_at returns None
  // (below/at the baseline) and the loop skips them, then collects 6,7.
  let n = Node::new("n")
  n.install_snapshot({
    last_index: 5,
    last_term: 2,
    data: b"",
    conf_state: ConfState::empty(),
  })
  n.append(2, b"a") |> ignore
  n.append(2, b"b") |> ignore
  assert_eq(n.last_log_index(), 7)
  let tail = n.entries_after(3)
  assert_eq(tail.length(), 2)
  assert_eq(tail[0].index, 6)
  let capped = n.entries_after_limited(3, 10)
  assert_eq(capped.length(), 2)
  assert_eq(capped[0].index, 6)
}

///|
test "coverage: create_snapshot rejects an index at or before the current one" {
  let s = MemoryStorage::new()
  s.append([ent(1, 1), ent(2, 1), ent(3, 1)])
  s.create_snapshot(3, b"snap", conf_state=Some(ConfState::empty())) |> ignore
  // A second snapshot at or below index 3 is out of date.
  assert_raises(
    () => s.create_snapshot(2, b"older") |> ignore,
    SnapOutOfDate,
  )
}

///|
test "coverage: load_from tolerates a temporarily unavailable snapshot" {
  // storage_snapshot raises SnapshotTemporarilyUnavailable while a snapshot is
  // being generated; load_from must fall back to an empty snapshot and still
  // load the hard state and entries.
  let s = MemoryStorage::new()
  s.append([ent(1, 1), ent(2, 1)])
  s.set_hard_state({ term: 1, vote: None, commit: 2 })
  s.set_snapshot_pending(true)
  let n = Node::new("n")
  n.load_from(s)
  s.set_snapshot_pending(false)
  assert_eq(n.last_log_index(), 2)
}

///|
test "coverage: RawNode read_index on a follower yields nothing" {
  let raw = RaftNode::new("f", ["a", "b"], seed=1).raw()
  raw.read_index(b"ctx")
  assert_eq(raw.ready().read_states.length(), 0)
}

///|
test "coverage: RawNode transfer_leader emits a message from a leader" {
  let c = Cluster::new(["1", "2", "3"], seed=5)
  let leader = c.run_until_leader(200).unwrap()
  let target = if leader == "1" { "2" } else { "1" }
  let raw = c.node(leader).raw()
  raw.transfer_leader(target)
  let rd = raw.ready()
  assert_true(rd.messages.length() > 0)
}

///|
test "coverage: RawNode step_apply_resp is a no-op for an empty batch" {
  let raw = RaftNode::new("1", [], pre_vote=false).raw_async()
  let commit_before = raw.node().commit_index()
  raw.step_apply_resp({ entries: [] })
  assert_eq(raw.node().commit_index(), commit_before)
}

///|
test "coverage: RawNode stabilize collects the messages a Ready carries" {
  let raw = RaftNode::new("a", ["b", "c"], pre_vote=false).raw()
  raw.campaign()
  let msgs = raw.stabilize()
  assert_true(msgs.length() > 0)
}

///|
test "coverage: store is a no-op under async storage writes" {
  let raw = RaftNode::new("1", [], pre_vote=false).raw_async()
  raw.campaign()
  let rd = raw.ready()
  let stable_before = raw.log.last_index()
  raw.store(rd)
  // The append is driven by the storage directive, not by store, so store must
  // not move the stable watermark.
  assert_eq(raw.log.last_index(), stable_before)
}

///|
test "coverage: async append resp carries no id when nothing is unstable, and no hard state when it is unchanged" {
  let raw = RaftNode::new("1", ["2", "3"], pre_vote=false).raw_async()
  // First append establishes term 1 (hard changes) and one entry.
  raw.step(append_msg("2", 1, 0, 0, [ent(1, 1)], 0))
  let rd1 = raw.ready()
  raw.step_append_resp(rd1.storage_append.unwrap().resp)
  raw.advance(rd1)
  // Second append at the same term adds an entry but changes no hard state, so
  // the directive's hard_state is None while entries are still handed off.
  raw.step(append_msg("2", 1, 1, 1, [ent(2, 1)], 0))
  let rd2 = raw.ready()
  let sa2 = rd2.storage_append.unwrap()
  assert_true(sa2.hard_state is None)
  assert_eq(sa2.entries.length(), 1)
  raw.step_append_resp(sa2.resp)
  raw.advance(rd2)
  // Third append adds no entries but advances the commit index (hard changes).
  // With nothing left unstable, the append-resp attests index 0.
  raw.step(append_msg("2", 1, 2, 1, [], 2))
  let rd3 = raw.ready()
  let sa3 = rd3.storage_append.unwrap()
  assert_eq(sa3.entries.length(), 0)
  assert_eq(sa3.resp.index, 0)
}
