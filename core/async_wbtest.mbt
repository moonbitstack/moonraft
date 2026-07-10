// Copyright 2019 The etcd Authors
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
// AsyncStorageWrites (etcd Config.AsyncStorageWrites), ported from
// interaction_test.go testdata/async_storage_writes*.txt and
// node_test.go:TestCommitPaginationWithAsyncStorageWrites. Under async writes a
// Ready hands its entries/committed entries to local storage threads as
// directives; the thread does the work and returns the paired response through
// step_append_resp / step_apply_resp.

///|
/// Build one Append RPC as a routed message from `leader` to node "1".
fn append_msg(
  leader : String,
  term : UInt64,
  prev_index : UInt64,
  prev_term : UInt64,
  entries : Array[Entry],
  commit : UInt64,
) -> Message {
  Message::new(
    leader,
    "1",
    Append({
      term,
      leader_id: leader,
      prev_log_index: prev_index,
      prev_log_term: prev_term,
      entries,
      leader_commit: commit,
    }),
  )
}

///|
/// Drive an async node to quiescence, delivering each Ready's storage directives
/// back as responses, and collect the entries handed to the apply thread.
fn drive_async(raw : RawNode) -> Array[Entry] {
  let applied : Array[Entry] = []
  let mut rounds = 0
  while raw.has_ready() && rounds < 1000 {
    rounds = rounds + 1
    let rd = raw.ready()
    match rd.storage_append {
      Some(sa) => raw.step_append_resp(sa.resp)
      None => ()
    }
    match rd.storage_apply {
      Some(sap) => {
        for e in sap.entries {
          applied.push(e)
        }
        raw.step_apply_resp(sap.resp)
      }
      None => ()
    }
    raw.advance(rd)
  }
  applied
}

///|
test "async: a single node commits its no-op through storage directives" {
  let raw = RaftNode::new("1", [], pre_vote=false).raw_async()
  assert_true(raw.is_async())
  raw.campaign()
  // The very first Ready hands the election no-op to the append thread, not to
  // the caller as `entries`-to-persist-then-advance.
  let rd = raw.ready()
  assert_true(rd.storage_append is Some(_))
  match rd.storage_append {
    Some(sa) => {
      assert_eq(sa.entries.length(), 1)
      assert_eq(sa.resp.index, 1)
      assert_eq(sa.resp.log_term, 1)
      raw.step_append_resp(sa.resp)
    }
    None => ()
  }
  raw.advance(rd)
  // Drive the rest: the no-op is applied via a StorageApply directive.
  let applied = drive_async(raw)
  assert_eq(applied.length(), 1)
  assert_eq(applied[0].index, 1)
  assert_eq(raw.node().commit_index(), 1)
}

///|
/// The term of entry `i` in the node's storage (through the raftLog), or 0.
fn storage_term_at(raw : RawNode, i : UInt64) -> UInt64 {
  raw.log.storage.storage_term(i) catch {
    _ => 0
  }
}

///|
test "async ABA: a stale append ack does not truncate a rewritten log" {
  // testdata/async_storage_writes_append_aba_race.txt. A follower's slow append
  // response arrives after a new leader has overwritten those unstable entries at
  // a higher term. The stale ack must not be mistaken for a confirmation of the
  // new entries — otherwise the unstable log would be truncated while stable
  // storage still holds (or is about to hold) the wrong entries.
  let raw = RaftNode::new("1", ["2", "3"], pre_vote=false).raw_async()

  // Leader 2 (term 1) replicates two entries; the append is slow.
  raw.step(append_msg("2", 1, 0, 0, [ent(1, 1), ent(2, 1)], 0))
  let rd1 = raw.ready()
  let sa1 = rd1.storage_append.unwrap() // term-1 append directive
  assert_eq(sa1.resp.log_term, 1)
  raw.advance(rd1)

  // Before that ack returns, leader 3 (term 2) overwrites the same indices.
  raw.step(append_msg("3", 2, 0, 0, [ent(1, 2), ent(2, 2)], 0))
  let rd2 = raw.ready()
  let sa2 = rd2.storage_append.unwrap() // term-2 append directive
  assert_eq(sa2.resp.log_term, 2)
  raw.advance(rd2)

  // The stale term-1 ack arrives now — it must be ignored (lower term).
  raw.step_append_resp(sa1.resp)
  assert_eq(storage_term_at(raw, 2), 0) // nothing stabilized from the stale ack

  // The current term-2 ack then confirms the rewritten entries.
  raw.step_append_resp(sa2.resp)
  assert_eq(raw.node().node().term_at(1), 2)
  assert_eq(raw.node().node().term_at(2), 2)
  assert_eq(storage_term_at(raw, 1), 2)
  assert_eq(storage_term_at(raw, 2), 2)
}

///|
test "async ABA: an ack is dropped when the term has cycled, even if the entry term matches again" {
  // The subtle ABA (etcd's 5-node example): entries retain their original term
  // when re-replicated, so at index i the log can read term X, be overwritten by
  // term Y, then be overwritten by term X *again* by a later leader — while the
  // node's current term keeps climbing. A stale ack whose (index, term) matches
  // the re-appeared entry would wrongly stabilize it; the `resp.term < current`
  // check is what defeats this.
  let raw = RaftNode::new("1", ["2", "3", "4"], pre_vote=false).raw_async()
  // Leader A at term 1 replicates two term-1 entries; the append is slow.
  raw.step(append_msg("2", 1, 0, 0, [ent(1, 1), ent(2, 1)], 0))
  let sa1 = raw.ready().storage_append.unwrap()
  // Leader C at term 2 overwrites those indices with term-2 entries.
  raw.step(append_msg("3", 2, 0, 0, [ent(1, 2), ent(2, 2)], 0))
  raw.ready() |> ignore
  // Leader A' at term 3 re-replicates the ORIGINAL term-1 entries (entries keep
  // their term). The log at indices 1,2 reads term 1 again — but current term is 3.
  raw.step(append_msg("4", 3, 0, 0, [ent(1, 1), ent(2, 1)], 0))
  let sa3 = raw.ready().storage_append.unwrap()
  // The stale ack from A (term 1) arrives. Its (index 2, term 1) MATCHES the log
  // again, but its send-term (1) is below the current term (3), so it is dropped.
  assert_eq(sa1.resp.term, 1)
  raw.step_append_resp(sa1.resp)
  assert_eq(storage_term_at(raw, 2), 0) // nothing stabilized from the stale ack
  // The current ack (term 3) confirms the re-replicated term-1 entries.
  raw.step_append_resp(sa3.resp)
  assert_eq(storage_term_at(raw, 1), 1)
  assert_eq(storage_term_at(raw, 2), 1)
}

///|
test "TestCommitPaginationWithAsyncStorageWrites" {
  // node_test.go:TestCommitPaginationWithAsyncStorageWrites. Under async writes
  // AND a per-Ready commit byte cap, a burst of proposals is applied in order
  // over paginated StorageApply directives — no entry is dropped.
  let esz = entry_encoding_size(Entry::normal(1, 1, b"a"))
  let raw = RaftNode::new("1", [], pre_vote=false).raw_async_sized(esz * 2)
  raw.campaign()
  drive_async(raw) |> ignore
  // Three single-byte proposals; the cap admits two per apply batch.
  raw.propose(b"a")
  raw.propose(b"a")
  raw.propose(b"a")
  // Drive by hand to observe the apply-batch sizes.
  let batches : Array[Int] = []
  let applied : Array[UInt64] = []
  let mut rounds = 0
  while raw.has_ready() && rounds < 1000 {
    rounds = rounds + 1
    let rd = raw.ready()
    match rd.storage_append {
      Some(sa) => raw.step_append_resp(sa.resp)
      None => ()
    }
    match rd.storage_apply {
      Some(sap) => {
        batches.push(sap.entries.length())
        for e in sap.entries {
          applied.push(e.index)
        }
        raw.step_apply_resp(sap.resp)
      }
      None => ()
    }
    raw.advance(rd)
  }
  // Indices 2,3,4 all applied, in order, none dropped.
  assert_eq(applied.length(), 3)
  assert_eq(applied[0], 2)
  assert_eq(applied[1], 3)
  assert_eq(applied[2], 4)
  // The three commits were paginated: no apply batch exceeded two entries, and at
  // least one batch held two (the cap actually split the burst).
  let mut saw_two = false
  for b in batches {
    assert_true(b <= 2)
    if b == 2 {
      saw_two = true
    }
  }
  assert_true(saw_two)
}
