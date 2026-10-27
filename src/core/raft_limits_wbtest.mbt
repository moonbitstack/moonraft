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

// Ported from etcd-io/raft (Apache-2.0): raft_test.go TestUncommittedEntryLimit
// (MaxUncommittedEntriesSize) and the MaxSizePerMsg batch cap. `ack_from` is
// shared from confchange_apply_wbtest.mbt.

///|
test "TestUncommittedEntryLimit" {
  let max_entries = 1024
  let test_data = b"testdata" // 8-byte payload
  let max_entry_size = (max_entries * 8).to_uint64()
  let r = RaftNode::new(
    "1",
    ["2", "3"],
    pre_vote=false,
    max_uncommitted_size=max_entry_size,
  )
  r.node().become_candidate()
  r.become_leader()
  // The leader's initial no-op has no payload, so the tally stays zero.
  assert_eq(r.uncommitted_size(), 0)
  r.progress["2"].become_replicate()
  r.progress["3"].become_replicate()
  r.uncommitted_size = 0

  // The first 1024 proposals are appended; the tally reaches exactly the cap.
  for _i in 0..<max_entries {
    let before = r.node().last_log_index()
    r.propose(test_data) |> ignore
    assert_eq(r.node().last_log_index(), before + 1)
  }
  // One more is dropped.
  let before = r.node().last_log_index()
  r.propose(test_data) |> ignore
  assert_eq(r.node().last_log_index(), before)

  // Release the bytes as if the entries had committed.
  r.reduce_uncommitted_size(max_entry_size)
  assert_eq(r.uncommitted_size(), 0)

  // A single large proposal is accepted because the tail was empty beforehand,
  // even though it pushes past the cap.
  let large = Bytes::make(2 * max_entries * 8, b'a')
  let b2 = r.node().last_log_index()
  r.propose(large) |> ignore
  assert_eq(r.node().last_log_index(), b2 + 1)
  // Now a normal proposal is dropped again.
  let b3 = r.node().last_log_index()
  r.propose(test_data) |> ignore
  assert_eq(r.node().last_log_index(), b3)
  // But an empty (zero-byte) entry always appends.
  let b4 = r.node().last_log_index()
  r.propose(b"") |> ignore
  assert_eq(r.node().last_log_index(), b4 + 1)
  r.reduce_uncommitted_size((2 * max_entries * 8).to_uint64())
  assert_eq(r.uncommitted_size(), 0)
}

///|
/// MaxSizePerMsg caps a single AppendEntries batch by encoded byte size, always
/// keeping at least one entry.
test "MaxSizePerMsg bounds the AppendEntries batch" {
  let cap = 250UL
  let r = RaftNode::new("1", ["2"], pre_vote=false, max_msg_bytes=cap)
  r.node().become_candidate()
  r.become_leader()
  // Append several ~100-byte entries beyond the no-op.
  for _i in 0..<10 {
    let _ = r.node().append(r.term(), Bytes::make(100, b'x'))
  }
  // Probe from the start of the log; the batch must respect the byte cap.
  r.progress["2"].next_index = 1
  let msg = r.send_to("2").unwrap()
  match msg.payload {
    Append(a) => {
      assert_true(a.entries.length() >= 1)
      assert_true(ents_size(a.entries[:]) <= cap || a.entries.length() == 1)
      // The cap admits fewer than the full run of 11 entries.
      assert_true(a.entries.length() < 11)
    }
    _ => abort("expected an Append")
  }
}

///|
test "entries_after_limited caps the batch size" {
  let a = Node::new("a")
  let mut i = 0
  while i < 5 {
    let _ = a.append(1, b"x")
    i = i + 1
  }
  assert_eq(a.entries_after_limited(0, 2).length(), 2)
  assert_eq(a.entries_after_limited(0, 10).length(), 5)
  assert_eq(a.entries_after_limited(3, 2).length(), 2)
  assert_eq(a.entries_after_limited(4, 9).length(), 1)
}

///|
test "bounded append batches still replicate a large log" {
  // A tight byte cap (etcd's MaxSizePerMsg) forces the leader to split its
  // AppendEntries into several small batches; replication still converges.
  let nodes : Map[String, RaftNode] = {
    "a": RaftNode::new("a", ["b"], seed=1, max_msg_bytes=24),
    "b": RaftNode::new("b", ["a"], seed=2),
  }
  let a = nodes.get("a").unwrap()
  deliver(nodes, a.campaign())
  assert_true(a.is_leader())
  let mut k = 0
  while k < 6 {
    deliver(nodes, a.propose(b"cmd"))
    k = k + 1
  }
  let b = nodes.get("b").unwrap()
  assert_eq(b.node().last_log_index(), a.node().last_log_index())
  assert_eq(a.commit_index(), a.node().last_log_index())
}

///|
test "the randomized election timeout stays within its band" {
  let mut seed : UInt64 = 1
  let mut i = 0
  while i < 20 {
    seed = seed + 1
    let n = RaftNode::new("n", ["a", "b"], seed~, election_timeout=10)
    let d = n.election_deadline()
    assert_true(d >= 10)
    assert_true(d < 20)
    i = i + 1
  }
}

///|
test "a candidate denied a vote remains a candidate" {
  let nodes = three_nodes()
  let a = nodes.get("a").unwrap()
  // Force a into a real election by disabling nothing and campaigning; then feed
  // it a single rejection and confirm it neither wins nor steps down.
  let _ = a.campaign() // pre-vote round begins at would-be term 1
  // Grant the pre-vote from b so a becomes a real candidate in term 1.
  let _ = a.step(
    Message::new("b", "a", PreVoteResp({ term: 1, vote_granted: true, })),
  )
  assert_true(a.role() == Candidate)
  assert_eq(a.term(), 1)
  // A rejection in the same term neither wins the election nor steps a down.
  let _ = a.step(
    Message::new("c", "a", VoteResp({ term: 1, vote_granted: false, })),
  )
  assert_true(a.role() == Candidate)
}

///|
test "scenario: a seven-node cluster elects and replicates" {
  let c = Cluster::new(["a", "b", "c", "d", "e", "f", "g"], seed=61)
  let leader = c.run_until_leader(300)
  assert_true(leader != None)
  assert_eq(c.leaders().length(), 1)
  let _ = c.propose(b"seven-1")
  let _ = c.propose(b"seven-2")
  assert_true(c.run_until_committed(3, 400))
  assert_true(c.committed_agrees())
  assert_true(c.same_committed_commands())
  assert_true(c.logs_consistent())
}

///|
test "scenario: a seven-node cluster tolerates three crashes" {
  let c = Cluster::new(["a", "b", "c", "d", "e", "f", "g"], seed=62)
  let _ = c.run_until_leader(300)
  let _ = c.propose(b"base")
  let _ = c.run_until_committed(2, 300)
  // Crash three of seven; a four-node majority remains and keeps committing.
  c.crash("e")
  c.crash("f")
  c.crash("g")
  let survivor = c.run_until_leader(500)
  assert_true(survivor != None)
  let _ = c.propose(b"after-crashes")
  c.run(300)
  assert_true(c.committed_agrees())
  assert_true(c.one_leader_per_term())
}
