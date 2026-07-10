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

// Ports of raft_test.go election cases that start from a seeded log or vote. See
// GAP_core.md census. Mirrors etcd's `newNetworkWithConfig` with `nil` /
// `nopStepper` / `entsWithConfig` / `votedWithConfig` peers.

///|
/// How a node in a seeded network is set up.
enum NodeSeed {
  // etcd `nil`: a fresh voter.
  Live
  // etcd `nopStepper` (blackHole): a voter that never responds — modelled as a
  // configured member absent from the delivery map.
  Black
  // etcd `entsWithConfig`: a voter whose log holds one entry per given term.
  Ents(Array[UInt64])
  // etcd `votedWithConfig`: a voter that has already voted for an id at a term.
  Voted(String, UInt64)
}

///|
/// Build a network of `1..n` from per-node seeds, leaving every configured id
/// (including black holes) a voter in each live node's configuration.
fn build_seeded_net(seeds : Array[NodeSeed], pre_vote : Bool) -> Net {
  let ids : Array[String] = []
  for i in 0..<seeds.length() {
    ids.push((i + 1).to_string())
  }
  let net = Net::new(ids, pre_vote~)
  for i, s in seeds {
    let id = ids[i]
    match s {
      Live => ()
      Black => net.nodes.remove(id)
      Ents(terms) => {
        let n = net.node(id)
        for t in terms {
          n.node().append(t, b"") |> ignore
        }
        n.become_follower(terms[terms.length() - 1])
      }
      Voted(vote, term) => {
        let n = net.node(id)
        n.become_follower(term)
        n.node().voted_for = Some(vote)
      }
    }
  }
  net
}

///|
/// testLeaderElection: node 1 campaigns across networks of varying reachable
/// quorum. It leads when a majority can answer, stalls as a (pre-)candidate when
/// it cannot, and falls back to follower when a quorum with longer logs rejects it
/// (raft_test.go:341). `pre_vote` selects the stalled state and term.
fn leader_election_cases(pre_vote : Bool) -> Unit raise {
  let cand : Role = if pre_vote { PreCandidate } else { Candidate }
  let cterm : UInt64 = if pre_vote { 0 } else { 1 }
  let cases : Array[(Array[NodeSeed], Role, UInt64)] = [
    ([Live, Live, Live], Leader, 1),
    ([Live, Live, Black], Leader, 1),
    ([Live, Black, Black], cand, cterm),
    ([Live, Black, Black, Live], cand, cterm),
    ([Live, Black, Black, Live, Live], Leader, 1),
    // Three logs further along, same term, so votes are rejected not ignored.
    ([Live, Ents([1]), Ents([1]), Ents([1, 1]), Live], Follower, 1),
  ]
  for c in cases {
    let (seeds, wstate, wterm) = c
    let net = build_seeded_net(seeds, pre_vote)
    net.campaign("1")
    assert_true(net.node("1").role() == wstate)
    assert_eq(net.node("1").term(), wterm)
  }
}

///|
/// TestLeaderElection (raft_test.go:334).
test "TestLeaderElection" {
  leader_election_cases(false)
}

///|
/// TestLeaderElectionPreVote (raft_test.go:338).
test "TestLeaderElectionPreVote" {
  leader_election_cases(true)
}

///|
/// testLeaderElectionOverwriteNewerLogs: a newly-elected leader that lacks the
/// highest-term entries overwrites the losers' newer entries with its own once it
/// wins a later term (raft_test.go:491). Nodes 1 and 2 hold a term-1 entry, node 3
/// a term-2 entry, nodes 4 and 5 voted at term 2 without logs.
fn overwrite_newer_logs(pre_vote : Bool) -> Unit raise {
  let net = build_seeded_net(
    [Ents([1]), Ents([1]), Ents([2]), Voted("3", 2), Voted("3", 2)],
    pre_vote,
  )
  // Node 1's first campaign fails — a quorum knows of the term-2 election — and
  // its term is pushed to 2.
  net.campaign("1")
  assert_true(net.node("1").role() == Follower)
  assert_eq(net.node("1").term(), 2)
  // A second campaign at a higher term succeeds.
  net.campaign("1")
  assert_true(net.node("1").is_leader())
  assert_eq(net.node("1").term(), 3)
  // Every node now agrees: term 1 at index 1, term 3 at index 2.
  for id in ["1", "2", "3", "4", "5"] {
    let core = net.node(id).node()
    assert_eq(core.last_log_index(), 2)
    assert_eq(core.term_at(1), 1)
    assert_eq(core.term_at(2), 3)
  }
}

///|
/// TestLeaderElectionOverwriteNewerLogs (raft_test.go:491).
test "TestLeaderElectionOverwriteNewerLogs" {
  overwrite_newer_logs(false)
}

///|
/// TestLeaderElectionOverwriteNewerLogsPreVote (raft_test.go:495).
test "TestLeaderElectionOverwriteNewerLogsPreVote" {
  overwrite_newer_logs(true)
}
