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
// The three benchmarks upstream ships, ported case for case:
// `BenchmarkStatus` and `BenchmarkRawNode` from `rawnode_test.go`, and
// `BenchmarkMajorityConfig_CommittedIndex` from `quorum/bench_test.go`. MoonBit
// ships no `go test -bench`, so this executable times the same operations with
// core/bench's monotonic clock and prints ns/op per sub-benchmark; the figures
// feed the README.

///|
/// Answer one request the way an idle peer would: grant the vote, or accept the
/// append at its last index. Other traffic is left alone.
#coverage.skip
fn answer(rn : @raft.RawNode, m : @raft.Message) -> Unit {
  match m.payload {
    Vote(a) =>
      rn.step(
        @raft.Message::new(
          m.to,
          m.from,
          VoteResp({ term: a.term, vote_granted: true, }),
        ),
      )
    Append(a) => {
      let match_index = if a.entries.is_empty() {
        a.prev_log_index
      } else {
        a.entries[a.entries.length() - 1].index
      }
      rn.step(
        @raft.Message::new(
          m.to,
          m.from,
          AppendResp({
            term: a.term,
            success: true,
            match_index,
            conflict_index: 0UL,
            conflict_term: 0UL,
            reject_index: 0UL,
          }),
        ),
      )
    }
    _ => ()
  }
}

///|
/// Drive every pending Ready of a node to quiescence, answering the peers it
/// writes to (upstream's `stabilize` closure in `benchmarkRawNodeImpl`). Returns
/// the last index handed to the application.
#coverage.skip
fn stabilize(rn : @raft.RawNode) -> UInt64 {
  let mut applied = 0UL
  while rn.has_ready() {
    let rd = rn.ready()
    let n = rd.committed_entries.length()
    if n > 0 {
      applied = rd.committed_entries[n - 1].index
    }
    rn.store(rd)
    for m in rd.messages {
      answer(rn, m)
    }
    rn.advance(rd)
  }
  applied
}

///|
/// Build a node with `members` voters (this one is server 1) and drive it to
/// leader, answering the election traffic the way a peer would.
#coverage.skip
fn leader_with(members : Int32) -> @raft.RawNode {
  let peers : Array[String] = []
  for i = 2; i <= members; i = i + 1 {
    peers.push(i.to_string())
  }
  let rn = @raft.RaftNode::new("1", peers, pre_vote=false).raw()
  rn.campaign()
  stabilize(rn) |> ignore
  rn
}

///|
/// Time `body` for `n` iterations and report ns/op. The returned accumulator
/// keeps `body`'s result alive so the optimizer cannot delete the measured work.
#coverage.skip
fn bench(name : String, n : Int32, body : () -> UInt64) -> UInt64 {
  for _warm = 0; _warm < 1000; _warm = _warm + 1 {
    ignore(body())
  }
  let t0 = @bench.monotonic_clock_start()
  let mut acc = 0UL
  for _i = 0; _i < n; _i = _i + 1 {
    acc = acc + body()
  }
  let us = @bench.monotonic_clock_end(t0)
  let ns_per_op = us * 1000.0 / n.to_double()
  println("\{name}: \{ns_per_op} ns/op  (\{n} ops)")
  acc
}

///|
/// `BenchmarkStatus`: the cost of reading a node's status for a cluster of
/// 1, 3, 5 and 100 members, through each accessor upstream times (`Status`, the
/// documented sum-the-matches example, `BasicStatus`, and `WithProgress` with
/// and without its example visitor).
#coverage.skip
fn bench_status() -> UInt64 {
  let mut sink = 0UL
  for members in [1, 3, 5, 100] {
    let rn = leader_with(members)
    let tag = "members=\{members}"
    sink = sink ^ bench("\{tag}/Status", 20000, fn() { rn.status().term })
    sink = sink ^
      bench("\{tag}/Status-example", 20000, fn() {
        // Upstream's `Status-example` sums the per-follower match indexes out of
        // the one struct; this port splits the progress view out of `RaftStatus`,
        // so the example sums it from `full_status`.
        let s = rn.full_status()
        let mut n = 0UL
        for p in s.progress {
          n = n + p.match_index
        }
        n
      })
    sink = sink ^
      bench("\{tag}/BasicStatus", 20000, fn() { rn.basic_status().term })
    sink = sink ^
      bench("\{tag}/WithProgress", 20000, fn() {
        rn.with_progress(fn(_id, _p) { () })
        0UL
      })
    sink = sink ^
      bench("\{tag}/WithProgress-example", 20000, fn() {
        let mut n = 0UL
        rn.with_progress(fn(_id, p) { n = n + p.match_index })
        n
      })
  }
  sink
}

///|
/// `BenchmarkRawNode`: one propose -> persist -> commit -> apply turn, for a
/// single-voter and a two-voter group, with the same applied-everything check
/// upstream makes.
#coverage.skip
fn bench_raw_node() -> UInt64 {
  let mut sink = 0UL
  for tc in [(1, "single-voter"), (2, "two-voters")] {
    let (members, name) = tc
    let rn = leader_with(members)
    let iterations = 5000
    let mut applied = 0UL
    let t0 = @bench.monotonic_clock_start()
    for _i = 0; _i < iterations; _i = _i + 1 {
      rn.propose(b"foo")
      applied = stabilize(rn)
    }
    let us = @bench.monotonic_clock_end(t0)
    let ns_per_op = us * 1000.0 / iterations.to_double()
    println("BenchmarkRawNode/\{name}: \{ns_per_op} ns/op  (\{iterations} ops)")
    if applied < iterations.to_uint64() {
      abort(
        "BenchmarkRawNode/\{name}: did not apply everything: \{applied} < \{iterations}",
      )
    }
    sink = sink ^ applied
  }
  sink
}

///|
/// `BenchmarkMajorityConfig_CommittedIndex`: the commit index of a simple
/// majority over 1, 3, 5, 7, 9 and 11 voters with assorted acknowledgement
/// indexes (upstream draws these at random; a fixed spread keeps runs
/// comparable while still mixing the acked and the lagging half).
#coverage.skip
fn bench_committed_index() -> UInt64 {
  let mut sink = 0UL
  let spread = [0, 7, 3, 11, 5, 13, 1, 9, 2, 15, 4]
  for n in [1, 3, 5, 7, 9, 11] {
    let ids : Array[String] = []
    let acked : Map[String, UInt64] = Map([])
    for i = 0; i < n; i = i + 1 {
      let id = (i + 1).to_string()
      ids.push(id)
      acked[id] = spread[i].to_uint64()
    }
    sink = sink ^
      bench("BenchmarkMajorityConfig_CommittedIndex/voters=\{n}", 200000, fn() {
        @raft.committed_index(ids, [], acked)
      })
  }
  sink
}

///|
#coverage.skip
fn main {
  let mut sink = 0UL
  sink = sink ^ bench_status()
  sink = sink ^ bench_raw_node()
  sink = sink ^ bench_committed_index()
  ignore(sink)
}
