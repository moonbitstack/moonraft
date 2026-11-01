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

// Ported from etcd-io/raft (Apache-2.0): node_bench_test.go (BenchmarkOneNode)
// and the upstream Status / MajorityConfig benchmarks. MoonBit ships no
// `go test -bench`, so this executable times the same operations with
// core/bench's monotonic clock and prints ns/op; the figures feed the README.

///|
/// Drive every pending Ready of a single node to quiescence (etcd's `stabilize`).
#coverage.skip
fn stabilize(rn : @raft.RawNode) -> Unit {
  while rn.has_ready() {
    let rd = rn.ready()
    rn.store(rd)
    rn.advance(rd)
  }
}

///|
/// Time `body` for `n` iterations and report ns/op. The returned accumulator
/// keeps `body`'s result alive so the optimizer cannot delete the measured work.
#coverage.skip
fn bench(name : String, n : Int32, body : () -> UInt64) -> UInt64 {
  for _ in 0..<1000 {
    ignore(body())
  }
  let t0 = @bench.monotonic_clock_start()
  let mut acc = 0UL
  for _i in 0..<n {
    acc = acc + body()
  }
  let us = @bench.monotonic_clock_end(t0)
  let ns_per_op = us * 1000.0 / n.to_double()
  println("\{name}: \{ns_per_op} ns/op  (\{n} ops)")
  acc
}

///|
#coverage.skip
fn main {
  let mut sink = 0UL
  // BenchmarkStatus: the cost of computing a node's status snapshot.
  let rn = @raft.RaftNode::new("1", [], pre_vote=false).raw()
  rn.campaign()
  stabilize(rn)
  let a = bench("BenchmarkStatus", 20000, fn() { rn.status().term })
  sink = sink ^ a
  // BenchmarkRawNode: the propose -> persist -> commit -> apply pipeline.
  let rn2 = @raft.RaftNode::new("1", [], pre_vote=false).raw()
  rn2.campaign()
  stabilize(rn2)
  let b = bench("BenchmarkRawNode", 20000, fn() {
    rn2.propose(b"x")
    let mut acc = 0UL
    while rn2.has_ready() {
      let rd = rn2.ready()
      rn2.store(rd)
      rn2.advance(rd)
      acc = acc + rd.committed_entries.length().to_uint64()
    }
    acc
  })
  sink = sink ^ b
  // BenchmarkMajorityConfig_CommittedIndex: a joint quorum's commit index over a
  // five-node set with a partially-replicated tail.
  let ids = ["1", "2", "3", "4", "5"]
  let acked : Map[String, UInt64] = Map::from_array([
    ("1", 10UL),
    ("2", 10UL),
    ("3", 10UL),
    ("4", 8UL),
    ("5", 8UL),
  ])
  let c = bench("BenchmarkMajorityConfig_CommittedIndex", 200000, fn() {
    @raft.committed_index(ids, ids, acked)
  })
  sink = sink ^ c
  ignore(sink)
}
