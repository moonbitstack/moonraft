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

// Ported from etcd-io/raft (Apache-2.0): quorum/quick_test.go TestQuick — the
// fast committed-index computation checked against a slow, obviously-correct
// brute-force reference over many random configurations.

///|
/// The obvious O(n·max) reference: the largest index acknowledged by a strict
/// majority of `voters` (a missing voter counts as 0). Empty set → no constraint.
fn ref_committed(voters : Array[String], acked : Map[String, UInt64]) -> UInt64 {
  let n = voters.length()
  if n == 0 {
    return 18446744073709551615UL
  }
  let vals : Array[UInt64] = []
  for v in voters {
    vals.push(
      match acked.get(v) {
        Some(i) => i
        None => 0
      },
    )
  }
  let cands : Array[UInt64] = [0]
  for x in vals {
    cands.push(x)
  }
  let mut best = 0UL
  for cand in cands {
    let mut cnt = 0
    for x in vals {
      if x >= cand {
        cnt = cnt + 1
      }
    }
    if cnt > n / 2 && cand > best {
      best = cand
    }
  }
  best
}

///|
fn qmin(a : UInt64, b : UInt64) -> UInt64 {
  if a < b {
    a
  } else {
    b
  }
}

///|
test "TestQuick: committed_index matches a brute-force reference" {
  let rng : Array[UInt64] = [0x2545F4914F6CDD1DUL]
  fn next() -> UInt64 {
    rng[0] = rng[0] * 6364136223846793005UL + 1442695040888963407UL
    rng[0] >> 33
  }

  for _iter in 0..<3000 {
    // A simple majority config of 1..7 voters, ~75% of them acked at 0..15.
    let n = (next() % 7).to_int() + 1
    let voters : Array[String] = []
    for i in 0..<n {
      voters.push((i + 1).to_string())
    }
    let acked : Map[String, UInt64] = {}
    for v in voters {
      if next() % 4 != 0 {
        acked[v] = next() % 16
      }
    }
    assert_eq(committed_index(voters, [], acked), ref_committed(voters, acked))

    // A joint config: a second (possibly empty) half over ids n+1..; the joint
    // committed index is the smaller of the two halves' (empty half → no bound).
    let m = (next() % 4).to_int()
    let cfgj : Array[String] = []
    for j in 0..<m {
      let id = (n + j + 1).to_string()
      cfgj.push(id)
      if next() % 4 != 0 {
        acked[id] = next() % 16
      }
    }
    let expected = qmin(
      ref_committed(voters, acked),
      ref_committed(cfgj, acked),
    )
    assert_eq(committed_index(voters, cfgj, acked), expected)
  }
}
