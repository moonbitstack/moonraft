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

// Ported from etcd-io/raft (Apache-2.0): confchange/restore_test.go TestRestore.
// Restore rebuilds a configuration from a ConfState; reading the configuration
// back out as a ConfState must yield an equivalent state. Checked on the four
// hand-written unit cases and on 1000 randomly generated *valid* ConfStates,
// using the same generation constraints as etcd's rndConfChange.Generate
// (voters, learners and removed voters pairwise disjoint; staged learners drawn
// only from removed voters).

///|
fn restore_roundtrip(cs : ConfState) -> Unit raise {
  let c = Changer::new(last_index=10)
  assert_true(c.restore(cs) is None)
  let cs2 = c.conf_state()
  // Reading the configuration back out yields an equivalent ConfState.
  assert_true(cs2.equivalent(cs))
  assert_true(cs.equivalent(cs2))
}

///|
test "TestRestore: unit cases" {
  restore_roundtrip(ConfState::empty())
  restore_roundtrip({
    voters: ["1", "2", "3"],
    learners: [],
    voters_outgoing: [],
    learners_next: [],
    auto_leave: false,
  })
  restore_roundtrip({
    voters: ["1", "2", "3"],
    learners: ["4", "5", "6"],
    voters_outgoing: [],
    learners_next: [],
    auto_leave: false,
  })
  restore_roundtrip({
    voters: ["1", "2", "3"],
    learners: ["5"],
    voters_outgoing: ["1", "2", "4", "6"],
    learners_next: ["4"],
    auto_leave: false,
  })
}

///|
test "TestRestore: 1000 random valid ConfStates round-trip" {
  let rng : Array[UInt64] = [0x9E3779B97F4A7C15UL]
  fn next() -> UInt64 {
    rng[0] = rng[0] * 6364136223846793005UL + 1442695040888963407UL
    rng[0] >> 33
  }

  // A uniform integer in [0, n).
  fn below(n : Int) -> Int {
    (next() % n.to_uint64()).to_int()
  }

  for _iter in 0..<1000 {
    let n_voters = 1 + below(5)
    let n_learners = below(5)
    let n_removed = below(3)
    let k = n_voters + n_learners + n_removed
    // A shuffled permutation of the ids 1..=2k (Fisher-Yates), giving distinct
    // ids with room to spare so the sets never accidentally overlap.
    let ids : Array[String] = []
    for i in 0..<(2 * k) {
      ids.push((i + 1).to_string())
    }
    let mut si = 2 * k - 1
    while si > 0 {
      let j = below(si + 1)
      let tmp = ids[si]
      ids[si] = ids[j]
      ids[j] = tmp
      si = si - 1
    }
    fn slice(from : Int, len : Int) -> Array[String] {
      let out : Array[String] = []
      for i in 0..<len {
        out.push(ids[from + i])
      }
      out
    }

    let voters = slice(0, n_voters)
    let learners = slice(n_voters, n_learners)
    let removed_start = n_voters + n_learners
    let removed = slice(removed_start, n_removed)
    let n_retained = below(n_voters + 1)
    let voters_outgoing : Array[String] = []
    if n_retained > 0 || n_removed > 0 {
      for i in 0..<n_retained {
        voters_outgoing.push(voters[i])
      }
      for r in removed {
        voters_outgoing.push(r)
      }
    }
    let learners_next : Array[String] = []
    if n_removed > 0 {
      let n_ln = below(n_removed + 1)
      for i in 0..<n_ln {
        learners_next.push(removed[i])
      }
    }
    let auto_leave = voters_outgoing.length() > 0 && below(2) == 1
    restore_roundtrip({
      voters,
      learners,
      voters_outgoing,
      learners_next,
      auto_leave,
    })
  }
}
