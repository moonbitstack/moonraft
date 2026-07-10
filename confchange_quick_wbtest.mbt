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

// Ported from etcd-io/raft (Apache-2.0): confchange/quick_test.go
// TestConfChangeQuick. The core invariant of joint consensus: applying a batch
// of changes one at a time as simple changes reaches the *same* configuration
// as entering a joint config with the whole batch and then leaving it. Checked
// over 1000 random batches. LastIndex is held at 10 throughout (as in etcd's
// quick test), so every progress is anchored at next=10 and the two paths'
// progress maps are comparable id-for-id.

///|
/// Compare two Changers that should have reached the same non-joint config: the
/// voter and learner sets match, and the progress maps track the same ids with
/// the same learner flags.
fn assert_same_config(a : Changer, b : Changer) -> Unit raise {
  assert_false(a.cfg.is_joint())
  assert_false(b.cfg.is_joint())
  assert_true(set_eq(a.cfg.incoming, b.cfg.incoming))
  assert_true(set_eq(a.cfg.learners, b.cfg.learners))
  // Same tracked ids.
  let ka : Array[String] = []
  for id, _ in a.prs {
    ka.push(id)
  }
  let kb : Array[String] = []
  for id, _ in b.prs {
    kb.push(id)
  }
  assert_true(set_eq(ka, kb))
  // Same learner flag per id.
  for id in ka {
    assert_eq(a.prs[id].is_learner, b.prs[id].is_learner)
  }
}

///|
fn run_simple(c : Changer, ccs : Array[(String, String)]) -> Unit raise {
  for cc in ccs {
    assert_true(c.simple([cc]) is None)
  }
}

///|
test "TestConfChangeQuick: simple and joint reach the same config" {
  let rng : Array[UInt64] = [0xD1B54A32D192ED03UL]
  fn next() -> UInt64 {
    rng[0] = rng[0] * 6364136223846793005UL + 1442695040888963407UL
    rng[0] >> 33
  }

  fn below(n : Int) -> Int {
    (next() % n.to_uint64()).to_int()
  }

  for _iter in 0..<1000 {
    // Initial config: node 1 is always a voter (keeps every simple change able
    // to make progress), plus a few random voter-adds of ids 1..5.
    let setup : Array[(String, String)] = [("v", "1")]
    let n_setup = 1 + below(5)
    for _i in 0..<n_setup {
      let id = (1 + (1 + below(5))).to_string()
      setup.push(("v", id))
    }
    // The batch under test: 1..9 changes of a random type over ids 2..10, so it
    // never touches node 1 and thus never empties the voter set.
    let ccs : Array[(String, String)] = []
    let n_cc = 1 + below(9)
    for _i in 0..<n_cc {
      let op = match below(4) {
        0 => "v"
        1 => "r"
        2 => "u"
        _ => "l"
      }
      let id = (1 + (1 + below(9))).to_string()
      ccs.push((op, id))
    }

    // Path A: apply the batch one change at a time as simple changes.
    let a = Changer::new(last_index=10)
    run_simple(a, setup)
    run_simple(a, ccs)

    // Path B: enter a joint config with the whole batch, then leave it.
    let b = Changer::new(last_index=10)
    run_simple(b, setup)
    assert_true(b.enter_joint(false, ccs) is None)
    assert_true(b.leave_joint() is None)
    assert_same_config(a, b)
  }
}
