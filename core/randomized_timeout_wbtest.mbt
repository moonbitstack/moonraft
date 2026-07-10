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

// Ported from etcd-io/raft (Apache-2.0): raft_paper_test.go, the randomized
// election-timeout properties (§5.2). Every state transition re-randomizes the
// timeout (etcd's reset()), which is what these tests exercise.

///|
/// testNonleaderElectionTimeoutRandomized: over many transitions a follower's
/// (or candidate's) election timeout takes every value in [et, 2*et).
fn nonleader_timeout_randomized(candidate : Bool) -> Unit raise {
  let et = 10
  let r = RaftNode::new("1", ["2", "3"], pre_vote=false, election_timeout=et)
  let seen : Map[Int, Bool] = {}
  for _round in 0..<(50 * et) {
    if candidate {
      r.become_candidate()
    } else {
      r.become_follower(r.term() + 1)
    }
    let mut time = 0
    let mut fired = false
    while !fired {
      let msgs = r.tick()
      time = time + 1
      if msgs.length() > 0 {
        fired = true
      }
    }
    seen[time] = true
  }
  for d in et..<(2 * et) {
    assert_true(seen.get(d) == Some(true))
  }
}

///|
test "testNonleaderElectionTimeoutRandomized (follower)" {
  nonleader_timeout_randomized(false)
}

///|
test "testNonleaderElectionTimeoutRandomized (candidate)" {
  nonleader_timeout_randomized(true)
}

///|
/// testNonleadersElectionTimeoutNonconflict: with independently seeded timers,
/// usually only a single server times out on any given tick, keeping split
/// votes rare (fewer than 30% of rounds see a simultaneous timeout).
fn nonleaders_timeout_nonconflict(candidate : Bool) -> Unit raise {
  let et = 10
  let size = 5
  let rs : Array[RaftNode] = []
  for k in 0..<size {
    let id = (k + 1).to_string()
    let peers : Array[String] = []
    for j in 0..<size {
      if j != k {
        peers.push((j + 1).to_string())
      }
    }
    rs.push(
      RaftNode::new(
        id,
        peers,
        pre_vote=false,
        election_timeout=et,
        seed=(k + 1).to_uint64(),
      ),
    )
  }
  let mut conflicts = 0
  for _round in 0..<1000 {
    for r in rs {
      if candidate {
        r.become_candidate()
      } else {
        r.become_follower(r.term() + 1)
      }
    }
    let mut timeout_num = 0
    while timeout_num == 0 {
      for r in rs {
        if r.tick().length() > 0 {
          timeout_num = timeout_num + 1
        }
      }
    }
    if timeout_num > 1 {
      conflicts = conflicts + 1
    }
  }
  assert_true(conflicts.to_double() / 1000.0 <= 0.3)
}

///|
test "testNonleadersElectionTimeoutNonconflict (follower)" {
  nonleaders_timeout_nonconflict(false)
}

///|
test "testNonleadersElectionTimeoutNonconflict (candidate)" {
  nonleaders_timeout_nonconflict(true)
}
