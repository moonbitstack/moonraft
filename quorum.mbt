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

// Ported from etcd-io/raft (Apache-2.0): quorum/majority.go, quorum/joint.go.
// CommittedIndex / VoteResult for majority and joint-consensus quorums.

///|
/// The outcome of counting votes against a configuration: still pending (no
/// majority either way), won (a majority granted), or lost (a majority denied).
pub enum VoteState {
  VoteWon
  VoteLost
  VotePending
} derive(Eq)

///|
/// The largest UInt64, standing in for "no constraint": the committed index of
/// an empty voter set, so that in a joint quorum an empty half defers entirely
/// to the other half.
const MAX_U64 : UInt64 = 18446744073709551615UL

///|
/// The commit index a single majority quorum agrees on, given each voter's
/// acknowledged index (0 when a voter has not reported). This is the largest
/// index stored on a majority: sort the acked indices and take the one a
/// majority is at or above. An empty set imposes no constraint.
fn majority_committed(
  voters : Array[String],
  acked : Map[String, UInt64],
) -> UInt64 {
  let n = voters.length()
  if n == 0 {
    return MAX_U64
  }
  let srt = Array::new(capacity=n)
  for v in voters {
    srt.push(
      match acked.get(v) {
        Some(i) => i
        None => 0
      },
    )
  }
  srt.sort()
  // From the end, move n/2+1 to the left; that position is acked by a majority.
  let pos = n - (n / 2 + 1)
  srt[pos]
}

///|
/// The committed index of a (possibly joint) configuration. `cfg` is the
/// incoming half and `cfgj` the outgoing half; an empty half is the zero
/// quorum. A joint configuration can only commit an index that *both* halves'
/// majorities agree on, so the result is the smaller of the two (Raft §6).
pub fn committed_index(
  cfg : Array[String],
  cfgj : Array[String],
  acked : Map[String, UInt64],
) -> UInt64 {
  let a = majority_committed(cfg, acked)
  let b = majority_committed(cfgj, acked)
  if a < b {
    a
  } else {
    b
  }
}

///|
/// The vote outcome for one majority quorum. An empty set has, by convention,
/// already won — which makes a half-populated joint quorum behave like a plain
/// majority quorum.
fn majority_vote(
  voters : Array[String],
  votes : Map[String, Bool],
) -> VoteState {
  let n = voters.length()
  if n == 0 {
    return VoteWon
  }
  let mut yes = 0
  let mut missing = 0
  for id in voters {
    match votes.get(id) {
      None => missing = missing + 1
      Some(true) => yes = yes + 1
      Some(false) => ()
    }
  }
  let q = n / 2 + 1
  if yes >= q {
    VoteWon
  } else if yes + missing >= q {
    VotePending
  } else {
    VoteLost
  }
}

///|
/// The vote outcome for a (possibly joint) configuration. A joint vote is won
/// only when both halves win, lost as soon as either half loses, and pending
/// otherwise — the discipline that keeps a membership change from splitting the
/// cluster's decision (Raft §6).
pub fn vote_result(
  cfg : Array[String],
  cfgj : Array[String],
  votes : Map[String, Bool],
) -> VoteState {
  let r1 = majority_vote(cfg, votes)
  let r2 = majority_vote(cfgj, votes)
  if r1 == VoteWon && r2 == VoteWon {
    VoteWon
  } else if r1 == VoteLost || r2 == VoteLost {
    VoteLost
  } else {
    VotePending
  }
}

///|
/// The committed index this configuration agrees on, given the acked indices,
/// accounting for the joint transition when one is in progress.
pub fn Membership::committed_index(
  self : Membership,
  acked : Map[String, UInt64],
) -> UInt64 {
  let out = if self.joint { self.outgoing } else { [] }
  committed_index(self.members, out, acked)
}

///|
/// The vote outcome for this configuration, accounting for a joint transition.
pub fn Membership::vote_result(
  self : Membership,
  votes : Map[String, Bool],
) -> VoteState {
  let out = if self.joint { self.outgoing } else { [] }
  vote_result(self.members, out, votes)
}
