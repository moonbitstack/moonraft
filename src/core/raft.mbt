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

///|
/// The role a node plays in the Raft protocol at a given moment.
///
/// A node starts as a `Follower`. On election timeout it becomes a
/// `Candidate` and stands for election; if it wins a majority it becomes
/// the `Leader` for that term.
#derive(Eq)
pub(all) enum Role {
  Follower
  // The pre-vote probe phase (etcd's `StatePreCandidate`): the node solicits
  // pre-votes under a hypothetical next term without adopting it, and only
  // becomes a real `Candidate` if a majority would grant it. It is a first-class
  // state reported through `SoftState`, distinct from `Follower`.
  PreCandidate
  Candidate
  Leader
}

///|
pub extend Role with Eq::{not_equal, equal}
