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
/// The small piece of durable state Raft must flush before answering any RPC:
/// the current term, the vote cast in that term, and the commit index. etcd
/// calls this the HardState; persisting it is what lets a node rejoin without
/// violating the election-safety or state-machine-safety properties (§5.3).
#derive(Eq)
pub(all) struct HardState {
  term : UInt64
  vote : String?
  commit : UInt64
}

///|
/// The HardState a fresh node starts from: term 0, no vote, nothing committed.
pub fn HardState::initial() -> HardState {
  { term: 0, vote: None, commit: 0, }
}

///|
/// Whether a HardState is the zero value (etcd's `IsEmptyHardState`): a fresh
/// node that has neither voted nor committed anything has nothing to persist.
pub fn HardState::is_empty(self : HardState) -> Bool {
  self == HardState::initial()
}

///|
/// Whether two hard states are equal in all three durable fields — term, vote,
/// and commit (etcd's `isHardStateEqual`). This is the test that keeps a `Ready`
/// from carrying a redundant HardState when nothing durable has moved.
pub fn hard_state_equal(a : HardState, b : HardState) -> Bool {
  a == b
}

///|
pub extend HardState with Eq::{not_equal, equal}
