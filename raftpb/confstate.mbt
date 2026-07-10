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
/// A membership snapshot as carried by a Raft snapshot (etcd's `ConfState`).
pub(all) struct ConfState {
  voters : Array[String]
  learners : Array[String]
  voters_outgoing : Array[String]
  learners_next : Array[String]
  auto_leave : Bool
} derive(Eq)

///|
/// The empty membership state, carried by a snapshot with no recorded
/// configuration (a fresh node, or one written before ConfState existed).
pub fn ConfState::empty() -> ConfState {
  {
    voters: [],
    learners: [],
    voters_outgoing: [],
    learners_next: [],
    auto_leave: false,
  }
}

///|
/// Whether this conf state records no membership at all.
pub fn ConfState::is_empty(self : ConfState) -> Bool {
  self.voters.length() == 0 &&
  self.learners.length() == 0 &&
  self.voters_outgoing.length() == 0 &&
  self.learners_next.length() == 0
}

///|
fn set_eq(a : Array[String], b : Array[String]) -> Bool {
  let sa = a.copy()
  sa.sort()
  let sb = b.copy()
  sb.sort()
  if sa.length() != sb.length() {
    return false
  }
  for i in 0..<sa.length() {
    if sa[i] != sb[i] {
      return false
    }
  }
  true
}

///|
/// Whether two `ConfState`s describe the same configuration (etcd's
/// `ConfState.Equivalent`): the four id lists match as *sets* (order- and
/// nil/empty-insensitive) and the auto-leave flags agree.
pub fn ConfState::equivalent(self : ConfState, other : ConfState) -> Bool {
  set_eq(self.voters, other.voters) &&
  set_eq(self.learners, other.learners) &&
  set_eq(self.voters_outgoing, other.voters_outgoing) &&
  set_eq(self.learners_next, other.learners_next) &&
  self.auto_leave == other.auto_leave
}
