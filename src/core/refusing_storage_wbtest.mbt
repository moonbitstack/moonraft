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
/// A backend that claims a log range but refuses to hand it over — the case a
/// real store hits when it is still loading, or has just compacted underneath a
/// reader. `RaftLog::stable_entries` must answer empty rather than propagate.
struct RefusingStorage {}

///|
impl RaftStorage for RefusingStorage with fn initial_state(_self) {
  HardState::initial()
}

///|
impl RaftStorage for RefusingStorage with fn storage_entries(
  _self,
  _lo,
  _hi,
  _max,
) {
  raise Unavailable
}

///|
impl RaftStorage for RefusingStorage with fn storage_term(_self, _index) {
  raise Unavailable
}

///|
impl RaftStorage for RefusingStorage with fn first_index(_self) {
  1UL
}

///|
impl RaftStorage for RefusingStorage with fn last_index(_self) {
  3UL
}

///|
impl RaftStorage for RefusingStorage with fn storage_snapshot(_self) {
  raise Unavailable
}

///|
impl RaftStorage for RefusingStorage with fn append(_self, _entries) {

}

///|
test "RaftLog stable_entries answers empty when the backend refuses" {
  let l = RaftLog::new_with_size(RefusingStorage::{ }, no_limit)
  // The range looks readable (1..3) but the read raises, and a reader that only
  // wants to print the log treats that as nothing to print.
  assert_eq(l.storage_first_index(), 1UL)
  assert_eq(l.storage_last_index(), 3UL)
  assert_eq(l.stable_entries().length(), 0)
}
