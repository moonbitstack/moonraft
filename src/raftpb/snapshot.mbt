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
/// A point-in-time snapshot of the state machine that stands in for the log
/// prefix up to and including `last_index`. Raft uses snapshots to bound log
/// growth and to bring a badly lagging follower up to date in one shot (§7).
#derive(Eq)
pub(all) struct Snapshot {
  last_index : UInt64
  last_term : UInt64
  data : Bytes
  // The membership as of the snapshot (etcd's SnapshotMetadata.ConfState), so a
  // node restoring from it rebuilds its voter/learner sets rather than losing
  // them. Empty for a snapshot written without a recorded configuration.
  conf_state : ConfState
}

///|
/// The empty snapshot: no state captured, baseline at index 0.
pub fn Snapshot::empty() -> Snapshot {
  { last_index: 0, last_term: 0, data: b"", conf_state: ConfState::empty(), }
}

///|
/// Whether this snapshot actually captures any state. A baseline index of 0 is
/// the empty snapshot a fresh node starts with.
pub fn Snapshot::is_empty(self : Snapshot) -> Bool {
  self.last_index == 0
}

///|
pub extend Snapshot with Eq::{not_equal, equal}
