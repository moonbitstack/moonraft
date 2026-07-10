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
/// A `RaftLog` over fresh in-memory storage seeded with `ents`.
fn log_appended(ents : Array[Entry]) -> RaftLog {
  let l = RaftLog::new(MemoryStorage::new())
  l.append(ents) |> ignore
  l
}

///|
/// A snapshot baseline at `(index, term)` with an empty conf state.
fn mk_snap(index : UInt64, term : UInt64) -> Snapshot {
  {
    last_index: index,
    last_term: term,
    data: b"",
    conf_state: ConfState::empty(),
  }
}

///|
/// An `Unstable` with the given entries, offset, optional snapshot, and the two
/// in-progress markers.
fn us(
  entries : Array[Entry],
  offset : UInt64,
  snap : Snapshot?,
  oip : UInt64,
  sip : Bool,
) -> Unstable {
  {
    snapshot: snap,
    entries,
    offset,
    offset_in_progress: oip,
    snapshot_in_progress: sip,
  }
}
