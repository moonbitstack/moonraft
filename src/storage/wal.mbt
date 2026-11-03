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
/// One record in the write-ahead log. A real Raft node appends these to a file
/// (fsync'd before it replies to any RPC) and replays them in order after a
/// crash to rebuild exactly the state it had promised (Raft §5.3, §7). A
/// `WalSnapshot` marks a compaction baseline; the `WalEntry` records after it
/// carry the log tail; `WalHardState` records the term/vote/commit at the time.
#derive(Eq)
pub(all) enum WalRecord {
  WalHardState(HardState)
  WalEntry(Entry)
  WalSnapshot(Snapshot)
}

///|
/// Durable, append-only write-ahead log. Implemented over a file in production;
/// the in-memory `MemWal` in the tests is used for tests and single-process
/// runs.
pub(open) trait WalStore {
  fn append_record(Self, WalRecord) -> Unit
  fn load(Self) -> Array[WalRecord]
}

///|
pub extend WalRecord with Eq::{not_equal, equal}
