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

// Local aliases bringing the imported sub-package types and free functions into
// unqualified scope, so the core sources read against bare names (the prelude
// pattern of `moonbitlang/core`).

///|
using @tracker {type Progress, type ProgressState}

///|
using @raftpb {
  type Entry,
  type Message,
  type ReadIndexResp,
  type RequestVoteArgs,
  type RequestVoteReply,
  type HeartbeatArgs,
  type HeartbeatReply,
  type AppendEntriesArgs,
  type AppendEntriesReply,
  type InstallSnapshotArgs,
  type InstallSnapshotReply,
  type Snapshot,
  type HardState,
  type ConfState,
  type StorageAppendResp,
  type StorageApplyResp,
  no_limit,
  ents_size,
  limit_size,
  payload_size,
}

///|
using @confchange {type Membership, type ConfChange, type ConfChangeV2}

///|
using @storage {
  type MemoryStorage,
  type Persisted,
  type WalRecord,
  trait RaftStorage,
  trait WalStore,
}

///|
using @log {type RaftLog}
