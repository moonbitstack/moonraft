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

// The public facade. Every name a consumer reaches through `@raft.*` is
// re-exported here from the sub-package that owns it, so `moon add
// Lfan-ke/raft-moonbit` plus the `@raft.*` usage in the README stays stable
// while the implementation is split into auditable packages.

///|
pub using @quorum {type VoteState, committed_index, vote_result, describe}

///|
pub using @tracker {
  type Progress,
  type ProgressState,
  type Inflight,
  type Inflights,
}

///|
pub using @raftpb {
  type Entry,
  type EntryType,
  type EntryId,
  type Message,
  type Payload,
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
  no_limit,
  entry_encoding_size,
  ents_size,
  limit_size,
  payload_size,
  payloads_size,
  hard_state_equal,
}

///|
pub using @confchange {
  type Membership,
  type Changer,
  type ChangerConfig,
  type ConfChange,
  type ConfChangeType,
  type ConfChangeTransition,
  type ConfChangeV2,
}

///|
pub using @storage {
  type MemoryStorage,
  type Persisted,
  type StorageError,
  type WalRecord,
  trait RaftStorage,
  trait LogStore,
  trait WalStore,
}

///|
pub using @log {
  type RaftLog,
  type Unstable,
  type LogSlice,
  type LogCompacted,
  describe_entry,
  describe_entries,
  describe_hard_state,
  describe_conf_state,
  describe_snapshot,
}

///|
pub using @core {
  type Node,
  type RaftNode,
  type RawNode,
  type Config,
  type ConfigError,
  type ConfDriver,
  type Role,
  type SoftState,
  type ReadState,
  type Ready,
  type StorageAppend,
  type StorageAppendResp,
  type StorageApply,
  type StorageApplyResp,
  type RaftStatus,
  type FullStatus,
  type ProgressStatus,
  type ReadOnlyOption,
  type LogLevel,
  type NopLogger,
  type NopTracer,
  type TraceEvent,
  type Cluster,
  type DemoReport,
  trait Logger,
  trait Tracer,
  trait Transport,
  trait StateMachine,
  must_sync,
  replicate,
  run_election,
  run_single_node,
  demo_run,
  demo_report_lines,
}
