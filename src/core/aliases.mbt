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
