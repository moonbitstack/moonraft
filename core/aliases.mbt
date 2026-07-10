// Transitional local aliases: while the consensus core still lives in the root
// package, these bring the already-extracted sub-package types into unqualified
// scope so the un-moved sources compile unchanged. Removed once every source
// file has migrated to its sub-package and the root becomes a pure facade.

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
