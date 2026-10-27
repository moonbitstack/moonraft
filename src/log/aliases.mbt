// Local aliases for the imported types the log sources name unqualified.

///|
using @raftpb {
  type Entry,
  type EntryId,
  type HardState,
  type ConfState,
  type Snapshot,
  no_limit,
  limit_size,
  ents_size,
}

///|
using @storage {type MemoryStorage, type StorageError, trait RaftStorage}
