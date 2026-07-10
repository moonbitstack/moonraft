// Transitional local aliases visible to the root package's white-box tests only
// (types the migrated tests still name unqualified). Removed with the rest of
// the transitional scaffolding once every test has moved to its sub-package.

///|
using @tracker {type Inflights}

///|
using @raftpb {
  hard_state_equal,
  entry_encoding_size,
  payloads_size,
  type EntryId,
}

///|
using @storage {type StorageError}

///|
using @log {
  describe_entry,
  describe_entries,
  describe_hard_state,
  describe_conf_state,
  describe_snapshot,
  type LogSlice,
  type Unstable,
}
