// Local aliases visible to the core package white-box tests only: sub-package
// names the tests reach for that the non-test sources do not.

///|
using @tracker {type Inflights}

///|
using @raftpb {
  hard_state_equal,
  entry_encoding_size,
  payloads_size,
  type EntryId,
  type Payload,
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
