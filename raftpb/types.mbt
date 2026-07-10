///|
/// What a log entry carries. A `Normal` entry holds an opaque state-machine
/// command; a `ConfChange` entry holds a serialized membership change that the
/// consensus layer applies to the cluster configuration once the entry is
/// committed (Raft §6). Keeping the kind on the entry lets the apply loop route
/// each committed entry to the right place.
pub(all) enum EntryType {
  Normal
  ConfChange
} derive(Eq)

///|
/// One command entry in the replicated log.
///
/// `term` is the leader's term — a monotonically increasing logical clock —
/// at the moment the entry was created. `index` is the entry's 1-based
/// position in the log; index 0 is the empty position before the first entry.
/// `entry_type` tells the apply loop whether `command` is an application
/// command or a membership change. `command` is the opaque payload handed to
/// the state machine (or membership decoder) once the entry is committed.
pub(all) struct Entry {
  term : UInt64
  index : UInt64
  entry_type : EntryType
  command : Bytes
} derive(Eq)

///|
/// Build a normal application entry.
pub fn Entry::normal(term : UInt64, index : UInt64, command : Bytes) -> Entry {
  { term, index, entry_type: Normal, command }
}

///|
/// Build a configuration-change entry.
pub fn Entry::conf(term : UInt64, index : UInt64, command : Bytes) -> Entry {
  { term, index, entry_type: ConfChange, command }
}

///|
/// Whether this entry carries a membership change.
pub fn Entry::is_conf_change(self : Entry) -> Bool {
  self.entry_type == ConfChange
}

///|
/// Uniquely identifies a log entry by the term that first appended it and its
/// index. There is only ever one leader per term and a leader never issues two
/// entries at the same index, so `(term, index)` pins an entry down. Mirrors
/// etcd's `entryID`.
pub(all) struct EntryId {
  term : UInt64
  index : UInt64
} derive(Eq)

///|
/// The identity `(term, index)` of this entry.
pub fn Entry::id(self : Entry) -> EntryId {
  { term: self.term, index: self.index }
}
