///|
/// A contiguous, well-formed slice of a raft log considered under a specific
/// leader `term`. `prev` is the entry immediately before `entries`. Mirrors
/// etcd's `logSlice`, whose invariants a well-formed append must satisfy:
/// entries are contiguous after `prev`, entry terms never regress, and no entry
/// carries a term newer than the leader term.
pub(all) struct LogSlice {
  term : UInt64
  prev : EntryId
  entries : Array[Entry]
}

///|
/// The index of the last entry, or `prev.index` when the slice is empty.
pub fn LogSlice::last_index(self : LogSlice) -> UInt64 {
  self.prev.index + self.entries.length().to_uint64()
}

///|
/// The identity of the last entry, or `prev` when the slice is empty.
pub fn LogSlice::last_entry_id(self : LogSlice) -> EntryId {
  let n = self.entries.length()
  if n != 0 {
    self.entries[n - 1].id()
  } else {
    self.prev
  }
}

///|
/// Whether the slice is well-formed: every entry follows the previous one by
/// exactly one index, entry terms never regress below the preceding entry, and
/// the last entry's term does not exceed the leader term. This is the "gateway"
/// check etcd runs on a slice sourced from a message or from storage.
pub fn LogSlice::valid(self : LogSlice) -> Bool {
  let mut prev = self.prev
  for e in self.entries {
    let id = e.id()
    if id.term < prev.term || id.index != prev.index + 1 {
      return false
    }
    prev = id
  }
  self.term >= prev.term
}

///|
/// One nibble as a lowercase hex digit.
fn hex_digit(n : Int) -> Char {
  if n < 10 {
    ('0'.to_int() + n).unsafe_to_char()
  } else {
    ('a'.to_int() + (n - 10)).unsafe_to_char()
  }
}

///|
/// Go `%q`-style quoting of a byte string: wrap in double quotes, escaping the
/// quote, backslash and the usual control characters, and rendering any other
/// non-printable byte as `\xHH`. This is the default `describe_entry` renderer.
fn quote_bytes(data : Bytes) -> String {
  let buf = StringBuilder::new()
  buf.write_char('"')
  for i in 0..<data.length() {
    let b = data[i].to_int()
    if b == 0x22 {
      buf.write_string("\\\"")
    } else if b == 0x5c {
      buf.write_string("\\\\")
    } else if b == 0x0a {
      buf.write_string("\\n")
    } else if b == 0x09 {
      buf.write_string("\\t")
    } else if b == 0x0d {
      buf.write_string("\\r")
    } else if b >= 0x20 && b <= 0x7e {
      buf.write_char(b.unsafe_to_char())
    } else {
      buf.write_string("\\x")
      buf.write_char(hex_digit(b / 16))
      buf.write_char(hex_digit(b % 16))
    }
  }
  buf.write_char('"')
  buf.to_string()
}

///|
/// A concise, human-readable description of an entry for debugging:
/// `term/index Type payload`. `format` renders the payload; when it is `None`
/// the default Go `%q`-style quoting is used. Mirrors etcd's `DescribeEntry`.
pub fn describe_entry(e : Entry, format : ((Bytes) -> String)?) -> String {
  let formatted = match format {
    Some(f) => f(e.command)
    None => quote_bytes(e.command)
  }
  let type_name = match e.entry_type {
    Normal => "EntryNormal"
    ConfChange => "EntryConfChange"
  }
  let head = "\{e.term}/\{e.index} \{type_name}"
  if formatted != "" {
    head + " " + formatted
  } else {
    head
  }
}

///|
/// Each entry described, one per line (etcd's `DescribeEntries`).
pub fn describe_entries(
  entries : ArrayView[Entry],
  format : ((Bytes) -> String)?,
) -> String {
  let buf = StringBuilder::new()
  for e in entries {
    buf.write_string(describe_entry(e, format))
    buf.write_char('\n')
  }
  buf.to_string()
}

///|
/// Render a set of ids the way Go's `%v` renders a slice: `[a b c]`.
fn describe_ids(ids : ArrayView[String]) -> String {
  let buf = StringBuilder::new()
  buf.write_char('[')
  let mut first = true
  for id in ids {
    if !first {
      buf.write_char(' ')
    }
    buf.write_string(id)
    first = false
  }
  buf.write_char(']')
  buf.to_string()
}

///|
/// A concise description of a HardState for debugging (etcd's
/// `DescribeHardState`): `Term:N [Vote:v ]Commit:N`, the vote shown only when a
/// vote was cast.
pub fn describe_hard_state(hs : HardState) -> String {
  let vote = match hs.vote {
    Some(v) => " Vote:\{v}"
    None => ""
  }
  "Term:\{hs.term}\{vote} Commit:\{hs.commit}"
}

///|
/// A concise description of a ConfState (etcd's `DescribeConfState`).
pub fn describe_conf_state(cs : ConfState) -> String {
  "Voters:\{describe_ids(cs.voters[:])} VotersOutgoing:\{describe_ids(cs.voters_outgoing[:])} Learners:\{describe_ids(cs.learners[:])} LearnersNext:\{describe_ids(cs.learners_next[:])} AutoLeave:\{cs.auto_leave}"
}

///|
/// A concise description of a Snapshot (etcd's `DescribeSnapshot`).
pub fn describe_snapshot(snap : Snapshot) -> String {
  "Index:\{snap.last_index} Term:\{snap.last_term} ConfState:\{describe_conf_state(snap.conf_state)}"
}
