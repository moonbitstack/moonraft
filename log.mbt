///|
/// The index of the last entry in the log, or 0 if the log is empty.
pub fn Node::last_log_index(self : Node) -> UInt64 {
  let n = self.log.length()
  if n == 0 {
    0
  } else {
    self.log[n - 1].index
  }
}

///|
/// The term of the last entry in the log, or 0 if the log is empty.
pub fn Node::last_log_term(self : Node) -> UInt64 {
  let n = self.log.length()
  if n == 0 {
    0
  } else {
    self.log[n - 1].term
  }
}

///|
/// Append a command to the log under `term`, returning the newly stored entry.
/// The new entry takes the next sequential index.
pub fn Node::append(self : Node, term : UInt64, command : Bytes) -> Entry {
  let entry = { term, index: self.last_log_index() + 1, command }
  self.log.push(entry)
  entry
}

///|
/// The term of the entry at `index`, or 0 when `index` is 0 (the empty
/// position before the first entry) or past the end of the log. Used by the
/// log-matching consistency check.
pub fn Node::term_at(self : Node, index : UInt64) -> UInt64 {
  if index == 0 || index > self.last_log_index() {
    0
  } else {
    self.log[(index - 1).to_int()].term
  }
}
