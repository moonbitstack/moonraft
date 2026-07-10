// Copyright 2015 The etcd Authors
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

///|
/// The index of the last entry in the log, or the snapshot baseline when the
/// log is empty (0 on a fresh node).
pub fn Node::last_log_index(self : Node) -> UInt64 {
  let n = self.log.length()
  if n == 0 {
    self.snapshot_index
  } else {
    self.log[n - 1].index
  }
}

///|
/// The term of the last entry in the log, or the snapshot baseline term when
/// the log is empty (0 on a fresh node).
pub fn Node::last_log_term(self : Node) -> UInt64 {
  let n = self.log.length()
  if n == 0 {
    self.snapshot_term
  } else {
    self.log[n - 1].term
  }
}

///|
/// The number of entries physically held in memory (those after the snapshot
/// baseline). Useful for compaction bookkeeping and tests.
pub fn Node::log_len(self : Node) -> Int {
  self.log.length()
}

///|
/// Append a command to the log under `term`, returning the newly stored entry.
/// The new entry takes the next sequential index.
pub fn Node::append(self : Node, term : UInt64, command : Bytes) -> Entry {
  let entry = Entry::normal(term, self.last_log_index() + 1, command)
  self.log.push(entry)
  entry
}

///|
/// Append a configuration-change command to the log under `term`. Membership
/// changes travel through the log like any other entry so that every server
/// adopts them at the same point in the sequence (Raft §6).
pub fn Node::append_conf(self : Node, term : UInt64, command : Bytes) -> Entry {
  let entry = Entry::conf(term, self.last_log_index() + 1, command)
  self.log.push(entry)
  entry
}

///|
/// Fetch the entry at absolute `index`, or `None` when it is at or before the
/// snapshot baseline or past the end of the log.
pub fn Node::entry_at(self : Node, index : UInt64) -> Entry? {
  if index <= self.snapshot_index || index > self.last_log_index() {
    None
  } else {
    Some(self.log[(index - self.snapshot_index - 1).to_int()])
  }
}

///|
/// The term of the entry at `index`, or 0 when `index` is 0 (the empty
/// position before the first entry) or past the end of the log. Used by the
/// log-matching consistency check.
pub fn Node::term_at(self : Node, index : UInt64) -> UInt64 {
  if index == self.snapshot_index {
    self.snapshot_term
  } else if index <= self.snapshot_index || index > self.last_log_index() {
    0
  } else {
    self.log[(index - self.snapshot_index - 1).to_int()].term
  }
}

///|
/// Every entry strictly after absolute `index`, in order. The leader uses this
/// to assemble the payload of an AppendEntries that repairs a lagging follower.
pub fn Node::entries_after(self : Node, index : UInt64) -> Array[Entry] {
  let out : Array[Entry] = []
  let last = self.last_log_index()
  let mut i = index
  while i < last {
    if self.entry_at(i + 1) is Some(e) {
      out.push(e)
    }
    i = i + 1
  }
  out
}

///|
/// At most `max` entries strictly after absolute `index`, in order. A leader
/// caps the size of an AppendEntries with this so a badly lagging follower is
/// caught up over several bounded messages instead of one huge one.
pub fn Node::entries_after_limited(
  self : Node,
  index : UInt64,
  max : UInt64,
) -> Array[Entry] {
  let out : Array[Entry] = []
  let last = self.last_log_index()
  let mut i = index
  while i < last && out.length().to_uint64() < max {
    if self.entry_at(i + 1) is Some(e) {
      out.push(e)
    }
    i = i + 1
  }
  out
}
