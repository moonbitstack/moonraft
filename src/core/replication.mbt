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
/// A best guess at where our log stops matching another log whose only known
/// point is `(index, term)` (Raft §5.3, `findConflictByTerm`). Returns the
/// greatest `i <= index` whose term is `<= term` (or is unknown because that
/// index is compacted), together with that term. Both the follower (building a
/// reject hint) and the leader (jumping back on that hint) use it, so a whole
/// run of mismatched terms is skipped in one retry instead of one index at a
/// time.
pub fn Node::find_conflict_by_term(
  self : Node,
  index : UInt64,
  term : UInt64,
) -> (UInt64, UInt64) {
  let mut i = index
  while i > 0 {
    let our = self.term_at(i)
    // term_at yields 0 for a compacted/out-of-range index, which `<= term`
    // treats as a possible match — exactly etcd's "unknown term" case.
    if our <= term {
      return (i, our)
    }
    i = i - 1
  }
  (0, 0)
}

///|
/// Handle an AppendEntries RPC (Raft §5.3). This performs the log-matching
/// consistency check, stores the entries while truncating any conflicting
/// suffix, and advances the commit index. An empty `entries` acts as the
/// leader's heartbeat.
pub fn Node::handle_append_entries(
  self : Node,
  args : AppendEntriesArgs,
) -> AppendEntriesReply {
  if args.term < self.current_term {
    // etcd answers a stale MsgApp with a bare MsgAppResp (raft.go:1157): no
    // reject hint and `Index` unset, its only purpose being to reveal our term
    // so the superseded leader steps down. `reject_index` is therefore 0.
    return {
      term: self.current_term,
      success: false,
      match_index: 0,
      conflict_index: 0,
      conflict_term: 0,
      reject_index: 0,
    }
  }
  // A legitimate leader whose term is at least ours: adopt the term and step
  // down to follower for it.
  if args.term > self.current_term {
    self.become_follower(args.term)
  } else {
    self.role = Follower
  }
  // The prefix at or below our commit index is immutable and, by the Log
  // Matching property, already agrees with any legitimate leader. Accept it
  // outright and report our commit index, so the leader re-anchors there
  // instead of probing into committed history (etcd).
  if args.prev_log_index < self.commit_index {
    return {
      term: self.current_term,
      success: true,
      match_index: self.commit_index,
      conflict_index: 0,
      conflict_term: 0,
      reject_index: 0,
    }
  }
  // Log matching: accept only if our log holds an entry at `prev_log_index`
  // carrying `prev_log_term`.
  if args.prev_log_index <= self.last_log_index() &&
    self.term_at(args.prev_log_index) == args.prev_log_term {
    let last_new = self.store_entries(args.prev_log_index, args.entries)
    if args.leader_commit > self.commit_index {
      self.commit_index = if args.leader_commit < last_new {
        args.leader_commit
      } else {
        last_new
      }
    }
    return {
      term: self.current_term,
      success: true,
      match_index: last_new,
      conflict_index: 0,
      conflict_term: 0,
      reject_index: 0,
    }
  }
  // Rejection: hint the leader with the highest (index, term) at or below the
  // probe whose term does not exceed the leader's, so it can skip our divergent
  // tail in one jump.
  let hint = if args.prev_log_index < self.last_log_index() {
    args.prev_log_index
  } else {
    self.last_log_index()
  }
  let (ci, ct) = self.find_conflict_by_term(hint, args.prev_log_term)
  // etcd echoes the rejected probe point back as `MsgAppResp.Index`
  // (raft.go:1828), which the leader feeds to `MaybeDecrTo` as `rejected`.
  {
    term: self.current_term,
    success: false,
    match_index: 0,
    conflict_index: ci,
    conflict_term: ct,
    reject_index: args.prev_log_index,
  }
}

///|
/// Store `entries` immediately after `prev_index`. Where an incoming entry
/// conflicts with an existing one (same index, different term) the local log
/// is truncated at that point before appending. Matching prefixes are left
/// untouched, so applying the same request twice is harmless. Returns the
/// index of the last entry the request covers.
fn Node::store_entries(
  self : Node,
  prev_index : UInt64,
  entries : Array[Entry],
) -> UInt64 {
  let mut idx = prev_index
  let mut i = 0
  while i < entries.length() {
    idx = idx + 1
    let entry = entries[i]
    if idx > self.last_log_index() {
      self.log.push(entry)
    } else if self.term_at(idx) != entry.term {
      // B5: a conflict at or below the commit index would delete a committed
      // entry, violating State-Machine Safety. The log-matching check and the
      // stale-term guard make this unreachable for a correct leader; assert it
      // so a regression surfaces loudly rather than corrupting the log.
      if idx <= self.commit_index {
        abort("append conflicts with committed entry")
      }
      self.truncate_from(idx)
      self.log.push(entry)
    }
    i = i + 1
  }
  idx
}

///|
/// Delete every entry from `index` (1-based) to the end of the log.
fn Node::truncate_from(self : Node, index : UInt64) -> Unit {
  let keep = (index - self.snapshot_index - 1).to_int()
  while self.log.length() > keep {
    self.log.pop() |> ignore
  }
}
