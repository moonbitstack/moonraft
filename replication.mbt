///|
/// The first index this node holds that carries `term`, walking back from
/// `from`. Used to build a fast-backoff hint: when an AppendEntries is rejected
/// on a term mismatch, the leader is told the start of the conflicting term so
/// it can skip the whole run in one retry.
fn Node::first_index_of_term(
  self : Node,
  from : UInt64,
  term : UInt64,
) -> UInt64 {
  let mut i = from
  while i > self.snapshot_index + 1 && self.term_at(i - 1) == term {
    i = i - 1
  }
  i
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
    return {
      term: self.current_term,
      success: false,
      match_index: 0,
      conflict_index: 0,
    }
  }
  // A legitimate leader whose term is at least ours: adopt the term and step
  // down to follower for it.
  if args.term > self.current_term {
    self.become_follower(args.term)
  } else {
    self.role = Follower
  }
  // Log matching: reject unless our log holds an entry at `prev_log_index`
  // carrying `prev_log_term`. On rejection, hint where to retry.
  if args.prev_log_index > self.last_log_index() {
    return {
      term: self.current_term,
      success: false,
      match_index: 0,
      conflict_index: self.last_log_index() + 1,
    }
  }
  if self.term_at(args.prev_log_index) != args.prev_log_term {
    let bad_term = self.term_at(args.prev_log_index)
    return {
      term: self.current_term,
      success: false,
      match_index: 0,
      conflict_index: self.first_index_of_term(args.prev_log_index, bad_term),
    }
  }
  let last_new = self.store_entries(args.prev_log_index, args.entries)
  if args.leader_commit > self.commit_index {
    self.commit_index = if args.leader_commit < last_new {
      args.leader_commit
    } else {
      last_new
    }
  }
  {
    term: self.current_term,
    success: true,
    match_index: last_new,
    conflict_index: 0,
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
