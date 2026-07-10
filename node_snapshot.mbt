///|
/// Compact the log by discarding every entry at or before `upto`, keeping that
/// index's term as the new snapshot baseline. `data` is the serialized state
/// machine those entries produced. Only committed entries past the current
/// baseline may be compacted; otherwise the log is left untouched.
pub fn Node::compact(
  self : Node,
  upto : UInt64,
  data : Bytes,
  conf_state? : ConfState = ConfState::empty(),
) -> Snapshot {
  if upto <= self.snapshot_index || upto > self.commit_index {
    return {
      last_index: self.snapshot_index,
      last_term: self.snapshot_term,
      data,
      conf_state,
    }
  }
  let term = self.term_at(upto)
  let keep : Array[Entry] = []
  for entry in self.log {
    if entry.index > upto {
      keep.push(entry)
    }
  }
  self.log.clear()
  for entry in keep {
    self.log.push(entry)
  }
  self.snapshot_index = upto
  self.snapshot_term = term
  { last_index: upto, last_term: term, data, conf_state }
}

///|
/// Install a snapshot sent by the leader, discarding the whole log in favour
/// of the snapshot baseline. Used when a follower has fallen so far behind that
/// the leader has already compacted the entries it would otherwise need (§7).
pub fn Node::install_snapshot(self : Node, snapshot : Snapshot) -> Unit {
  self.log.clear()
  self.snapshot_index = snapshot.last_index
  self.snapshot_term = snapshot.last_term
  if self.commit_index < snapshot.last_index {
    self.commit_index = snapshot.last_index
  }
  if self.last_applied < snapshot.last_index {
    self.last_applied = snapshot.last_index
  }
}

///|
/// Handle an InstallSnapshot RPC (Raft §7). A stale leader is rejected. A
/// current-or-newer leader makes this node adopt the term, step down, and — if
/// the snapshot is newer than what it already holds — replace its log with the
/// snapshot baseline. A snapshot that is not newer is ignored, so a delayed
/// duplicate cannot roll the state machine backwards.
pub fn Node::handle_install_snapshot(
  self : Node,
  args : InstallSnapshotArgs,
) -> InstallSnapshotReply {
  if args.term < self.current_term {
    return { term: self.current_term }
  }
  if args.term > self.current_term {
    self.become_follower(args.term)
  } else {
    self.role = Follower
  }
  if args.last_index <= self.snapshot_index {
    return { term: self.current_term }
  }
  // If the follower already holds a matching entry at last_index, keep the
  // suffix after it rather than discarding entries it still needs; otherwise
  // the snapshot supersedes the whole log.
  if args.last_index <= self.last_log_index() &&
    self.term_at(args.last_index) == args.last_term {
    let keep = self.entries_after(args.last_index)
    self.log.clear()
    for e in keep {
      self.log.push(e)
    }
  } else {
    self.log.clear()
  }
  self.snapshot_index = args.last_index
  self.snapshot_term = args.last_term
  if self.commit_index < args.last_index {
    self.commit_index = args.last_index
  }
  if self.last_applied < args.last_index {
    self.last_applied = args.last_index
  }
  { term: self.current_term }
}
