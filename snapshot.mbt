///|
/// A point-in-time snapshot of the state machine that stands in for the log
/// prefix up to and including `last_index`. Raft uses snapshots to bound log
/// growth and to bring a badly lagging follower up to date in one shot (§7).
pub(all) struct Snapshot {
  last_index : UInt64
  last_term : UInt64
  data : Bytes
}

///|
/// Compact the log by discarding every entry at or before `upto`, keeping that
/// index's term as the new snapshot baseline. `data` is the serialized state
/// machine those entries produced. Only committed entries past the current
/// baseline may be compacted; otherwise the log is left untouched.
pub fn Node::compact(self : Node, upto : UInt64, data : Bytes) -> Snapshot {
  if upto <= self.snapshot_index || upto > self.commit_index {
    return {
      last_index: self.snapshot_index,
      last_term: self.snapshot_term,
      data,
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
  { last_index: upto, last_term: term, data }
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
