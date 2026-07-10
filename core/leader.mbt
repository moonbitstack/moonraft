///|
/// Build AppendEntries arguments carrying every entry after `prev` (the
/// leader's nextIndex − 1 for a follower), used to replicate or repair.
fn Node::append_args_from(self : Node, prev : UInt64) -> AppendEntriesArgs {
  let entries : Array[Entry] = []
  let last = self.last_log_index()
  let mut i = prev
  while i < last {
    entries.push(self.log[(i - self.snapshot_index).to_int()])
    i = i + 1
  }
  {
    term: self.current_term,
    leader_id: self.id,
    prev_log_index: prev,
    prev_log_term: self.term_at(prev),
    entries,
    leader_commit: self.commit_index,
  }
}

///|
/// Replicate the leader's log to one follower, backing off the previous index
/// on rejection until the two logs agree (Raft's nextIndex decrement). Returns
/// whether the follower now holds the leader's log.
fn bring_into_sync(leader : Node, follower : Node) -> Bool {
  let mut prev = leader.last_log_index()
  let mut running = true
  let mut synced = false
  while running {
    let reply = follower.handle_append_entries(leader.append_args_from(prev))
    if reply.term > leader.current_term() {
      leader.become_follower(reply.term)
      running = false
    } else if reply.success {
      synced = true
      running = false
    } else {
      // A rejection is a log-matching failure, which can only occur above the
      // shared baseline (index 0 always agrees at equal term), so `prev` is
      // strictly positive here and the decrement never underflows: the walk-back
      // terminates once it reaches the baseline and the append is accepted.
      prev = prev - 1
    }
  }
  synced
}

///|
/// Drive one replication round from `leader` to every follower and advance the
/// commit index once a majority (the leader included) store the last entry.
/// Returns the resulting commit index.
pub fn replicate(leader : Node, followers : Array[Node]) -> UInt64 {
  let last = leader.last_log_index()
  let mut in_sync = 1
  for f in followers {
    if bring_into_sync(leader, f) {
      in_sync = in_sync + 1
    }
  }
  let majority = (followers.length() + 1) / 2 + 1
  if in_sync >= majority {
    leader.advance_commit(last)
  }
  leader.commit_index
}
