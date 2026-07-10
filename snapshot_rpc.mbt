///|
/// Arguments for the InstallSnapshot RPC (Raft §7). A leader sends this when a
/// follower has fallen so far behind that the entries it needs have already
/// been compacted away. The whole state-machine image up to `last_index` is
/// shipped in `data`; real deployments chunk it, which the offset/done pair
/// leaves room for.
pub(all) struct InstallSnapshotArgs {
  term : UInt64
  leader_id : String
  last_index : UInt64
  last_term : UInt64
  offset : UInt64
  data : Bytes
  done : Bool
  // The membership recorded in the snapshot, so the follower rebuilds its
  // voter/learner sets on install (§7). Empty when none was recorded.
  conf_state : ConfState
} derive(Eq)

///|
/// Build InstallSnapshot arguments that carry a whole snapshot in one message
/// (`offset` 0, `done` true), the common case for an in-memory transport.
pub fn InstallSnapshotArgs::whole(
  term : UInt64,
  leader_id : String,
  snapshot : Snapshot,
) -> InstallSnapshotArgs {
  {
    term,
    leader_id,
    last_index: snapshot.last_index,
    last_term: snapshot.last_term,
    offset: 0,
    data: snapshot.data,
    done: true,
    conf_state: snapshot.conf_state,
  }
}

///|
/// Reply to an InstallSnapshot RPC. `term` lets a stale leader discover it has
/// been superseded.
pub(all) struct InstallSnapshotReply {
  term : UInt64
} derive(Eq)

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
