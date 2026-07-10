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
