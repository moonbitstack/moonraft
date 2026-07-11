///|
/// Arguments for the RequestVote RPC (Raft §5.2), sent by a candidate to
/// gather votes for the term it is standing in.
pub(all) struct RequestVoteArgs {
  term : UInt64
  candidate_id : String
  last_log_index : UInt64
  last_log_term : UInt64
} derive(Eq)

///|
/// Reply to a RequestVote RPC. `term` lets a stale candidate discover it has
/// been superseded; `vote_granted` is true when the vote was given.
pub(all) struct RequestVoteReply {
  term : UInt64
  vote_granted : Bool
} derive(Eq)

///|
/// Arguments for the Heartbeat RPC (Raft §5.2): a leader's periodic liveness
/// beat. It carries no entries — only the leader's `commit` index (capped at
/// what the follower is known to hold) so a follower can advance its commit
/// index between AppendEntries. Modelling it as a distinct message (rather than
/// an empty AppendEntries) lets the leader treat a heartbeat acknowledgement as
/// pure liveness, separate from log-matching.
pub(all) struct HeartbeatArgs {
  term : UInt64
  leader_id : String
  commit : UInt64
  // An opaque read-index context echoed back by the follower, used by the
  // ReadOnlySafe path to confirm leadership with a fresh quorum round-trip
  // (Raft §6.4). Empty for an ordinary liveness beat.
  context : Bytes
} derive(Eq)

///|
/// Reply to a Heartbeat RPC: the follower's term and the read-index context it
/// is acknowledging (echoed verbatim).
pub(all) struct HeartbeatReply {
  term : UInt64
  context : Bytes
} derive(Eq)

///|
/// Arguments for the AppendEntries RPC (Raft §5.3), used by the leader both
/// to replicate log entries and, with an empty `entries`, as a heartbeat.
pub(all) struct AppendEntriesArgs {
  term : UInt64
  leader_id : String
  prev_log_index : UInt64
  prev_log_term : UInt64
  entries : Array[Entry]
  leader_commit : UInt64
} derive(Eq)

///|
/// Reply to an AppendEntries RPC. `success` is true when the follower's log
/// contained a matching entry at `prev_log_index` and the entries were stored.
/// On success `match_index` is the highest index the follower now agrees on, so
/// the leader can advance its progress without guessing. On rejection
/// `conflict_index` is a hint at where the two logs diverge, letting the leader
/// back off in one jump instead of decrementing nextIndex one entry at a time
/// (the optimization sketched in Raft §5.3).
pub(all) struct AppendEntriesReply {
  term : UInt64
  success : Bool
  match_index : UInt64
  conflict_index : UInt64
  // The term at `conflict_index` (0 if unknown), so the leader can run the same
  // findConflictByTerm jump on its own log and skip a whole run of mismatched
  // terms in one retry (Raft §5.3, two-sided).
  conflict_term : UInt64
  /// The probe point the follower rejected, echoing back the leader's
  /// `prev_log_index`. This is etcd's `MsgAppResp.Index` on a rejection
  /// (raft.go:1828, `Index: m.GetIndex()`): the leader feeds it as the
  /// `rejected` argument to `MaybeDecrTo`, whose staleness guard compares it
  /// against `next_index - 1` so a reordered reject for an entry no longer in
  /// flight is discarded rather than driving a spurious back-off. On success it
  /// is 0 and carries no meaning.
  reject_index : UInt64
} derive(Eq)
