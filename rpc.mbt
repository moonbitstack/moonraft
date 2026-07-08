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
} derive(Eq)
