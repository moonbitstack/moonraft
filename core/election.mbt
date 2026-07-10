///|
/// Whether a candidate's log, described by its last entry's term and index,
/// is at least as up-to-date as this node's log (Raft §5.4.1). A log with a
/// higher last term wins; on a tie the longer log wins.
fn Node::candidate_log_up_to_date(
  self : Node,
  last_log_term : UInt64,
  last_log_index : UInt64,
) -> Bool {
  let my_term = self.last_log_term()
  last_log_term > my_term ||
  (last_log_term == my_term && last_log_index >= self.last_log_index())
}

///|
/// Handle a RequestVote RPC (Raft §5.2, §5.4.1). A larger term makes this
/// node step down first. The vote is granted only when this node has not yet
/// voted for a different candidate in the term and the candidate's log is at
/// least as up-to-date as its own.
pub fn Node::handle_request_vote(
  self : Node,
  args : RequestVoteArgs,
) -> RequestVoteReply {
  if args.term < self.current_term {
    return { term: self.current_term, vote_granted: false }
  }
  if args.term > self.current_term {
    self.become_follower(args.term)
  }
  let can_vote = match self.voted_for {
    None => true
    Some(id) => id == args.candidate_id
  }
  let granted = can_vote &&
    self.candidate_log_up_to_date(args.last_log_term, args.last_log_index)
  if granted {
    self.voted_for = Some(args.candidate_id)
  }
  { term: self.current_term, vote_granted: granted }
}
