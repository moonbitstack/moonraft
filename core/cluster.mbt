///|
/// Build the RequestVote arguments this node would send while standing for
/// election in its current term.
pub fn Node::request_vote_args(self : Node) -> RequestVoteArgs {
  {
    term: self.current_term,
    candidate_id: self.id,
    last_log_index: self.last_log_index(),
    last_log_term: self.last_log_term(),
  }
}

///|
/// Run one round of leader election synchronously: `candidate` advances its
/// term and asks every peer for a vote, counting its own. It becomes leader on
/// a majority. If a peer reveals a higher term the candidate steps down at once
/// and the round fails. Returns whether the candidate won.
pub fn run_election(candidate : Node, peers : Array[Node]) -> Bool {
  candidate.become_candidate()
  let args = candidate.request_vote_args()
  let mut votes = 1
  for peer in peers {
    let reply = peer.handle_request_vote(args)
    if reply.term > candidate.current_term() {
      candidate.become_follower(reply.term)
      return false
    }
    if reply.vote_granted {
      votes = votes + 1
    }
  }
  let majority = (peers.length() + 1) / 2 + 1
  if votes >= majority {
    candidate.become_leader()
    true
  } else {
    false
  }
}
