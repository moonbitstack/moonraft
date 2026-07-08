///|
/// Whether a candidate log described by `(cand_term, cand_index)` is at least as
/// up-to-date as `core`'s (Raft §5.4.1): a higher last term wins, and on a tie
/// the longer log wins.
fn log_up_to_date(core : Node, cand_term : UInt64, cand_index : UInt64) -> Bool {
  let my_term = core.last_log_term()
  cand_term > my_term ||
  (cand_term == my_term && cand_index >= core.last_log_index())
}

///|
/// Process one incoming message and return the messages to send in reply. This
/// is the single entry point the transport drives; together with `tick` it is
/// the whole externally-visible behaviour of a server.
pub fn RaftNode::step(self : RaftNode, msg : Message) -> Array[Message] {
  // Adopt a strictly higher term and step down — except for pre-vote traffic,
  // which is hypothetical and must not move our term or make us yield.
  match msg.payload {
    PreVote(_) | PreVoteResp(_) => ()
    _ =>
      if msg.term() > self.core.current_term() {
        self.core.become_follower(msg.term())
        self.in_pre_campaign = false
        self.leader_id = match msg.payload {
          Append(a) => Some(a.leader_id)
          Snapshot(a) => Some(a.leader_id)
          _ => None
        }
        self.reset_timeout()
      }
  }
  match msg.payload {
    PreVote(args) => self.handle_pre_vote(msg.from, args)
    PreVoteResp(reply) => self.handle_pre_vote_resp(msg.from, reply)
    Vote(args) => self.handle_vote(msg.from, args)
    VoteResp(reply) => self.handle_vote_resp(msg.from, reply)
    Append(args) => self.handle_append(msg.from, args)
    AppendResp(reply) => self.handle_append_resp(msg.from, reply)
    Snapshot(args) => self.handle_snapshot(msg.from, args)
    SnapshotResp(reply) => self.handle_snapshot_resp(msg.from, reply)
  }
}

///|
fn RaftNode::handle_pre_vote(
  self : RaftNode,
  from : String,
  args : RequestVoteArgs,
) -> Array[Message] {
  let granted = args.term > self.core.current_term() &&
    log_up_to_date(self.core, args.last_log_term, args.last_log_index)
  let term = if granted { args.term } else { self.core.current_term() }
  [Message::new(self.id, from, PreVoteResp({ term, vote_granted: granted }))]
}

///|
fn RaftNode::handle_pre_vote_resp(
  self : RaftNode,
  from : String,
  reply : RequestVoteReply,
) -> Array[Message] {
  if !self.in_pre_campaign {
    return []
  }
  if !reply.vote_granted && reply.term > self.core.current_term() {
    self.core.become_follower(reply.term)
    self.in_pre_campaign = false
    return []
  }
  self.votes[from] = reply.vote_granted
  if self.config.has_majority(self.granted()) {
    return self.start_real_campaign()
  }
  []
}

///|
fn RaftNode::handle_vote(
  self : RaftNode,
  from : String,
  args : RequestVoteArgs,
) -> Array[Message] {
  let reply = self.core.handle_request_vote(args)
  if reply.vote_granted {
    self.election_elapsed = 0
    self.leader_id = None
  }
  [Message::new(self.id, from, VoteResp(reply))]
}

///|
fn RaftNode::handle_vote_resp(
  self : RaftNode,
  from : String,
  reply : RequestVoteReply,
) -> Array[Message] {
  if self.core.role() != Candidate {
    return []
  }
  if reply.term > self.core.current_term() {
    self.core.become_follower(reply.term)
    return []
  }
  self.votes[from] = reply.vote_granted
  if self.config.has_majority(self.granted()) {
    self.become_leader()
    return self.bcast_append()
  }
  []
}

///|
fn RaftNode::handle_append(
  self : RaftNode,
  from : String,
  args : AppendEntriesArgs,
) -> Array[Message] {
  let reply = self.core.handle_append_entries(args)
  // reply.term == args.term means we did not reject for being ahead: the sender
  // is a legitimate current leader, so recognise it and defer our election.
  if reply.term == args.term {
    self.leader_id = Some(args.leader_id)
    self.election_elapsed = 0
    self.in_pre_campaign = false
  }
  [Message::new(self.id, from, AppendResp(reply))]
}

///|
fn RaftNode::handle_append_resp(
  self : RaftNode,
  from : String,
  reply : AppendEntriesReply,
) -> Array[Message] {
  if self.core.role() != Leader {
    return []
  }
  if reply.term > self.core.current_term() {
    self.core.become_follower(reply.term)
    return []
  }
  match self.progress.get(from) {
    None => []
    Some(p) => {
      p.mark_active()
      if reply.success {
        let _ = p.maybe_update(reply.match_index)
        p.become_replicate()
        if self.maybe_commit() {
          self.bcast_append()
        } else {
          []
        }
      } else if p.maybe_decrease(reply.conflict_index) {
        p.become_probe()
        [self.send_to(from)]
      } else {
        []
      }
    }
  }
}

///|
fn RaftNode::handle_snapshot(
  self : RaftNode,
  from : String,
  args : InstallSnapshotArgs,
) -> Array[Message] {
  let reply = self.core.handle_install_snapshot(args)
  if reply.term == args.term {
    self.leader_id = Some(args.leader_id)
    self.election_elapsed = 0
    self.in_pre_campaign = false
  }
  [Message::new(self.id, from, SnapshotResp(reply))]
}

///|
fn RaftNode::handle_snapshot_resp(
  self : RaftNode,
  from : String,
  reply : InstallSnapshotReply,
) -> Array[Message] {
  if self.core.role() != Leader {
    return []
  }
  if reply.term > self.core.current_term() {
    self.core.become_follower(reply.term)
    return []
  }
  match self.progress.get(from) {
    None => []
    Some(p) => {
      let _ = p.maybe_update(self.core.snapshot_index)
      p.become_probe()
      p.mark_active()
      [self.send_to(from)]
    }
  }
}
