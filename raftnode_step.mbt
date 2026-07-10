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
/// How an incoming message's term compares with ours. Classifying once, up
/// front, is what lets `step` treat a stale message structurally instead of
/// leaving each handler to remember the check — the omission that was bug B1.
priv enum TermRel {
  Stale
  Aligned
  Ahead
}

///|
fn RaftNode::term_rel(self : RaftNode, msg : Message) -> TermRel {
  let t = msg.term()
  let cur = self.core.current_term()
  if t < cur {
    Stale
  } else if t > cur {
    Ahead
  } else {
    Aligned
  }
}

///|
/// Process one incoming message and return the messages to send in reply. This
/// is the single entry point the transport drives; together with `tick` it is
/// the whole externally-visible behaviour of a server.
///
/// The term is classified first (Raft §5.1). A stale message is never allowed
/// to reach a handler as if it were current: acting on a stale *response* is
/// exactly what let a late `VoteResp` be miscounted into a false majority
/// (double leader) or a late `AppendResp` inflate a follower's progress.
pub fn RaftNode::step(self : RaftNode, msg : Message) -> Array[Message] {
  match self.term_rel(msg) {
    Stale => self.on_stale(msg)
    Ahead => {
      // A higher-term vote solicitation is refused while we are leased to a
      // leader — and crucially *without* adopting the challenger's term, so a
      // disruptive candidate cannot force the cluster up a term (§4.2.3). Pre-
      // vote is already handled in its own dispatch (it never bumps our term).
      if self.in_lease() && msg.payload is Vote(_) {
        return [
          Message::new(
            self.id,
            msg.from,
            VoteResp({ term: self.core.current_term(), vote_granted: false }),
          ),
        ]
      }
      self.adopt_higher_term(msg)
      self.dispatch(msg)
    }
    Aligned => self.dispatch(msg)
  }
}

///|
/// Handle a message stamped with a term below ours. Responses are dropped, so a
/// reply from a superseded term can never move our state. Requests are still
/// answered — the reply carries our current term, which is how the sender
/// learns it is behind and steps down — and their handlers already reject on
/// the stale term. (etcd's `Step` does the same: ignore stale responses; for
/// stale MsgApp/MsgVote/MsgSnap reveal our term to the sender.)
fn RaftNode::on_stale(self : RaftNode, msg : Message) -> Array[Message] {
  match msg.payload {
    VoteResp(_)
    | PreVoteResp(_)
    | AppendResp(_)
    | SnapshotResp(_)
    | TimeoutNow(_) => []
    PreVote(_) | Vote(_) | Append(_) | Snapshot(_) => self.dispatch(msg)
  }
}

///|
/// Adopt a strictly higher term and step down — except for pre-vote traffic,
/// which is hypothetical (a pre-candidate probes under a term it has not
/// adopted) and must neither move our term nor make us yield.
fn RaftNode::adopt_higher_term(self : RaftNode, msg : Message) -> Unit {
  match msg.payload {
    PreVote(_) | PreVoteResp(_) => ()
    _ => {
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
}

///|
/// Route a term-checked message to its handler.
fn RaftNode::dispatch(self : RaftNode, msg : Message) -> Array[Message] {
  match msg.payload {
    PreVote(args) => self.handle_pre_vote(msg.from, args)
    PreVoteResp(reply) => self.handle_pre_vote_resp(msg.from, reply)
    Vote(args) => self.handle_vote(msg.from, args)
    VoteResp(reply) => self.handle_vote_resp(msg.from, reply)
    Append(args) => self.handle_append(msg.from, args)
    AppendResp(reply) => self.handle_append_resp(msg.from, reply)
    Snapshot(args) => self.handle_snapshot(msg.from, args)
    SnapshotResp(reply) => self.handle_snapshot_resp(msg.from, reply)
    TimeoutNow(term) => self.handle_timeout_now(term)
  }
}

///|
/// Handle a TimeoutNow from the leader (Raft §3.10, leadership transfer). The
/// target campaigns at once — skipping pre-vote and the usual election-timeout
/// wait — so an orderly handover completes in about one round trip rather than
/// waiting for the old leader to look dead.
fn RaftNode::handle_timeout_now(
  self : RaftNode,
  term : UInt64,
) -> Array[Message] {
  if term < self.core.current_term() || self.core.role() == Leader {
    return []
  }
  self.reset_timeout()
  self.start_real_campaign()
}

///|
fn RaftNode::handle_pre_vote(
  self : RaftNode,
  from : String,
  args : RequestVoteArgs,
) -> Array[Message] {
  // B3: while we still have contact with a leader (check-quorum lease), refuse
  // to encourage a challenger, so a partitioned node that keeps timing out
  // cannot force needless elections and inflate the term (§4.2.3 disruption).
  let granted = !self.in_lease() &&
    args.term > self.core.current_term() &&
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
  guard self.in_pre_campaign else { return [] }
  // A rejection revealing a higher term ends the pre-campaign (etcd only skips
  // stepping down for a *granted* pre-vote, which carries our own future term).
  if !reply.vote_granted && reply.term > self.core.current_term() {
    self.core.become_follower(reply.term)
    self.in_pre_campaign = false
    return []
  }
  self.votes[from] = reply.vote_granted
  match self.config.vote_result(self.votes) {
    VoteWon => self.start_real_campaign()
    VoteLost => {
      self.in_pre_campaign = false
      []
    }
    VotePending => []
  }
}

///|
fn RaftNode::handle_vote(
  self : RaftNode,
  from : String,
  args : RequestVoteArgs,
) -> Array[Message] {
  // B3: reject a real vote too while leased to a leader (§4.2.3).
  if self.in_lease() {
    return [
      Message::new(
        self.id,
        from,
        VoteResp({ term: self.core.current_term(), vote_granted: false }),
      ),
    ]
  }
  let reply = self.core.handle_request_vote(args)
  if reply.vote_granted {
    self.election_elapsed = 0
    self.leader_id = None
  }
  [Message::new(self.id, from, VoteResp(reply))]
}

///|
/// Whether we are within a check-quorum lease: check-quorum is on, we currently
/// recognise a leader, and our election timer has not yet expired. During the
/// lease we treat the leader as alive and reject vote solicitations.
fn RaftNode::in_lease(self : RaftNode) -> Bool {
  self.check_quorum &&
  self.leader_id is Some(_) &&
  self.election_elapsed < self.election_timeout
}

///|
fn RaftNode::handle_vote_resp(
  self : RaftNode,
  from : String,
  reply : RequestVoteReply,
) -> Array[Message] {
  guard self.core.role() == Candidate else { return [] }
  self.votes[from] = reply.vote_granted
  // Count with the quorum's three-valued result so a candidate that is denied
  // by a majority steps down at once instead of waiting out its timer.
  match self.config.vote_result(self.votes) {
    VoteWon => {
      self.become_leader()
      self.bcast_append()
    }
    VoteLost => {
      self.core.become_follower(self.core.current_term())
      []
    }
    VotePending => []
  }
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
  // A follower folds committed configuration changes into its own view of the
  // membership as its commit index advances, so every server shares one config.
  self.apply_committed_conf()
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
        p.maybe_update(reply.match_index) |> ignore
        // A first ack promotes a prober to streaming; a streaming follower just
        // releases the acknowledged slots of its in-flight window.
        match p.state {
          Probe => p.become_replicate()
          Replicate => p.free_le(reply.match_index)
          Snapshot => ()
        }
        let out : Array[Message] = []
        // A leadership-transfer target that has now caught up to the leader's
        // last index is told to time out and campaign at once (§3.10).
        if self.lead_transferee == Some(from) &&
          p.match_index == self.core.last_log_index() {
          out.push(
            Message::new(self.id, from, TimeoutNow(self.core.current_term())),
          )
        }
        if self.maybe_commit() {
          // A new commit: tell every follower, which also carries entries to
          // any that are behind.
          for m in self.bcast_append() {
            out.push(m)
          }
        } else if p.match_index < self.core.last_log_index() && !p.is_paused() {
          // This follower acked (often a heartbeat) but is still behind; push
          // it the next entries now instead of waiting for a client proposal.
          out.push(self.send_to(from))
        }
        out
      } else {
        // Back off. The follower's hint is refined against the leader's own log
        // (two-sided findConflictByTerm): if it reported the term at its
        // conflict point, jump to the last leader index whose term does not
        // exceed it, skipping a whole run of superseded entries in one retry.
        let rejected = p.next_index - 1
        let next_probe = if reply.conflict_term > 0 {
          let (idx, _) = self.core.find_conflict_by_term(
            reply.conflict_index,
            reply.conflict_term,
          )
          idx
        } else {
          reply.conflict_index
        }
        if p.maybe_decr_to(rejected, next_probe) {
          if p.state == Replicate {
            p.become_probe()
          }
          [self.send_to(from)]
        } else {
          []
        }
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
