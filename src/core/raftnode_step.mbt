// Copyright 2015 The etcd Authors
// Copyright 2026 Leo Cheng
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
  let out = self.step_message(msg)
  // A configuration change folded into the configuration can emit messages of its
  // own — the probe to a server that just joined — so they are collected too.
  for m in self.take_pending() {
    out.push(m)
  }
  if !self.defer_responses {
    return out
  }
  self.split_responses(out)
}

///|
/// Whether `m` is a response whose publication depends on state being durable
/// (etcd's `send`: `MsgAppResp`, `MsgVoteResp`, `MsgPreVoteResp`). Publishing a
/// vote before it is on disk would let a server vote twice across a restart, and
/// acknowledging an append before the entry is on disk would let a committed
/// entry be lost — so these wait (Raft thesis §3.8).
fn is_durability_response(m : Message) -> Bool {
  match m.payload {
    VoteResp(_) | PreVoteResp(_) | AppendResp(_) => true
    _ => false
  }
}

///|
/// Route the durability responses in `out` to the queue that `RawNode` drains
/// once the Ready they were decided in has been handled (etcd's
/// `msgsAfterAppend`), and return the messages that may go out at once.
fn RaftNode::split_responses(
  self : RaftNode,
  out : Array[Message],
) -> Array[Message] {
  let immediate : Array[Message] = []
  for m in out {
    if is_durability_response(m) {
      self.deferred.push(m)
    } else {
      immediate.push(m)
    }
  }
  immediate
}

///|
/// The message dispatch behind `step`: term handling, then the role's handler.
fn RaftNode::step_message(self : RaftNode, msg : Message) -> Array[Message] {
  self.tracer.on_event(ReceiveMessage(msg))
  // Local, routable messages (etcd's Term-0 messages) bypass the term check: a
  // client request carries no term, so it is neither stale nor ahead. The read-
  // index *reply* does carry a term and is dispatched normally below.
  match msg.payload {
    // Storage-thread replies are driver-level: the log cursors they advance live
    // in the Ready driver, not in the core, so a core that is stepped directly
    // has nothing to do with them.
    StorageAppendResp(_) | StorageApplyResp(_) => return []
    Propose(entries) => return self.step_propose(msg.from, entries)
    ReadIndex(ctx) => return self.step_read_index(msg.from, ctx)
    TransferLeader(target) => return self.step_transfer_leader(msg.from, target)
    ForgetLeader => return self.step_forget_leader()
    _ => ()
  }
  match self.term_rel(msg) {
    Stale => self.on_stale(msg)
    Ahead => {
      // A higher-term vote solicitation is refused while we are leased to a
      // leader — and crucially *without* adopting the challenger's term, so a
      // disruptive candidate cannot force the cluster up a term (§4.2.3). A
      // *forced* vote (a leadership transfer, etcd's campaignTransfer) is exempt:
      // the outgoing leader is deliberately handing off, so the lease must not
      // block it. Pre-vote is handled in its own dispatch (it never bumps term).
      // The solicitation is dropped without a reply: answering would tell the
      // challenger our term, and a disruptive candidate would only inflate it.
      if self.in_lease() && !msg.force {
        match msg.payload {
          Vote(args) => {
            self.log_lease_ignored("MsgVote", msg.from, args)
            return []
          }
          PreVote(args) => {
            self.log_lease_ignored("MsgPreVote", msg.from, args)
            return []
          }
          _ => ()
        }
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
    | HeartbeatResp(_)
    | ReadIndexResp(_)
    | TimeoutNow(_) => []
    PreVote(_) | Vote(_) | Append(_) | Heartbeat(_) | Snapshot(_) => {
      self.logger.log(
        Info,
        "\{self.id} [term: \{self.core.current_term()}] ignored a \{msg.type_name()} message with lower term from \{msg.from} [term: \{msg.term()}]",
      )
      self.dispatch(msg)
    }
    // Local, routable messages are intercepted term-free in `step` and never
    // reach the term classifier, so one arriving here is a routing bug.
    Propose(_)
    | ReadIndex(_)
    | TransferLeader(_)
    | ForgetLeader
    | StorageAppendResp(_)
    | StorageApplyResp(_) => abort("local message reached on_stale")
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
      self.logger.log(
        Info,
        "\{self.id} [term: \{self.core.current_term()}] received a \{msg.type_name()} message with higher term from \{msg.from} [term: \{msg.term()}]",
      )
      // Stepping down goes through the same transition every other path uses, so
      // it announces `became follower` in upstream's wording and resets the
      // election timer exactly once (etcd's `becomeFollower`).
      self.become_follower(msg.term())
      // A current leader that talks to us is recorded as the leader we follow;
      // a vote solicitation carries no leader.
      self.leader_id = match msg.payload {
        Append(a) => Some(a.leader_id)
        Heartbeat(a) => Some(a.leader_id)
        Snapshot(a) => Some(a.leader_id)
        _ => None
      }
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
    Heartbeat(args) => self.handle_heartbeat(msg.from, args)
    HeartbeatResp(reply) => self.handle_heartbeat_resp(msg.from, reply)
    Snapshot(args) => self.handle_snapshot(msg.from, args)
    TimeoutNow(term) => self.handle_timeout_now(term)
    ReadIndexResp(reply) => self.handle_read_index_resp(reply)
    // Local, routable messages are handled term-free in `step` and never reach
    // dispatch, so one arriving here is a routing bug.
    Propose(_)
    | ReadIndex(_)
    | TransferLeader(_)
    | ForgetLeader
    | StorageAppendResp(_)
    | StorageApplyResp(_) => abort("local message reached dispatch")
  }
}

///|
/// Step a client proposal (etcd's MsgProp). On a leader the carried entries are
/// appended under the current term and replicated. On a follower the proposal is
/// forwarded to the known leader — preserving the original requester as `from` so
/// a chain of forwards still points back to the origin — unless forwarding is
/// disabled or no leader is known, in which case it is dropped (etcd's
/// ErrProposalDropped). A candidate has no leader to forward to and drops it.
fn RaftNode::step_propose(
  self : RaftNode,
  from : String,
  entries : Array[Entry],
) -> Array[Message] {
  match self.core.role() {
    Leader => {
      // A leader handing off leadership stops accepting proposals so the transfer
      // target can catch up to a fixed log (§3.10).
      if self.lead_transferee is Some(_) {
        return []
      }
      let mut total = 0UL
      for e in entries {
        total = total + e.command.length().to_uint64()
      }
      // Drop the batch if it would push the uncommitted tail over its quota
      // (etcd's ErrProposalDropped), unless the tail is currently empty.
      if !self.increase_uncommitted_size(total) {
        return []
      }
      for e in entries {
        if e.is_conf_change() {
          let is_leave = match ConfChangeV2::decode(e.command) {
            Some(v2) => v2.is_leave()
            None => false
          }
          if self.conf_change_refused(is_leave) {
            // A refused change is announced with the reason, because the client's
            // proposal silently turns into a no-op entry and the log line is the
            // only place that says why (etcd's `stepLeader` MsgProp).
            let what = match ConfChangeV2::decode(e.command) {
              Some(v2) => v2.describe()
              None =>
                match ConfChange::decode(e.command) {
                  Some(cc) => cc.describe_proto()
                  None => ""
                }
            }
            let reason = if self.config.is_joint() && !is_leave {
              "must transition out of joint config first"
            } else {
              "previous configuration change has not been applied yet"
            }
            self.logger.log(
              Info,
              "\{self.id} ignoring conf change \{what} at config \{self.config.describe()}: \{reason}",
            )
            // etcd replaces a refused conf change with an empty Normal entry: the
            // log position is still consumed but no membership change is applied.
            self.core.append(self.core.current_term(), b"") |> ignore
          } else {
            self.core.append_conf(self.core.current_term(), e.command) |> ignore
            self.pending_conf_index = self.core.last_log_index()
          }
        } else {
          self.core.append(self.core.current_term(), e.command) |> ignore
        }
      }
      // Self-ack the appended tail before committing, as etcd's leader self-acks
      // its own writes (it sends no MsgApp to itself); the leader's own progress
      // must reflect its stored log for `maybe_commit` to count it.
      self.self_ack()
      self.maybe_commit() |> ignore
      self.bcast_append()
    }
    // A (pre-)candidate has no leader to forward to and drops the proposal
    // (etcd's stepCandidate). PreCandidate is unreachable via `core.role()` —
    // the core stays a follower during pre-vote — but the arm keeps the match
    // exhaustive and mirrors candidate behaviour.
    Candidate | PreCandidate => []
    Follower =>
      match self.leader_id {
        Some(lead) =>
          if self.no_forward {
            []
          } else {
            [Message::new(from, lead, Propose(entries))]
          }
        None => []
      }
  }
}

///|
/// Step a linearizable-read request (etcd's MsgReadIndex). A leader serves it
/// (locally, or by answering a remote requester once its leadership is confirmed
/// for the read). A follower forwards it to the leader, preserving the origin as
/// `from` so the answer routes back to the requester; with no known leader it is
/// dropped.
fn RaftNode::step_read_index(
  self : RaftNode,
  from : String,
  ctx : Bytes,
) -> Array[Message] {
  if self.core.role() == Leader {
    return self.lead_read_index(from, ctx)
  }
  match self.leader_id {
    Some(lead) => [Message::new(from, lead, ReadIndex(ctx))]
    None => []
  }
}

///|
/// A follower records the read index the leader confirmed (etcd appends it to
/// readStates), so a caller waiting on `take_read_states` observes the index the
/// state machine must reach before answering the read.
fn RaftNode::handle_read_index_resp(
  self : RaftNode,
  reply : ReadIndexResp,
) -> Array[Message] {
  self.read_only.ready.push({ index: reply.index, request_ctx: reply.context, })
  []
}

///|
/// Step a leadership-transfer request (etcd's MsgTransferLeader). On a leader it
/// begins the handoff to `target`; on a follower it is forwarded to the leader so
/// a transfer may be requested from any server.
fn RaftNode::step_transfer_leader(
  self : RaftNode,
  from : String,
  target : String,
) -> Array[Message] {
  if self.core.role() == Leader {
    self.logger.log(
      Info,
      "\{self.id} [term \{self.core.current_term()}] starts to transfer leadership to \{target}",
    )
    return self.transfer_leadership(target)
  }
  match self.leader_id {
    Some(lead) => [Message::new(from, lead, TransferLeader(target))]
    None => []
  }
}

///|
/// Step a forget-leader request (etcd's MsgForgetLeader): a follower drops its
/// recognised leader without moving its term or resetting its election timer, so
/// a follower stuck behind a partition can re-enable (pre)votes at once rather
/// than waiting out a full election timeout. Ignored under lease-based reads,
/// where forgetting the leader would undermine the lease that reads depend on
/// (etcd rejects MsgForgetLeader when ReadOnlyLeaseBased). A candidate or leader,
/// which recognises no current leader anyway, is a no-op.
fn RaftNode::step_forget_leader(self : RaftNode) -> Array[Message] {
  if !self.read_only.safe {
    return []
  }
  if self.core.role() == Follower {
    if self.leader_id is Some(lead) {
      self.logger.log(
        Info,
        "\{self.id} forgetting leader \{lead} at term \{self.core.current_term()}",
      )
    }
    self.leader_id = None
  }
  []
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
  // A transfer campaign routes through the same guard as a timeout election
  // (etcd's `hup(campaignTransfer)`): refuse while a committed conf change is
  // unapplied.
  if !self.config.contains(self.id) || self.has_unapplied_conf_changes() {
    return []
  }
  self.reset_timeout()
  self.start_real_campaign(force=true)
}

///|
fn RaftNode::handle_pre_vote(
  self : RaftNode,
  from : String,
  args : RequestVoteArgs,
) -> Array[Message] {
  // etcd's `canVote` (raft.go:1214) governs pre-votes as well as real votes: a
  // node may (pre)vote when it has already voted for this candidate, or when it
  // has neither voted nor recognised a leader this term, or — the pre-vote-only
  // clause — when the solicitation is for a strictly future term. The first two
  // clauses grant a pre-vote at the *same* term, which the earlier port missed by
  // requiring a strictly higher term outright (FINDINGS_LEDGER #23).
  let can_vote = self.core.voted_for == Some(from) ||
    (self.core.voted_for == None && self.leader_id == None) ||
    args.term > self.core.current_term()
  // B3: while we still have contact with a leader (check-quorum lease), refuse
  // to encourage a challenger, so a partitioned node that keeps timing out
  // cannot force needless elections and inflate the term (§4.2.3 disruption).
  let granted = !self.in_lease() &&
    can_vote &&
    log_up_to_date(self.core, args.last_log_term, args.last_log_index)
  let term = if granted { args.term } else { self.core.current_term() }
  self.log_vote_decision("MsgPreVote", from, args, granted, self.core.voted_for)
  [Message::new(self.id, from, PreVoteResp({ term, vote_granted: granted, }))]
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
  self.log_vote_response(from, reply.vote_granted, "MsgPreVoteResp")
  self.record_vote(from, reply.vote_granted)
  let (granted, rejected) = self.vote_tally()
  self.logger.log(
    Info,
    "\{self.id} has received \{granted} MsgPreVoteResp votes and \{rejected} vote rejections",
  )
  match self.config.vote_result(self.votes) {
    @quorum.VoteWon => self.start_real_campaign()
    @quorum.VoteLost => {
      // A majority declined the pre-vote: step back to follower rather than
      // linger in the PreCandidate state (the cluster already has a leader).
      self.become_follower(self.core.current_term())
      []
    }
    @quorum.VotePending => []
  }
}

///|
fn RaftNode::handle_vote(
  self : RaftNode,
  from : String,
  args : RequestVoteArgs,
) -> Array[Message] {
  // At the *same* term a lease does not silence us: the reply is a plain refusal
  // (etcd's `canVote` finds a leader, so the vote is not granted, and the sender
  // learns we already follow someone).
  if self.in_lease() {
    self.log_vote_decision("MsgVote", from, args, false, self.core.voted_for)
    return [
      Message::new(
        self.id,
        from,
        VoteResp({ term: self.core.current_term(), vote_granted: false, }),
      ),
    ]
  }
  let vote_before = self.core.voted_for
  let reply = self.core.handle_request_vote(args)
  self.log_vote_decision("MsgVote", from, args, reply.vote_granted, vote_before)
  if reply.vote_granted {
    self.election_elapsed = 0
    self.leader_id = None
  }
  [Message::new(self.id, from, VoteResp(reply))]
}

///|
/// Announce a solicitation ignored because we are still leased to a leader:
/// `1 [logterm: 1, index: 11, vote: 1] ignored MsgVote from 2 [logterm: 1,
/// index: 11] at term 1: lease is not expired (remaining ticks: 3)`.
fn RaftNode::log_lease_ignored(
  self : RaftNode,
  kind : String,
  from : String,
  args : RequestVoteArgs,
) -> Unit {
  let vote = match self.core.voted_for {
    Some(v) => v
    None => "0"
  }
  let remaining = self.election_timeout - self.election_elapsed
  self.logger.log(
    Info,
    "\{self.id} [logterm: \{self.core.last_log_term()}, index: \{self.core.last_log_index()}, vote: \{vote}] ignored \{kind} from \{from} [logterm: \{args.last_log_term}, index: \{args.last_log_index}] at term \{self.core.current_term()}: lease is not expired (remaining ticks: \{remaining})",
  )
}

///|
/// Announce one (pre-)vote acknowledgement, as etcd's `poll` does. A rejection
/// is logged as such: a run of rejections is how a candidate discovers it cannot
/// win, and the count that follows says by how much.
fn RaftNode::log_vote_response(
  self : RaftNode,
  from : String,
  granted : Bool,
  kind : String,
) -> Unit {
  if granted {
    self.logger.log(
      Info,
      "\{self.id} received \{kind} from \{from} at term \{self.core.current_term()}",
    )
  } else {
    self.logger.log(
      Info,
      "\{self.id} received \{kind} rejection from \{from} at term \{self.core.current_term()}",
    )
  }
}

///|
/// Announce a (pre-)vote decision in upstream's shape:
/// `1 [logterm: 1, index: 2, vote: 0] cast MsgVote for 2 [logterm: 1, index: 2]
/// at term 1`. The leading bracket is this node's own log position and vote, the
/// trailing one the candidate's, so one line says whether the candidate was up
/// to date and which way the vote went.
fn RaftNode::log_vote_decision(
  self : RaftNode,
  kind : String,
  from : String,
  args : RequestVoteArgs,
  granted : Bool,
  vote_before : String?,
) -> Unit {
  let vote = match vote_before {
    Some(v) => v
    None => "0"
  }
  // Upstream distinguishes the two verdicts by preposition: a granted vote is
  // cast *for* the candidate, a refused one is rejected *from* it.
  let verdict = if granted { "cast" } else { "rejected" }
  let preposition = if granted { "for" } else { "from" }
  self.logger.log(
    Info,
    "\{self.id} [logterm: \{self.core.last_log_term()}, index: \{self.core.last_log_index()}, vote: \{vote}] \{verdict} \{kind} \{preposition} \{from} [logterm: \{args.last_log_term}, index: \{args.last_log_index}] at term \{self.core.current_term()}",
  )
}

///|
/// How many (pre-)votes were granted and how many were rejected, as etcd's
/// tracker `TallyVotes` reports them.
fn RaftNode::vote_tally(self : RaftNode) -> (Int32, Int32) {
  let mut granted = 0
  let mut rejected = 0
  for _, v in self.votes {
    if v {
      granted = granted + 1
    } else {
      rejected = rejected + 1
    }
  }
  (granted, rejected)
}

///|
/// Record a (pre-)vote from `id` (etcd's tracker `RecordVote`): the first vote a
/// node casts in a term is kept, and a later differing response from the same
/// node is ignored. This is what stops a duplicated or reordered vote message
/// from flipping a vote already counted toward the tally.
fn RaftNode::record_vote(self : RaftNode, id : String, granted : Bool) -> Unit {
  if !self.votes.contains(id) {
    self.votes[id] = granted
  }
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
  self.log_vote_response(from, reply.vote_granted, "MsgVoteResp")
  self.record_vote(from, reply.vote_granted)
  // Count with the quorum's three-valued result so a candidate that is denied
  // by a majority steps down at once instead of waiting out its timer.
  let (granted, rejected) = self.vote_tally()
  self.logger.log(
    Info,
    "\{self.id} has received \{granted} MsgVoteResp votes and \{rejected} vote rejections",
  )
  match self.config.vote_result(self.votes) {
    @quorum.VoteWon => {
      self.become_leader()
      self.bcast_append()
    }
    @quorum.VoteLost => {
      self.core.become_follower(self.core.current_term())
      []
    }
    @quorum.VotePending => []
  }
}

///|

///|
/// Recognise `lead` as the leader at `term` and defer our own election: a
/// follower keeps its role, while a candidate or pre-candidate steps down and
/// says so (`becomeFollower` logs once, and only when the role actually moves).
fn RaftNode::adopt_leader(
  self : RaftNode,
  term : UInt64,
  lead : String,
) -> Unit {
  if self.core.role() != Follower {
    self.become_follower(term)
  }
  self.leader_id = Some(lead)
  self.election_elapsed = 0
  self.in_pre_campaign = false
}

///|
fn RaftNode::handle_append(
  self : RaftNode,
  from : String,
  args : AppendEntriesArgs,
) -> Array[Message] {
  let reply = self.core.handle_append_entries(args)
  if !reply.success && reply.term == args.term {
    // Our log does not match at the anchor. Upstream names the term we hold
    // there (zero when the index is past our log) beside the leader's, which is
    // what makes a divergent tail diagnosable from one line.
    self.logger.log(
      Debug,
      "\{self.id} [logterm: \{self.core.term_at(args.prev_log_index)}, index: \{args.prev_log_index}] rejected MsgApp [logterm: \{args.prev_log_term}, index: \{args.prev_log_index}] from \{args.leader_id}",
    )
  }
  // reply.term == args.term means we did not reject for being ahead: the sender
  // is a legitimate current leader, so recognise it and defer our election.
  if reply.term == args.term {
    // A current leader talking to us: recognise it, and step down if we were
    // campaigning — a candidate that hears from a leader at its own term yields
    // to it (etcd's `stepCandidate`, which calls becomeFollower for MsgApp).
    self.adopt_leader(args.term, args.leader_id)
  }
  // A follower folds committed configuration changes into its own view of the
  // membership as its commit index advances, so every server shares one config.
  self.apply_committed_conf()
  [Message::new(self.id, from, AppendResp(reply))]
}

///|
/// Handle a heartbeat from the current leader (Raft §5.2): recognise the leader,
/// defer our election, advance our commit index toward the leader's, and reply.
fn RaftNode::handle_heartbeat(
  self : RaftNode,
  from : String,
  args : HeartbeatArgs,
) -> Array[Message] {
  if args.term >= self.core.current_term() {
    self.adopt_leader(args.term, args.leader_id)
    // Commit only up to what we actually hold; the leader already caps `commit`.
    let upto = if args.commit < self.core.last_log_index() {
      args.commit
    } else {
      self.core.last_log_index()
    }
    self.core.advance_commit(upto)
    self.apply_committed_conf()
  }
  [
    Message::new(
      self.id,
      from,
      HeartbeatResp({ term: self.core.current_term(), context: args.context, }),
    ),
  ]
}

///|
/// Handle a heartbeat acknowledgement (Raft §5.2). It is pure liveness — no log
/// index — so it marks the follower active (for check-quorum and lease reads)
/// and resumes replication: a follower still behind is sent the entries it
/// lacks, or a content-free probe when its in-flight window is full.
fn RaftNode::handle_heartbeat_resp(
  self : RaftNode,
  from : String,
  reply : HeartbeatReply,
) -> Array[Message] {
  guard self.core.role() == Leader else { return [] }
  let out : Array[Message] = []
  // ReadOnlySafe: an ack echoing a read-index context counts toward confirming
  // the leader still commands a quorum for that read (§6.4). A confirmed read is
  // delivered to its originator — recorded locally, or answered to the follower
  // that forwarded it with a ReadIndexResp.
  if !reply.context.is_empty() {
    self.read_only.recv_ack(from, reply.context)
    for req in self.read_only.maybe_advance(self.config) {
      for m in self.deliver_read(req) {
        out.push(m)
      }
    }
  }
  match self.progress.get(from) {
    None => out
    Some(p) => {
      p.mark_active()
      // A heartbeat response clears the flow throttle (etcd), so one more
      // message goes out to a follower that is still behind.
      p.unpause()
      // The follower was just marked active, so `send_to` yields a message (it
      // withholds only from an inactive follower behind the snapshot).
      if p.match_index < self.core.last_log_index() && p.state != Snapshot {
        if self.send_to(from) is Some(m) {
          out.push(m)
        }
      }
      out
    }
  }
}

///|
/// Report the outcome of a snapshot the leader shipped (etcd's ReportSnapshot /
/// MsgSnapStatus). A failure discards the pending snapshot; either way the
/// follower leaves the Snapshot state and is probed, paused until its next
/// AppendEntries response confirms where it now stands.
pub fn RaftNode::report_snapshot(
  self : RaftNode,
  id : String,
  reject : Bool,
) -> Unit {
  guard self.core.role() == Leader else { return }
  if self.progress.get(id) is Some(p) && p.state == Snapshot {
    if reject {
      p.pending_snapshot = 0
    }
    p.become_probe()
    p.msg_app_flow_paused = true
  }
}

///|
/// Report that a follower is unreachable (etcd's ReportUnreachable). A streaming
/// follower drops back to probing so the leader stops optimistically advancing
/// into messages that are being lost.
pub fn RaftNode::report_unreachable(self : RaftNode, id : String) -> Unit {
  guard self.core.role() == Leader else { return }
  match self.progress.get(id) {
    Some(p) => {
      // Optimistic replication assumes the follower is keeping up; a report that
      // it is unreachable means the in-flight appends are probably lost, so fall
      // back to probing one entry at a time (etcd's MsgUnreachable handling).
      if p.state == Replicate {
        p.become_probe()
      }
      self.logger.log(
        Debug,
        "\{self.id} failed to send message to \{id} because it is unreachable [\{progress_text(p)}]",
      )
    }
    None => ()
  }
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
  // No higher-term guard here: `step` classifies the term first and
  // `adopt_higher_term` steps a leader down before dispatch, so this handler
  // only ever runs at the aligned term (a stale reply is dropped in `on_stale`).
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
          // A snapshotting follower whose ack reconnects it to the leader's log
          // (its match is at or past the snapshot baseline) resumes streaming,
          // aborting the pending snapshot (etcd's StateSnapshot recovery).
          Snapshot =>
            if p.match_index >= self.core.snapshot_index {
              self.logger.log(
                Debug,
                "\{self.id} recovered from needing snapshot, resumed sending replication messages to \{from} [\{progress_text(p)}]",
              )
              p.become_probe()
              p.become_replicate()
            }
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
          // The commit index advanced into the current term, so it is now safe
          // to answer any read-index requests that were waiting for it (etcd
          // releases pending reads here before broadcasting).
          for m in self.release_pending_read_index() {
            out.push(m)
          }
          // A new commit: tell every follower, which also carries entries to
          // any that are behind.
          for m in self.bcast_append() {
            out.push(m)
          }
          //
        } else if (
            p.match_index < self.core.last_log_index() ||
            p.can_bump_commit(self.core.commit_index)
          ) &&
          !p.is_paused() {
          // No new commit from this ack, but this follower is still behind — on
          // entries (match < last) or merely on the commit index it has been told
          // (etcd's `CanBumpCommit`, which skips a redundant commit-only MsgApp
          // when the follower already knows the latest in-flight commit). Push it
          // the next append now rather than waiting for a client proposal. The
          // follower was just marked active, so `send_to` yields a message.
          if self.send_to(from) is Some(m) {
            out.push(m)
          }
        }
        out
      } else {
        // Back off. The follower's hint is refined against the leader's own log
        // (two-sided findConflictByTerm): if it reported the term at its
        // conflict point, jump to the last leader index whose term does not
        // exceed it, skipping a whole run of superseded entries in one retry.
        // `rejected` is the probe point the follower echoed back (etcd's
        // `m.GetIndex()`, raft.go:1512), not a value re-derived from `next_index`:
        // feeding the carried index lets `maybe_decr_to`'s staleness guard
        // (`next_index - 1 != rejected`) discard a reordered reject for an entry
        // no longer in flight instead of driving a spurious back-off.
        // Name the hint the follower gave before refining it (etcd logs this at
        // the top of the rejection branch, so the raw hint and the position it
        // was for are both visible).
        self.logger.log(
          Debug,
          "\{self.id} received MsgAppResp(rejected, hint: (index \{reply.conflict_index}, term \{reply.conflict_term})) from \{from} for index \{reply.reject_index}",
        )
        let rejected = reply.reject_index
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
          self.logger.log(
            Debug,
            "\{self.id} decreased progress of \{from} to [\{progress_text(p)}]",
          )
          if p.state == Replicate {
            p.become_probe()
          }
          // The follower was just marked active, so `send_to` yields the retry.
          let out : Array[Message] = []
          if self.send_to(from) is Some(m) {
            out.push(m)
          }
          out
        } else {
          []
        }
      }
    }
  }
}

///|

///|
/// Whether `id` appears in none of a `ConfState`'s membership sets (voters,
/// learners, or the outgoing voters of a joint config). etcd checks these three;
/// `learners_next` need not be checked because such a peer is also in
/// `voters_outgoing` (raft.go:1888).
fn not_in_conf_state(cs : ConfState, id : String) -> Bool {
  !cs.voters.contains(id) &&
  !cs.learners.contains(id) &&
  !cs.voters_outgoing.contains(id)
}

///|
fn RaftNode::handle_snapshot(
  self : RaftNode,
  from : String,
  args : InstallSnapshotArgs,
) -> Array[Message] {
  // Defense-in-depth (etcd's `restore`, raft.go:1901): refuse a snapshot whose
  // recorded configuration does not list us at all — installing it would drop us
  // from every membership set and leave the progress tracker assuming we are
  // present. This should never happen for a correct leader, so it is only a
  // guard: the term/leader were already adopted in `step` before dispatch. A
  // refusal answers like any failed restore — an `AppendResp` at our commit
  // index (etcd's `handleSnapshot` false branch, raft.go:1852).
  if !args.conf_state.is_empty() && not_in_conf_state(args.conf_state, self.id) {
    return [self.snapshot_ack(from, self.core.commit_index)]
  }
  // The pre-install log state, as etcd's `raftLog.String()` prints it. It is
  // captured before the install and reported with the restore, because that is
  // the shape of upstream's line: the log that is about to be replaced.
  let log_before = "committed=\{self.core.commit_index}, applied=\{self.core.last_applied}, applying=\{self.core.last_applied}, unstable.offset=\{self.core.last_log_index() + 1}, unstable.offsetInProgress=\{self.core.last_log_index() + 1}, len(unstable.Entries)=0"
  let before = self.core.snapshot_index
  let reply = self.core.handle_install_snapshot(args)
  if reply.term == args.term {
    // A current leader talking to us: recognise it, and step down if we were
    // campaigning — a candidate that hears from a leader at its own term yields
    // to it (etcd's `stepCandidate`, which calls becomeFollower for MsgApp).
    self.adopt_leader(args.term, args.leader_id)
  }
  // If the snapshot was actually installed (its baseline advanced ours) and it
  // records a configuration, rebuild the live membership from it (§7) — so a
  // follower does not silently lose its voter/learner sets on restore.
  let installed = self.core.snapshot_index > before
  if installed {
    self.logger.log(
      Info,
      "log [\{log_before}] starts to restore snapshot [index: \{args.last_index}, term: \{args.last_term}]",
    )
    if !args.conf_state.is_empty() {
      self.restore_conf_state(args.conf_state)
    }
    // Restoring discards the log and adopts the snapshot's configuration, so the
    // membership is announced exactly as any other switch is.
    self.logger.log(
      Info,
      "\{self.id} switched to configuration \{self.config.describe()}",
    )
    self.logger.log(
      Info,
      "\{self.id} [commit: \{self.core.commit_index}, lastindex: \{self.core.last_log_index()}, lastterm: \{self.core.term_at(self.core.last_log_index())}] restored snapshot [index: \{args.last_index}, term: \{args.last_term}]",
    )
    self.logger.log(
      Info,
      "\{self.id} [commit: \{self.core.commit_index}] restored snapshot [index: \{args.last_index}, term: \{args.last_term}]",
    )
  }
  // Answer with an `AppendResp`, as etcd's `handleSnapshot` answers `MsgSnap`
  // with `MsgAppResp` (raft.go:1848/1852): the follower's last index once the
  // snapshot took, or its commit index when the snapshot was stale and left the
  // log untouched. The leader's ordinary append-response handler then folds this
  // into the follower's progress, aborting the pending snapshot on success.
  let index = if installed {
    self.core.last_log_index()
  } else {
    self.core.commit_index
  }
  [self.snapshot_ack(from, index)]
}

///|
/// The follower's answer to an InstallSnapshot: a non-rejecting `AppendResp` at
/// `index`, the shape etcd gives `handleSnapshot`'s `MsgAppResp` reply. Carrying
/// the real index (rather than a bare term) lets the leader learn where the
/// follower now stands and resume replication through the shared append path.
fn RaftNode::snapshot_ack(
  self : RaftNode,
  from : String,
  index : UInt64,
) -> Message {
  Message::new(
    self.id,
    from,
    AppendResp({
      term: self.core.current_term(),
      success: true,
      match_index: index,
      conflict_index: 0,
      conflict_term: 0,
      reject_index: 0,
    }),
  )
}
