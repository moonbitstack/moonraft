///|
/// The body of a message exchanged between nodes. A heartbeat is modelled as an
/// `Append` with no entries, so the same replication path carries both. Keeping
/// every request paired with its response in one enum lets a transport move
/// Raft traffic without knowing what any particular message means.
pub(all) enum Payload {
  PreVote(RequestVoteArgs)
  PreVoteResp(RequestVoteReply)
  Vote(RequestVoteArgs)
  VoteResp(RequestVoteReply)
  Append(AppendEntriesArgs)
  AppendResp(AppendEntriesReply)
  Heartbeat(HeartbeatArgs)
  HeartbeatResp(HeartbeatReply)
  // A follower answers a `Snapshot` with an `AppendResp`, not a dedicated reply:
  // etcd's `handleSnapshot` (raft.go:1840) responds to `MsgSnap` with a
  // `MsgAppResp` carrying `lastIndex()` on install or `committed` when the
  // snapshot is refused, so the leader's ordinary append-response path folds it
  // back into the follower's progress.
  Snapshot(InstallSnapshotArgs)
  TimeoutNow(UInt64)
  // Routable client-request messages (etcd's local, term-0 messages): a follower
  // that receives one forwards it to the leader rather than dropping it, so a
  // client may address any server. `Propose` carries the entries to append;
  // `ReadIndex` the read's opaque context; `TransferLeader` the target's id;
  // `ForgetLeader` nothing. The step function stamps them with no term.
  Propose(Array[Entry])
  ReadIndex(Bytes)
  ReadIndexResp(ReadIndexResp)
  TransferLeader(String)
  ForgetLeader
}

///|
/// The leader's answer to a forwarded `ReadIndex` (etcd's MsgReadIndexResp): the
/// commit index the read is anchored at, echoed back with the caller's context to
/// the server that originated the read. Unlike the request it carries the
/// leader's term, so it is term-checked like any other reply.
pub(all) struct ReadIndexResp {
  term : UInt64
  index : UInt64
  context : Bytes
} derive(Eq)

///|
/// A routed message: a payload with the ids of the sender and intended
/// recipient. The step function consumes these and produces more of them; a
/// transport (real or simulated) is only responsible for delivery.
pub(all) struct Message {
  from : String
  to : String
  payload : Payload
  // A leadership-transfer campaign stamps its vote solicitations `force` so a
  // recipient still leased to the outgoing leader grants them, letting the
  // handover complete (etcd carries `campaignTransfer` in the message context to
  // the same effect, raft.go:1102). Off for every ordinary message; a disruptive
  // candidate that merely timed out cannot set it.
  force : Bool
}

///|
/// Build a routed message. `force` is set only for leadership-transfer votes.
pub fn Message::new(
  from : String,
  to : String,
  payload : Payload,
  force? : Bool = false,
) -> Message {
  { from, to, payload, force }
}

///|
/// The term the message was stamped with. Every payload carries the sender's
/// term; a receiver uses it to decide whether to step down or reject.
pub fn Message::term(self : Message) -> UInt64 {
  match self.payload {
    PreVote(a) => a.term
    PreVoteResp(r) => r.term
    Vote(a) => a.term
    VoteResp(r) => r.term
    Append(a) => a.term
    AppendResp(r) => r.term
    Heartbeat(a) => a.term
    HeartbeatResp(r) => r.term
    Snapshot(a) => a.term
    TimeoutNow(t) => t
    // Local, routable messages carry no term (etcd sends MsgProp/MsgReadIndex
    // with Term 0); the read-index reply does carry the leader's term.
    ReadIndexResp(r) => r.term
    Propose(_) | ReadIndex(_) | TransferLeader(_) | ForgetLeader => 0
  }
}

///|
/// Whether this message is a response rather than a request. The simulator uses
/// it only for readable traces.
pub fn Message::is_response(self : Message) -> Bool {
  match self.payload {
    PreVoteResp(_)
    | VoteResp(_)
    | AppendResp(_)
    | HeartbeatResp(_)
    | ReadIndexResp(_) => true
    _ => false
  }
}

///|
/// A short, stable tag for the payload kind, for logs and scenario traces.
pub fn Message::kind(self : Message) -> String {
  match self.payload {
    PreVote(_) => "PreVote"
    PreVoteResp(_) => "PreVoteResp"
    Vote(_) => "Vote"
    VoteResp(_) => "VoteResp"
    Append(a) => if a.entries.is_empty() { "Probe" } else { "Append" }
    AppendResp(_) => "AppendResp"
    Heartbeat(_) => "Heartbeat"
    HeartbeatResp(_) => "HeartbeatResp"
    Snapshot(_) => "Snapshot"
    TimeoutNow(_) => "TimeoutNow"
    Propose(_) => "Propose"
    ReadIndex(_) => "ReadIndex"
    ReadIndexResp(_) => "ReadIndexResp"
    TransferLeader(_) => "TransferLeader"
    ForgetLeader => "ForgetLeader"
  }
}

///|
/// etcd classifies a handful of message types as node-local — MsgHup, MsgBeat,
/// MsgSnapStatus, MsgCheckQuorum and the storage-thread messages — which never
/// travel over the transport. This port has no such messages: those triggers are
/// direct method calls (tick / campaign / step), not `Payload`s. Every
/// transportable `Payload` is therefore, by construction, a network message.
pub fn Payload::is_local(self : Payload) -> Bool {
  match self {
    _ => false
  }
}
