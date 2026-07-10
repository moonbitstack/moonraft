///|
/// The body of a message exchanged between nodes. A heartbeat is modelled as an
/// `Append` with no entries, so the same replication path carries both. Keeping
/// every request paired with its response in one enum lets a transport move
/// Raft traffic without knowing what any particular message means.
pub enum Payload {
  PreVote(RequestVoteArgs)
  PreVoteResp(RequestVoteReply)
  Vote(RequestVoteArgs)
  VoteResp(RequestVoteReply)
  Append(AppendEntriesArgs)
  AppendResp(AppendEntriesReply)
  Heartbeat(HeartbeatArgs)
  HeartbeatResp(HeartbeatReply)
  Snapshot(InstallSnapshotArgs)
  SnapshotResp(InstallSnapshotReply)
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
}

///|
/// Build a routed message.
pub fn Message::new(from : String, to : String, payload : Payload) -> Message {
  { from, to, payload }
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
    SnapshotResp(r) => r.term
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
    | SnapshotResp(_)
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
    Append(a) => if a.entries.length() == 0 { "Probe" } else { "Append" }
    AppendResp(_) => "AppendResp"
    Heartbeat(_) => "Heartbeat"
    HeartbeatResp(_) => "HeartbeatResp"
    Snapshot(_) => "Snapshot"
    SnapshotResp(_) => "SnapshotResp"
    TimeoutNow(_) => "TimeoutNow"
    Propose(_) => "Propose"
    ReadIndex(_) => "ReadIndex"
    ReadIndexResp(_) => "ReadIndexResp"
    TransferLeader(_) => "TransferLeader"
    ForgetLeader => "ForgetLeader"
  }
}
