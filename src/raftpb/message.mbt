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
  // Local storage-thread replies (etcd's MsgStorageAppendResp and
  // MsgStorageApplyResp): what the append and apply threads send back once a
  // directive has been made durable or applied. They are messages rather than
  // return values so that a driver without language-level async can queue them
  // like any other delivery — which is exactly the ordering the async-write
  // scenarios record.
  StorageAppendResp(StorageAppendResp)
  StorageApplyResp(StorageApplyResp)
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
  force~ : Bool = false,
) -> Message {
  { from, to, payload, force, }
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
    // A storage reply carries the term the write was issued under, which is what
    // makes a reply that arrives after a term change identifiable as stale.
    StorageAppendResp(r) => r.term
    StorageApplyResp(_) => 0
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
/// The protobuf message-type name, as etcd's `pb.MessageType` prints it. A log
/// record that names the message kind (`ignored a MsgApp message`) uses this,
/// so the name a reader sees is the one the wire format and the scripts use.
pub fn Message::type_name(self : Message) -> String {
  match self.payload {
    PreVote(_) => "MsgPreVote"
    PreVoteResp(_) => "MsgPreVoteResp"
    Vote(_) => "MsgVote"
    VoteResp(_) => "MsgVoteResp"
    Append(_) => "MsgApp"
    AppendResp(_) => "MsgAppResp"
    Heartbeat(_) => "MsgHeartbeat"
    HeartbeatResp(_) => "MsgHeartbeatResp"
    Snapshot(_) => "MsgSnap"
    TimeoutNow(_) => "MsgTimeoutNow"
    ReadIndexResp(_) => "MsgReadIndexResp"
    Propose(_) => "MsgProp"
    ReadIndex(_) => "MsgReadIndex"
    TransferLeader(_) => "MsgTransferLeader"
    ForgetLeader => "MsgForgetLeader"
    StorageAppendResp(_) => "MsgStorageAppendResp"
    StorageApplyResp(_) => "MsgStorageApplyResp"
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
    StorageAppendResp(_) => "StorageAppendResp"
    StorageApplyResp(_) => "StorageApplyResp"
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

///|
pub extend ReadIndexResp with Eq::{not_equal, equal}

///|
/// The acknowledgement a storage thread sends back once a `StorageAppend` has
/// been made durable (etcd's `MsgStorageAppendResp`). It attests the last log
/// `(index, log_term)` written and the `term` the raft node held when the
/// directive was issued — the two together defeat the ABA race: a response that
/// arrives after the term has moved on, or after the unstable log at that index
/// was overwritten, is ignored rather than mistaken for an ack of the new log.
pub(all) struct StorageAppendResp {
  index : UInt64
  log_term : UInt64
  term : UInt64
} derive(Eq)

///|
pub extend StorageAppendResp with Eq::{not_equal, equal}

///|
/// The acknowledgement a state machine sends back once a `StorageApply` batch has
/// been applied (etcd's `MsgStorageApplyResp`). It carries the applied entries so
/// the raft node can advance its applied cursor and release their quota; committed
/// entries are term-independent, so no ABA guard is needed.
pub(all) struct StorageApplyResp {
  entries : Array[Entry]
}
