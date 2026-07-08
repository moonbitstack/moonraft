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
  Snapshot(InstallSnapshotArgs)
  SnapshotResp(InstallSnapshotReply)
}

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
    Snapshot(a) => a.term
    SnapshotResp(r) => r.term
  }
}

///|
/// Whether this message is a response rather than a request. The simulator uses
/// it only for readable traces.
pub fn Message::is_response(self : Message) -> Bool {
  match self.payload {
    PreVoteResp(_) | VoteResp(_) | AppendResp(_) | SnapshotResp(_) => true
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
    Append(a) => if a.entries.length() == 0 { "Heartbeat" } else { "Append" }
    AppendResp(_) => "AppendResp"
    Snapshot(_) => "Snapshot"
    SnapshotResp(_) => "SnapshotResp"
  }
}
