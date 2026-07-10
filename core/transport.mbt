///|
/// Pluggable transport for delivering Raft RPCs to a peer, addressed by node
/// id. A real cluster implements this over TCP, HTTP or gRPC; an in-process
/// cluster can call the handlers directly. Keeping it a trait decouples the
/// consensus core from any particular networking stack.
pub trait Transport {
  fn request_vote(Self, String, RequestVoteArgs) -> RequestVoteReply
  fn append_entries(Self, String, AppendEntriesArgs) -> AppendEntriesReply
}
