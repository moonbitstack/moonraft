///|
/// A point-in-time snapshot of a server's observable state, for monitoring and
/// tests. It is a plain value copied out of the node, so reading it never
/// disturbs the running protocol.
pub(all) struct RaftStatus {
  id : String
  role : Role
  term : UInt64
  leader : String?
  commit : UInt64
  last_index : UInt64
  applied : UInt64
} derive(Eq)

///|
/// Capture this server's current status.
pub fn RaftNode::status(self : RaftNode) -> RaftStatus {
  {
    id: self.id,
    role: self.core.role(),
    term: self.core.current_term(),
    leader: self.leader_id,
    commit: self.core.commit_index,
    last_index: self.core.last_log_index(),
    applied: self.core.last_applied,
  }
}

///|
/// The role name as a lowercase word.
fn role_name(role : Role) -> String {
  match role {
    Follower => "follower"
    PreCandidate => "precandidate"
    Candidate => "candidate"
    Leader => "leader"
  }
}

///|
/// A one-line, human-readable summary, e.g. `a leader term=3 leader=a commit=7`.
pub fn RaftStatus::describe(self : RaftStatus) -> String {
  let lead = match self.leader {
    Some(l) => l
    None => "-"
  }
  "\{self.id} \{role_name(self.role)} term=\{self.term} leader=\{lead} commit=\{self.commit} last=\{self.last_index}"
}
