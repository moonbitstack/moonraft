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
/// A point-in-time snapshot of a server's observable state, for monitoring and
/// tests. It is a plain value copied out of the node, so reading it never
/// disturbs the running protocol.
#derive(Eq)
pub(all) struct RaftStatus {
  id : String
  role : Role
  term : UInt64
  leader : String?
  commit : UInt64
  last_index : UInt64
  applied : UInt64
}

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

///|
pub extend RaftStatus with Eq::{not_equal, equal}
