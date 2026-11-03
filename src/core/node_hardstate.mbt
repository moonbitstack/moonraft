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
/// Capture this node's HardState for durable storage.
pub fn Node::hard_state(self : Node) -> HardState {
  { term: self.current_term, vote: self.voted_for, commit: self.commit_index, }
}

///|
/// Adopt a HardState read back from storage. The commit index never moves
/// backwards: a stale HardState replayed after a snapshot must not un-commit
/// entries the snapshot already covers.
pub fn Node::apply_hard_state(self : Node, hs : HardState) -> Unit {
  self.current_term = hs.term
  self.voted_for = hs.vote
  if hs.commit > self.commit_index {
    self.commit_index = hs.commit
  }
}
