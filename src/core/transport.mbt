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
/// Pluggable transport for delivering Raft RPCs to a peer, addressed by node
/// id. A real cluster implements this over TCP, HTTP or gRPC; an in-process
/// cluster can call the handlers directly. Keeping it a trait decouples the
/// consensus core from any particular networking stack.
pub trait Transport {
  fn request_vote(Self, String, RequestVoteArgs) -> RequestVoteReply
  fn append_entries(Self, String, AppendEntriesArgs) -> AppendEntriesReply
}
