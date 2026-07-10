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
/// The volatile part of a server's state, useful for logging and monitoring but
/// never persisted (etcd's `SoftState`). `lead` is `None` when no leader is
/// known. A `Ready` carries a `SoftState` only when it has changed, which is why
/// the leader/role transition is the trigger rather than the current value.
pub(all) struct SoftState {
  lead : String?
  state : Role
} derive(Eq)

///|
/// Whether two soft states are equal (etcd's `SoftState.equal`). A change in
/// either the known leader or the role is what makes a new `Ready` report one.
pub fn SoftState::equal(self : SoftState, other : SoftState) -> Bool {
  self == other
}

///|
/// A linearizable-read ticket (etcd's `ReadState`): the commit index a read must
/// wait for the state machine to reach, tagged with the caller's opaque context
/// so a returned index can be matched back to the request that asked for it.
pub(all) struct ReadState {
  index : UInt64
  request_ctx : Bytes
} derive(Eq)
