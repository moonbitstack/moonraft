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

///|
/// Whether a HardState is the zero value (etcd's `IsEmptyHardState`): a fresh
/// node that has neither voted nor committed anything has nothing to persist.
pub fn HardState::is_empty(self : HardState) -> Bool {
  self == HardState::initial()
}

///|
/// Whether two hard states are equal in all three durable fields — term, vote,
/// and commit (etcd's `isHardStateEqual`). This is the test that keeps a `Ready`
/// from carrying a redundant HardState when nothing durable has moved.
pub fn hard_state_equal(a : HardState, b : HardState) -> Bool {
  a == b
}

///|
/// The batch of outstanding work the application must handle for one turn of the
/// state machine (etcd's `Ready`). The contract is synchronous, not a coroutine:
/// the caller persists `entries`/`hard_state`/`snapshot`, sends `messages`,
/// applies `committed_entries`, serves `read_states`, then calls `advance`.
///
/// A field carries work only when there is some: `soft_state` and `hard_state`
/// are `Some` only on change (expressed as `Option`, not a sentinel), and the
/// arrays are empty when idle. `must_sync` says whether the persist must be a
/// durable (fsync) write or may be lazily flushed.
pub(all) struct Ready {
  soft_state : SoftState?
  hard_state : HardState?
  read_states : Array[ReadState]
  entries : Array[Entry]
  snapshot : Snapshot?
  committed_entries : Array[Entry]
  messages : Array[Message]
  must_sync : Bool
}

///|
/// Whether a synchronous durable write is required before replying to any RPC
/// (etcd's `MustSync`). The persistent state on every server — currentTerm,
/// votedFor, and the log — must be flushed when new entries are appended or the
/// term or vote changed; a bare commit-index bump may be flushed lazily (§5).
pub fn must_sync(hs : HardState, prev : HardState, entries_num : Int) -> Bool {
  entries_num != 0 || hs.vote != prev.vote || hs.term != prev.term
}
