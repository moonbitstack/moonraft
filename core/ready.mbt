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
  // Local storage directives, populated only under AsyncStorageWrites. Instead
  // of the caller persisting `entries` / applying `committed_entries` inline and
  // then calling `advance`, the work is handed to a local storage thread as a
  // directive; the thread does it and delivers the paired response back through
  // `step_append_resp` / `step_apply_resp`. `None` in the synchronous mode.
  storage_append : StorageAppend?
  storage_apply : StorageApply?
}

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
/// A directive to the local append thread: write these unstable entries (and the
/// hard state / snapshot, if present) to stable storage, then return `resp`
/// (etcd's `MsgStorageAppend`).
pub(all) struct StorageAppend {
  entries : Array[Entry]
  hard_state : HardState?
  snapshot : Snapshot?
  resp : StorageAppendResp
}

///|
/// The acknowledgement a state machine sends back once a `StorageApply` batch has
/// been applied (etcd's `MsgStorageApplyResp`). It carries the applied entries so
/// the raft node can advance its applied cursor and release their quota; committed
/// entries are term-independent, so no ABA guard is needed.
pub(all) struct StorageApplyResp {
  entries : Array[Entry]
}

///|
/// A directive to the local apply thread: apply these committed entries to the
/// state machine, then return `resp` (etcd's `MsgStorageApply`).
pub(all) struct StorageApply {
  entries : Array[Entry]
  resp : StorageApplyResp
}

///|
/// Whether a synchronous durable write is required before replying to any RPC
/// (etcd's `MustSync`). The persistent state on every server — currentTerm,
/// votedFor, and the log — must be flushed when new entries are appended or the
/// term or vote changed; a bare commit-index bump may be flushed lazily (§5).
pub fn must_sync(hs : HardState, prev : HardState, entries_num : Int) -> Bool {
  entries_num != 0 || hs.vote != prev.vote || hs.term != prev.term
}
