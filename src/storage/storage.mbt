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
/// The persistent state a Raft node must save to stable storage before
/// responding to any RPC (Raft §5.3): the current term, the vote cast in that
/// term, and the log. It is exactly what a node reloads to recover after a
/// crash.
pub(all) struct Persisted {
  term : UInt64
  voted_for : String?
  log : Array[Entry]
}

///|
/// Durable storage for a node's persistent state. A real deployment backs
/// this with a file or a database; the tests use an in-memory implementation.
/// Keeping it a trait decouples the consensus core from any particular I/O.
pub(open) trait LogStore {
  fn persist(Self, Persisted) -> Unit
  fn restore(Self) -> Persisted?
}
