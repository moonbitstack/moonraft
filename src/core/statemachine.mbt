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
/// A replicated state machine: it consumes committed commands in log order.
/// Implementors decide what each command means. Keeping this pluggable lets the
/// same consensus core drive a key-value store, a config registry or anything
/// else.
pub(open) trait StateMachine {
  fn apply(Self, Bytes) -> Unit
}

///|
/// Apply every committed-but-unapplied entry to `sm` in index order and
/// advance `last_applied` (Raft §5.3). This is how a committed log turns into
/// application state; calling it again once caught up does nothing.
pub fn Node::apply_committed(self : Node, sm : &StateMachine) -> Unit {
  while self.last_applied < self.commit_index {
    let next = self.last_applied + 1
    sm.apply(self.log[(next - 1).to_int()].command)
    self.last_applied = next
  }
}
