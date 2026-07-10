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
/// Append a configuration change to the leader's log as a `ConfChange` entry
/// under `term`, returning the stored entry. The change takes effect on the
/// cluster only once the entry is committed and applied.
pub fn Node::propose_conf_change(
  self : Node,
  term : UInt64,
  change : ConfChange,
) -> Entry {
  self.append_conf(term, change.encode())
}

///|
/// A configuration driver: it walks a node's committed log and folds every
/// committed `ConfChange` entry into a `Membership`, tracking how far it has
/// applied so it never applies a change twice. This is the "configuration
/// state machine" that runs beside the application state machine.
pub struct ConfDriver {
  config : Membership
  mut applied : UInt64
}

///|
/// Create a configuration driver starting from an initial voter set.
pub fn ConfDriver::new(members : Array[String]) -> ConfDriver {
  { config: Membership::new(members), applied: 0 }
}

///|
/// The configuration this driver has folded up to.
pub fn ConfDriver::config(self : ConfDriver) -> Membership {
  self.config
}

///|
/// Fold every committed-but-unapplied `ConfChange` entry of `node` into the
/// configuration, in log order, advancing the applied marker. Normal entries
/// are skipped. Returns how many changes were applied in this pass.
pub fn ConfDriver::drive(self : ConfDriver, node : Node) -> Int {
  let mut applied = 0
  while self.applied < node.commit_index {
    let next = self.applied + 1
    if node.entry_at(next) is Some(entry) {
      if entry.is_conf_change() {
        if ConfChange::decode(entry.command) is Some(cc) {
          cc.apply_to(self.config)
          applied = applied + 1
        }
      }
    }
    self.applied = next
  }
  applied
}
