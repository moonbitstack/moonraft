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
/// The kind of a single-server configuration change (Raft §6, §4.2.1).
pub enum ConfChangeType {
  AddNode
  RemoveNode
  AddLearnerNode
} derive(Eq)

///|
/// A configuration change carried by a `ConfChange` log entry: which server is
/// joining or leaving. It is serialized into the entry's command so every
/// server applies the same change at the same log position.
pub(all) struct ConfChange {
  change_type : ConfChangeType
  node_id : String
} derive(Eq)

///|
/// A change that adds `id` to the cluster.
pub fn ConfChange::add(id : String) -> ConfChange {
  { change_type: AddNode, node_id: id }
}

///|
/// A change that removes `id` from the cluster.
pub fn ConfChange::remove(id : String) -> ConfChange {
  { change_type: RemoveNode, node_id: id }
}

///|
/// A change that adds `id` as a learner (a non-voting member).
pub fn ConfChange::add_learner(id : String) -> ConfChange {
  { change_type: AddLearnerNode, node_id: id }
}

///|
/// Serialize this change into a log-entry command. The encoding is a one-
/// character tag ('+' add voter, '-' remove, 'L' add learner) followed by the
/// server id, which keeps it human-readable in dumps and trivially reversible.
pub fn ConfChange::encode(self : ConfChange) -> Bytes {
  let tag = match self.change_type {
    AddNode => "+"
    RemoveNode => "-"
    AddLearnerNode => "L"
  }
  // Store the change as the UTF-16LE code units of `tag ++ id`, which is how
  // MoonBit strings read back through `to_unchecked_string`.
  let arr : Array[Byte] = []
  for c in tag + self.node_id {
    let code = c.to_int()
    arr.push((code & 0xff).to_byte())
    arr.push(((code >> 8) & 0xff).to_byte())
  }
  Bytes::from_array(arr)
}

///|
/// Recover a configuration change from a log-entry command, or `None` if the
/// bytes are not a well-formed change.
pub fn ConfChange::decode(command : Bytes) -> ConfChange? {
  let s = command.to_unchecked_string()
  if s.length() < 1 {
    return None
  }
  let id = s[1:].to_owned()
  if s.has_prefix("+") {
    Some({ change_type: AddNode, node_id: id })
  } else if s.has_prefix("-") {
    Some({ change_type: RemoveNode, node_id: id })
  } else if s.has_prefix("L") {
    Some({ change_type: AddLearnerNode, node_id: id })
  } else {
    None
  }
}

///|
/// Apply this change to a configuration in place.
pub fn ConfChange::apply_to(self : ConfChange, config : Membership) -> Unit {
  match self.change_type {
    AddNode => config.add(self.node_id)
    RemoveNode => config.remove(self.node_id)
    AddLearnerNode => config.add_learner(self.node_id)
  }
}

///|
/// The one-character tag for a single change ('+'/'-'/'L').
fn ConfChange::tag(self : ConfChange) -> String {
  match self.change_type {
    AddNode => "+"
    RemoveNode => "-"
    AddLearnerNode => "L"
  }
}

///|
/// A batch configuration change (etcd's `ConfChangeV2`): several single changes
/// applied atomically. A non-empty batch enters joint consensus C(old,new); an
/// empty batch leaves it. `auto_leave` asks the leader to append the matching
/// leave automatically once the entry commits (Raft §4.3, joint consensus).
pub(all) struct ConfChangeV2 {
  changes : Array[ConfChange]
  auto_leave : Bool
} derive(Eq)

///|
/// Enter joint consensus with `changes`, optionally leaving automatically once
/// committed.
pub fn ConfChangeV2::enter_joint(
  changes : Array[ConfChange],
  auto_leave? : Bool = true,
) -> ConfChangeV2 {
  { changes, auto_leave }
}

///|
/// Leave joint consensus (an empty batch).
pub fn ConfChangeV2::leave_joint() -> ConfChangeV2 {
  { changes: [], auto_leave: false }
}

///|
/// Whether this is the empty batch that leaves joint consensus.
pub fn ConfChangeV2::is_leave(self : ConfChangeV2) -> Bool {
  self.changes.length() == 0
}

///|
/// Serialize as `V` + `1`/`0` (auto-leave) + `;`-separated `<tag><id>` changes.
/// The `V` prefix distinguishes a batch from a single change on decode.
pub fn ConfChangeV2::encode(self : ConfChangeV2) -> Bytes {
  let mut s = "V" + (if self.auto_leave { "1" } else { "0" })
  for c in self.changes {
    s = s + ";" + c.tag() + c.node_id
  }
  let arr : Array[Byte] = []
  for ch in s {
    let code = ch.to_int()
    arr.push((code & 0xff).to_byte())
    arr.push(((code >> 8) & 0xff).to_byte())
  }
  Bytes::from_array(arr)
}

///|
/// Recover a batch change, or `None` if the bytes are not a well-formed batch.
pub fn ConfChangeV2::decode(command : Bytes) -> ConfChangeV2? {
  let s = command.to_unchecked_string()
  if !s.has_prefix("V") {
    return None
  }
  let auto_leave = s.length() >= 2 && s[1:2].to_owned() == "1"
  let changes : Array[ConfChange] = []
  // Split the remainder on ';', skipping the leading flag segment.
  let mut cur = ""
  let mut seg = 0
  fn flush() -> Unit {
    if seg > 0 && cur.length() >= 1 {
      let id = cur[1:].to_owned()
      let ct = match cur[0:1].to_owned() {
        "+" => Some(AddNode)
        "-" => Some(RemoveNode)
        "L" => Some(AddLearnerNode)
        _ => None
      }
      match ct {
        Some(t) => changes.push({ change_type: t, node_id: id })
        None => ()
      }
    }
    cur = ""
  }

  for ch in s {
    if ch == ';' {
      flush()
      seg = seg + 1
    } else {
      cur = cur + ch.to_string()
    }
  }
  flush()
  Some({ changes, auto_leave })
}

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
    match node.entry_at(next) {
      Some(entry) =>
        if entry.is_conf_change() {
          match ConfChange::decode(entry.command) {
            Some(cc) => {
              cc.apply_to(self.config)
              applied = applied + 1
            }
            None => ()
          }
        }
      None => ()
    }
    self.applied = next
  }
  applied
}
