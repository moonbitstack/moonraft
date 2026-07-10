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
pub(all) enum ConfChangeType {
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
/// How a `ConfChangeV2` transitions the configuration (etcd's
/// `ConfChangeTransition`): `Auto` applies a batch simply when it safely can
/// (at most one voter changed) and otherwise enters an auto-leaving joint;
/// `JointImplicit` always enters joint and auto-leaves; `JointExplicit` always
/// enters joint and waits for an explicit leave.
pub(all) enum ConfChangeTransition {
  Auto
  JointImplicit
  JointExplicit
} derive(Eq)

///|
/// A batch configuration change (etcd's `ConfChangeV2`): several single changes
/// applied atomically. An empty batch leaves joint consensus. Whether a non-empty
/// batch is applied simply or via a joint transition — and whether that joint
/// auto-leaves — is governed by `transition` (Raft §4.3, joint consensus).
pub(all) struct ConfChangeV2 {
  changes : Array[ConfChange]
  transition : ConfChangeTransition
} derive(Eq)

///|
/// Enter joint consensus with `changes`. `auto_leave` selects the implicit
/// (auto-leaving) or explicit joint transition — the historical API.
pub fn ConfChangeV2::enter_joint(
  changes : Array[ConfChange],
  auto_leave? : Bool = true,
) -> ConfChangeV2 {
  {
    changes,
    transition: if auto_leave {
      JointImplicit
    } else {
      JointExplicit
    },
  }
}

///|
/// A batch with the `Auto` transition: applied simply when it can be (at most one
/// voter changed), otherwise as an auto-leaving joint change — etcd's default.
pub fn ConfChangeV2::auto(changes : Array[ConfChange]) -> ConfChangeV2 {
  { changes, transition: Auto }
}

///|
/// Leave joint consensus (an empty batch).
pub fn ConfChangeV2::leave_joint() -> ConfChangeV2 {
  { changes: [], transition: Auto }
}

///|
/// Whether this change leaves a joint configuration (etcd's
/// `ConfChangeV2.LeaveJoint`). This is the case only for the `Auto` transition
/// with no changes: an *explicit* joint transition carrying no changes still
/// *enters* an (empty) joint config and must not be mistaken for a leave, which
/// is exactly the complement of `enters_joint`.
pub fn ConfChangeV2::is_leave(self : ConfChangeV2) -> Bool {
  self.transition == Auto && self.changes.length() == 0
}

///|
/// Whether this batch enters a joint configuration, and if so whether that joint
/// auto-leaves (etcd's `ConfChangeV2.EnterJoint`). `Auto` with at most one change
/// is applied *simply* — no joint; anything else is joint, auto-leaving unless
/// the transition is explicit.
pub fn ConfChangeV2::enters_joint(self : ConfChangeV2) -> (Bool, Bool) {
  if self.transition != Auto || self.changes.length() > 1 {
    let auto = match self.transition {
      Auto => true
      JointImplicit => true
      JointExplicit => false
    }
    (auto, true)
  } else {
    (false, false)
  }
}

///|
/// Whether this joint change auto-leaves once committed.
pub fn ConfChangeV2::auto_leave(self : ConfChangeV2) -> Bool {
  self.enters_joint().0
}

///|
fn ConfChangeTransition::tag(self : ConfChangeTransition) -> String {
  match self {
    Auto => "a"
    JointImplicit => "i"
    JointExplicit => "e"
  }
}

///|
/// Serialize as `V` + transition tag (`a`/`i`/`e`) + `;`-separated `<tag><id>`
/// changes. The `V` prefix distinguishes a batch from a single change on decode.
pub fn ConfChangeV2::encode(self : ConfChangeV2) -> Bytes {
  let mut s = "V" + self.transition.tag()
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
  let transition = if s.length() >= 2 {
    match s[1:2].to_owned() {
      "i" => JointImplicit
      "e" => JointExplicit
      _ => Auto
    }
  } else {
    Auto
  }
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
  Some({ changes, transition })
}
