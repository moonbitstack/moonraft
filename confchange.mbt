///|
/// The kind of a single-server configuration change (Raft §6).
pub enum ConfChangeType {
  AddNode
  RemoveNode
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
/// Serialize this change into a log-entry command. The encoding is a one-
/// character tag ('+' to add, '-' to remove) followed by the server id, which
/// keeps it human-readable in dumps and trivially reversible.
pub fn ConfChange::encode(self : ConfChange) -> Bytes {
  let tag = match self.change_type {
    AddNode => "+"
    RemoveNode => "-"
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
  }
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
