///|
/// The set of servers that currently form the cluster. Raft changes membership
/// through the log so every server adopts each change at the same point in the
/// sequence (Raft §6). Two disciplines are supported: single-server changes,
/// where the old and new majorities always overlap; and joint consensus, where
/// the cluster passes through a transitional configuration C(old,new) that
/// needs a majority of *both* the old and the new voter sets to agree.
///
/// `members` is the incoming configuration C(new). `outgoing` holds the old
/// configuration C(old) and is non-empty only while `joint` is true.
pub(all) struct Membership {
  members : Array[String]
  outgoing : Array[String]
  mut joint : Bool
}

///|
/// Create a simple (non-joint) cluster configuration from an initial voter set.
pub fn Membership::new(members : Array[String]) -> Membership {
  { members: members.copy(), outgoing: [], joint: false }
}

///|
/// The majority size of the current (incoming) configuration. Meaningful for a
/// simple configuration; in a joint configuration use `has_majority`, which
/// accounts for both halves.
pub fn Membership::quorum(self : Membership) -> Int {
  self.members.length() / 2 + 1
}

///|
/// Whether `id` is a voter in the current configuration — in the incoming set,
/// or, during a joint transition, in either half.
pub fn Membership::contains(self : Membership, id : String) -> Bool {
  self.members.contains(id) || (self.joint && self.outgoing.contains(id))
}

///|
/// The number of servers in the incoming configuration.
pub fn Membership::size(self : Membership) -> Int {
  self.members.length()
}

///|
/// Whether the configuration is currently in the joint (transitional) state.
pub fn Membership::is_joint(self : Membership) -> Bool {
  self.joint
}

///|
/// The number of `granted` ids that are voters in `voters`.
fn count_in(voters : Array[String], granted : Array[String]) -> Int {
  let mut n = 0
  for id in voters {
    if granted.contains(id) {
      n = n + 1
    }
  }
  n
}

///|
/// Whether `granted` (the ids that agreed, e.g. voted or acknowledged) forms a
/// majority. A simple configuration needs a majority of the incoming set; a
/// joint configuration needs a majority of the incoming set *and* a majority of
/// the outgoing set, which is what makes joint consensus safe against split
/// decisions during a change (Raft §6).
pub fn Membership::has_majority(
  self : Membership,
  granted : Array[String],
) -> Bool {
  let inc = count_in(self.members, granted) > self.members.length() / 2
  if !self.joint {
    return inc
  }
  let out = count_in(self.outgoing, granted) > self.outgoing.length() / 2
  inc && out
}

///|
/// Add a server to the incoming configuration, unless it is already a voter.
pub fn Membership::add(self : Membership, id : String) -> Unit {
  if self.members.contains(id) {
    return
  }
  self.members.push(id)
}

///|
/// Remove a server from the incoming configuration.
pub fn Membership::remove(self : Membership, id : String) -> Unit {
  self.members.retain(fn(m) { m != id })
}

///|
/// Enter joint consensus, moving to the target voter set `new_members` while
/// keeping the current set as the outgoing half. Until the transition is
/// committed and `leave_joint` is called, decisions need both majorities.
pub fn Membership::enter_joint(
  self : Membership,
  new_members : Array[String],
) -> Unit {
  self.outgoing.clear()
  for m in self.members {
    self.outgoing.push(m)
  }
  self.members.clear()
  for m in new_members {
    self.members.push(m)
  }
  self.joint = true
}

///|
/// Leave joint consensus once C(old,new) is committed: the outgoing half is
/// dropped and the cluster runs on the incoming configuration alone.
pub fn Membership::leave_joint(self : Membership) -> Unit {
  self.outgoing.clear()
  self.joint = false
}

///|
/// The voters of the current configuration, de-duplicated across both halves.
pub fn Membership::voters(self : Membership) -> Array[String] {
  let out : Array[String] = []
  for m in self.members {
    out.push(m)
  }
  if self.joint {
    for m in self.outgoing {
      if !out.contains(m) {
        out.push(m)
      }
    }
  }
  out
}
