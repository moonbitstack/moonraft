///|
/// The set of servers that currently form the cluster. Raft changes membership
/// one server at a time, so that the old and new majorities always overlap and
/// two leaders can never be elected for the same term.
pub(all) struct Membership {
  members : Array[String]
}

///|
/// Create a cluster configuration from an initial set of server ids.
pub fn Membership::new(members : Array[String]) -> Membership {
  { members: members.copy() }
}

///|
/// The number of votes that forms a majority of the current cluster.
pub fn Membership::quorum(self : Membership) -> Int {
  self.members.length() / 2 + 1
}

///|
/// Whether `id` is currently a member of the cluster.
pub fn Membership::contains(self : Membership, id : String) -> Bool {
  self.members.contains(id)
}

///|
/// The current number of servers in the cluster.
pub fn Membership::size(self : Membership) -> Int {
  self.members.length()
}

///|
/// Add a server to the cluster, unless it is already a member.
pub fn Membership::add(self : Membership, id : String) -> Unit {
  if self.members.contains(id) {
    return
  }
  self.members.push(id)
}

///|
/// Remove a server from the cluster.
pub fn Membership::remove(self : Membership, id : String) -> Unit {
  self.members.retain(fn(m) { m != id })
}
