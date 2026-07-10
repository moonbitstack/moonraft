///|
/// The role a node plays in the Raft protocol at a given moment.
///
/// A node starts as a `Follower`. On election timeout it becomes a
/// `Candidate` and stands for election; if it wins a majority it becomes
/// the `Leader` for that term.
pub(all) enum Role {
  Follower
  // The pre-vote probe phase (etcd's `StatePreCandidate`): the node solicits
  // pre-votes under a hypothetical next term without adopting it, and only
  // becomes a real `Candidate` if a majority would grant it. It is a first-class
  // state reported through `SoftState`, distinct from `Follower`.
  PreCandidate
  Candidate
  Leader
} derive(Eq)
