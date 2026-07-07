///|
/// The role a node plays in the Raft protocol at a given moment.
///
/// A node starts as a `Follower`. On election timeout it becomes a
/// `Candidate` and stands for election; if it wins a majority it becomes
/// the `Leader` for that term.
pub enum Role {
  Follower
  Candidate
  Leader
} derive(Eq)
