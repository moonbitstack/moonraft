///|
/// The small piece of durable state Raft must flush before answering any RPC:
/// the current term, the vote cast in that term, and the commit index. etcd
/// calls this the HardState; persisting it is what lets a node rejoin without
/// violating the election-safety or state-machine-safety properties (§5.3).
pub(all) struct HardState {
  term : UInt64
  vote : String?
  commit : UInt64
} derive(Eq)

///|
/// The HardState a fresh node starts from: term 0, no vote, nothing committed.
pub fn HardState::initial() -> HardState {
  { term: 0, vote: None, commit: 0 }
}

///|
/// Whether a HardState is the zero value (etcd's `IsEmptyHardState`): a fresh
/// node that has neither voted nor committed anything has nothing to persist.
pub fn HardState::is_empty(self : HardState) -> Bool {
  self == HardState::initial()
}

///|
/// Whether two hard states are equal in all three durable fields — term, vote,
/// and commit (etcd's `isHardStateEqual`). This is the test that keeps a `Ready`
/// from carrying a redundant HardState when nothing durable has moved.
pub fn hard_state_equal(a : HardState, b : HardState) -> Bool {
  a == b
}
