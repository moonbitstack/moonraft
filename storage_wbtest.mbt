///|
/// An in-memory `LogStore`, for tests and single-process use.
struct MemStore {
  mut saved : Persisted?
}

///|
impl LogStore for MemStore with fn persist(self, state) {
  self.saved = Some(state)
}

///|
impl LogStore for MemStore with fn restore(self) {
  self.saved
}

///|
test "a node recovers its term, vote and log from a store" {
  let a = Node::new("a")
  a.become_candidate()
  let _ = a.append(1, b"x")
  let _ = a.append(1, b"y")
  let store : MemStore = { saved: None }
  store.persist(a.persisted())

  // The node crashes and restarts blank, then reloads from the store.
  let restarted = Node::new("a")
  match store.restore() {
    Some(state) => restarted.recover(state)
    None => abort("store lost the persisted state")
  }
  assert_eq(restarted.current_term(), 1)
  assert_eq(restarted.last_log_index(), 2)
  assert_true(restarted.voted_for == Some("a"))
}

///|
test "a persisted image is not disturbed by later log growth" {
  let a = Node::new("a")
  let _ = a.append(1, b"x")
  let store : MemStore = { saved: None }
  store.persist(a.persisted())
  let _ = a.append(1, b"y")
  match store.restore() {
    Some(state) => assert_eq(state.log.length(), 1)
    None => abort("store lost the persisted state")
  }
}
