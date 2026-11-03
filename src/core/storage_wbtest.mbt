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

///|
/// An in-memory `LogStore`, for tests and single-process use.
struct MemStore {
  mut saved : Persisted?
}

///|
impl @storage.LogStore for MemStore with fn persist(self, state) {
  self.saved = Some(state)
}

///|
impl @storage.LogStore for MemStore with fn restore(self) {
  self.saved
}

///|
test "a node recovers its term, vote and log from a store" {
  let a = Node::new("a")
  a.become_candidate()
  let _ = a.append(1, b"x")
  let _ = a.append(1, b"y")
  let store : MemStore = { saved: None, }
  @storage.LogStore::persist(store, a.persisted())

  // The node crashes and restarts blank, then reloads from the store.
  let restarted = Node::new("a")
  match @storage.LogStore::restore(store) {
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
  let store : MemStore = { saved: None, }
  @storage.LogStore::persist(store, a.persisted())
  let _ = a.append(1, b"y")
  match @storage.LogStore::restore(store) {
    Some(state) => assert_eq(state.log.length(), 1)
    None => abort("store lost the persisted state")
  }
}
