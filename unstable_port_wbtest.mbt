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
//
// Equivalents for etcd-io/raft log_unstable_test.go. This port has no `unstable`
// structure: the Node keeps a single contiguous log anchored at the snapshot
// baseline, and there is no Ready/in-progress persistence pipeline. The
// index/term/truncate semantics that DO have a counterpart are reproduced here
// against the Node; the in-progress bookkeeping tests
// (maybeFirstIndex/nextEntries/nextSnapshot/acceptInProgress/stableTo/restore)
// have no analogue and are documented as N/A in PORTING.md.

///|
test "unstable maybeLastIndex equivalent (last_log_index)" {
  // last in entries: snapshot(4,1) + entry(5,1) -> 5
  let a = log_node(eid(4, 1), [ent(5, 1)])
  assert_eq(a.last_log_index(), 5)
  // last in snapshot only -> snapshot index 4
  let b = log_node(eid(4, 1), [])
  assert_eq(b.last_log_index(), 4)
  // empty log and no snapshot -> 0
  let c = log_node(eid(0, 0), [])
  assert_eq(c.last_log_index(), 0)
}

///|
test "unstable maybeTerm equivalent (term_at)" {
  // snapshot(4,1) + entry(5,1)
  let n = log_node(eid(4, 1), [ent(5, 1)])
  assert_eq(n.term_at(5), 1) // term from the entry
  assert_eq(n.term_at(6), 0) // past the end -> zero
  assert_eq(n.term_at(4), 1) // term from the snapshot baseline
  assert_eq(n.term_at(3), 0) // before the baseline -> zero
  // snapshot only: term available at the baseline, empty beyond it
  let s = log_node(eid(4, 1), [])
  assert_eq(s.term_at(4), 1)
  assert_eq(s.term_at(5), 0)
}

///|
test "unstable truncateAndAppend equivalent (store_entries)" {
  // append to the end: index(5).terms(1) + index(6).terms(1,1) => index(5).terms(1,1,1)
  let a = log_node(eid(4, 1), [ent(5, 1)])
  let _ = a.store_entries(5, [ent(6, 1), ent(7, 1)])
  assert_eq(a.last_log_index(), 7)
  assert_eq(a.term_at(5), 1)
  assert_eq(a.term_at(6), 1)
  assert_eq(a.term_at(7), 1)

  // replace the unstable entries: index(5).terms(1) + index(5).terms(2,2) => index(5).terms(2,2)
  let b = log_node(eid(4, 1), [ent(5, 1)])
  let _ = b.store_entries(4, [ent(5, 2), ent(6, 2)])
  assert_eq(b.last_log_index(), 6)
  assert_eq(b.term_at(5), 2)
  assert_eq(b.term_at(6), 2)

  // truncate then append: index(5).terms(1,1,1) + index(6).terms(2) => index(5).terms(1,2)
  let c = log_node(eid(4, 1), [ent(5, 1), ent(6, 1), ent(7, 1)])
  let _ = c.store_entries(5, [ent(6, 2)])
  assert_eq(c.last_log_index(), 6)
  assert_eq(c.term_at(5), 1)
  assert_eq(c.term_at(6), 2)
  assert_eq(c.log_len(), 2)
}
