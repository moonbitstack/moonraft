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
test "entry constructors tag the entry kind" {
  let n = Entry::normal(1, 1, b"set x")
  let c = Entry::conf(1, 2, b"add s2")
  assert_false(n.is_conf_change())
  assert_true(c.is_conf_change())
  assert_true(n.entry_type == Normal)
  assert_true(c.entry_type == ConfChange)
}

///|
test "append_conf records a configuration entry in the log" {
  let a = Node::new("a")
  let _ = a.append(1, b"x")
  let e = a.append_conf(1, b"add s2")
  assert_eq(e.index, 2)
  assert_true(e.is_conf_change())
  assert_eq(a.log_len(), 2)
}

///|
test "entry_at returns entries by absolute index and None out of range" {
  let a = Node::new("a")
  let _ = a.append(1, b"x")
  let _ = a.append(1, b"y")
  match a.entry_at(2) {
    Some(e) => assert_eq(e.index, 2)
    None => abort("expected an entry at index 2")
  }
  assert_true(a.entry_at(0) == None)
  assert_true(a.entry_at(3) == None)
}

///|
test "entries_after returns the suffix strictly past an index" {
  let a = Node::new("a")
  let _ = a.append(1, b"x")
  let _ = a.append(1, b"y")
  let _ = a.append(1, b"z")
  let tail = a.entries_after(1)
  assert_eq(tail.length(), 2)
  assert_eq(tail[0].index, 2)
  assert_eq(tail[1].index, 3)
  assert_eq(a.entries_after(3).length(), 0)
}
