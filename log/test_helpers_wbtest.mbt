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

///|
/// One empty-payload entry at `(index, term)`.
fn ent(index : UInt64, term : UInt64) -> Entry {
  Entry::normal(term, index, b"")
}

///|
/// Build `[start, start+len)` entries carrying the given terms and empty
/// payloads. Mirrors etcd's `index(start).terms(...)`.
fn ents_at(start : UInt64, terms : Array[UInt64]) -> Array[Entry] {
  let out : Array[Entry] = []
  let mut i = start
  for t in terms {
    out.push(Entry::normal(t, i, b""))
    i = i + 1
  }
  out
}

///|
/// Assert two entry runs match on index and term (etcd's requireEqualEntries).
fn assert_entries(got : Array[Entry], want : Array[Entry]) -> Unit raise {
  assert_eq(got.length(), want.length())
  for i in 0..<got.length() {
    assert_eq(got[i].index, want[i].index)
    assert_eq(got[i].term, want[i].term)
  }
}

///|
/// Assert that `thunk` raises exactly `want`.
fn assert_raises(
  thunk : () -> Unit raise @storage.StorageError,
  want : @storage.StorageError,
) -> Unit raise {
  match
    (try {
      thunk()
      None
    } catch {
      e => Some(e)
    }) {
    Some(e) => assert_true(e == want)
    None => abort("expected a StorageError but the call succeeded")
  }
}
