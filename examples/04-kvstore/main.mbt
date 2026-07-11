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
/// The capstone: consensus plus a state machine. A replicated key-value store.
/// Commands are applied to each replica in committed log order, so every node
/// that has applied the same prefix holds the same map - which is the whole
/// reason a replicated log exists.
///
///     moon run examples/04-kvstore

///|
/// The application state. `apply` is the only method consensus calls; commands
/// are UTF-16 text - "s<key>=<value>" sets a key, "d<key>" deletes one.
struct KVStore {
  data : Map[String, String]
}

///|
fn KVStore::new() -> KVStore {
  { data: {} }
}

///|
fn KVStore::get(self : KVStore, key : String) -> String? {
  self.data.get(key)
}

///|
impl @raft.StateMachine for KVStore with fn apply(self, command) {
  let s = command.to_unchecked_string()
  if s.has_prefix("s") {
    let body = s[1:].to_owned()
    let mut key = ""
    let mut value = ""
    let mut seen_eq = false
    for c in body {
      if !seen_eq && c == '=' {
        seen_eq = true
      } else if seen_eq {
        value = value + "\{c}"
      } else {
        key = key + "\{c}"
      }
    }
    self.data[key] = value
  } else if s.has_prefix("d") {
    self.data.remove(s[1:].to_owned())
  }
}

///|
/// Encode a string as the UTF-16LE bytes `to_unchecked_string` reads back.
fn to_wire(s : String) -> Bytes {
  let arr : Array[Byte] = []
  for c in s {
    let code = c.to_int()
    arr.push((code & 0xff).to_byte())
    arr.push(((code >> 8) & 0xff).to_byte())
  }
  Bytes::from_array(arr)
}

///|
fn kv_set(key : String, value : String) -> Bytes {
  to_wire("s" + key + "=" + value)
}

///|
fn kv_delete(key : String) -> Bytes {
  to_wire("d" + key)
}

///|
fn show(v : String?) -> String {
  match v {
    Some(s) => s
    None => "<none>"
  }
}

///|
fn main {
  let ids = ["a", "b", "c", "d", "e"]
  let cluster = @raft.Cluster::new(ids, seed=31)
  let leader = cluster.run_until_leader(200).unwrap()
  println("leader: \{leader}")

  // Replicate a short history of writes as consensus commands.
  let _ = cluster.propose(kv_set("service", "echelon"))
  let _ = cluster.propose(kv_set("lang", "moonbit"))
  let _ = cluster.propose(kv_set("service", "raft-moonbit"))
  let _ = cluster.propose(kv_delete("lang"))
  println("all four writes committed: \{cluster.run_until_committed(5, 300)}")

  // Each replica folds its *own* committed log into a fresh store; the results
  // must be identical.
  println("--- each replica's store, built from its own committed log ---")
  for id in ids {
    let kv = KVStore::new()
    cluster.node(id).node().apply_committed(kv)
    println("  \{id}: service=\{show(kv.get("service"))}, lang=\{show(kv.get("lang"))}")
  }
  println(
    "replicas hold identical committed commands: \{cluster.same_committed_commands()}",
  )
}
