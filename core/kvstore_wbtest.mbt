///|
/// A replicated key-value store: the sample application the consensus core
/// drives. Commands are applied in committed log order, so every replica that
/// has applied the same prefix holds the same map — that is the whole point of
/// the log. Commands are encoded as UTF-16 text: "s<key>=<value>" sets a key and
/// "d<key>" deletes one.
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
fn KVStore::len(self : KVStore) -> Int {
  self.data.length()
}

///|
/// Encode a string as the UTF-16LE bytes a MoonBit string reads back.
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
/// The command that sets `key` to `value`.
fn kv_set(key : String, value : String) -> Bytes {
  to_wire("s" + key + "=" + value)
}

///|
/// The command that deletes `key`.
fn kv_delete(key : String) -> Bytes {
  to_wire("d" + key)
}

///|
impl StateMachine for KVStore with fn apply(self, command) {
  let s = command.to_unchecked_string()
  if s.has_prefix("s") {
    let body = s[1:].to_owned()
    let mut key = ""
    let mut value = ""
    let mut seen = false
    for c in body {
      if !seen && c == '=' {
        seen = true
      } else if seen {
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
test "the key-value store applies set and delete commands" {
  let kv = KVStore::new()
  kv.apply(kv_set("name", "raft"))
  kv.apply(kv_set("lang", "moonbit"))
  assert_true(kv.get("name") == Some("raft"))
  assert_true(kv.get("lang") == Some("moonbit"))
  kv.apply(kv_set("name", "echelon"))
  assert_true(kv.get("name") == Some("echelon"))
  kv.apply(kv_delete("lang"))
  assert_true(kv.get("lang") == None)
  assert_eq(kv.len(), 1)
}

///|
/// Fold a node's whole committed log into a fresh key-value store.
fn kv_of(node : Node) -> KVStore {
  let kv = KVStore::new()
  node.apply_committed(kv)
  kv
}

///|
test "scenario: a replicated key-value store converges across the cluster" {
  let c = Cluster::new(["a", "b", "c"], seed=31)
  let _ = c.run_until_leader(200)
  let _ = c.propose(kv_set("k1", "v1"))
  let _ = c.propose(kv_set("k2", "v2"))
  let _ = c.propose(kv_set("k1", "v1b"))
  let _ = c.propose(kv_delete("k2"))
  assert_true(c.run_until_committed(5, 300))

  // Every node, applying its own committed log, reaches the same store.
  let ka = kv_of(c.node("a").node())
  let kb = kv_of(c.node("b").node())
  let kc = kv_of(c.node("c").node())
  assert_true(ka.get("k1") == Some("v1b"))
  assert_true(ka.get("k2") == None)
  assert_eq(ka.len(), 1)
  assert_true(kb.get("k1") == Some("v1b"))
  assert_true(kc.get("k1") == Some("v1b"))
  assert_eq(kb.len(), 1)
  assert_eq(kc.len(), 1)
}

///|
test "scenario: the store still converges after leader changes" {
  let c = Cluster::new(["a", "b", "c", "d", "e"], seed=32)
  let first = c.run_until_leader(200).unwrap()
  let _ = c.propose(kv_set("x", "1"))
  let _ = c.run_until_committed(2, 200)
  // The leader fails; a new one takes over and more writes land.
  c.crash(first)
  let _ = c.run_until_leader(400)
  let _ = c.propose(kv_set("y", "2"))
  let _ = c.propose(kv_set("x", "3"))
  c.run(300)
  // Bring the old leader back; it catches up.
  c.restart(first)
  c.run(400)
  assert_true(c.same_committed_commands())
  // The two live followers that never led agree on the final store.
  let live : Array[String] = []
  for id in ["a", "b", "c", "d", "e"] {
    if id != first {
      live.push(id)
    }
  }
  let ref_kv = kv_of(c.node(live[0]).node())
  assert_true(ref_kv.get("x") == Some("3"))
  assert_true(ref_kv.get("y") == Some("2"))
}
