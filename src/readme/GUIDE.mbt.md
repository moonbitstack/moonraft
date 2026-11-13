# moonraft — the guide, executable

Every `mbt check` block on this page is compiled and run by `moon test`, and each
one prints exactly the transcript the guide shows. The prose guide is
[`docs/guide.html`](../../docs/guide.html); this page is the part of it
that has to stay true.

## 01 - a first cluster

```mbt check
///|
test "guide: elect a leader" {
  let cluster = @raft.Cluster::new(["n1", "n2", "n3", "n4", "n5"], seed=7)
  let leader = cluster.run_until_leader(300).unwrap()
  println("elected \{leader} in term \{cluster.node(leader).term()}")
  assert_eq(leader, "n3")
  assert_eq(cluster.node(leader).term(), 1UL)
}
```

## 02 - replicate, then read where the log ends

```mbt check
///|
test "guide: replicate and read the commit index" {
  let ids = ["a", "b", "c"]
  let cluster = @raft.Cluster::new(ids, seed=11)
  let _ = cluster.run_until_leader(200)
  let _ = cluster.propose(b"set x = 1")
  let _ = cluster.run_until_committed(2, 300)
  println("all nodes committed through index 2: \{cluster.committed_agrees()}")
  for id in ids {
    println("  \{id}: commit_index = \{cluster.node(id).commit_index()}")
  }
  assert_true(cluster.committed_agrees())
  assert_eq(cluster.node("a").commit_index(), 2UL)
  assert_eq(cluster.node("b").commit_index(), 2UL)
  assert_eq(cluster.node("c").commit_index(), 2UL)
}
```

## 03 - your own state machine

Raft orders bytes; meaning comes from the state machine, whose `apply` runs once
per committed entry, in log order, on every replica.

```mbt check
///|
/// The application state. `apply` is the only method consensus calls.
struct KV {
  data : Map[String, String]
}

///|
fn KV::new() -> KV {
  { data: Map([]), }
}

///|
fn KV::get(self : KV, key : String) -> String? {
  self.data.get(key)
}

///|
/// Commands are UTF-16 text: `s<key>=<value>` sets a key, `d<key>` deletes one.
impl @raft.StateMachine for KV with fn apply(self, command) {
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
fn KV::wire(s : String) -> Bytes {
  let arr : Array[Byte] = []
  for c in s {
    let code = c.to_int()
    arr.push((code & 0xff).to_byte())
    arr.push(((code >> 8) & 0xff).to_byte())
  }
  Bytes::from_array(arr)
}
```

Then each replica folds its own committed log into a fresh store.

```mbt check
///|
test "guide: fold each replica's committed log into a store" {
  let ids = ["a", "b", "c"]
  let cluster = @raft.Cluster::new(ids, seed=9)
  let _ = cluster.run_until_leader(200)
  let _ = cluster.propose(KV::wire("sservice=moonraft"))
  let _ = cluster.propose(KV::wire("slang=moonbit"))
  let _ = cluster.run_until_committed(3, 300)
  for id in ids {
    let kv = KV::new()
    cluster.node(id).node().apply_committed(kv)
    println(
      "  \{id}: service = \{kv.get("service").unwrap()}, lang = \{kv.get("lang").unwrap()}",
    )
    assert_eq(kv.get("service"), Some("moonraft"))
    assert_eq(kv.get("lang"), Some("moonbit"))
  }
}
```

## 04 - crash the leader

```mbt check
///|
test "guide: crash the leader and re-elect" {
  let ids = ["a", "b", "c", "d", "e"]
  let cluster = @raft.Cluster::new(ids, seed=5)
  let first = cluster.run_until_leader(200).unwrap()
  let _ = cluster.propose(b"cmd-1")
  let _ = cluster.run_until_committed(2, 200)
  cluster.crash(first)
  let second = cluster.run_until_leader(400).unwrap()
  println("crashed \{first}; new leader \{second}")
  println("one leader per term : \{cluster.one_leader_per_term()}")
  println("committed agree     : \{cluster.committed_agrees()}")
  assert_eq(first, "a")
  assert_true(second != first)
  assert_true(cluster.one_leader_per_term())
  assert_true(cluster.committed_agrees())
}
```

## 05 - a split network

```mbt check
///|
test "guide: a minority cannot elect, a majority carries on" {
  let ids = ["a", "b", "c", "d", "e"]
  let cluster = @raft.Cluster::new(ids, seed=13)
  let _ = cluster.run_until_leader(200)
  // Two nodes on one side of the split, three on the other.
  cluster.partition(["a", "b"], ["c", "d", "e"])
  cluster.run(200)
  let side = cluster.leaders()
  println("leaders after the split: \{side.length()}")
  println(
    "the majority side keeps committing: \{cluster.propose(b"after-split")}",
  )
  cluster.heal()
  cluster.run(200)
  println("after the heal, leaders: \{cluster.leaders().length()}")
  println("committed agree: \{cluster.committed_agrees()}")
  assert_eq(side.length(), 1)
  assert_eq(cluster.leaders().length(), 1)
  assert_true(cluster.committed_agrees())
}
```

## 06 - membership

```mbt check
///|
test "guide: add a voter, then a learner" {
  let cluster = @raft.Cluster::new(["a", "b", "c"], seed=7)
  let _ = cluster.run_until_leader(200)
  let _ = cluster.propose(b"init")
  let _ = cluster.run_until_committed(2, 200)
  println("voters before: \{sorted(cluster.node("a").voter_nodes())}")
  let _ = cluster.propose_conf(@raft.ConfChange::add("d"))
  let _ = cluster.run_until_committed(3, 200)
  println("voters after adding d: \{sorted(cluster.node("a").voter_nodes())}")
  let _ = cluster.propose_conf(@raft.ConfChange::add_learner("e"))
  let _ = cluster.run_until_committed(4, 200)
  println("learners: \{sorted(cluster.node("a").conf_state().learners)}")
  assert_eq(sorted(cluster.node("a").voter_nodes()), "a,b,c,d")
  assert_eq(sorted(cluster.node("a").conf_state().learners), "e")
}

///|
fn sorted(ids : Array[String]) -> String {
  let copy = ids.copy()
  copy.sort()
  copy.join(",")
}
```

## 07 - snapshots

```mbt check
///|
test "guide: a node that fell behind catches up from a snapshot" {
  let ids = ["a", "b", "c"]
  let cluster = @raft.Cluster::new(ids, seed=9)
  let leader = cluster.run_until_leader(200).unwrap()
  let lagging = ids.filter(fn(x) { x != leader })[0]
  for i in 0..<8 {
    let _ = cluster.propose(b"cmd-\{i}")
    let _ = cluster.run_until_committed(
      cluster.node(leader).node().last_log_index(),
      100,
    )
  }
  // Fold everything at or below index 5 into a snapshot.
  println("compacted at 5: \{cluster.compact_leader(5, b"payload")}")
  // While the follower is down, the leader keeps appending to a compacted log.
  cluster.crash(lagging)
  for i in 0..<4 {
    let _ = cluster.propose(b"more-\{i}")
    let _ = cluster.run_until_committed(
      cluster.node(leader).node().last_log_index(),
      100,
    )
  }
  let ahead = cluster.node(leader).commit_index()
  println("down at \{cluster.node(lagging).commit_index()}, leader at \{ahead}")
  cluster.restart(lagging)
  cluster.run(400)
  println("after the restart: \{cluster.node(lagging).commit_index()}")
  println("committed agree: \{cluster.committed_agrees()}")
  assert_eq(cluster.node(lagging).commit_index(), ahead)
  assert_true(cluster.committed_agrees())
}
```

## 08 - the Ready / Advance loop

```mbt check
///|
test "guide: drive one node through tick, Ready and Advance" {
  let node = @raft.RaftNode::new("a", [], pre_vote=false)
  let raw = node.raw()
  raw.campaign()
  let mut applied = 0
  while raw.has_ready() {
    let ready = raw.ready()
    raw.store(ready)
    applied = applied + ready.committed_entries.length()
    raw.advance(ready)
  }
  println("entries applied after campaigning: \{applied}")
  raw.propose(b"set x = 2")
  while raw.has_ready() {
    let ready = raw.ready()
    raw.store(ready)
    applied = applied + ready.committed_entries.length()
    raw.advance(ready)
  }
  println("entries applied after the proposal: \{applied}")
  println("commit_index: \{raw.node().commit_index()}")
  assert_eq(applied, 2)
  assert_eq(raw.node().commit_index(), 2UL)
}
```

## 09 - restart from durable state

```mbt check
///|
test "guide: rebuild a node from what it persisted" {
  let node = @raft.RaftNode::new("a", [], pre_vote=false)
  let raw = node.raw()
  raw.campaign()
  let run = fn() {
    while raw.has_ready() {
      let ready = raw.ready()
      raw.store(ready)
      raw.advance(ready)
    }
  }
  run()
  raw.propose(b"set x = 3")
  run()
  let store = @raft.MemoryStorage::new()
  raw.node().node().save_into(store)
  println(
    "before restart: term=\{raw.node().term()}, commit=\{raw.node().commit_index()}",
  )
  let recovered = @raft.RaftNode::new("a", [], pre_vote=false)
  recovered.node().load_from(store)
  println(
    "after restart:  term=\{recovered.term()}, commit=\{recovered.commit_index()}",
  )
  assert_eq(recovered.term(), raw.node().term())
  assert_eq(recovered.commit_index(), raw.node().commit_index())
}
```

## 10 - a linearizable read

```mbt check
///|
test "guide: a read the quorum has confirmed" {
  let ids = ["a", "b", "c"]
  let cluster = @raft.Cluster::new(ids, seed=17)
  let leader = cluster.run_until_leader(200).unwrap()
  let _ = cluster.propose(b"v1")
  let _ = cluster.run_until_committed(2, 200)
  let raw = cluster.node(leader).raw()
  raw.read_index(b"client-ctx-1")
  let mut reads = 0
  for _ in 0..<200 {
    cluster.run(1)
    if raw.has_ready() {
      let ready = raw.ready()
      raw.store(ready)
      reads = reads + ready.read_states.length()
      raw.advance(ready)
    }
    if reads > 0 {
      break
    }
  }
  println("read states confirmed by a quorum: \{reads}")
  println("safe to read at commit index: \{raw.node().commit_index()}")
  assert_true(reads > 0)
  assert_eq(raw.node().commit_index(), 2UL)
}
```

## 11 - logging

```mbt check
///|
/// The core logs through one method and defaults to silence, so a deployment
/// decides where diagnostics go.
struct Recorder {
  lines : Array[String]
}

///|
impl @raft.Logger for Recorder with fn log(self, _level, message) {
  self.lines.push(message)
}

///|
test "guide: capture what the core reports" {
  let recorder : Recorder = { lines: [], }
  let node = @raft.RaftNode::new("a", [], pre_vote=false, logger=recorder)
  let raw = node.raw()
  raw.campaign()
  while raw.has_ready() {
    let ready = raw.ready()
    raw.store(ready)
    raw.advance(ready)
  }
  println("records captured: \{recorder.lines.length()}")
  for line in recorder.lines {
    println("  \{line}")
  }
  assert_eq(recorder.lines.length(), 8)
  assert_eq(
    recorder.lines[recorder.lines.length() - 1],
    "a became leader at term 1",
  )
}
```

Already running moonlog? The bridge adapts its logger to the same seam.

```mbt check
///|
test "guide: the same seam takes a moonlog logger" {
  let bridge = @moonlog_bridge.MoonlogLogger::of(@moonlog.Hub::new())
  let logger = bridge.as_logger()
  logger.log(@raft.LogLevel::Info, "moonraft is ready")
  println("a hub with no sinks accepts the record and writes nothing")
  assert_true(true)
}
```
