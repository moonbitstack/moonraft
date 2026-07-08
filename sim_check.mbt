///|
/// Election Safety (Raft §5.2): no two servers ever believe they are leader in
/// the same term. Different terms are fine — that is normal succession. This is
/// the invariant a partition-and-heal scenario must never break.
pub fn Cluster::one_leader_per_term(self : Cluster) -> Bool {
  let seen : Map[UInt64, String] = {}
  for id in self.ids {
    let n = self.nodes[id]
    if n.is_leader() {
      let t = n.term()
      match seen.get(t) {
        Some(_) => return false
        None => seen[t] = id
      }
    }
  }
  true
}

///|
/// The smallest commit index across all running nodes: the length of the log
/// prefix every live server has agreed to apply.
fn Cluster::min_commit(self : Cluster) -> UInt64 {
  let mut m : UInt64 = 0xffffffffffffffff
  let mut any = false
  for id in self.ids {
    if self.down.get(id) != Some(true) {
      any = true
      let c = self.nodes[id].commit_index()
      if c < m {
        m = c
      }
    }
  }
  if any {
    m
  } else {
    0
  }
}

///|
/// State Machine Safety (Raft §5.4.3): if any two servers have committed an
/// entry at a given index, it is the same entry. Checked over the common
/// committed prefix by comparing the term at every index — a divergence there
/// would mean two different commands were committed at the same slot.
pub fn Cluster::committed_agrees(self : Cluster) -> Bool {
  let upto = self.min_commit()
  let mut i : UInt64 = 1
  while i <= upto {
    let mut term : UInt64? = None
    for id in self.ids {
      if self.down.get(id) != Some(true) {
        let t = self.nodes[id].node().term_at(i)
        match term {
          None => term = Some(t)
          Some(tt) => if tt != t { return false }
        }
      }
    }
    i = i + 1
  }
  true
}

///|
/// Log Matching (Raft §5.3): wherever two running logs both hold an entry at
/// some index with the same term, every preceding entry matches too. Checked
/// pairwise against the first running node as a reference over the indices both
/// physically retain (past any snapshot baseline).
pub fn Cluster::logs_consistent(self : Cluster) -> Bool {
  let live : Array[String] = []
  for id in self.ids {
    if self.down.get(id) != Some(true) {
      live.push(id)
    }
  }
  if live.length() < 2 {
    return true
  }
  let mut a = 0
  while a < live.length() {
    let mut b = a + 1
    while b < live.length() {
      if !self.pair_consistent(live[a], live[b]) {
        return false
      }
      b = b + 1
    }
    a = a + 1
  }
  true
}

///|
/// Whether two logs never disagree on the term at a shared, physically-present
/// index.
fn Cluster::pair_consistent(self : Cluster, x : String, y : String) -> Bool {
  let nx = self.nodes[x].node()
  let ny = self.nodes[y].node()
  let last = if nx.last_log_index() < ny.last_log_index() {
    nx.last_log_index()
  } else {
    ny.last_log_index()
  }
  let base = if nx.snapshot_index > ny.snapshot_index {
    nx.snapshot_index
  } else {
    ny.snapshot_index
  }
  let mut i = base + 1
  while i <= last {
    match (nx.entry_at(i), ny.entry_at(i)) {
      (Some(ex), Some(ey)) => if ex.term != ey.term { return false }
      _ => ()
    }
    i = i + 1
  }
  true
}

///|
/// Whether every stated invariant holds right now. A scenario asserts this after
/// each interesting step.
pub fn Cluster::invariants_hold(self : Cluster) -> Bool {
  self.one_leader_per_term() &&
  self.committed_agrees() &&
  self.logs_consistent()
}
