///|
/// Election Safety (Raft §5.2): no two servers ever believe they are leader in
/// the same term. Different terms are fine — that is normal succession. This is
/// the invariant a partition-and-heal scenario must never break.
pub fn Cluster::one_leader_per_term(self : Cluster) -> Bool {
  let terms : Array[UInt64] = []
  for id in self.ids {
    let n = self.nodes[id]
    if n.is_leader() {
      terms.push(n.term())
    }
  }
  terms_are_distinct(terms)
}

///|
/// Election Safety (Raft §5.2) as a predicate over the terms in which some
/// server currently believes it leads: a correct run yields distinct terms, and
/// a repeat is exactly the double-election the property forbids.
///
/// Parameters:
/// - `terms` : one entry per server that considers itself leader.
///
/// Returns whether every term is distinct.
fn terms_are_distinct(terms : Array[UInt64]) -> Bool {
  let seen : Map[UInt64, Unit] = {}
  for t in terms {
    match seen.get(t) {
      Some(_) => return false
      None => seen[t] = ()
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
  // Below the highest snapshot baseline the entries are covered by a snapshot on
  // at least one node, which no longer keeps their terms; comparison starts past
  // it, where every live node still physically holds the committed entries.
  let mut base : UInt64 = 0
  for id in self.ids {
    if self.down.get(id) != Some(true) {
      let s = self.nodes[id].node().snapshot_index
      if s > base {
        base = s
      }
    }
  }
  let mut i : UInt64 = base + 1
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
  let xs : Array[UInt64?] = []
  let ys : Array[UInt64?] = []
  let mut i = base + 1
  while i <= last {
    xs.push(nx.entry_at(i).map(fn(e) { e.term }))
    ys.push(ny.entry_at(i).map(fn(e) { e.term }))
    i = i + 1
  }
  aligned_terms_consistent(xs, ys)
}

///|
/// Log Matching (Raft §5.3) as a predicate over two aligned term sequences,
/// `None` where a log does not physically retain that position. The logs
/// disagree only where both retain the position but record different terms; a
/// position only one side retains carries no obligation and is skipped.
///
/// Parameters:
/// - `xs` / `ys` : the two term sequences, aligned index-for-index.
///
/// Returns whether the two never contradict each other on a shared position.
fn aligned_terms_consistent(xs : Array[UInt64?], ys : Array[UInt64?]) -> Bool {
  let n = if xs.length() < ys.length() { xs.length() } else { ys.length() }
  let mut i = 0
  while i < n {
    match (xs[i], ys[i]) {
      (Some(a), Some(b)) => if a != b { return false }
      _ => ()
    }
    i = i + 1
  }
  true
}

///|
/// The strongest agreement check: every live node holds byte-for-byte the same
/// command at every committed index past the highest snapshot baseline. Where
/// `committed_agrees` only compares terms, this compares the actual replicated
/// commands, so a run that commits distinct values proves they land in the same
/// order everywhere — a stand-in for linearizability of the committed prefix.
pub fn Cluster::same_committed_commands(self : Cluster) -> Bool {
  let upto = self.min_commit()
  let mut base : UInt64 = 0
  let live : Array[String] = []
  for id in self.ids {
    if self.down.get(id) != Some(true) {
      live.push(id)
      let s = self.nodes[id].node().snapshot_index
      if s > base {
        base = s
      }
    }
  }
  let slots : Array[Array[Bytes?]] = []
  let mut i = base + 1
  while i <= upto {
    let commands : Array[Bytes?] = []
    for id in live {
      commands.push(self.nodes[id].node().entry_at(i).map(fn(e) { e.command }))
    }
    slots.push(commands)
    i = i + 1
  }
  all_slots_agree(slots)
}

///|
/// Whether every committed slot's per-server commands agree. A slot on which the
/// servers disagree is a State Machine Safety violation (Raft §5.4.3), which a
/// correct run never produces but the check must still surface.
///
/// Parameters:
/// - `slots` : one per committed index, each holding every live server's command
///   at that index.
///
/// Returns whether all slots agree.
fn all_slots_agree(slots : Array[Array[Bytes?]]) -> Bool {
  for commands in slots {
    if !same_command(commands) {
      return false
    }
  }
  true
}

///|
/// Whether the commands a set of servers each hold at one committed index are
/// all present and identical — a stand-in for linearizability of that slot. A
/// `None` (a live server missing an entry it has committed) or a mismatch is the
/// divergence the check is meant to catch.
///
/// Parameters:
/// - `commands` : each live server's command at the index, `None` if absent.
///
/// Returns whether every server agrees on the same command.
fn same_command(commands : Array[Bytes?]) -> Bool {
  let mut chosen : Bytes? = None
  for c in commands {
    match c {
      None => return false
      Some(v) =>
        match chosen {
          None => chosen = Some(v)
          Some(prev) => if prev != v { return false }
        }
    }
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
