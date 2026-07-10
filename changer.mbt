// Copyright 2019 The etcd Authors
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

// The configuration Changer (etcd's confchange.Changer): it applies batches of
// single membership changes to a configuration, enforcing the joint-consensus
// invariants — in particular `learners_next`, the staging area for a voter
// demoted *during* a joint change, which stays a voter in the outgoing half and
// only becomes a learner on LeaveJoint. This is the confchange-package model,
// separate from the live `RaftNode` apply path.

///|
/// A membership snapshot as carried by a Raft snapshot (etcd's `ConfState`).
pub(all) struct ConfState {
  voters : Array[String]
  learners : Array[String]
  voters_outgoing : Array[String]
  learners_next : Array[String]
  auto_leave : Bool
} derive(Eq)

///|
fn set_eq(a : Array[String], b : Array[String]) -> Bool {
  let sa = a.copy()
  sa.sort()
  let sb = b.copy()
  sb.sort()
  if sa.length() != sb.length() {
    return false
  }
  for i in 0..<sa.length() {
    if sa[i] != sb[i] {
      return false
    }
  }
  true
}

///|
/// Whether two `ConfState`s describe the same configuration (etcd's
/// `ConfState.Equivalent`): the four id lists match as *sets* (order- and
/// nil/empty-insensitive) and the auto-leave flags agree.
pub fn ConfState::equivalent(self : ConfState, other : ConfState) -> Bool {
  set_eq(self.voters, other.voters) &&
  set_eq(self.learners, other.learners) &&
  set_eq(self.voters_outgoing, other.voters_outgoing) &&
  set_eq(self.learners_next, other.learners_next) &&
  self.auto_leave == other.auto_leave
}

///|
/// The full configuration the Changer maintains: the incoming and outgoing voter
/// halves (`outgoing` non-empty iff joint), the learners, and the learners that
/// will become learners once a joint configuration is left (`learners_next`).
pub struct ChangerConfig {
  incoming : Array[String]
  outgoing : Array[String]
  learners : Array[String]
  learners_next : Array[String]
  mut auto_leave : Bool
}

///|
fn ChangerConfig::empty() -> ChangerConfig {
  {
    incoming: [],
    outgoing: [],
    learners: [],
    learners_next: [],
    auto_leave: false,
  }
}

///|
fn ChangerConfig::clone(self : ChangerConfig) -> ChangerConfig {
  {
    incoming: self.incoming.copy(),
    outgoing: self.outgoing.copy(),
    learners: self.learners.copy(),
    learners_next: self.learners_next.copy(),
    auto_leave: self.auto_leave,
  }
}

///|
fn ChangerConfig::is_joint(self : ChangerConfig) -> Bool {
  self.outgoing.length() > 0
}

///|
fn arr_add(a : Array[String], id : String) -> Unit {
  if !a.contains(id) {
    a.push(id)
  }
}

///|
fn arr_del(a : Array[String], id : String) -> Unit {
  a.retain(fn(x) { x != id })
}

///|
/// The Changer drives a configuration through single and joint changes. Each
/// public operation is transactional: on error the configuration is left
/// untouched (etcd's checkAndCopy semantics).
pub struct Changer {
  mut cfg : ChangerConfig
  mut prs : Map[String, Progress]
  mut last_index : UInt64
  max_inflight : Int
}

///|
/// A Changer over an empty configuration, with progress next-indices anchored at
/// `last_index` and a per-follower window of `max_inflight`.
pub fn Changer::new(
  last_index? : UInt64 = 0,
  max_inflight? : Int = 256,
) -> Changer {
  { cfg: ChangerConfig::empty(), prs: {}, last_index, max_inflight }
}

///|

///|
/// Advance the index a newly-added follower's progress is anchored at. The
/// datadriven harness bumps this once per command so `next` reveals which
/// "round" a progress was created in (proving a demoted voter's progress is
/// preserved, not recreated, across a joint transition).
pub fn Changer::advance_index(self : Changer) -> Unit {
  self.last_index = self.last_index + 1
}

///|
fn clone_prs(prs : Map[String, Progress]) -> Map[String, Progress] {
  let m : Map[String, Progress] = {}
  for k, v in prs {
    m[k] = v.copy()
  }
  m
}

///|
fn Changer::init_progress(
  self : Changer,
  id : String,
  is_learner : Bool,
) -> Unit {
  if is_learner {
    arr_add(self.cfg.learners, id)
  } else {
    arr_add(self.cfg.incoming, id)
  }
  let next = if self.last_index > 1 { self.last_index } else { 1 }
  let p = Progress::new(next, max_inflight=self.max_inflight)
  p.is_learner = is_learner
  // A freshly-added node is treated as recently active so check-quorum does not
  // immediately count it against the leader.
  p.recent_active = true
  self.prs[id] = p
}

///|
fn Changer::make_voter(self : Changer, id : String) -> Unit {
  match self.prs.get(id) {
    None => self.init_progress(id, false)
    Some(pr) => {
      pr.is_learner = false
      arr_del(self.cfg.learners, id)
      arr_del(self.cfg.learners_next, id)
      arr_add(self.cfg.incoming, id)
    }
  }
}

///|
fn Changer::remove_id(self : Changer, id : String) -> Unit {
  if self.prs.get(id) is None {
    return
  }
  arr_del(self.cfg.incoming, id)
  arr_del(self.cfg.learners, id)
  arr_del(self.cfg.learners_next, id)
  // A peer still voting in the outgoing half keeps its progress.
  if !self.cfg.outgoing.contains(id) {
    self.prs.remove(id)
  }
}

///|
fn Changer::make_learner(self : Changer, id : String) -> Unit {
  match self.prs.get(id) {
    None => self.init_progress(id, true)
    Some(pr) => {
      if pr.is_learner {
        return
      }
      // Drop the voter but keep its progress, then stage or add the learner.
      self.remove_id(id)
      self.prs[id] = pr
      if self.cfg.outgoing.contains(id) {
        // Can't be a learner and an (outgoing) voter at once: stage it.
        arr_add(self.cfg.learners_next, id)
      } else {
        pr.is_learner = true
        arr_add(self.cfg.learners, id)
      }
    }
  }
}

///|
/// Apply a batch of single changes (`v`=add voter, `l`=add/demote learner,
/// `r`=remove, `u`=update/no-op). Returns an error message if it empties the
/// voter set.
fn Changer::apply(self : Changer, changes : Array[(String, String)]) -> String? {
  for c in changes {
    let (op, id) = c
    match op {
      "v" => self.make_voter(id)
      "l" => self.make_learner(id)
      "r" => self.remove_id(id)
      "u" => ()
      _ => return Some("unexpected conf type " + op)
    }
  }
  if self.cfg.incoming.length() == 0 {
    return Some("removed all voters")
  }
  None
}

///|
fn symdiff(a : Array[String], b : Array[String]) -> Int {
  let mut n = 0
  for x in a {
    if !b.contains(x) {
      n = n + 1
    }
  }
  for x in b {
    if !a.contains(x) {
      n = n + 1
    }
  }
  n
}

///|
/// A simple (non-joint) change: it may mutate the incoming voter set by at most
/// one, and may not run while joint.
pub fn Changer::simple(
  self : Changer,
  changes : Array[(String, String)],
) -> String? {
  let saved_cfg = self.cfg.clone()
  let saved_prs = clone_prs(self.prs)
  fn rollback(msg : String) -> String? {
    self.cfg = saved_cfg
    self.prs = saved_prs
    Some(msg)
  }

  if self.cfg.is_joint() {
    return rollback("can't apply simple config change in joint config")
  }
  let before = self.cfg.incoming.copy()
  match self.apply(changes) {
    Some(e) => return rollback(e)
    None => ()
  }
  if symdiff(before, self.cfg.incoming) > 1 {
    return rollback("more than one voter changed without entering joint config")
  }
  None
}

///|
/// Enter joint consensus C(new,old): rotate the incoming voters into the
/// outgoing half, then apply the batch to the incoming half.
pub fn Changer::enter_joint(
  self : Changer,
  auto_leave : Bool,
  changes : Array[(String, String)],
) -> String? {
  let saved_cfg = self.cfg.clone()
  let saved_prs = clone_prs(self.prs)
  fn rollback(msg : String) -> String? {
    self.cfg = saved_cfg
    self.prs = saved_prs
    Some(msg)
  }

  if self.cfg.is_joint() {
    return rollback("config is already joint")
  }
  if self.cfg.incoming.length() == 0 {
    return rollback("can't make a zero-voter config joint")
  }
  self.cfg.outgoing.clear()
  for id in self.cfg.incoming {
    self.cfg.outgoing.push(id)
  }
  match self.apply(changes) {
    Some(e) => return rollback(e)
    None => ()
  }
  self.cfg.auto_leave = auto_leave
  None
}

///|
/// Leave joint consensus: promote any staged `learners_next` to learners,
/// preserving their progress, and drop the outgoing half.
pub fn Changer::leave_joint(self : Changer) -> String? {
  if !self.cfg.is_joint() {
    return Some("can't leave a non-joint config")
  }
  for id in self.cfg.learners_next {
    arr_add(self.cfg.learners, id)
    match self.prs.get(id) {
      Some(pr) => pr.is_learner = true
      None => ()
    }
  }
  self.cfg.learners_next.clear()
  for id in self.cfg.outgoing {
    let is_voter = self.cfg.incoming.contains(id)
    let is_learner = self.cfg.learners.contains(id)
    if !is_voter && !is_learner {
      self.prs.remove(id)
    }
  }
  self.cfg.outgoing.clear()
  self.cfg.auto_leave = false
  None
}

///|
/// Rebuild a configuration from a `ConfState` (etcd's `Restore`), running the
/// same sequence of changes the state describes. Returns an error message on an
/// inconsistent state.
pub fn Changer::restore(self : Changer, cs : ConfState) -> String? {
  // Outgoing first (as a temporary non-joint config), then the incoming batch.
  let outgoing : Array[(String, String)] = []
  for id in cs.voters_outgoing {
    outgoing.push(("v", id))
  }
  let incoming : Array[(String, String)] = []
  for id in cs.voters_outgoing {
    incoming.push(("r", id))
  }
  for id in cs.voters {
    incoming.push(("v", id))
  }
  for id in cs.learners {
    incoming.push(("l", id))
  }
  for id in cs.learners_next {
    incoming.push(("l", id))
  }
  if outgoing.length() == 0 {
    // A non-joint config: apply the incoming changes one at a time.
    for c in incoming {
      match self.simple([c]) {
        Some(e) => return Some(e)
        None => ()
      }
    }
    None
  } else {
    for c in outgoing {
      match self.simple([c]) {
        Some(e) => return Some(e)
        None => ()
      }
    }
    self.enter_joint(cs.auto_leave, incoming)
  }
}

///|
fn majority_str(ids : Array[String]) -> String {
  let s = ids.copy()
  s.sort()
  let mut out = "("
  for i in 0..<s.length() {
    if i > 0 {
      out = out + " "
    }
    out = out + s[i]
  }
  out + ")"
}

///|
/// The configuration and its per-follower progress, in etcd's datadriven format:
/// `voters=(…)[&&(…)] [learners=(…)] [learners_next=(…)] [autoleave]`, then one
/// line per follower.
pub fn Changer::describe(self : Changer) -> String {
  let mut out = "voters=" + majority_str(self.cfg.incoming)
  if self.cfg.is_joint() {
    out = out + "&&" + majority_str(self.cfg.outgoing)
  }
  if self.cfg.learners.length() > 0 {
    out = out + " learners=" + majority_str(self.cfg.learners)
  }
  if self.cfg.learners_next.length() > 0 {
    out = out + " learners_next=" + majority_str(self.cfg.learners_next)
  }
  if self.cfg.auto_leave {
    out = out + " autoleave"
  }
  out = out + "\n"
  let ids : Array[String] = []
  for id, _ in self.prs {
    ids.push(id)
  }
  ids.sort()
  for id in ids {
    out = out + id + ": " + self.prs[id].to_string() + "\n"
  }
  out
}
