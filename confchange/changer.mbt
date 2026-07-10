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
  max_inflight_bytes : UInt64
}

///|
/// A Changer over an empty configuration, with progress next-indices anchored at
/// `last_index`, a per-follower window of `max_inflight` messages and, when
/// non-zero, `max_inflight_bytes` bytes (etcd's `MakeProgressTracker`
/// (maxInflight, maxBytes); 0 = no byte limit).
pub fn Changer::new(
  last_index? : UInt64 = 0,
  max_inflight? : Int = 256,
  max_inflight_bytes? : UInt64 = 0,
) -> Changer {
  {
    cfg: ChangerConfig::empty(),
    prs: {},
    last_index,
    max_inflight,
    max_inflight_bytes,
  }
}

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
  let p = Progress::new(
    next,
    max_inflight=self.max_inflight,
    max_inflight_bytes=self.max_inflight_bytes,
  )
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
/// The configuration as a `ConfState` (etcd's `ProgressTracker.ConfState`): the
/// incoming voters, the outgoing half, the learners and the staged learners,
/// plus the auto-leave flag. Used to round-trip a configuration through a
/// snapshot.
pub fn Changer::conf_state(self : Changer) -> ConfState {
  {
    voters: self.cfg.incoming.copy(),
    voters_outgoing: self.cfg.outgoing.copy(),
    learners: self.cfg.learners.copy(),
    learners_next: self.cfg.learners_next.copy(),
    auto_leave: self.cfg.auto_leave,
  }
}

///|
/// Validate that a configuration and its progress map are mutually consistent
/// (etcd's `checkInvariants`). Returns an error message describing the first
/// violation, or `None`. This is the same defensive check etcd runs on the
/// result of every configuration change; it never fires in correct operation
/// but pins the joint-consensus invariants (learners disjoint from voters, a
/// staged learner still an outgoing voter, empty auxiliary sets when not joint).
fn check_invariants(
  cfg : ChangerConfig,
  prs : Map[String, Progress],
) -> String? {
  // Every voter (either half), learner and staged learner needs a progress.
  let ids : Array[String] = []
  for id in cfg.incoming {
    if !ids.contains(id) {
      ids.push(id)
    }
  }
  for id in cfg.outgoing {
    if !ids.contains(id) {
      ids.push(id)
    }
  }
  for id in cfg.learners {
    if !ids.contains(id) {
      ids.push(id)
    }
  }
  for id in cfg.learners_next {
    if !ids.contains(id) {
      ids.push(id)
    }
  }
  for id in ids {
    if prs.get(id) is None {
      return Some("no progress for " + id)
    }
  }
  // A staged learner was staged because an outgoing voter blocked a direct add.
  for id in cfg.learners_next {
    if !cfg.outgoing.contains(id) {
      return Some(id + " is in LearnersNext, but not Voters[1]")
    }
    if prs.get(id) is Some(pr) && pr.is_learner {
      return Some(id + " is in LearnersNext, but is already marked as learner")
    }
  }
  // Conversely, learners never intersect the voter halves.
  for id in cfg.learners {
    if cfg.outgoing.contains(id) {
      return Some(id + " is in Learners and Voters[1]")
    }
    if cfg.incoming.contains(id) {
      return Some(id + " is in Learners and Voters[0]")
    }
    if prs.get(id) is Some(pr) && !pr.is_learner {
      return Some(id + " is in Learners, but is not marked as learner")
    }
  }
  // AutoLeave is only meaningful in a joint config. A non-joint config with a
  // non-empty LearnersNext is already rejected upstream by the "in LearnersNext
  // but not Voters[1]" check (a non-joint config has an empty outgoing half), so
  // it never reaches here — etcd carries the same shadowed guard.
  if !cfg.is_joint() && cfg.auto_leave {
    return Some("AutoLeave must be false when not joint")
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
  if self.apply(changes) is Some(e) {
    return rollback(e)
  }
  if symdiff(before, self.cfg.incoming) > 1 {
    return rollback("more than one voter changed without entering joint config")
  }
  if check_invariants(self.cfg, self.prs) is Some(e) {
    return rollback(e)
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
  if self.apply(changes) is Some(e) {
    return rollback(e)
  }
  self.cfg.auto_leave = auto_leave
  if check_invariants(self.cfg, self.prs) is Some(e) {
    return rollback(e)
  }
  None
}

///|
/// Leave joint consensus: promote any staged `learners_next` to learners,
/// preserving their progress, and drop the outgoing half.
pub fn Changer::leave_joint(self : Changer) -> String? {
  let saved_cfg = self.cfg.clone()
  let saved_prs = clone_prs(self.prs)
  fn rollback(msg : String) -> String? {
    self.cfg = saved_cfg
    self.prs = saved_prs
    Some(msg)
  }

  if !self.cfg.is_joint() {
    return Some("can't leave a non-joint config")
  }
  for id in self.cfg.learners_next {
    arr_add(self.cfg.learners, id)
    if self.prs.get(id) is Some(pr) {
      pr.is_learner = true
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
  if check_invariants(self.cfg, self.prs) is Some(e) {
    return rollback(e)
  }
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
      if self.simple([c]) is Some(e) {
        return Some(e)
      }
    }
    None
  } else {
    // Each outgoing op is a single voter add applied to a non-joint config, which
    // never fails (it changes one voter, keeps at least one, and preserves every
    // invariant), so the temporary reconstruction cannot error before the joint
    // batch below.
    for c in outgoing {
      self.simple([c]) |> ignore
    }
    self.enter_joint(cs.auto_leave, incoming)
  }
}

///|
fn majority_str(ids : Array[String]) -> String {
  let s = ids.copy()
  s.sort()
  "(" + s.join(" ") + ")"
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
