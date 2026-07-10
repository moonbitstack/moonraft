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
/// How the leader is currently replicating to one follower.
///
/// `Probe` sends one AppendEntries at a time until the follower's match point
/// is found; `Replicate` streams entries once the logs are known to agree; and
/// `Snapshot` means the follower is so far behind that the next thing it needs
/// has already been compacted, so a snapshot must be shipped first (etcd's
/// three progress states).
pub enum ProgressState {
  Probe
  Replicate
  Snapshot
} derive(Eq)

///|
/// The leader's view of one follower's replication progress. `next_index` is
/// the next log index to send; `match_index` is the highest index known to be
/// stored on the follower. `recent_active` records whether the follower has
/// answered since the last liveness sweep, which the read-index and lease paths
/// use to confirm the leader still commands a quorum.
pub struct Progress {
  mut next_index : UInt64
  mut match_index : UInt64
  mut state : ProgressState
  mut recent_active : Bool
  // Throttle flag: set when the MsgApp flow to this follower is paused (a probe
  // was sent, or the in-flight window filled). A periodic message is still sent
  // once it clears — on an ack or a heartbeat response — so progress resumes.
  mut msg_app_flow_paused : Bool
  // In `Snapshot`, the last index of the snapshot the leader shipped; replication
  // is paused until the follower reconnects to the log past it.
  mut pending_snapshot : UInt64
  // Whether this follower is a learner (non-voting). Kept here too so a progress
  // dump is self-describing; the authoritative set lives in `Membership`.
  mut is_learner : Bool
  inflights : Inflights
}

///|
/// A fresh progress that will start probing from `next`, with a replication
/// flow-control window of `max_inflight` outstanding AppendEntries.
pub fn Progress::new(next : UInt64, max_inflight? : Int = 256) -> Progress {
  {
    next_index: next,
    match_index: 0,
    state: Probe,
    recent_active: false,
    msg_app_flow_paused: false,
    pending_snapshot: 0,
    is_learner: false,
    inflights: Inflights::new(max_inflight, 0),
  }
}

///|
fn progress_state_name(s : ProgressState) -> String {
  match s {
    Probe => "StateProbe"
    Replicate => "StateReplicate"
    Snapshot => "StateSnapshot"
  }
}

///|
/// A one-line, self-describing summary of this progress (etcd's Progress.String).
pub fn Progress::to_string(self : Progress) -> String {
  let mut s = progress_state_name(self.state) +
    " match=" +
    self.match_index.to_string() +
    " next=" +
    self.next_index.to_string()
  if self.is_learner {
    s = s + " learner"
  }
  if self.is_paused() {
    s = s + " paused"
  }
  if self.pending_snapshot > 0 {
    s = s + " pendingSnap=" + self.pending_snapshot.to_string()
  }
  if !self.recent_active {
    s = s + " inactive"
  }
  let n = self.inflights.count()
  if n > 0 {
    s = s + " inflight=" + n.to_string()
    if self.inflights.full() {
      s = s + "[full]"
    }
  }
  s
}

///|
/// Move into `state`, clearing the throttle, pending snapshot and in-flight
/// window (etcd's ResetState).
fn Progress::reset_state(self : Progress, state : ProgressState) -> Unit {
  self.msg_app_flow_paused = false
  self.pending_snapshot = 0
  self.state = state
  self.inflights.reset()
}

///|
/// Whether replication to this follower is currently throttled: while a snapshot
/// is pending, or whenever the MsgApp flow has been paused (a probe in flight, or
/// a full in-flight window).
pub fn Progress::is_paused(self : Progress) -> Bool {
  match self.state {
    Probe => self.msg_app_flow_paused
    Replicate => self.msg_app_flow_paused
    Snapshot => true
  }
}

///|
/// Clear the flow-control throttle (on an ack or a heartbeat response), so one
/// more message may be sent.
pub fn Progress::unpause(self : Progress) -> Unit {
  self.msg_app_flow_paused = false
}

///|
/// Record that a replication message ending at `last` and carrying `has_entries`
/// entries was sent. In `Replicate` this consumes an in-flight slot and pauses
/// once the window fills; in `Probe` any non-empty send pauses until acked.
pub fn Progress::sent_entries(
  self : Progress,
  last : UInt64,
  has_entries : Bool,
) -> Unit {
  match self.state {
    Replicate => {
      if has_entries {
        self.optimistic_advance(last)
        self.inflights.add(last, 1)
      }
      // Re-evaluate the throttle even for an empty probe (etcd SentEntries): a
      // full window keeps the flow paused.
      self.msg_app_flow_paused = self.inflights.full()
    }
    Probe => if has_entries { self.msg_app_flow_paused = true }
    Snapshot => ()
  }
}

///|
/// Free every in-flight slot up through the acknowledged `index`.
pub fn Progress::free_le(self : Progress, index : UInt64) -> Unit {
  self.inflights.free_le(index)
}

///|
/// Record that the follower answered during the current liveness sweep.
pub fn Progress::mark_active(self : Progress) -> Unit {
  self.recent_active = true
}

///|
/// Clear the liveness flag at the start of a new sweep.
pub fn Progress::reset_active(self : Progress) -> Unit {
  self.recent_active = false
}

///|
/// Whether the follower has answered since the last sweep.
pub fn Progress::is_active(self : Progress) -> Bool {
  self.recent_active
}

///|
/// Move to streaming replication, sending from just past the match point.
pub fn Progress::become_replicate(self : Progress) -> Unit {
  self.reset_state(Replicate)
  self.next_index = self.match_index + 1
}

///|
/// Move back to cautious probing, one entry at a time. Coming out of `Snapshot`,
/// resume just past the snapshot the follower was sent (etcd's BecomeProbe).
pub fn Progress::become_probe(self : Progress) -> Unit {
  let from_snapshot = self.state == Snapshot
  let pending = self.pending_snapshot
  self.reset_state(Probe)
  self.next_index = if from_snapshot {
    let a = self.match_index + 1
    let b = pending + 1
    if a > b {
      a
    } else {
      b
    }
  } else {
    self.match_index + 1
  }
}

///|
/// Mark the follower as needing a snapshot up to `snapshot_index`; probing will
/// resume just past it once the snapshot is acknowledged.
pub fn Progress::become_snapshot(
  self : Progress,
  snapshot_index : UInt64,
) -> Unit {
  self.reset_state(Snapshot)
  self.pending_snapshot = snapshot_index
  self.next_index = snapshot_index + 1
}

///|
/// Fold in a successful acknowledgement up through `index`. Advances the match
/// and next indices, never backwards, and returns whether the match point moved
/// forward (which is what can let the leader commit new entries).
pub fn Progress::maybe_update(self : Progress, index : UInt64) -> Bool {
  let advanced = index > self.match_index
  if advanced {
    self.match_index = index
    // A genuine advance means the follower is keeping up: resume the flow.
    self.msg_app_flow_paused = false
  }
  if self.next_index < index + 1 {
    self.next_index = index + 1
  }
  advanced
}

///|
/// Optimistically advance `next_index` past `last` while streaming, so the next
/// AppendEntries carries the following batch without waiting for the ack.
pub fn Progress::optimistic_advance(self : Progress, last : UInt64) -> Unit {
  if self.next_index < last + 1 {
    self.next_index = last + 1
  }
}

///|
/// Adjust to a rejected AppendEntries (etcd `MaybeDecrTo`). `rejected` is the
/// prev-index the follower rejected; `match_hint` is where we want to retry
/// (the leader-side findConflictByTerm result). A rejection is stale — and
/// ignored — if it cannot pertain to an entry still in flight. Returns whether
/// `next_index` moved.
pub fn Progress::maybe_decr_to(
  self : Progress,
  rejected : UInt64,
  match_hint : UInt64,
) -> Bool {
  if self.state == Replicate {
    // In flight streaming: a rejection at or below the match point is stale.
    if rejected <= self.match_index {
      return false
    }
    self.next_index = self.match_index + 1
    return true
  }
  // Probing sends one entry at a time, so a rejection must be for the entry we
  // last probed; otherwise it is a stale duplicate.
  if self.next_index - 1 != rejected {
    return false
  }
  let capped = if rejected < match_hint + 1 { rejected } else { match_hint + 1 }
  let floor = self.match_index + 1
  self.next_index = if capped > floor { capped } else { floor }
  self.msg_app_flow_paused = false
  true
}

///|
/// Back off after a rejected AppendEntries, using the follower's conflict hint
/// to jump rather than decrement by one. Never rewinds below the match point or
/// below index 1. Returns whether `next_index` actually moved.
pub fn Progress::maybe_decrease(self : Progress, hint : UInt64) -> Bool {
  let floor = self.match_index + 1
  let target = if hint > floor { hint } else { floor }
  if target < self.next_index {
    self.next_index = target
    true
  } else {
    false
  }
}
