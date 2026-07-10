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
    inflights: Inflights::new(max_inflight, 0),
  }
}

///|
/// Whether replication to this follower is currently throttled: in `Replicate`
/// once the in-flight window is full, and always while a snapshot is pending.
/// A `Probe` follower is never throttled here — it is paced one message per
/// heartbeat by the caller.
pub fn Progress::is_paused(self : Progress) -> Bool {
  match self.state {
    Replicate => self.inflights.full()
    Snapshot => true
    Probe => false
  }
}

///|
/// Record that a replication message ending at `last` was sent, consuming a slot
/// of the in-flight window. Only meaningful in `Replicate`.
pub fn Progress::sent_entries(self : Progress, last : UInt64) -> Unit {
  if self.state == Replicate {
    self.optimistic_advance(last)
    self.inflights.add(last, 1)
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
  self.inflights.reset()
  self.state = Replicate
  self.next_index = self.match_index + 1
}

///|
/// Move back to cautious probing, one entry at a time.
pub fn Progress::become_probe(self : Progress) -> Unit {
  self.inflights.reset()
  self.state = Probe
}

///|
/// Mark the follower as needing a snapshot up to `snapshot_index`; probing will
/// resume just past it once the snapshot is acknowledged.
pub fn Progress::become_snapshot(
  self : Progress,
  snapshot_index : UInt64,
) -> Unit {
  self.inflights.reset()
  self.state = Snapshot
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
