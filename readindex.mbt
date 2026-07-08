///|
/// Whether a quorum of the configuration has been heard from within the current
/// liveness window — the leader itself plus every follower whose progress is
/// flagged active. This is what confirms the leader still commands the cluster,
/// underpinning both check-quorum and lease reads.
pub fn RaftNode::quorum_active(self : RaftNode) -> Bool {
  let acked : Array[String] = [self.id]
  for peer, p in self.progress {
    if p.is_active() {
      acked.push(peer)
    }
  }
  self.config.has_majority(acked)
}

///|
/// Start a fresh liveness window: forget who has answered so the next window
/// measures only recent contact.
fn RaftNode::reset_recent_active(self : RaftNode) -> Unit {
  for _peer, p in self.progress {
    p.reset_active()
  }
}

///|
/// Serve a linearizable read (Raft §6.4). A follower cannot answer, so `None` is
/// returned. A leader may answer only once it has committed an entry in its own
/// term — the no-op appended on election, once committed, guarantees its commit
/// index reflects the latest term — and only while a quorum has confirmed its
/// leadership this window. The returned index is the commit index the caller
/// must wait for the state machine to reach before replying to the client, which
/// is what makes the read see every previously-acknowledged write.
pub fn RaftNode::read_index(self : RaftNode) -> UInt64? {
  if self.core.role() != Leader {
    return None
  }
  if self.core.term_at(self.core.commit_index) != self.core.current_term() {
    return None
  }
  if self.peers.length() == 0 || self.quorum_active() {
    Some(self.core.commit_index)
  } else {
    None
  }
}

///|
/// Whether this leader currently holds a valid lease — a quorum has confirmed it
/// this window — and may therefore serve reads without a fresh round trip.
pub fn RaftNode::has_lease(self : RaftNode) -> Bool {
  self.core.role() == Leader &&
  (self.peers.length() == 0 || self.quorum_active())
}
