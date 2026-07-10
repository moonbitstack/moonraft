// Ported from etcd-io/raft (Apache-2.0): tracker/progress_test.go.
// TestProgressString / TestProgressResume are omitted: they assert the debug
// String() renderer and the MsgAppFlowPaused throttle flag, neither of which
// this model carries (Probe pacing is done by the caller, not a per-Progress
// flag). See PORTING.md.

///|
fn probe_progress(match_ : UInt64, next : UInt64) -> Progress {
  let p = Progress::new(next)
  let _ = p.maybe_update(match_)
  p.next_index = next
  p
}

///|
test "TestProgressIsPaused" {
  // Probe: never throttled by this model (the caller paces it).
  let probe = probe_progress(0, 1)
  assert_false(probe.is_paused())
  // Replicate with room: not paused; with a full window: paused.
  let repl = probe_progress(0, 1)
  repl.become_replicate()
  assert_false(repl.is_paused())
  let full = Progress::new(1, max_inflight=1)
  full.become_replicate()
  full.sent_entries(1)
  assert_true(full.is_paused())
  // Snapshot: always paused.
  let snap = probe_progress(1, 2)
  snap.become_snapshot(10)
  assert_true(snap.is_paused())
}

///|
test "TestProgressBecomeReplicate" {
  let p = probe_progress(1, 5)
  p.become_replicate()
  assert_true(p.state == Replicate)
  assert_eq(p.match_index, 1)
  assert_eq(p.next_index, p.match_index + 1)
}

///|
test "TestProgressBecomeSnapshot" {
  let p = probe_progress(1, 5)
  p.become_snapshot(10)
  assert_true(p.state == Snapshot)
  assert_eq(p.match_index, 1)
  // We track the pending-snapshot index as next_index - 1 (= 10).
  assert_eq(p.next_index - 1, 10)
}

///|
test "TestProgressUpdate" {
  let prev_m = 3UL
  let prev_n = 5UL
  // (update, want_match, want_next, want_ok)
  let cases : Array[(UInt64, UInt64, UInt64, Bool)] = [
    (prev_m - 1, prev_m, prev_n, false),
    (prev_m, prev_m, prev_n, false),
    (prev_m + 1, prev_m + 1, prev_n, true),
    (prev_m + 2, prev_m + 2, prev_n + 1, true),
  ]
  for tc in cases {
    let (update, wm, wn, wok) = tc
    let p = Progress::new(prev_n)
    let _ = p.maybe_update(prev_m)
    p.next_index = prev_n
    assert_eq(p.maybe_update(update), wok)
    assert_eq(p.match_index, wm)
    assert_eq(p.next_index, wn)
  }
}

///|
test "TestProgressMaybeDecr" {
  // (state, match, next, rejected, hint, want_ok, want_next)
  let cases : Array[
    (ProgressState, UInt64, UInt64, UInt64, UInt64, Bool, UInt64),
  ] = [
    (Replicate, 5, 10, 5, 5, false, 10),
    (Replicate, 5, 10, 4, 4, false, 10),
    (Replicate, 5, 10, 9, 9, true, 6),
    (Probe, 0, 0, 0, 0, false, 0),
    (Probe, 0, 10, 5, 5, false, 10),
    (Probe, 0, 10, 9, 9, true, 9),
    (Probe, 0, 2, 1, 1, true, 1),
    (Probe, 0, 1, 0, 0, true, 1),
    (Probe, 0, 10, 9, 2, true, 3),
    (Probe, 0, 10, 9, 0, true, 1),
  ]
  for tc in cases {
    let (state, m, n, rejected, hint, wok, wn) = tc
    let p = Progress::new(n)
    p.match_index = m
    p.state = state
    assert_eq(p.maybe_decr_to(rejected, hint), wok)
    assert_eq(p.match_index, m)
    assert_eq(p.next_index, wn)
  }
}
