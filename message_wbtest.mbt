///|
test "a message exposes its route, term and kind" {
  let m = Message::new(
    "a",
    "b",
    Vote({ term: 3, candidate_id: "a", last_log_index: 2, last_log_term: 1 }),
  )
  assert_eq(m.from, "a")
  assert_eq(m.to, "b")
  assert_eq(m.term(), 3)
  assert_eq(m.kind(), "Vote")
  assert_false(m.is_response())
}

///|
test "a heartbeat and an empty-append probe are distinct kinds" {
  let hb = Message::new(
    "L",
    "f",
    Heartbeat({ term: 4, leader_id: "L", commit: 1 }),
  )
  assert_eq(hb.kind(), "Heartbeat")
  assert_false(hb.is_response())
  // An empty AppendEntries is now a probe, not a heartbeat.
  let probe = Message::new(
    "L",
    "f",
    Append({
      term: 4,
      leader_id: "L",
      prev_log_index: 1,
      prev_log_term: 1,
      entries: [],
      leader_commit: 1,
    }),
  )
  assert_eq(probe.kind(), "Probe")
  let app = Message::new(
    "L",
    "f",
    Append({
      term: 4,
      leader_id: "L",
      prev_log_index: 1,
      prev_log_term: 1,
      entries: [Entry::normal(4, 2, b"x")],
      leader_commit: 1,
    }),
  )
  assert_eq(app.kind(), "Append")
}

///|
test "responses are recognised" {
  let r = Message::new(
    "f",
    "L",
    AppendResp({
      term: 4,
      success: true,
      match_index: 3,
      conflict_index: 0,
      conflict_term: 0,
    }),
  )
  assert_true(r.is_response())
  assert_eq(r.term(), 4)
  assert_eq(r.kind(), "AppendResp")
}
