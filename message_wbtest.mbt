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
test "an empty append is reported as a heartbeat" {
  let hb = Message::new(
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
  assert_eq(hb.kind(), "Heartbeat")
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
    AppendResp({ term: 4, success: true, match_index: 3, conflict_index: 0 }),
  )
  assert_true(r.is_response())
  assert_eq(r.term(), 4)
  assert_eq(r.kind(), "AppendResp")
}
