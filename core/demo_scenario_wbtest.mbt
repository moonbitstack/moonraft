///|
/// The introductory demo elects a leader, replicates a command, and — after the
/// leader is crashed — a different survivor takes over with every safety
/// property intact.
test "the demo elects, replicates, and survives the leader crashing" {
  let report = demo_run(["a", "b", "c", "d", "e"], 1, 200, 200, 400)
  assert_eq(report.node_count, 5)
  assert_true(report.first_leader is Some(_))
  assert_true(report.proposal_accepted)
  assert_true(report.committed)
  assert_true(report.second_leader is Some(_))
  assert_true(report.second_leader != report.first_leader)
  assert_true(report.one_leader_per_term)
  assert_true(report.committed_agrees)
  assert_true(report.invariants_hold)
}

///|
/// With a zero election budget no leader emerges, so the demo proposes nothing,
/// commits nothing, and elects no successor.
test "the demo records no leader when the election budget is zero" {
  let report = demo_run(["a", "b", "c"], 1, 0, 0, 0)
  assert_true(report.first_leader is None)
  assert_false(report.proposal_accepted)
  assert_false(report.committed)
  assert_true(report.second_leader is None)
}

///|
/// On the successful run the transcript is exactly the lines `cmd/example`
/// prints.
test "the demo transcript matches what the example prints" {
  let ok = demo_report_lines(
    demo_run(["a", "b", "c", "d", "e"], 1, 200, 200, 400),
  )
  assert_eq(ok[0], "cluster of 5 nodes, seed 1")
  assert_eq(ok[1].has_prefix("elected leader: "), true)
  assert_eq(ok[2], "committed 'set x = 1' on a majority")
  assert_eq(ok[3], "crashed the leader")
  assert_eq(ok[4].has_prefix("new leader: "), true)
  assert_eq(ok[5], "one leader per term : true")
  assert_eq(ok[6], "committed prefixes agree : true")
  assert_eq(ok[7], "safety invariants hold : true")
}

///|
/// The transcript stops at whichever step the run failed to reach.
test "the demo transcript stops at the step that failed" {
  let none_leader = demo_report_lines(demo_run(["a", "b", "c"], 1, 0, 0, 0))
  assert_eq(none_leader[none_leader.length() - 1], "no leader within 200 ticks")
  let refused = demo_report_lines({
    seed: 1,
    node_count: 3,
    first_leader: Some("a"),
    proposal_accepted: false,
    committed: false,
    second_leader: None,
    one_leader_per_term: true,
    committed_agrees: true,
    invariants_hold: true,
  })
  assert_eq(refused[refused.length() - 1], "leader refused the proposal")
  let uncommitted = demo_report_lines({
    seed: 1,
    node_count: 3,
    first_leader: Some("a"),
    proposal_accepted: true,
    committed: false,
    second_leader: None,
    one_leader_per_term: true,
    committed_agrees: true,
    invariants_hold: true,
  })
  assert_eq(
    uncommitted[uncommitted.length() - 1],
    "command did not commit within 200 ticks",
  )
  let no_successor = demo_report_lines({
    seed: 1,
    node_count: 3,
    first_leader: Some("a"),
    proposal_accepted: true,
    committed: true,
    second_leader: None,
    one_leader_per_term: true,
    committed_agrees: true,
    invariants_hold: true,
  })
  assert_eq(
    no_successor[no_successor.length() - 1],
    "no leader re-elected within 400 ticks",
  )
}
