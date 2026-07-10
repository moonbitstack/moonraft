///|
/// The Election-Safety predicate accepts distinct leader terms and rejects a
/// repeated one — the double-election a correct run never produces but the
/// checker must still catch.
test "distinct leader terms pass and a repeated term fails" {
  assert_true(terms_are_distinct([1UL, 2UL, 3UL]))
  assert_false(terms_are_distinct([4UL, 4UL]))
}

///|
/// The Log-Matching predicate agrees on positions both logs retain, skips a
/// position only one retains, and reports a divergence where both retain the
/// position with different terms.
test "aligned term sequences agree unless a shared position differs" {
  assert_true(aligned_terms_consistent([Some(1UL)], [Some(1UL)]))
  // The shorter sequence bounds the comparison from either side.
  assert_true(aligned_terms_consistent([Some(1UL)], [Some(1UL), Some(2UL)]))
  assert_true(aligned_terms_consistent([Some(1UL), Some(2UL)], [Some(1UL)]))
  assert_true(
    aligned_terms_consistent([Some(1UL), None], [Some(1UL), Some(2UL)]),
  )
  assert_true(
    aligned_terms_consistent([Some(1UL), Some(2UL)], [Some(1UL), None]),
  )
  assert_false(
    aligned_terms_consistent([Some(1UL), Some(2UL)], [Some(1UL), Some(3UL)]),
  )
}

///|
/// The committed-command predicate holds only when every server records the
/// same present command; a missing entry or a mismatch is a divergence.
test "committed commands agree only when present and identical" {
  assert_true(same_command([Some(b"x"), Some(b"x")]))
  assert_false(same_command([Some(b"x"), None]))
  assert_false(same_command([Some(b"x"), Some(b"y")]))
}

///|
/// The committed prefix agrees only when every slot agrees; one disagreeing slot
/// (a State Machine Safety violation) fails the whole prefix.
test "a disagreeing committed slot fails the prefix check" {
  assert_true(
    all_slots_agree([[Some(b"x"), Some(b"x")], [Some(b"y"), Some(b"y")]]),
  )
  assert_false(
    all_slots_agree([[Some(b"x"), Some(b"x")], [Some(b"y"), Some(b"z")]]),
  )
}
