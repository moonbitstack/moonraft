// Ported from etcd-io/raft (Apache-2.0): quorum/testdata/{majority,joint}_{commit,vote}.txt
// Every datadriven case is expanded into an explicit assertion below.

///|
test "quorum committed #1: committed" {
  let acked : Map[String, UInt64] = {}
  assert_eq(committed_index([], [], acked), 18446744073709551615UL)
}

///|
test "quorum committed #2: committed cfg=(1) idx=(_)" {
  let acked : Map[String, UInt64] = {}
  assert_eq(committed_index(["1"], [], acked), 0UL)
}

///|
test "quorum committed #3: committed cfg=(1) idx=(12)" {
  let acked : Map[String, UInt64] = { "1": 12UL }
  assert_eq(committed_index(["1"], [], acked), 12UL)
}

///|
test "quorum committed #4: committed cfg=(1, 2) idx=(_,_)" {
  let acked : Map[String, UInt64] = {}
  assert_eq(committed_index(["1", "2"], [], acked), 0UL)
}

///|
test "quorum committed #5: committed cfg=(1, 2) idx=(12,_)" {
  let acked : Map[String, UInt64] = { "1": 12UL }
  assert_eq(committed_index(["1", "2"], [], acked), 0UL)
}

///|
test "quorum committed #6: committed cfg=(1,2) idx=(12,5)" {
  let acked : Map[String, UInt64] = { "1": 12UL, "2": 5UL }
  assert_eq(committed_index(["1", "2"], [], acked), 5UL)
}

///|
test "quorum committed #7: committed cfg=(1,2,3) idx=(_,_,_)" {
  let acked : Map[String, UInt64] = {}
  assert_eq(committed_index(["1", "2", "3"], [], acked), 0UL)
}

///|
test "quorum committed #8: committed cfg=(1,2,3) idx=(12,_,_)" {
  let acked : Map[String, UInt64] = { "1": 12UL }
  assert_eq(committed_index(["1", "2", "3"], [], acked), 0UL)
}

///|
test "quorum committed #9: committed cfg=(1,2,3) idx=(12,5,_)" {
  let acked : Map[String, UInt64] = { "1": 12UL, "2": 5UL }
  assert_eq(committed_index(["1", "2", "3"], [], acked), 5UL)
}

///|
test "quorum committed #10: committed cfg=(1,2,3) idx=(12,5,6)" {
  let acked : Map[String, UInt64] = { "1": 12UL, "2": 5UL, "3": 6UL }
  assert_eq(committed_index(["1", "2", "3"], [], acked), 6UL)
}

///|
test "quorum committed #11: committed cfg=(1,2,3) idx=(12,5,4)" {
  let acked : Map[String, UInt64] = { "1": 12UL, "2": 5UL, "3": 4UL }
  assert_eq(committed_index(["1", "2", "3"], [], acked), 5UL)
}

///|
test "quorum committed #12: committed cfg=(1,2,3) idx=(5,5,_)" {
  let acked : Map[String, UInt64] = { "1": 5UL, "2": 5UL }
  assert_eq(committed_index(["1", "2", "3"], [], acked), 5UL)
}

///|
test "quorum committed #13: committed cfg=(1,2,3) idx=(5,5,12)" {
  let acked : Map[String, UInt64] = { "1": 5UL, "2": 5UL, "3": 12UL }
  assert_eq(committed_index(["1", "2", "3"], [], acked), 5UL)
}

///|
test "quorum committed #14: committed cfg=(1, 2, 3) idx=(100, 101, 103)" {
  let acked : Map[String, UInt64] = { "1": 100UL, "2": 101UL, "3": 103UL }
  assert_eq(committed_index(["1", "2", "3"], [], acked), 101UL)
}

///|
test "quorum committed #15: committed cfg=(1, 2, 3, 4, 5) idx=(101, 104, 103, 103,_)" {
  let acked : Map[String, UInt64] = {
    "1": 101UL,
    "2": 104UL,
    "3": 103UL,
    "4": 103UL,
  }
  assert_eq(committed_index(["1", "2", "3", "4", "5"], [], acked), 103UL)
}

///|
test "quorum committed #16: committed cfg=(1, 2, 3, 4, 5) idx=(101, 102, 103, 103,_)" {
  let acked : Map[String, UInt64] = {
    "1": 101UL,
    "2": 102UL,
    "3": 103UL,
    "4": 103UL,
  }
  assert_eq(committed_index(["1", "2", "3", "4", "5"], [], acked), 102UL)
}

///|
test "quorum committed #17: committed cfg=(1,2,3) cfgj=zero idx=(100,101,99)" {
  let acked : Map[String, UInt64] = { "1": 100UL, "2": 101UL, "3": 99UL }
  assert_eq(committed_index(["1", "2", "3"], [], acked), 100UL)
}

///|
test "quorum committed #18: committed cfg=(1) cfgj=(2) idx=(_,_)" {
  let acked : Map[String, UInt64] = {}
  assert_eq(committed_index(["1"], ["2"], acked), 0UL)
}

///|
test "quorum committed #19: committed cfg=(1) cfgj=(2) idx=(100,_)" {
  let acked : Map[String, UInt64] = { "1": 100UL }
  assert_eq(committed_index(["1"], ["2"], acked), 0UL)
}

///|
test "quorum committed #20: committed cfg=(1) cfgj=(2) idx=(13, 100)" {
  let acked : Map[String, UInt64] = { "1": 13UL, "2": 100UL }
  assert_eq(committed_index(["1"], ["2"], acked), 13UL)
}

///|
test "quorum committed #21: committed cfg=(1) cfgj=(1) idx=(_)" {
  let acked : Map[String, UInt64] = {}
  assert_eq(committed_index(["1"], ["1"], acked), 0UL)
}

///|
test "quorum committed #22: committed cfg=(1) cfgj=(1) idx=(100)" {
  let acked : Map[String, UInt64] = { "1": 100UL }
  assert_eq(committed_index(["1"], ["1"], acked), 100UL)
}

///|
test "quorum committed #23: committed cfg=(1,3) cfgj=(2) idx=(_,_,_)" {
  let acked : Map[String, UInt64] = {}
  assert_eq(committed_index(["1", "3"], ["2"], acked), 0UL)
}

///|
test "quorum committed #24: committed cfg=(1,3) cfgj=(2) idx=(100,_,_)" {
  let acked : Map[String, UInt64] = { "1": 100UL }
  assert_eq(committed_index(["1", "3"], ["2"], acked), 0UL)
}

///|
test "quorum committed #25: committed cfg=(1,3) cfgj=(2) idx=(100,_,50)" {
  let acked : Map[String, UInt64] = { "1": 100UL, "2": 50UL }
  assert_eq(committed_index(["1", "3"], ["2"], acked), 0UL)
}

///|
test "quorum committed #26: committed cfg=(1,3) cfgj=(2) idx=(100,45,50)" {
  let acked : Map[String, UInt64] = { "1": 100UL, "3": 45UL, "2": 50UL }
  assert_eq(committed_index(["1", "3"], ["2"], acked), 45UL)
}

///|
test "quorum committed #27: committed cfg=(1,2) cfgj=(2) idx=(_,_)" {
  let acked : Map[String, UInt64] = {}
  assert_eq(committed_index(["1", "2"], ["2"], acked), 0UL)
}

///|
test "quorum committed #28: committed cfg=(1,2) cfgj=(2) idx=(100,_)" {
  let acked : Map[String, UInt64] = { "1": 100UL }
  assert_eq(committed_index(["1", "2"], ["2"], acked), 0UL)
}

///|
test "quorum committed #29: committed cfg=(1,2) cfgj=(2) idx=(_,100)" {
  let acked : Map[String, UInt64] = { "2": 100UL }
  assert_eq(committed_index(["1", "2"], ["2"], acked), 0UL)
}

///|
test "quorum committed #30: committed cfg=(1,2) cfgj=(2) idx=(50,100)" {
  let acked : Map[String, UInt64] = { "1": 50UL, "2": 100UL }
  assert_eq(committed_index(["1", "2"], ["2"], acked), 50UL)
}

///|
test "quorum committed #31: committed cfg=(1,2) cfgj=(2) idx=(100,50)" {
  let acked : Map[String, UInt64] = { "1": 100UL, "2": 50UL }
  assert_eq(committed_index(["1", "2"], ["2"], acked), 50UL)
}

///|
test "quorum committed #32: committed cfg=(1,2) cfgj=(3,4) idx=(50,_,_,_)" {
  let acked : Map[String, UInt64] = { "1": 50UL }
  assert_eq(committed_index(["1", "2"], ["3", "4"], acked), 0UL)
}

///|
test "quorum committed #33: committed cfg=(1,2) cfgj=(3,4) idx=(50,_,49,_)" {
  let acked : Map[String, UInt64] = { "1": 50UL, "3": 49UL }
  assert_eq(committed_index(["1", "2"], ["3", "4"], acked), 0UL)
}

///|
test "quorum committed #34: committed cfg=(1,2) cfgj=(3,4) idx=(50,48,49,_)" {
  let acked : Map[String, UInt64] = { "1": 50UL, "2": 48UL, "3": 49UL }
  assert_eq(committed_index(["1", "2"], ["3", "4"], acked), 0UL)
}

///|
test "quorum committed #35: committed cfg=(1,2) cfgj=(3,4) idx=(50,48,49,47)" {
  let acked : Map[String, UInt64] = {
    "1": 50UL,
    "2": 48UL,
    "3": 49UL,
    "4": 47UL,
  }
  assert_eq(committed_index(["1", "2"], ["3", "4"], acked), 47UL)
}

///|
test "quorum committed #36: committed cfg=(1,2) cfgj=(2,3) idx=(_,_,_)" {
  let acked : Map[String, UInt64] = {}
  assert_eq(committed_index(["1", "2"], ["2", "3"], acked), 0UL)
}

///|
test "quorum committed #37: committed cfg=(1,2) cfgj=(2,3) idx=(100,_,_)" {
  let acked : Map[String, UInt64] = { "1": 100UL }
  assert_eq(committed_index(["1", "2"], ["2", "3"], acked), 0UL)
}

///|
test "quorum committed #38: committed cfg=(1,2) cfgj=(2,3) idx=(_,100,_)" {
  let acked : Map[String, UInt64] = { "2": 100UL }
  assert_eq(committed_index(["1", "2"], ["2", "3"], acked), 0UL)
}

///|
test "quorum committed #39: committed cfg=(1,2) cfgj=(2,3) idx=(_,100,99)" {
  let acked : Map[String, UInt64] = { "2": 100UL, "3": 99UL }
  assert_eq(committed_index(["1", "2"], ["2", "3"], acked), 0UL)
}

///|
test "quorum committed #40: committed cfg=(1,2) cfgj=(2,3) idx=(101,100,99)" {
  let acked : Map[String, UInt64] = { "1": 101UL, "2": 100UL, "3": 99UL }
  assert_eq(committed_index(["1", "2"], ["2", "3"], acked), 99UL)
}

///|
test "quorum committed #41: committed cfg=(1,2) cfgj=(1,2) idx=(_,_)" {
  let acked : Map[String, UInt64] = {}
  assert_eq(committed_index(["1", "2"], ["1", "2"], acked), 0UL)
}

///|
test "quorum committed #42: committed cfg=(1,2) cfgj=(1,2) idx=(_,40)" {
  let acked : Map[String, UInt64] = { "2": 40UL }
  assert_eq(committed_index(["1", "2"], ["1", "2"], acked), 0UL)
}

///|
test "quorum committed #43: committed cfg=(1,2) cfgj=(1,2) idx=(41,40)" {
  let acked : Map[String, UInt64] = { "1": 41UL, "2": 40UL }
  assert_eq(committed_index(["1", "2"], ["1", "2"], acked), 40UL)
}

///|
test "quorum committed #44: committed cfg=(1,2,3) cfgj=(4,5,6) idx=(_,_,_,_,_,_)" {
  let acked : Map[String, UInt64] = {}
  assert_eq(committed_index(["1", "2", "3"], ["4", "5", "6"], acked), 0UL)
}

///|
test "quorum committed #45: committed cfg=(1,2,3) cfgj=(4,5,6) idx=(100,_,_,_,_,_)" {
  let acked : Map[String, UInt64] = { "1": 100UL }
  assert_eq(committed_index(["1", "2", "3"], ["4", "5", "6"], acked), 0UL)
}

///|
test "quorum committed #46: committed cfg=(1,2,3) cfgj=(4,5,6) idx=(100,_,_,90,_,_)" {
  let acked : Map[String, UInt64] = { "1": 100UL, "4": 90UL }
  assert_eq(committed_index(["1", "2", "3"], ["4", "5", "6"], acked), 0UL)
}

///|
test "quorum committed #47: committed cfg=(1,2,3) cfgj=(4,5,6) idx=(100,99,_,_,_,_)" {
  let acked : Map[String, UInt64] = { "1": 100UL, "2": 99UL }
  assert_eq(committed_index(["1", "2", "3"], ["4", "5", "6"], acked), 0UL)
}

///|
test "quorum committed #48: committed cfg=(1,2,3) cfgj=(4,5,6) idx=(_,99,90,97,95,_)" {
  let acked : Map[String, UInt64] = {
    "2": 99UL,
    "3": 90UL,
    "4": 97UL,
    "5": 95UL,
  }
  assert_eq(committed_index(["1", "2", "3"], ["4", "5", "6"], acked), 90UL)
}

///|
test "quorum committed #49: committed cfg=(1,2,3) cfgj=(4,5,6) idx=(92,99,90,97,95,_)" {
  let acked : Map[String, UInt64] = {
    "1": 92UL,
    "2": 99UL,
    "3": 90UL,
    "4": 97UL,
    "5": 95UL,
  }
  assert_eq(committed_index(["1", "2", "3"], ["4", "5", "6"], acked), 92UL)
}

///|
test "quorum committed #50: committed cfg=(1,2,3) cfgj=(4,5,6) idx=(92,99,90,97,95,77)" {
  let acked : Map[String, UInt64] = {
    "1": 92UL,
    "2": 99UL,
    "3": 90UL,
    "4": 97UL,
    "5": 95UL,
    "6": 77UL,
  }
  assert_eq(committed_index(["1", "2", "3"], ["4", "5", "6"], acked), 92UL)
}

///|
test "quorum committed #51: committed cfg=(1,2,3) cfgj=(1,4,5) idx=(_,_,_,_,_)" {
  let acked : Map[String, UInt64] = {}
  assert_eq(committed_index(["1", "2", "3"], ["1", "4", "5"], acked), 0UL)
}

///|
test "quorum committed #52: committed cfg=(1,2,3) cfgj=(1,4,5) idx=(100,_,_,_,_)" {
  let acked : Map[String, UInt64] = { "1": 100UL }
  assert_eq(committed_index(["1", "2", "3"], ["1", "4", "5"], acked), 0UL)
}

///|
test "quorum committed #53: committed cfg=(1,2,3) cfgj=(1,4,5) idx=(100,101,_,_,_)" {
  let acked : Map[String, UInt64] = { "1": 100UL, "2": 101UL }
  assert_eq(committed_index(["1", "2", "3"], ["1", "4", "5"], acked), 0UL)
}

///|
test "quorum committed #54: committed cfg=(1,2,3) cfgj=(1,4,5) idx=(100,101,100,_,_)" {
  let acked : Map[String, UInt64] = { "1": 100UL, "2": 101UL, "3": 100UL }
  assert_eq(committed_index(["1", "2", "3"], ["1", "4", "5"], acked), 0UL)
}

///|
test "quorum committed #55: committed cfg=(1,2,3) cfgj=(1,4,5) idx=(_,100,_,99,98)" {
  let acked : Map[String, UInt64] = { "2": 100UL, "4": 99UL, "5": 98UL }
  assert_eq(committed_index(["1", "2", "3"], ["1", "4", "5"], acked), 0UL)
}

///|
test "quorum committed #56: committed cfg=(1,2,3) cfgj=(1,4,5) idx=(_,100,99,99,98)" {
  let acked : Map[String, UInt64] = {
    "2": 100UL,
    "3": 99UL,
    "4": 99UL,
    "5": 98UL,
  }
  assert_eq(committed_index(["1", "2", "3"], ["1", "4", "5"], acked), 98UL)
}

///|
test "quorum committed #57: committed cfg=(1,2,3) cfgj=(1,4,5) idx=(1,100,99,99,98)" {
  let acked : Map[String, UInt64] = {
    "1": 1UL,
    "2": 100UL,
    "3": 99UL,
    "4": 99UL,
    "5": 98UL,
  }
  assert_eq(committed_index(["1", "2", "3"], ["1", "4", "5"], acked), 98UL)
}

///|
test "quorum committed #58: committed cfg=(1,2,3) cfgj=(1,4,5) idx=(100,100,99,99,98)" {
  let acked : Map[String, UInt64] = {
    "1": 100UL,
    "2": 100UL,
    "3": 99UL,
    "4": 99UL,
    "5": 98UL,
  }
  assert_eq(committed_index(["1", "2", "3"], ["1", "4", "5"], acked), 99UL)
}

///|
test "quorum committed #59: committed cfg=(1,2,3) cfgj=(2,3,4) idx=(_,_,_,_)" {
  let acked : Map[String, UInt64] = {}
  assert_eq(committed_index(["1", "2", "3"], ["2", "3", "4"], acked), 0UL)
}

///|
test "quorum committed #60: committed cfg=(1,2,3) cfgj=(2,3,4) idx=(_,100,99,_)" {
  let acked : Map[String, UInt64] = { "2": 100UL, "3": 99UL }
  assert_eq(committed_index(["1", "2", "3"], ["2", "3", "4"], acked), 99UL)
}

///|
test "quorum committed #61: committed cfg=(1,2,3) cfgj=(2,3,4) idx=(98,100,99,_)" {
  let acked : Map[String, UInt64] = { "1": 98UL, "2": 100UL, "3": 99UL }
  assert_eq(committed_index(["1", "2", "3"], ["2", "3", "4"], acked), 99UL)
}

///|
test "quorum committed #62: committed cfg=(1,2,3) cfgj=(2,3,4) idx=(100,100,99,_)" {
  let acked : Map[String, UInt64] = { "1": 100UL, "2": 100UL, "3": 99UL }
  assert_eq(committed_index(["1", "2", "3"], ["2", "3", "4"], acked), 99UL)
}

///|
test "quorum committed #63: committed cfg=(1,2,3) cfgj=(2,3,4) idx=(100,100,99,98)" {
  let acked : Map[String, UInt64] = {
    "1": 100UL,
    "2": 100UL,
    "3": 99UL,
    "4": 98UL,
  }
  assert_eq(committed_index(["1", "2", "3"], ["2", "3", "4"], acked), 99UL)
}

///|
test "quorum committed #64: committed cfg=(1,2,3) cfgj=(2,3,4) idx=(100,_,_,101)" {
  let acked : Map[String, UInt64] = { "1": 100UL, "4": 101UL }
  assert_eq(committed_index(["1", "2", "3"], ["2", "3", "4"], acked), 0UL)
}

///|
test "quorum committed #65: committed cfg=(1,2,3) cfgj=(2,3,4) idx=(100,99,_,101)" {
  let acked : Map[String, UInt64] = { "1": 100UL, "2": 99UL, "4": 101UL }
  assert_eq(committed_index(["1", "2", "3"], ["2", "3", "4"], acked), 99UL)
}

///|
test "quorum committed #66: committed cfg=(1,2,3) cfgj=(1,2,3) idx=(50,45,_)" {
  let acked : Map[String, UInt64] = { "1": 50UL, "2": 45UL }
  assert_eq(committed_index(["1", "2", "3"], ["1", "2", "3"], acked), 45UL)
}

///|
test "quorum vote #1: vote" {
  let votes : Map[String, Bool] = {}
  assert_true(vote_result([], [], votes) == VoteWon)
}

///|
test "quorum vote #2: vote cfg=(1) votes=(_)" {
  let votes : Map[String, Bool] = {}
  assert_true(vote_result(["1"], [], votes) == VotePending)
}

///|
test "quorum vote #3: vote cfg=(1) votes=(n)" {
  let votes : Map[String, Bool] = { "1": false }
  assert_true(vote_result(["1"], [], votes) == VoteLost)
}

///|
test "quorum vote #4: vote cfg=(123) votes=(y)" {
  let votes : Map[String, Bool] = { "123": true }
  assert_true(vote_result(["123"], [], votes) == VoteWon)
}

///|
test "quorum vote #5: vote cfg=(4,8) votes=(_,_)" {
  let votes : Map[String, Bool] = {}
  assert_true(vote_result(["4", "8"], [], votes) == VotePending)
}

///|
test "quorum vote #6: vote cfg=(4,8) votes=(n,_)" {
  let votes : Map[String, Bool] = { "4": false }
  assert_true(vote_result(["4", "8"], [], votes) == VoteLost)
}

///|
test "quorum vote #7: vote cfg=(4,8) votes=(y,_)" {
  let votes : Map[String, Bool] = { "4": true }
  assert_true(vote_result(["4", "8"], [], votes) == VotePending)
}

///|
test "quorum vote #8: vote cfg=(4,8) votes=(n,y)" {
  let votes : Map[String, Bool] = { "4": false, "8": true }
  assert_true(vote_result(["4", "8"], [], votes) == VoteLost)
}

///|
test "quorum vote #9: vote cfg=(4,8) votes=(y,y)" {
  let votes : Map[String, Bool] = { "4": true, "8": true }
  assert_true(vote_result(["4", "8"], [], votes) == VoteWon)
}

///|
test "quorum vote #10: vote cfg=(2,4,7) votes=(_,_,_)" {
  let votes : Map[String, Bool] = {}
  assert_true(vote_result(["2", "4", "7"], [], votes) == VotePending)
}

///|
test "quorum vote #11: vote cfg=(2,4,7) votes=(n,_,_)" {
  let votes : Map[String, Bool] = { "2": false }
  assert_true(vote_result(["2", "4", "7"], [], votes) == VotePending)
}

///|
test "quorum vote #12: vote cfg=(2,4,7) votes=(y,_,_)" {
  let votes : Map[String, Bool] = { "2": true }
  assert_true(vote_result(["2", "4", "7"], [], votes) == VotePending)
}

///|
test "quorum vote #13: vote cfg=(2,4,7) votes=(n,n,_)" {
  let votes : Map[String, Bool] = { "2": false, "4": false }
  assert_true(vote_result(["2", "4", "7"], [], votes) == VoteLost)
}

///|
test "quorum vote #14: vote cfg=(2,4,7) votes=(y,n,_)" {
  let votes : Map[String, Bool] = { "2": true, "4": false }
  assert_true(vote_result(["2", "4", "7"], [], votes) == VotePending)
}

///|
test "quorum vote #15: vote cfg=(2,4,7) votes=(y,y,_)" {
  let votes : Map[String, Bool] = { "2": true, "4": true }
  assert_true(vote_result(["2", "4", "7"], [], votes) == VoteWon)
}

///|
test "quorum vote #16: vote cfg=(2,4,7) votes=(y,y,n)" {
  let votes : Map[String, Bool] = { "2": true, "4": true, "7": false }
  assert_true(vote_result(["2", "4", "7"], [], votes) == VoteWon)
}

///|
test "quorum vote #17: vote cfg=(2,4,7) votes=(n,y,n)" {
  let votes : Map[String, Bool] = { "2": false, "4": true, "7": false }
  assert_true(vote_result(["2", "4", "7"], [], votes) == VoteLost)
}

///|
test "quorum vote #18: vote cfg=(1,2,3,4,5,6,7) votes=(y,y,n,y,_,_,_)" {
  let votes : Map[String, Bool] = {
    "1": true,
    "2": true,
    "3": false,
    "4": true,
  }
  assert_true(
    vote_result(["1", "2", "3", "4", "5", "6", "7"], [], votes) == VotePending,
  )
}

///|
test "quorum vote #19: vote cfg=(1,2,3,4,5,6,7) votes=(_,y,y,_,n,y,n)" {
  let votes : Map[String, Bool] = {
    "2": true,
    "3": true,
    "5": false,
    "6": true,
    "7": false,
  }
  assert_true(
    vote_result(["1", "2", "3", "4", "5", "6", "7"], [], votes) == VotePending,
  )
}

///|
test "quorum vote #20: vote cfg=(1,2,3,4,5,6,7) votes=(y,y,n,y,_,n,y)" {
  let votes : Map[String, Bool] = {
    "1": true,
    "2": true,
    "3": false,
    "4": true,
    "6": false,
    "7": true,
  }
  assert_true(
    vote_result(["1", "2", "3", "4", "5", "6", "7"], [], votes) == VoteWon,
  )
}

///|
test "quorum vote #21: vote cfg=(1,2,3,4,5,6,7) votes=(y,y,_,n,y,n,n)" {
  let votes : Map[String, Bool] = {
    "1": true,
    "2": true,
    "4": false,
    "5": true,
    "6": false,
    "7": false,
  }
  assert_true(
    vote_result(["1", "2", "3", "4", "5", "6", "7"], [], votes) == VotePending,
  )
}

///|
test "quorum vote #22: vote cfg=(1,2,3,4,5,6,7) votes=(y,y,n,y,n,n,n)" {
  let votes : Map[String, Bool] = {
    "1": true,
    "2": true,
    "3": false,
    "4": true,
    "5": false,
    "6": false,
    "7": false,
  }
  assert_true(
    vote_result(["1", "2", "3", "4", "5", "6", "7"], [], votes) == VoteLost,
  )
}

///|
test "quorum vote #23: vote cfgj=zero" {
  let votes : Map[String, Bool] = {}
  assert_true(vote_result([], [], votes) == VoteWon)
}

///|
test "quorum vote #24: vote cfg=(1) cfgj=zero votes=(_)" {
  let votes : Map[String, Bool] = {}
  assert_true(vote_result(["1"], [], votes) == VotePending)
}

///|
test "quorum vote #25: vote cfg=(1) cfgj=zero votes=(y)" {
  let votes : Map[String, Bool] = { "1": true }
  assert_true(vote_result(["1"], [], votes) == VoteWon)
}

///|
test "quorum vote #26: vote cfg=(1) cfgj=zero votes=(n)" {
  let votes : Map[String, Bool] = { "1": false }
  assert_true(vote_result(["1"], [], votes) == VoteLost)
}

///|
test "quorum vote #27: vote cfg=(1) cfgj=(1) votes=(_)" {
  let votes : Map[String, Bool] = {}
  assert_true(vote_result(["1"], ["1"], votes) == VotePending)
}

///|
test "quorum vote #28: vote cfg=(1) cfgj=(1) votes=(y)" {
  let votes : Map[String, Bool] = { "1": true }
  assert_true(vote_result(["1"], ["1"], votes) == VoteWon)
}

///|
test "quorum vote #29: vote cfg=(1) cfgj=(1) votes=(n)" {
  let votes : Map[String, Bool] = { "1": false }
  assert_true(vote_result(["1"], ["1"], votes) == VoteLost)
}

///|
test "quorum vote #30: vote cfg=(1) cfgj=(2) votes=(_,_)" {
  let votes : Map[String, Bool] = {}
  assert_true(vote_result(["1"], ["2"], votes) == VotePending)
}

///|
test "quorum vote #31: vote cfg=(1) cfgj=(2) votes=(y,_)" {
  let votes : Map[String, Bool] = { "1": true }
  assert_true(vote_result(["1"], ["2"], votes) == VotePending)
}

///|
test "quorum vote #32: vote cfg=(1) cfgj=(2) votes=(y,y)" {
  let votes : Map[String, Bool] = { "1": true, "2": true }
  assert_true(vote_result(["1"], ["2"], votes) == VoteWon)
}

///|
test "quorum vote #33: vote cfg=(1) cfgj=(2) votes=(y,n)" {
  let votes : Map[String, Bool] = { "1": true, "2": false }
  assert_true(vote_result(["1"], ["2"], votes) == VoteLost)
}

///|
test "quorum vote #34: vote cfg=(1) cfgj=(2) votes=(n,_)" {
  let votes : Map[String, Bool] = { "1": false }
  assert_true(vote_result(["1"], ["2"], votes) == VoteLost)
}

///|
test "quorum vote #35: vote cfg=(1) cfgj=(2) votes=(n,n)" {
  let votes : Map[String, Bool] = { "1": false, "2": false }
  assert_true(vote_result(["1"], ["2"], votes) == VoteLost)
}

///|
test "quorum vote #36: vote cfg=(1) cfgj=(2) votes=(n,y)" {
  let votes : Map[String, Bool] = { "1": false, "2": true }
  assert_true(vote_result(["1"], ["2"], votes) == VoteLost)
}

///|
test "quorum vote #37: vote cfg=(1,2) cfgj=(3,4) votes=(_,_,_,_)" {
  let votes : Map[String, Bool] = {}
  assert_true(vote_result(["1", "2"], ["3", "4"], votes) == VotePending)
}

///|
test "quorum vote #38: vote cfg=(1,2) cfgj=(3,4) votes=(y,_,_,_)" {
  let votes : Map[String, Bool] = { "1": true }
  assert_true(vote_result(["1", "2"], ["3", "4"], votes) == VotePending)
}

///|
test "quorum vote #39: vote cfg=(1,2) cfgj=(3,4) votes=(y,y,_,_)" {
  let votes : Map[String, Bool] = { "1": true, "2": true }
  assert_true(vote_result(["1", "2"], ["3", "4"], votes) == VotePending)
}

///|
test "quorum vote #40: vote cfg=(1,2) cfgj=(3,4) votes=(y,y,n,_)" {
  let votes : Map[String, Bool] = { "1": true, "2": true, "3": false }
  assert_true(vote_result(["1", "2"], ["3", "4"], votes) == VoteLost)
}

///|
test "quorum vote #41: vote cfg=(1,2) cfgj=(3,4) votes=(y,y,n,n)" {
  let votes : Map[String, Bool] = {
    "1": true,
    "2": true,
    "3": false,
    "4": false,
  }
  assert_true(vote_result(["1", "2"], ["3", "4"], votes) == VoteLost)
}

///|
test "quorum vote #42: vote cfg=(1,2) cfgj=(3,4) votes=(y,y,y,n)" {
  let votes : Map[String, Bool] = {
    "1": true,
    "2": true,
    "3": true,
    "4": false,
  }
  assert_true(vote_result(["1", "2"], ["3", "4"], votes) == VoteLost)
}

///|
test "quorum vote #43: vote cfg=(1,2) cfgj=(3,4) votes=(y,y,y,y)" {
  let votes : Map[String, Bool] = { "1": true, "2": true, "3": true, "4": true }
  assert_true(vote_result(["1", "2"], ["3", "4"], votes) == VoteWon)
}

///|
test "quorum vote #44: vote cfg=(1,2) cfgj=(2,3) votes=(_,_,_)" {
  let votes : Map[String, Bool] = {}
  assert_true(vote_result(["1", "2"], ["2", "3"], votes) == VotePending)
}

///|
test "quorum vote #45: vote cfg=(1,2) cfgj=(2,3) votes=(_,n,_)" {
  let votes : Map[String, Bool] = { "2": false }
  assert_true(vote_result(["1", "2"], ["2", "3"], votes) == VoteLost)
}

///|
test "quorum vote #46: vote cfg=(1,2) cfgj=(2,3) votes=(y,y,_)" {
  let votes : Map[String, Bool] = { "1": true, "2": true }
  assert_true(vote_result(["1", "2"], ["2", "3"], votes) == VotePending)
}

///|
test "quorum vote #47: vote cfg=(1,2) cfgj=(2,3) votes=(y,y,n)" {
  let votes : Map[String, Bool] = { "1": true, "2": true, "3": false }
  assert_true(vote_result(["1", "2"], ["2", "3"], votes) == VoteLost)
}

///|
test "quorum vote #48: vote cfg=(1,2) cfgj=(2,3) votes=(y,y,y)" {
  let votes : Map[String, Bool] = { "1": true, "2": true, "3": true }
  assert_true(vote_result(["1", "2"], ["2", "3"], votes) == VoteWon)
}

///|
test "quorum vote #49: vote cfg=(1,2) cfgj=(1,2) votes=(_,_)" {
  let votes : Map[String, Bool] = {}
  assert_true(vote_result(["1", "2"], ["1", "2"], votes) == VotePending)
}

///|
test "quorum vote #50: vote cfg=(1,2) cfgj=(1,2) votes=(y,_)" {
  let votes : Map[String, Bool] = { "1": true }
  assert_true(vote_result(["1", "2"], ["1", "2"], votes) == VotePending)
}

///|
test "quorum vote #51: vote cfg=(1,2) cfgj=(1,2) votes=(y,n)" {
  let votes : Map[String, Bool] = { "1": true, "2": false }
  assert_true(vote_result(["1", "2"], ["1", "2"], votes) == VoteLost)
}

///|
test "quorum vote #52: vote cfg=(1,2) cfgj=(1,2) votes=(n,_)" {
  let votes : Map[String, Bool] = { "1": false }
  assert_true(vote_result(["1", "2"], ["1", "2"], votes) == VoteLost)
}

///|
test "quorum vote #53: vote cfg=(1,2) cfgj=(1,2) votes=(n,n)" {
  let votes : Map[String, Bool] = { "1": false, "2": false }
  assert_true(vote_result(["1", "2"], ["1", "2"], votes) == VoteLost)
}

///|
test "quorum vote #54: vote cfg=(1,2,3) cfgj=(2,3,4) votes=(_,_,_,_)" {
  let votes : Map[String, Bool] = {}
  assert_true(
    vote_result(["1", "2", "3"], ["2", "3", "4"], votes) == VotePending,
  )
}

///|
test "quorum vote #55: vote cfg=(1,2,3) cfgj=(2,3,4) votes=(_,n,_,_)" {
  let votes : Map[String, Bool] = { "2": false }
  assert_true(
    vote_result(["1", "2", "3"], ["2", "3", "4"], votes) == VotePending,
  )
}

///|
test "quorum vote #56: vote cfg=(1,2,3) cfgj=(2,3,4) votes=(_,n,n,_)" {
  let votes : Map[String, Bool] = { "2": false, "3": false }
  assert_true(vote_result(["1", "2", "3"], ["2", "3", "4"], votes) == VoteLost)
}

///|
test "quorum vote #57: vote cfg=(1,2,3) cfgj=(2,3,4) votes=(_,y,y,_)" {
  let votes : Map[String, Bool] = { "2": true, "3": true }
  assert_true(vote_result(["1", "2", "3"], ["2", "3", "4"], votes) == VoteWon)
}

///|
test "quorum vote #58: vote cfg=(1,2,3) cfgj=(2,3,4) votes=(y,y,_,_)" {
  let votes : Map[String, Bool] = { "1": true, "2": true }
  assert_true(
    vote_result(["1", "2", "3"], ["2", "3", "4"], votes) == VotePending,
  )
}

///|
test "quorum vote #59: vote cfg=(1,2,3) cfgj=(2,3,4) votes=(y,y,n,_)" {
  let votes : Map[String, Bool] = { "1": true, "2": true, "3": false }
  assert_true(
    vote_result(["1", "2", "3"], ["2", "3", "4"], votes) == VotePending,
  )
}

///|
test "quorum vote #60: vote cfg=(1,2,3) cfgj=(2,3,4) votes=(y,y,n,n)" {
  let votes : Map[String, Bool] = {
    "1": true,
    "2": true,
    "3": false,
    "4": false,
  }
  assert_true(vote_result(["1", "2", "3"], ["2", "3", "4"], votes) == VoteLost)
}

///|
test "quorum vote #61: vote cfg=(1,2,3) cfgj=(2,3,4) votes=(y,y,n,y)" {
  let votes : Map[String, Bool] = {
    "1": true,
    "2": true,
    "3": false,
    "4": true,
  }
  assert_true(vote_result(["1", "2", "3"], ["2", "3", "4"], votes) == VoteWon)
}
