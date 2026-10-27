// Copyright 2025 The etcd Authors
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

// Ported from etcd-io/raft (Apache-2.0): quorum/majority_test.go TestDescribe.

///|
test "TestDescribe: different index for each voter" {
  let acked : Map[String, UInt64] = { "1": 101, "2": 102, "3": 103 }
  assert_eq(
    describe(["1", "2", "3"], acked),
    "       idx\n" +
    ">      101    (id=1)\n" +
    "x>     102    (id=2)\n" +
    "xx>    103    (id=3)\n",
  )
}

///|
test "TestDescribe: same index for all voters" {
  let acked : Map[String, UInt64] = { "1": 100, "2": 100, "3": 100 }
  assert_eq(
    describe(["1", "2", "3"], acked),
    "       idx\n" +
    ">      100    (id=1)\n" +
    ">      100    (id=2)\n" +
    ">      100    (id=3)\n",
  )
}

///|
test "TestDescribe: same index for part of the voters" {
  let acked : Map[String, UInt64] = { "1": 100, "2": 102, "3": 102 }
  assert_eq(
    describe(["1", "2", "3"], acked),
    "       idx\n" +
    ">      100    (id=1)\n" +
    "x>     102    (id=2)\n" +
    "x>     102    (id=3)\n",
  )
}

///|
test "TestDescribe: missing some indexes" {
  let acked : Map[String, UInt64] = { "1": 101, "3": 103 }
  assert_eq(
    describe(["1", "2", "3"], acked),
    "       idx\n" +
    "x>     101    (id=1)\n" +
    "?        0    (id=2)\n" +
    "xx>    103    (id=3)\n",
  )
}
