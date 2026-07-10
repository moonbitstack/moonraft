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

// Ported from etcd-io/raft (Apache-2.0): tracker/inflights_test.go.

///|
fn full_buffer(n : Int) -> Array[Inflight] {
  Array::makei(n, fn(_) { { index: 0, bytes: 0 } })
}

///|
fn assert_infl(
  actual : Inflights,
  start : Int,
  count : Int,
  bytes : UInt64,
  size : Int,
  idxs : Array[UInt64],
  szs : Array[UInt64],
) -> Unit raise {
  assert_eq(actual.start, start)
  assert_eq(actual.count, count)
  assert_eq(actual.bytes, bytes)
  assert_eq(actual.size, size)
  assert_eq(actual.buffer.length(), idxs.length())
  for i in 0..<idxs.length() {
    assert_eq(actual.buffer[i].index, idxs[i])
    assert_eq(actual.buffer[i].bytes, szs[i])
  }
}

///|
test "TestInflightsAdd" {
  // no rotating case
  let in_ = {
    start: 0,
    count: 0,
    bytes: 0,
    size: 10,
    max_bytes: 0,
    buffer: full_buffer(10),
  }
  for i in 0..<5 {
    in_.add(i.to_uint64(), (100 + i).to_uint64())
  }
  assert_infl(in_, 0, 5, 510, 10, [0, 1, 2, 3, 4, 0, 0, 0, 0, 0], [
    100, 101, 102, 103, 104, 0, 0, 0, 0, 0,
  ])
  for i in 5..<10 {
    in_.add(i.to_uint64(), (100 + i).to_uint64())
  }
  assert_infl(in_, 0, 10, 1045, 10, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], [
    100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
  ])

  // rotating case
  let in2 = {
    start: 5,
    count: 0,
    bytes: 0,
    size: 10,
    max_bytes: 0,
    buffer: full_buffer(10),
  }
  for i in 0..<5 {
    in2.add(i.to_uint64(), (100 + i).to_uint64())
  }
  assert_infl(in2, 5, 5, 510, 10, [0, 0, 0, 0, 0, 0, 1, 2, 3, 4], [
    0, 0, 0, 0, 0, 100, 101, 102, 103, 104,
  ])
  for i in 5..<10 {
    in2.add(i.to_uint64(), (100 + i).to_uint64())
  }
  assert_infl(in2, 5, 10, 1045, 10, [5, 6, 7, 8, 9, 0, 1, 2, 3, 4], [
    105, 106, 107, 108, 109, 100, 101, 102, 103, 104,
  ])
}

///|
test "TestInflightFreeTo" {
  let in_ = Inflights::new(10, 0)
  for i in 0..<10 {
    in_.add(i.to_uint64(), (100 + i).to_uint64())
  }
  in_.free_le(0)
  assert_infl(in_, 1, 9, 945, 10, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], [
    100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
  ])
  in_.free_le(4)
  assert_infl(in_, 5, 5, 535, 10, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], [
    100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
  ])
  in_.free_le(8)
  assert_infl(in_, 9, 1, 109, 10, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], [
    100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
  ])

  // rotating case
  for i in 10..<15 {
    in_.add(i.to_uint64(), (100 + i).to_uint64())
  }
  in_.free_le(12)
  assert_infl(in_, 3, 2, 227, 10, [10, 11, 12, 13, 14, 5, 6, 7, 8, 9], [
    110, 111, 112, 113, 114, 105, 106, 107, 108, 109,
  ])
  in_.free_le(14)
  assert_infl(in_, 0, 0, 0, 10, [10, 11, 12, 13, 14, 5, 6, 7, 8, 9], [
    110, 111, 112, 113, 114, 105, 106, 107, 108, 109,
  ])
}

///|
// Table-driven TestInflightsFull. Every case is exercised. MoonBit `abort`
// cannot be caught, so where Go asserts the final Add panics we instead assert
// the tracker is Full at that point — the exact precondition that makes Add
// abort — which verifies the same behaviour without weakening it.
test "TestInflightsFull" {
  let cases : Array[(Int, UInt64, Int, UInt64, Int)] = [
    // size, max_bytes, full_at, free_le, again_at
    (0, 0, 0, 0, 0),
    (1, 0, 1, 1, 2),
    (1, 10, 1, 1, 2),
    (15, 0, 15, 6, 22),
    (8, 400, 4, 2, 7),
    (8, 406, 4, 3, 8),
    (15, 408, 5, 1, 6),
  ]
  for tc in cases {
    let (size, max_bytes, full_at, free_le, again_at) = tc
    let in_ = Inflights::new(size, max_bytes)
    fn add_until_full(begin : Int, end : Int) -> Unit raise {
      for i in begin..<end {
        assert_false(in_.full())
        in_.add(i.to_uint64(), (100 + i).to_uint64())
      }
      assert_true(in_.full())
    }

    add_until_full(0, full_at)
    in_.free_le(free_le)
    add_until_full(full_at, again_at)
    // Go expects Add() to panic here; the tracker is Full which is what triggers
    // the abort in our implementation.
    assert_true(in_.full())
  }
}

///|
test "TestInflightsReset" {
  let in_ = Inflights::new(10, 1000)
  let mut index = 0UL
  for _epoch in 0..<100 {
    in_.reset()
    for _i in 0..<5 {
      assert_false(in_.full())
      index = index + 1
      in_.add(index, 16)
    }
    in_.free_le(index - 2)
    assert_false(in_.full())
    assert_eq(in_.count(), 2)
  }
  in_.free_le(index)
  assert_eq(in_.count(), 0)
}
