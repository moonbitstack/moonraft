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

///|
/// Set equality over string id lists, for asserting a configuration's voter and
/// learner sets regardless of order.
fn set_eq(a : Array[String], b : Array[String]) -> Bool {
  let sa = a.copy()
  sa.sort()
  let sb = b.copy()
  sb.sort()
  if sa.length() != sb.length() {
    return false
  }
  for i in 0..<sa.length() {
    if sa[i] != sb[i] {
      return false
    }
  }
  true
}
