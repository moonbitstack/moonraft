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
test "config carries the async-storage and committed-size options" {
  let on = Config::new(
    "1",
    ["2"],
    async_storage_writes=true,
    max_committed_size=128UL,
  )
  assert_true(on.async_storage_writes)
  assert_eq(on.max_committed_size, 128UL)
  // Both default off, as etcd's zero value.
  let off = Config::new("1", ["2"])
  assert_false(off.async_storage_writes)
  assert_eq(off.max_committed_size, 0UL)
}

///|
test "RawNode::from_config wires both storage options" {
  // Async with a per-Ready cap.
  let a = RawNode::from_config(
    Config::new("1", [], async_storage_writes=true, max_committed_size=64UL),
  ) catch {
    _ => abort("valid config rejected")
  }
  assert_true(a.is_async())
  // Async, unbounded.
  let b = RawNode::from_config(Config::new("1", [], async_storage_writes=true)) catch {
    _ => abort("valid config rejected")
  }
  assert_true(b.is_async())
  // Synchronous with a per-Ready cap.
  let c = RawNode::from_config(Config::new("1", [], max_committed_size=64UL)) catch {
    _ => abort("valid config rejected")
  }
  assert_false(c.is_async())
  // Synchronous, unbounded.
  let d = RawNode::from_config(Config::new("1", [])) catch {
    _ => abort("valid config rejected")
  }
  assert_false(d.is_async())
}
