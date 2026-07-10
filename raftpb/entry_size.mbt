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
/// The sentinel used for an unbounded size cap: no `entries`/`slice` request is
/// ever limited by size. Mirrors etcd's `noLimit = math.MaxUint64`.
pub let no_limit : UInt64 = 18446744073709551615

///|
/// The number of bytes a base-128 varint of `v` occupies.
fn varint_len(v : UInt64) -> UInt64 {
  let mut n : UInt64 = 1
  let mut x = v
  while x >= 128 {
    x = x / 128
    n = n + 1
  }
  n
}

///|
/// The protobuf wire size of one entry, following etcd's `proto.Size`: each
/// non-zero field costs a tag byte plus its varint/length-delimited encoding.
/// Only the byte total matters — it is what `limit_size` and `Storage.Entries`
/// budget against.
pub fn entry_encoding_size(e : Entry) -> UInt64 {
  // The entry type is a first-class field of every entry here (unlike etcd's
  // optional proto field), so its tag + one-byte enum value is always counted.
  let mut size : UInt64 = 2
  if e.term != 0 {
    size = size + 1 + varint_len(e.term)
  }
  if e.index != 0 {
    size = size + 1 + varint_len(e.index)
  }
  let dl = e.command.length().to_uint64()
  if dl > 0 {
    size = size + 1 + varint_len(dl) + dl
  }
  size
}

///|
/// The total encoding size of a run of entries.
pub fn ents_size(entries : ArrayView[Entry]) -> UInt64 {
  let mut size : UInt64 = 0
  for e in entries {
    size = size + entry_encoding_size(e)
  }
  size
}

///|
/// The longest prefix of `entries` whose total encoding size does not exceed
/// `max_size`. Always returns at least one entry when the input is non-empty —
/// so a single oversized entry is still returned — matching etcd's `limitSize`.
pub fn limit_size(
  entries : ArrayView[Entry],
  max_size : UInt64,
) -> Array[Entry] {
  if entries.length() == 0 {
    return []
  }
  let out : Array[Entry] = [entries[0]]
  let mut size = entry_encoding_size(entries[0])
  let mut i = 1
  while i < entries.length() {
    size = size + entry_encoding_size(entries[i])
    if size > max_size {
      break
    }
    out.push(entries[i])
    i = i + 1
  }
  out
}

///|
/// The size of an entry's payload: its command bytes only, independent of term
/// and index. Empty-payload entries (like the no-op a new leader appends) are
/// zero size, so they do not count against the uncommitted-log quota. Mirrors
/// etcd's `payloadSize`.
pub fn payload_size(e : Entry) -> UInt64 {
  e.command.length().to_uint64()
}

///|
/// The total payload size of a run of entries (etcd's `payloadsSize`).
pub fn payloads_size(entries : ArrayView[Entry]) -> UInt64 {
  let mut s : UInt64 = 0
  for e in entries {
    s = s + payload_size(e)
  }
  s
}
