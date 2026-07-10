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
/// Run a single-node Raft group through the Ready/Advance loop over a batch of
/// client commands, returning the commands the state machine applied, in order.
///
/// This is the smallest complete embedding of a `RawNode`, and it is what a real
/// application's event loop does in miniature: wrap the consensus core in the
/// Ready/Advance contract, campaign to leadership, then for each command propose
/// it and drain every `Ready` — treating its entries as persisted and applying
/// its committed entries — via `advance`. It is the non-test entry point that
/// wires `RawNode` into a runnable driver, and a demo or an embedder can call it
/// directly.
pub fn run_single_node(id : String, commands : Array[Bytes]) -> Array[Bytes] {
  let raw = RaftNode::new(id, []).raw()
  raw.campaign()
  raw.stabilize() |> ignore
  let applied : Array[Bytes] = []
  for cmd in commands {
    raw.propose(cmd)
    while raw.has_ready() {
      let rd = raw.ready()
      for e in rd.committed_entries {
        applied.push(e.command)
      }
      raw.advance(rd)
    }
  }
  applied
}
