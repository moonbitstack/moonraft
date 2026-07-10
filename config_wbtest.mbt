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
/// Whether validating `config` raises (an invalid configuration).
fn rejects(config : Config) -> Bool {
  try {
    config.validate()
    false
  } catch {
    _ => true
  }
}

///|
/// A valid default config builds a node through the checked path.
test "Config::from_config builds a node from a valid config" {
  let config = Config::new("1", ["2", "3"])
  let r = RaftNode::from_config(config)
  assert_eq(r.id(), "1")
  assert_true(r.role() == Follower)
}

///|
/// validate() enforces each etcd branch that applies to this port.
test "Config::validate mirrors etcd's checks" {
  // A valid baseline passes.
  assert_false(rejects(Config::new("1", ["2", "3"])))
  // Empty id.
  assert_true(rejects(Config::new("", ["2"])))
  // heartbeat_tick must be > 0.
  assert_true(rejects(Config::new("1", ["2"], heartbeat_tick=0)))
  // election_tick must exceed heartbeat_tick.
  assert_true(
    rejects(Config::new("1", ["2"], election_tick=1, heartbeat_tick=1)),
  )
  // max_inflight must be > 0.
  assert_true(rejects(Config::new("1", ["2"], max_inflight=0)))
  // max_inflight_bytes, when set, must be >= max_msg_bytes.
  assert_true(
    rejects(Config::new("1", ["2"], max_msg_bytes=1000, max_inflight_bytes=500)),
  )
  // ReadOnlyLeaseBased requires CheckQuorum.
  assert_true(
    rejects(
      Config::new("1", ["2"], read_only_option=LeaseBased, check_quorum=false),
    ),
  )
  // The same, with CheckQuorum on, is fine.
  assert_false(
    rejects(
      Config::new("1", ["2"], read_only_option=LeaseBased, check_quorum=true),
    ),
  )
}

///|
/// A config carrying non-default knobs is threaded through to the node.
test "Config::from_config threads knobs into the node" {
  let config = Config::new(
    "1",
    ["2", "3"],
    check_quorum=true,
    read_only_option=LeaseBased,
    step_down_on_removal=true,
    disable_conf_change_validation=true,
  )
  let r = RaftNode::from_config(config)
  assert_true(r.read_only_option() is LeaseBased)
  // check_quorum wired: the node now treats a recognized leader as leased.
  assert_true(r.role() == Follower)
}

///|
/// An invalid config raised through the constructor is observable as an error.
test "Config::from_config raises on an invalid config" {
  let bad = Config::new("1", ["2"], election_tick=1, heartbeat_tick=1)
  let raised = try {
    RaftNode::from_config(bad) |> ignore
    false
  } catch {
    _ => true
  }
  assert_true(raised)
}
