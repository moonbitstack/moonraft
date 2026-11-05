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
/// A hub at `level` that keeps what it is handed, so a test can assert on the records.
fn recording(level : @moonlog.Level) -> (@moonlog.Hub, @moonlog.Memory) raise {
  let memory = @moonlog.Memory::new()
  let hub = @moonlog.Hub::new()
  hub.set_level(level)
  ignore(hub.add(@moonlog.Slot::new(memory, name="memory")))
  (hub, memory)
}

///|
test "moonlog bridge: every consensus level reaches the sink at its own level" {
  let (hub, memory) = recording(@moonlog.trace)
  let core = MoonlogLogger::of(hub).as_logger()
  core.log(@core.Debug, "d")
  core.log(@core.Info, "i")
  core.log(@core.Warning, "w")
  core.log(@core.Error, "e")
  core.log(@core.Fatal, "f")
  let records = memory.records()
  let levels = [
    @moonlog.debug, @moonlog.info, @moonlog.warn, @moonlog.error, @moonlog.fatal,
  ]
  assert_eq(records.map(r => r.level.name()), levels.map(l => l.name()))
  assert_eq(records[1].msg, "i")
  assert_eq(records[1].target, "raft")
}

///|
test "moonlog bridge: Panic is logged as fatal, not as an abort" {
  let (hub, memory) = recording(@moonlog.trace)
  MoonlogLogger::of(hub).as_logger().log(@core.Panic, "boom")
  let records = memory.records()
  assert_eq(records.length(), 1)
  assert_eq(records[0].level.name(), @moonlog.fatal.name())
  assert_eq(records[0].msg, "boom")
}

///|
test "moonlog bridge: the hub's level drops a record before it is written" {
  let (hub, memory) = recording(@moonlog.warn)
  let core = MoonlogLogger::of(hub, target="consensus").as_logger()
  core.log(@core.Debug, "dropped")
  core.log(@core.Info, "dropped")
  core.log(@core.Warning, "kept")
  let records = memory.records()
  assert_eq(records.length(), 1)
  assert_eq(records[0].msg, "kept")
  assert_eq(records[0].target, "consensus")
}

///|
test "moonlog bridge: a wrapped logger can still be used directly" {
  let (hub, memory) = recording(@moonlog.error)
  let logger = MoonlogLogger::new(hub.logger("node"))
  logger.inner().error("routed")
  logger.as_logger().log(@core.Info, "filtered")
  let records = memory.records()
  assert_eq(records.length(), 1)
  assert_eq(records[0].msg, "routed")
  assert_eq(records[0].target, "node")
}
