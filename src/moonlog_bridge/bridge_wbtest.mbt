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
/// A sink that keeps what it was handed, so a test can assert on the records.
struct RecordingSink {
  records : Array[@moonlog.Record]
}

///|
impl @moonlog.Sink for RecordingSink with fn write(self, record) {
  self.records.push(record)
}

///|
test "moonlog bridge: every consensus level reaches the sink at its own level" {
  let sink : RecordingSink = { records: [], }
  let logger = MoonlogLogger::to_sink(sink, level=@moonlog.Trace)
  let core : &@core.Logger = logger.as_logger()
  core.log(@core.Debug, "d")
  core.log(@core.Info, "i")
  core.log(@core.Warning, "w")
  core.log(@core.Error, "e")
  core.log(@core.Fatal, "f")
  assert_eq(sink.records.length(), 5)
  let levels : Array[@moonlog.Level] = []
  for r in sink.records {
    levels.push(r.level)
  }
  assert_eq(levels, [
    @moonlog.Debug,
    @moonlog.Info,
    @moonlog.Warning,
    @moonlog.Error,
    @moonlog.Fatal,
  ])
  assert_eq(sink.records[1].message, "i")
}

///|
test "moonlog bridge: Panic is logged as Fatal, not as an abort" {
  let sink : RecordingSink = { records: [], }
  let logger = MoonlogLogger::to_sink(sink, level=@moonlog.Trace)
  logger.as_logger().log(@core.Panic, "boom")
  assert_eq(sink.records.length(), 1)
  assert_eq(sink.records[0].level, @moonlog.Fatal)
  assert_eq(sink.records[0].message, "boom")
}

///|
test "moonlog bridge: the moonlog filter drops a record before it is written" {
  let sink : RecordingSink = { records: [], }
  let logger = MoonlogLogger::to_sink(sink, level=@moonlog.Warning)
  let core : &@core.Logger = logger.as_logger()
  core.log(@core.Debug, "dropped")
  core.log(@core.Info, "dropped")
  core.log(@core.Warning, "kept")
  assert_eq(sink.records.length(), 1)
  assert_eq(sink.records[0].message, "kept")
}

///|
test "moonlog bridge: a wrapped logger keeps its own level" {
  let sink : RecordingSink = { records: [], }
  let inner = @moonlog.Logger::new(sink, level=@moonlog.Error)
  let logger = MoonlogLogger::new(inner)
  logger.inner().error("routed")
  logger.as_logger().log(@core.Info, "filtered")
  assert_eq(sink.records.length(), 1)
  assert_eq(sink.records[0].message, "routed")
}
