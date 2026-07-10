// Copyright 2024 The etcd Authors
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
/// One observable transition of the consensus core, surfaced to an external
/// tracer. These mirror the events etcd's `state_trace.go` records for its TLA+
/// conformance harness (`traceBecome*`, `traceCommit`, `traceReplicate`,
/// `trace{Send,Receive}Message`, `traceConfChangeEvent`, `traceInitState`).
/// etcd guards the machinery behind a `with_tla` build tag so a normal build
/// compiles the no-op variant (`state_trace_nop.go`); the equivalent here is
/// that the default `NopTracer` erases to nothing.
pub enum TraceEvent {
  // A node initialised its persistent state (etcd's traceInitState).
  InitState(String)
  // A node became follower/candidate/leader at a term (traceBecome*).
  StateChange(String, Role, UInt64)
  // A leader advanced its commit index (traceCommit).
  Commit(String, UInt64)
  // A leader appended entries to its own log (traceReplicate).
  Replicate(String, Array[Entry])
  // A message was scheduled to send (traceSendMessage).
  SendMessage(Message)
  // A message was received and about to be stepped (traceReceiveMessage).
  ReceiveMessage(Message)
  // The active configuration changed (traceConfChangeEvent).
  ConfChange(String, ConfState)
}

///|
/// A pluggable observer of the core's state transitions (etcd's `TraceLogger`).
/// A single dispatch method mirrors etcd's one-event-at-a-time trace stream;
/// wrappers below name the individual events. The default `NopTracer` discards
/// everything, so an untraced server pays nothing.
pub trait Tracer {
  fn on_event(Self, TraceEvent) -> Unit
}

///|
/// The default `Tracer`: it discards every event (matching etcd's default
/// `!with_tla` build, where every `trace*` call is an empty function).
pub struct NopTracer {}

///|
impl Tracer for NopTracer with fn on_event(_self, _event) {

}
