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
/// Why a configuration was rejected — one variant per etcd `Config.validate`
/// branch that applies to this port. Raised by `Config::validate` /
/// `RaftNode::from_config` (etcd returns an `error` from the same branches).
pub suberror ConfigError {
  /// `id` is empty (etcd: "cannot use none as id").
  EmptyId
  /// `heartbeat_tick <= 0`.
  HeartbeatTickNotPositive
  /// `election_tick <= heartbeat_tick`.
  ElectionTickNotGreater
  /// `max_inflight <= 0`.
  MaxInflightNotPositive
  /// `max_inflight_bytes` is set but below `max_msg_bytes`.
  MaxInflightBytesTooSmall
  /// `read_only_option` is `LeaseBased` without `check_quorum`.
  LeaseBasedNeedsCheckQuorum
} derive(Eq)

///|
/// The parameters to start a server, collected into one explicit value (etcd's
/// `raft.Config`). It gathers what `RaftNode::new` otherwise takes as a dozen
/// loose optional arguments, so a caller can build, inspect and `validate` a
/// configuration before constructing the node. `RaftNode::new` remains as a
/// convenience constructor for callers that just want defaults; `from_config`
/// is the validated path.
pub struct Config {
  // The identity of this server; cannot be empty.
  id : String
  // The other voters at start-up.
  peers : Array[String]
  // Ticks between elections; must exceed `heartbeat_tick`.
  election_tick : Int
  // Ticks between heartbeats; must be greater than 0.
  heartbeat_tick : Int
  // Byte cap on a single AppendEntries batch (etcd's MaxSizePerMsg).
  max_msg_bytes : UInt64
  // Byte cap on the uncommitted log tail (etcd's MaxUncommittedEntriesSize);
  // 0 disables the check (etcd maps 0 to no limit).
  max_uncommitted_size : UInt64
  // In-flight AppendEntries window per follower (etcd's MaxInflightMsgs); > 0.
  max_inflight : Int
  // In-flight byte window per follower (etcd's MaxInflightBytes); 0 = no limit,
  // otherwise must be >= max_msg_bytes.
  max_inflight_bytes : UInt64
  // Whether a leader checks quorum liveness and steps down when it lapses
  // (etcd's CheckQuorum). Required when `read_only_option` is `LeaseBased`.
  check_quorum : Bool
  // Whether to run pre-vote (etcd's PreVote).
  pre_vote : Bool
  // How linearizable reads are confirmed (etcd's ReadOnlyOption).
  read_only_option : ReadOnlyOption
  // Whether a removed/demoted leader steps down (etcd's StepDownOnRemoval).
  step_down_on_removal : Bool
  // Whether a follower drops rather than forwards proposals (etcd's
  // DisableProposalForwarding).
  disable_proposal_forwarding : Bool
  // Whether propose-time conf-change validation is off (etcd's
  // DisableConfChangeValidation).
  disable_conf_change_validation : Bool
  // The last index the application has already applied (etcd's Config.Applied),
  // set only when restarting so the core does not re-deliver applied entries.
  applied : UInt64
  // Diagnostics sink (etcd's Config.Logger).
  logger : &Logger
  // State-transition tracer (etcd's Config.TraceLogger).
  tracer : &Tracer
  // Seed for the deterministic election-timeout PRNG (this port's addition for
  // reproducible simulations; etcd uses a shared crypto RNG).
  seed : UInt64
}

///|
/// Build a `Config` for server `id` with the other voters `peers`, taking etcd's
/// defaults for everything unset. The result is not validated; call `validate`
/// or go through `RaftNode::from_config`.
pub fn Config::new(
  id : String,
  peers : Array[String],
  election_tick? : Int = 10,
  heartbeat_tick? : Int = 1,
  max_msg_bytes? : UInt64 = 18446744073709551615UL,
  max_uncommitted_size? : UInt64 = 0,
  max_inflight? : Int = 256,
  max_inflight_bytes? : UInt64 = 0,
  check_quorum? : Bool = false,
  // etcd's Config.PreVote is opt-in; its zero value is false.
  pre_vote? : Bool = false,
  read_only_option? : ReadOnlyOption = Safe,
  step_down_on_removal? : Bool = false,
  disable_proposal_forwarding? : Bool = false,
  disable_conf_change_validation? : Bool = false,
  applied? : UInt64 = 0,
  logger? : &Logger = NopLogger::{  },
  tracer? : &Tracer = NopTracer::{  },
  seed? : UInt64 = 1,
) -> Config {
  {
    id,
    peers,
    election_tick,
    heartbeat_tick,
    max_msg_bytes,
    max_uncommitted_size,
    max_inflight,
    max_inflight_bytes,
    check_quorum,
    pre_vote,
    read_only_option,
    step_down_on_removal,
    disable_proposal_forwarding,
    disable_conf_change_validation,
    applied,
    logger,
    tracer,
    seed,
  }
}

///|
/// Reject an unusable configuration, mirroring etcd's `Config.validate` branch
/// for branch. The etcd checks that do not apply to this port (a nil `Storage`,
/// a local-message-target id, `MaxCommittedSizePerReady` which lives on the
/// async-storage path) are noted in `GAP_core.md` rather than enforced here.
pub fn Config::validate(self : Config) -> Unit raise ConfigError {
  if self.id == "" {
    raise EmptyId
  }
  if self.heartbeat_tick <= 0 {
    raise HeartbeatTickNotPositive
  }
  if self.election_tick <= self.heartbeat_tick {
    raise ElectionTickNotGreater
  }
  if self.max_inflight <= 0 {
    raise MaxInflightNotPositive
  }
  if self.max_inflight_bytes != 0 &&
    self.max_inflight_bytes < self.max_msg_bytes {
    raise MaxInflightBytesTooSmall
  }
  if self.read_only_option is LeaseBased && !self.check_quorum {
    raise LeaseBasedNeedsCheckQuorum
  }
}

///|
/// Build a server from a validated `Config` (etcd's `newRaft`, which panics on an
/// invalid config; here the error is raised so the caller can handle it). This is
/// the explicit, checked counterpart to `RaftNode::new`.
pub fn RaftNode::from_config(config : Config) -> RaftNode raise ConfigError {
  config.validate()
  let r = RaftNode::new(
    config.id,
    config.peers,
    seed=config.seed,
    election_timeout=config.election_tick,
    heartbeat_timeout=config.heartbeat_tick,
    max_msg_bytes=config.max_msg_bytes,
    max_uncommitted_size=config.max_uncommitted_size,
    max_inflight=config.max_inflight,
    max_inflight_bytes=config.max_inflight_bytes,
    check_quorum=config.check_quorum,
    pre_vote=config.pre_vote,
    step_down_on_removal=config.step_down_on_removal,
    disable_conf_change_validation=config.disable_conf_change_validation,
    read_only_option=config.read_only_option,
    logger=config.logger,
    tracer=config.tracer,
  )
  if config.disable_proposal_forwarding {
    r.disable_proposal_forwarding()
  }
  // Seed the applied watermark on a restart (etcd's Config.Applied), so the core
  // does not re-deliver entries the application already applied. A no-op on a
  // fresh start (empty log, applied 0).
  if config.applied > 0 {
    r.advance_applied(config.applied)
  }
  r
}
