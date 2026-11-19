# Monitor public events: implementation plan

This document tracks the agreed event-contract refactor, completed increments, and remaining
work. [ARCHITECTURE.md](ARCHITECTURE.md) is the reference for implemented behavior.
Update both documents when a subsequent increment changes the contract.

## Scope and terminology

The monitor executes scenarios over network equipment and exposes task state and evidence
of operations. A scenario owns scheduling, branching, and decisions about continuing or
stopping. An operation is one invocation of an algorithm with its complete retry policy.
One attempt may contain concurrent SNMP and HTTP requests, parsing, conditional branches,
and subsequent actions. Retrying the operation invokes that algorithm again.

Keep scenarios as ordinary Rust code and preserve `Pollable` and `poll` as the generic
execution boundary. Universal steps, a declarative workflow engine, and an external
scenario-plugin API are deferred until concrete scenarios demonstrate the need.

## Workflow and progress

Start each subsequent increment only on the user's explicit command. After an increment,
report the changes and validation results and wait for approval before committing.
Approval to commit an increment does not authorize starting the next one.

| Increment | Status |
|---|---|
| 1. Define event payloads and operation identity | Completed |
| 2. Publish task descriptions | Completed |
| 3. Centralize task state transitions | Completed |
| 4. Publish operation start/completion and retain operation context | Not started; next increment |
| 5. Preserve reports during cancellation and caught panics | Not started; cancellation policy requires agreement |
| 6. Apply agreed metrics and limit accounting | Not started; accounting policy requires agreement |
| 7. Complete consumer migration and final verification | Pending; consumers and documentation are updated with each increment |

UDP tests remain deferred at the user's request. Run explicitly selected socket-free tests
and compilation/lint checks instead of the full test suite.

## Current and target event contract

| Event | Payload and current publication status |
|---|---|
| `TaskRegistered` | Published; description of the accepted initial revision |
| `TaskConfigurationChanged` | Published; description of the accepted replacement revision |
| `TaskRemoved` | Published; task removal |
| `TaskStateChanged` | Published; UTC change time, previous/current state, typed cause, and optional restart plan |
| `Preparation` | Published; existing preparation progress and evidence |
| `ExecutionFinished` / `ExecutionFailed` | Published temporarily; replaced in increment 4 |
| `OperationStarted` | Type defined; runtime publication pending in increment 4 |
| `OperationFinished` | Type defined; normal reports published in increment 4, interrupted reports in increment 5 |

Operation outcomes and scenario recovery are separate facts. Currently, a non-retryable
execution failure publishes its complete `ExecutionFailed` report before a recovery
`TaskStateChanged`. The target is `OperationFinished` for every terminal operation outcome,
with a separate transition only when task state actually changes.

`OperationReport::Completed` reuses the typed `ScenarioOutcome` and its `PollReport`.
Cancellation and panic payload types retain available timing, settings, completed attempts,
and a separately marked interrupted attempt. Defining these types does not guarantee their
runtime delivery. Cooperative interruption reporting is still pending.

Protocol and scenario-specific data belong inside reports, not in additional top-level
protocol-specific event variants. Static enums are sufficient for the implemented scenarios.
No `dyn` or untyped JSON payloads are required.

Serialized `MonitorEvent` records contain `metadata` and `data`. History versioning,
backward compatibility, and automatic migration are excluded from this refactor by the
user's decision. Intermediate payloads may change as the remaining increments land.

### Identity and observation

- `application_id` identifies a monitor instance; retain its existing public name.
- `task_id` identifies a task within that instance.
- `revision` identifies the accepted configuration version.
- `Generation` distinguishes scenario runs, including automatic restarts.
- `OperationId` and `OperationContext` types exist. Runtime allocation and retention with
  results/history remain pending in increment 4. Operation numbers must be independent of
  resettable metrics and scoped to a task's generation.
- `seq` orders public events within one monitor.
- Event metadata carries publication time. `TaskTransition.at` carries actual state-change
  time; operation reports carry action timing.

Preserve the atomic snapshot-plus-subscription boundary and recovery from receiver lag.
`TaskView.last_transition` already exposes the same state-change record as the event.
Applied configuration context for retained operation results is still pending; accepted
configuration and the revision that produced an older result must remain distinguishable.
A replacement snapshot restores current state, not lost operation history.

## Implementation sequence

### 1. Define public event types and operation identity — completed

Sources: [events](crates/tools-core/src/monitor/event.rs),
[operations](crates/tools-core/src/monitor/operation.rs),
[transitions](crates/tools-core/src/monitor/transition.rs), and
[public exports](crates/tools-core/src/monitor/mod.rs).

Implemented operation identity/context, typed transition causes and restart plans, terminal
operation statuses, and normal/cancelled/panicked report payloads. Structured preparation
and typed SNMP operation reports are preserved. Public payloads were introduced before
the corresponding runtime producers, allowing subsequent increments to remain buildable.

Validated serialization with a fixed JSON fixture, typed errors, binary values, original
run/revision context, and explicit interrupted evidence. Event records use ordinary Serde
serialization without a format-version field.

### 2. Add task descriptions for registration and configuration changes — completed

Sources: [task descriptions](crates/tools-core/src/monitor/task/description.rs),
[scenario descriptions](crates/tools-core/src/monitor/scenario/description.rs), and
[registration/update handlers](crates/tools-core/src/monitor/runtime/orchestrator.rs).

Implemented serializable `TaskDescription` and `ScenarioDescription` records derived from
validated configuration. They include name, scenario kind, target, profile, configured OID
definitions, encoded SET values, schedule, attempt settings, limit, and history depth.
Read/write communities and parser callbacks are excluded. Input aliases are normalized;
final OID instances are resolved later during preparation. Descriptions are observation
records, not executable configurations.

The runtime publishes `TaskRegistered` and `TaskConfigurationChanged` with descriptions
captured for their event revisions. `TaskAdded` and `TaskSpecUpdated` are removed. Initial
and dynamic registration share the same capture/publication path.

Validated consecutive updates followed by removal, revision correspondence, history-stream
contents, IPv6 targets, encoded binary SET values, and credential exclusion.

### 3. Centralize task state transitions — completed

Sources: [orchestrator](crates/tools-core/src/monitor/runtime/orchestrator.rs),
[transition payloads](crates/tools-core/src/monitor/transition.rs), and
[snapshots](crates/tools-core/src/monitor/task/view.rs).

`TaskEntry::transition` is the common mutation path. It records UTC change time,
previous/current `TaskStatus`, typed cause, and restart plan, then retains that transition.
`Orchestrator::transition` publishes the same record as `TaskStateChanged`. The private
`TaskState` duplicate is removed; command/report handlers still decide whether a transition
is appropriate.

Implemented transitions for initial and explicit starts, readiness, acknowledged pause and
resume, stop, configuration replacement, limit completion, typed preparation/execution
failures, caught panics, and scheduled restarts. `TaskView.last_transition` retains the
published context. Duplicate/no-op transitions are silent and preserve the previous record.

Restart plans expose an attempt number and UTC scheduled time. The timer uses the monotonic
deadline chosen with that plan, so waiting for event-history forwarding does not restart
the delay. Pause/stop/replacement clear a waiting plan. Stale timer/report checks remain.
Shutdown transitions live tasks to `Stopped` with cause `Shutdown` before clearing the
registry; idle and already-terminal tasks do not receive artificial transitions.

Public lifecycle/build/panic notifications are removed. `Preparation` and regular
`ExecutionFinished` / `ExecutionFailed` reports remain. Operation metrics, `limit` rules,
restart backoff policy, and command acknowledgement semantics are preserved.

Validation: 25 selected socket-free tests passed, including seven focused transition tests.
They cover snapshot/event correspondence, readiness, acknowledgement timing, duplicates,
paused updates, stale reports, typed failures/panics, completion ordering, shutdown, and
restart deadlines under history backpressure. `mise run check`, `mise run clippy`, test
compilation, example compilation, and formatting checks passed. Existing workspace warnings
remain. UDP tests were compiled but not executed.

### Current configuration-replacement behavior

`update_task` replaces the complete `TaskConfig`, increments `revision`, updates configuration
timestamps, resets the scenario-restart counter, and publishes `TaskConfigurationChanged`.
This happens even for an identical configuration or a name-only change.

| Current state | Effect of an accepted update |
|---|---|
| `Active`, `Restarting` | Abort the old scenario if present, create a new generation, transition to `Starting` with cause `ConfigurationChanged` |
| `Starting` | Abort the old scenario, create a new generation, retain `Starting`; no duplicate state-transition event |
| `Paused` | Abort the old scenario if present; remain paused and apply the scenario on `resume` |
| `Idle`, `Stopped`, `Completed` | Store the new configuration without starting execution |

Metrics, last result, and local history are retained. Late reports from a retired scenario
are ignored. Successful `update_task().await` confirms processing of the update, not network
readiness; readiness is reported separately. Configuration changes while already `Starting`
remain visible through the configuration event and subsequent preparation reports.

Current interruption uses `abort` and can lose an in-flight operation report; increment 5
addresses this. Updating `deep_history` changes the accepted setting without resizing an
existing history ring. That separate behavior is not fixed by this event refactor.

### 4. Add operation start/completion reporting to GET and SET — next, not started

Sources: [internal scenario reports](crates/tools-core/src/monitor/scenario/runtime.rs),
[GET](crates/tools-core/src/monitor/scenario/impls/repeat_snmp_get.rs),
[SET](crates/tools-core/src/monitor/scenario/impls/repeat_snmp_set.rs), and
[operation reports](crates/tools-core/src/polling/report.rs).

Allocate an `OperationId` before executing the entire retry loop. Send internal start and
finish reports with immutable run and applied-configuration context. The orchestrator
continues assigning public sequence numbers; scenarios do not publish directly.

Publish `OperationStarted` and `OperationFinished` for successful requests, exhausted
attempts, and non-retryable failures. Remove `ExecutionFinished` / `ExecutionFailed` after
migrating producers and consumers. Keep recovery transitions separate. Reuse `PollReport`
where possible and avoid independently mutable duplicate timing fields.

Retain operation identity, generation, and applied revision with the last result and local
history so later configuration changes cannot relabel old results. Keep current accounting
until increment 6 is explicitly agreed. Do not promise cancellation/panic terminal delivery
before increment 5.

Completion evidence: one operation covers all retries; start/finish events correlate;
non-retryable failures retain their reports; results/history keep their original context.

### 5. Complete operations during orderly cancellation and caught panics — not started

Sources: [polling](crates/tools-core/src/polling/poll.rs),
[scenario runtime](crates/tools-core/src/monitor/scenario/runtime.rs), and
[orchestrator](crates/tools-core/src/monitor/runtime/orchestrator.rs).

Before implementation, agree on cancellation acknowledgement and the forced-abort fallback.
Keep active-operation bookkeeping and completed-attempt evidence outside the future that
may be cancelled or panic. Reuse or extend the existing internal attempt observer.

Coordinate cancellation and await completion during stop, configuration replacement,
removal, and orderly shutdown. Preserve completed attempts and explicitly mark an
interrupted attempt. Caught panics must retain available evidence and panic context.
Local cancellation does not establish whether a device executed an already-sent command.

Give retiring runs an explicit terminal-report path: finalize their operations exactly once
with original identities, without mutating replacement-run state or results. Publish terminal
operation events before final stop/removal events; accepted configuration changes may precede
completion of old operations. Clear interrupted preparation progress as well.

The matching start/finish guarantee applies to orderly execution and handled failures,
not process crashes or emergency aborts.

Completion evidence: cancellation during an attempt or retry delay, panic, stop, update,
remove, and shutdown. Late/duplicate reports cannot mutate a replacement run.

### 6. Apply agreed accounting rules — not started; decision required

Sources: [task accounting](crates/tools-core/src/monitor/task/task.rs),
[metrics](crates/tools-core/src/monitor/task/metrics.rs), and
[limit enforcement](crates/tools-core/src/monitor/runtime/orchestrator.rs).

Current accounting excludes non-retryable operation failures from execution metrics and
`limit`, despite retaining their reports. Event migration must not silently change it.

Proposed policy, pending agreement:

- Count every normally completed operation once, including exhausted attempts and
  non-retryable failure.
- Keep attempt counts separate from operation counts.
- Track cancellation and panic separately from normally completed operations.
- For repeating GET/SET, let `limit` count normally completed operations, including failures.

Define health updates and counter resets consistently. Other scenarios may need their own
completion conditions rather than a periodic-request counter.

Completion evidence: explicit rules and tests for success, exhausted attempts, non-retryable
failure, cancellation, panic, explicit restart, and automatic restart. Limit completion
follows publication of the final operation result.

### 7. Finish consumer migration and final verification — pending

CLI, examples, public API tests, and architecture are updated with each increment. Final
verification must remove obsolete contract references and check the complete migration.
Snapshots remain the dashboard's state source; operation events provide execution evidence.

Verify history examples, event order, revision capture, snapshot consistency, old-run
completion, and interruption reporting. Keep tests meaningful and obey the current UDP-test
restriction unless the user changes it.

For each code increment, run relevant allowed tests, `mise run check`, and `mise run clippy`.
Report existing warnings and environmental blockers. The frozen `net-monitor` crate is
outside this work.

## Deferred work

Universal steps, new composite scenarios, prepared-action API extraction, coordination,
action expiry, and configurable recovery/history-delivery policies are outside this refactor.
Existing history-delivery/backpressure policy remains unless separately agreed.

When a concrete composite operation is added, design its typed partial evidence then.
The `Pollable` return value alone cannot retain intermediate successful data after an outer
timeout drops the attempt future. That requires an explicit reporting path.
