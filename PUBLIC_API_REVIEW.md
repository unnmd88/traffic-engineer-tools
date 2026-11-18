# Monitor public events: proposal and implementation plan

This document proposes the next public event contract and a sequence of implementation
increments. It describes planned behavior, not the current implementation.
[ARCHITECTURE.md](ARCHITECTURE.md) remains the reference for implemented behavior and must
be updated with each code change that changes the contract.

## Scope and terminology

The monitor executes scenarios over network equipment and exposes task state and evidence
of completed operations. A scenario owns its behavior, scheduling, branching, and decisions
about continuing or stopping.

An operation is one invocation of an algorithm with its complete retry policy. One attempt
may contain concurrent SNMP and HTTP requests, parsing, conditional branches, and subsequent
actions. Retrying the operation invokes that algorithm again. A single protocol request is
only one possible operation implementation.

Keep scenarios as ordinary Rust code and keep `Pollable` and `poll` as the existing generic
execution boundary. A universal steps model, declarative workflow engine, and external
scenario-plugin API are outside this change. Composite-operation implementations and their
additional evidence types should be introduced when a concrete scenario requires them.

## Proposed public contract

| Event | Meaning and payload |
|---|---|
| `TaskRegistered` | A task was registered; includes its description at this revision |
| `TaskConfigurationChanged` | A new configuration revision was accepted; includes its description |
| `TaskRemoved` | A task was removed |
| `TaskStateChanged` | Previous state, next state, typed cause, and state-specific details |
| `OperationStarted` | An operation began; includes its identity, purpose, start time, and attempt settings |
| `OperationFinished` | An operation terminated; includes its identity and report |
| `Preparation` | Existing preparation progress and evidence, retained during this refactor |

`Preparation` is retained to preserve live SCN discovery, OID resolution, and preparation
reports. It does not establish a general steps abstraction.

Operation outcomes and scenario recovery are independent facts. A failed operation emits
`OperationFinished`. If the scenario must restart, a separate `TaskStateChanged` describes
the transition and restart plan. A failure need not cause a state transition.

The operation report distinguishes success, failure, cancellation, and a caught panic.
Normal reports retain effective attempt settings, completed attempts, original typed errors,
results, and UTC and monotonic timing. Interrupted reports explicitly identify incomplete
execution and must not invent a normal response or a completed attempt.

Protocol and scenario-specific result types belong inside reports, not in additional
protocol-specific variants of the top-level event enum. Static enums are sufficient for
the implemented scenarios; this proposal does not require `dyn` or untyped JSON payloads.

### Identity and snapshots

- `application_id` identifies a monitor instance; retain the existing public name.
- `task_id` identifies a task within that instance.
- `revision` identifies the accepted configuration version.
- The existing `Generation` identifies a scenario run, including automatic restarts.
- A new `OperationId` identifies an operation within a run. Its full identity includes the
  monitor, task, and generation; it is not derived from metrics, which can reset.
- `seq` orders public events within one monitor.
- The envelope timestamp is publication time. Reports carry action timestamps.

Execution events and retained results must carry the configuration revision actually used
by their run. An accepted configuration change may precede its application to a new run.
Run and operation context should appear only where it has meaning, rather than inventing
an active run for a task that has not started.

Snapshots expose current state, its cause and restart details, operation context, results,
and accumulated metrics. Preserve the atomic snapshot-plus-subscription boundary and
recovery from receiver lag. A replacement snapshot restores current state, not lost history.

## Implementation sequence

Implement each increment with focused tests and the corresponding architecture update.
Intermediate states must document any guarantees that are not implemented yet.

Increment 1 now provides operation identity/context, transition causes and restart plans,
normal/interrupted report types, and the `TaskStateChanged`, `OperationStarted`, and
`OperationFinished` payloads. Existing runtime publishers remain in place until their
migration increments. Task descriptions and their event variants belong to increment 2.

Serialized `MonitorEvent` records contain `metadata` and `data`, without a format-version
field. The user explicitly excluded history versioning and legacy compatibility from
this refactor. Intermediate payloads may change as the migration proceeds.

UDP tests are deferred at the user's request. During these increments, run explicitly
selected socket-free tests and compilation/lint checks instead of the full test suite.

### 1. Define public event types and migrate existing publishers

Sources: [event types](crates/tools-core/src/monitor/event.rs),
[public exports](crates/tools-core/src/monitor/mod.rs), and
[scenario results](crates/tools-core/src/monitor/scenario/execution.rs).

Introduce the event payloads, typed transition causes, operation identity, and terminal
outcomes. Preserve structured preparation evidence and existing typed SNMP reports.
Document the serialized representation before replacing the current event variants.
Introduce payload types alongside existing publishers as needed
so this increment remains buildable; migrate publishers and remove obsolete variants as
the following increments land.

Keep the current metrics and limit semantics until the accounting decision below is agreed.
Do not promise cancellation reports before increment 5 implements their delivery.

Completion evidence: serialization examples and round-trip tests for the new payloads,
including typed failures, binary values, and interrupted outcomes. Document the treatment
of old JSONL records; do not claim automatic compatibility or migration.

### 2. Add task descriptions for registration and configuration changes

Sources: [task configuration](crates/tools-core/src/monitor/task/config.rs),
[scenario specifications](crates/tools-core/src/monitor/scenario/spec.rs), and
[SNMP request specifications](crates/tools-core/src/snmp/request.rs).

Introduce serializable `TaskDescription` and scenario-specific descriptions derived from
validated configuration. Include the task name, scenario kind, target, relevant request
parameters, schedule, attempt settings, limit, and history depth. Exclude credentials.

Capture the description when a revision is registered or accepted. Publish it in
`TaskRegistered` and `TaskConfigurationChanged` so reading an exported session does not
require querying a live monitor or guessing which configuration a revision referenced.

Completion evidence: descriptions correspond to their event revisions, survive task removal,
and exclude credentials. Rapid consecutive updates remain independently interpretable.

### 3. Centralize task state transitions

Sources: [orchestrator](crates/tools-core/src/monitor/runtime/orchestrator.rs),
[task state](crates/tools-core/src/monitor/task/task.rs), and
[snapshots](crates/tools-core/src/monitor/task/view.rs).

Introduce a single internal transition path that changes state, records its cause and
state-specific details, and prepares the corresponding public event. Cover explicit
commands, scenario readiness, pause/resume acknowledgements, completion, preparation
failure, execution failure, panic, and restart.

Store the current transition context in snapshots, including the scheduled restart time.
Use typed causes rather than requiring consumers to parse error strings. Treat command
acceptance and actual state change as distinct moments. A no-op command need not emit a
state transition.

Completion evidence: published transitions match snapshots, include their reasons, and
preserve the established acknowledgement semantics.

### 4. Add operation start/completion reporting to GET and SET

Sources: [internal scenario reports](crates/tools-core/src/monitor/scenario/runtime.rs),
[GET](crates/tools-core/src/monitor/scenario/impls/repeat_snmp_get.rs),
[SET](crates/tools-core/src/monitor/scenario/impls/repeat_snmp_set.rs), and
[operation reports](crates/tools-core/src/polling/report.rs).

Allocate an `OperationId` before executing the entire retry loop. Send internal start and
finish reports with immutable run and configuration context. The orchestrator assigns
public event sequence numbers; scenarios do not publish directly to subscribers.

Use `OperationFinished` for successful requests, exhausted attempts, and non-retryable
failures. Handle scenario continuation or restart separately. Keep `PollReport<T, E>` as
the normal operation evidence where possible, and avoid duplicating independently mutable
timing fields in monitor wrappers.

Retain operation identity, generation, and applied revision with the last result and local
history, so a configuration update cannot relabel an older result.

Completion evidence: one operation covers all retries; normal start/finish events correlate;
non-retryable failures preserve their reports; retained results keep their original context.

### 5. Complete operations during orderly cancellation and caught panics

Sources: [polling](crates/tools-core/src/polling/poll.rs),
[scenario runtime](crates/tools-core/src/monitor/scenario/runtime.rs), and
[orchestrator](crates/tools-core/src/monitor/runtime/orchestrator.rs).

Keep active-operation bookkeeping and completed-attempt evidence outside the future that
may be cancelled or panic. Reuse or extend the existing internal attempt observer rather
than introducing a steps engine. Extend the reporting path to return or assemble an
explicit interrupted outcome.

For stop, configuration replacement, removal, and orderly shutdown, coordinate cancellation
and await completion. Preserve completed attempts and mark any in-progress attempt as
interrupted. On caught panic, preserve the available evidence and panic context.
Cancellation does not establish whether a device executed an already-sent command.

Give retiring runs an explicit terminal-report path. Their operations may be finalized
exactly once with their original identities, without changing the state or results of a
replacement run. Emit terminal operation events before the final stop/removal event;
allow accepted configuration changes to precede completion of old operations.

Specify the fallback for forced abort separately. The matching start/finish guarantee
applies to orderly execution and handled failures, not process crashes or emergency aborts.
Clear interrupted preparation progress as part of orderly lifecycle handling as well.

Completion evidence: cancellation during an attempt and retry delay, panic, stop, update,
remove, and shutdown. Started operations receive one terminal event under the orderly
contract; late or duplicate reports cannot mutate a replacement run.

### 6. Apply the agreed accounting rules

Sources: [task accounting](crates/tools-core/src/monitor/task/task.rs),
[metrics](crates/tools-core/src/monitor/task/metrics.rs), and
[limit enforcement](crates/tools-core/src/monitor/runtime/orchestrator.rs).

Current behavior excludes non-retryable operation failures from execution metrics and
`limit`, despite retaining their reports. Changing event names must not silently change it.

Proposed policy, pending agreement:

- Count every normally completed operation once, including exhausted attempts and
  non-retryable failure.
- Keep attempt counts separate from operation counts.
- Track cancellation and panic separately from normally completed operations.
- For repeating GET/SET, let `limit` count normally completed operations, including failures.

Define health updates and counter resets consistently with the chosen policy. Other
scenarios may need their own completion conditions; do not force every scenario into a
periodic request counter.

Completion evidence: tests document success, exhausted attempts, non-retryable failure,
cancellation, panic, explicit restart, and automatic restart. Limit-triggered termination
publishes the operation result before task completion.

### 7. Finish consumer migration and document the contract

Update the CLI dashboard, JSONL writer expectations, public API tests, examples, and
[ARCHITECTURE.md](ARCHITECTURE.md). Snapshots remain the primary source for dashboard state;
operation events provide execution evidence for history and other consumers.

Verify example history records. Add focused tests for event order,
revision capture, snapshot consistency, old-run completion, and interruption reporting.
Keep tests meaningful; do not duplicate event constructors as implementation-mirroring tests.

Before completing each code increment, run the relevant tests, `mise run check`, and
`mise run clippy`. Record existing warnings or environmental blockers explicitly. Do not
modify the frozen `net-monitor` crate as part of this work.

## Decisions to settle before their implementation increments

- Accounting: whether to accept the proposed treatment of non-retryable failures,
  cancellation, and panic in metrics and `limit`.
- Orderly cancellation: acknowledgement semantics and the forced-abort fallback.

These decisions do not prevent defining payloads, descriptions, and transition causes.
Resolve them before dependent behavior changes rather than guessing during implementation.

## Deferred work

Do not include universal steps, a workflow engine, new composite scenarios, prepared-action
API extraction, coordination, action expiry, or configurable recovery/history-delivery
policies in this refactor. Existing subscription and history-delivery behavior remains in
force unless separately agreed.

When a concrete composite operation is added, design its typed partial evidence then.
The current `Pollable` return value alone cannot retain intermediate successful data after
an outer timeout drops the attempt future. Preserving that data requires an explicit
reporting path, not merely a new top-level event name.
