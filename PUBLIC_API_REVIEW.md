# Monitor and polling API: proposed next steps

This document replaces the previous public API review. It is a prioritized proposal based
on the current working tree and the intended repeating-action and coordination scenarios.
It describes future work, not additional implemented behavior. [ARCHITECTURE.md](ARCHITECTURE.md)
remains the reference for current behavior.

## Foundation already implemented

An operation is one execution of an action together with all of its attempts. The action
may perform one or several network requests. A scenario decides when to execute actions;
the polling mechanism executes an action with its timeout and retry settings.

The current implementation provides:

- `Pollable` with associated output and error types, without protocol dependencies;
- `PollReport<T, E>` with UTC start/end timestamps, monotonic total duration, effective
  attempt settings, every attempt, and the final operation outcome;
- attempt offsets, durations, successful attempts, polling timeouts, and original typed
  adapter errors with their retry classification;
- reports for success, exhausted attempts, and non-retryable failure;
- report propagation through GET/SET, task results and history, events, JSONL, and the CLI;
- preparation stage start/completion events, complete preparation reports, typed errors,
  raw SCN request reports, and current progress in snapshots and CLI;
- immediate source logging of preparation actions and SCN request attempts;
- unchanged scheduling and existing `limit` accounting, plus the correction allowing
  256 attempts for `retries = 255`.

Non-retryable failures now retain reports but still follow the existing scenario-restart
path and do not increment execution metrics or consume `limit`. Cooperative cancellation,
action expiry, and regular-operation reports surviving a panic are not implemented.
Preparation stages catch panics and preserve earlier completed stages; forced cancellation
can still interrupt preparation without a terminal report. Public polling types
and serialized result records changed; there is no automatic migration of old result data.

Relevant sources: [operation reports](crates/tools-core/src/polling/report.rs),
[polling](crates/tools-core/src/polling/poll.rs),
[scenario execution](crates/tools-core/src/monitor/scenario/execution.rs), and
[event contract](crates/tools-core/src/monitor/event.rs), and
[preparation contract](crates/tools-core/src/monitor/scenario/preparation.rs).

## 1. Complete operation reporting with cancellation and expiry

**Why first:** coordination must stop trying to install a phase once its validity interval
has ended. Stopping or replacing a task should also preserve the attempts already made.

Proposed changes:

- Give operation execution an explicit cancellation signal and optional validity deadline.
- Observe cancellation during both an attempt and the delay before another attempt.
- Distinguish explicit cancellation from expiry, timeout, and exhausted attempts.
- Return completed attempts and, when execution was interrupted, a terminal record of the
  in-progress attempt. Cancellation before the first attempt may legitimately have no attempts.
- Stop starting attempts once the action is obsolete. A deadline should also limit an
  attempt that would otherwise continue beyond the action's validity interval.
- Let the scenario await the terminal report during an orderly stop or configuration
  replacement; retain forced abort as a fallback with an explicitly weaker reporting guarantee.
- Preserve the generation and revision of the cancelled operation. The orchestrator must
  retain an authorized terminal report from a retiring run without letting it mutate the
  state of its replacement. Its current blanket rejection of inactive generations needs
  an explicit completion path for this purpose.

Completion evidence: deterministic tests for cancellation during an attempt, cancellation
between attempts, expiry before the next attempt, and stop/update while an operation runs.
The interrupted operation reaches history exactly once under the orderly-stop contract.
A lost SET acknowledgement or local cancellation must not be presented as proof that the
device did not execute the command.

## 2. Expose prepared SNMP actions independently of monitor scenarios

**Why:** the core library should support a one-off probe, a consumer-owned loop, and a
coordination scenario using the same request preparation and execution code.

Provide a public describe → prepare → execute path for GET and SET. Preparation resolves
profiles and SCN and builds reusable action objects. One action attempt performs its work
once; `poll` remains responsible for retries. Keep the existing scenario-owned schedules.
Move the private GET/SET adapters into the SNMP layer and reuse them from the monitor.

For an action containing several network steps, specify its retry boundary: invoking its
adapter again retries the action. Retrying an individual step belongs to the action's
implementation and must be explicit to avoid multiplying retry policies.

Completion evidence: an example executes a prepared request through `poll` without
constructing `Application`; GET/SET scenarios use that same public implementation.

Relevant sources: [request specifications](crates/tools-core/src/snmp/request.rs),
[GET adapter](crates/tools-core/src/monitor/scenario/impls/repeat_snmp_get.rs), and
[SET adapter](crates/tools-core/src/monitor/scenario/impls/repeat_snmp_set.rs).

## 3. Preserve the meaning and quality of returned data

**Current gap:** GET parser errors still become `BusinessValue::Text("parse error")`,
while the operation is classified as successful.

Represent no parser, decoded value, and decoding error explicitly, preserving `raw_value`
and the error. Represent SNMP absence/exception values as observable outcomes for each OID.
Check response cardinality and OID correspondence before associating values with parsers;
the current adapter associates them through `zip`.

Specify separate meanings for receiving a response and obtaining useful data. Decide when
a partially useful result is `Degraded` and when no usable requested data means failure.
Success after retries should remain success, with retry information available separately.

Completion evidence: mixed successful and failed OID decoding retains every available raw
value and typed decoding error; a consumer can distinguish these cases without parsing text.

Relevant sources: [SNMP samples](crates/tools-core/src/snmp/response.rs) and
[GET decoding](crates/tools-core/src/monitor/scenario/impls/repeat_snmp_get.rs).

## 4. Make retained reports independently interpretable

**Current gaps:** event metadata identifies a task, generation, and revision, but retained
`ScenarioExecution` values do not carry that context themselves. Task-add/update events
omit the request description. A snapshot may therefore combine a new configuration with
a result produced under an older one.

Proposed changes:

- Associate monitor operations with an operation identifier, execution generation, and
  applied configuration revision. Keep these monitor concepts outside generic `polling`.
- Retain a description of the request for each referenced revision, including its target
  and requested OIDs/SET values, while excluding credentials from the history description.
- Distinguish accepted configuration from the configuration applied by a running scenario.
- Include sufficient context in task-add/update events or explicit description records to
  interpret an exported session without access to a live monitor.
- Specify report invariants: attempt ordering, timing bounds, and consistency between the
  terminal attempt and operation outcome. Decide whether public construction/deserialization
  validates them before treating reports as a stable interchange contract.
- Derive scenario timing from the enclosed report instead of keeping independently mutable
  duplicate timing fields. The current constructor copies timing, but public fields and
  deserialization still allow contradictory values.
- Introduce an explicit history-format version before promising long-term compatibility.

Completion evidence: exporting a session, changing a task, and reading it later still
identifies exactly which action and configuration produced each result.

## 5. Separate operation failures from scenario recovery policy

**Current gap:** every SNMP error other than network/timeout is non-retryable, ends the
scenario, and schedules a restart. Successful preparation immediately resets restart backoff.

Keep three decisions distinct: the observed failure, permission to retry this action, and
whether the scenario can continue. A device rejection need not imply that recreating the
scenario will help. Make restart enablement, delays, and stopping conditions configurable.
Define recovery before resetting backoff so repeated post-build failures do not always
receive the initial delay.

Specify SET retry behavior in terms of the command's meaning and the possibility that it
was executed despite a missing acknowledgement. Coordination may need a read-back check
or a new decision based on the current time rather than another identical write.

Resolve accounting explicitly with the user: keep the existing meaning of `limit` as a
count of whole scenario iterations, and decide how non-retryable, cancelled, and expired
operations affect that count. Attempt counts remain a separate measurement. Do not silently
change the existing accounting as a side effect of report or recovery work.

Completion evidence: a persistent device rejection follows the selected policy, repeated
runtime failures follow the configured backoff, and tests document all counting rules.

Relevant sources: [error classification](crates/tools-core/src/monitor/scenario/runtime.rs),
[restart policy](crates/tools-core/src/monitor/runtime/restart_policy.rs), and
[orchestrator](crates/tools-core/src/monitor/runtime/orchestrator.rs).

## 6. Define observation and history-delivery guarantees

Preserve the consistent snapshot-plus-subscription boundary. Add examples of recovery from
lag and document command acknowledgement versus actual completion at public API methods.

For lifecycle events, propose an explicit previous state, next state, and typed cause;
report configuration acceptance separately from its application. Preserve result reports
as the evidence of what actions actually did.

History currently creates backpressure on the orchestrator and shutdown may wait for an
unread sink indefinitely. Agree on the required behavior under a slow or disconnected
consumer, delivery acknowledgement, and bounded shutdown. Distinguish channel delivery
from persistence. Recovering a snapshot restores current state but cannot recreate lost
operation reports.

Completion evidence: the documented policy is exercised with slow and disconnected sinks,
and clients can observe completion or recover current state without guessing event order.

## 7. Implement coordination as a scenario using the shared action mechanism

Use the existing epoch/cycle/phase calculations to derive the required phase from the
common time scale. A periodic decision loop can check once per second while an action is
in progress; it must remain able to expire or replace that action.

Phase installation is an action with an operation report and bounded attempts. Its validity
ends when the desired phase changes. An acknowledgement and an observation of the actual
phase are distinct facts. Specify command refresh/keepalive rules and avoid competing
writes from obsolete actions. Late execution must preserve the shared phase grid rather
than extending it or replaying missed phases.

Completion evidence: virtual-time tests cover slow responses, phase-boundary crossings
during retries, pause/resume, and resumption after a long gap. Old-phase retries never
continue past the chosen expiry policy.

Relevant source: [coordination calculations](crates/tools-core/src/monitor/coordination.rs).

## Recommended next implementation increment

Start with cancellation and expiry in `polling`, then integrate orderly terminal-report
handling into the current GET/SET scenarios and orchestrator. Agree on cancellation
acknowledgement and counting rules before changing observable lifecycle behavior. Keep
this increment separate from prepared-action extraction and the first coordination scenario.

Do not introduce a universal periodic runner or an external scenario-plugin API before
repeating GET/SET and coordination demonstrate which behavior is actually shared. The
stable common boundary is action execution with attempts and reports; schedule and
behavioral decisions remain owned by each scenario.
