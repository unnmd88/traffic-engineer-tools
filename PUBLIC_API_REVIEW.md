# tools-core public API review

Consumer-facing review of the public types, implementations, examples, and
`ARCHITECTURE.md`. These recommendations describe proposed improvements, not implemented
behavior or settled design decisions. The implementation backlog lives in [TODO.md](TODO.md).

## Overall assessment

The API has a useful foundation, but consumers still need too much knowledge of the
library's internals to reach their first useful result, especially when using the monitor.

Preserve the following strengths:

- `Application`, `RunningApplication`, and `ApplicationHandle` separate preparation,
  execution ownership, and cloneable access to commands.
- Configuration is validated before network execution.
- Identifiers are typed and errors belong to their modules.
- A snapshot and its event subscription can be obtained at one consistent boundary.
- SNMP results retain the raw value alongside the decoded business value.

## Recommendations, ordered by consumer impact

### 1. Execute prepared SNMP requests independently of the monitor

`SnmpGetSpec` and `SnmpSetSpec` are public descriptions, but consumers cannot execute them
through a public API with profile-aware OID completion, SCN discovery, and business-value
decoding. `SnmpClient` exposes network operations, while preparation and decoding remain
inside the monitor implementation.

Provide a public describe → prepare → execute path, allowing a prepared request to be
executed repeatedly. The monitor should reuse that mechanism. This supports one-off
probes, consumer-owned loops, and integration tests without requiring a monitor lifecycle.

Relevant sources: [request specs](crates/tools-core/src/snmp/request.rs),
[SNMP client](crates/tools-core/src/snmp/client.rs), and
[GET scenario](crates/tools-core/src/monitor/scenario/impls/repeat_snmp_get.rs).

### 2. Preserve decoding failures in the result

The GET implementation currently replaces a parser error with
`BusinessValue::Text("parse error")`; the request still receives `OutcomeStatus::Succeeded`.
A consumer cannot reliably distinguish that failure from a legitimate text value.

Represent three states explicitly: no parser, successful decoding, and failed decoding.
Keep the error details alongside `raw_value`. Consider using the existing `Degraded`
status for a partially useful response, with its exact meaning decided as part of the
result contract.

Relevant sources: [GET decoding](crates/tools-core/src/monitor/scenario/impls/repeat_snmp_get.rs),
[sample](crates/tools-core/src/snmp/response.rs), and
[outcome status](crates/tools-core/src/monitor/scenario/execution.rs).

### 3. Reduce boilerplate and replace positional magic numbers

A basic poll requires assembling a request, schedule, attempt configuration, and task.
The separation is useful, but a call such as
`TaskConfig::try_new("sysDescr".to_string(), scenario, 3, 3)` hides the meaning of its numbers.

Provide named configuration for optional settings, such as execution limit and history
depth, and accept names through `impl Into<String>`. Consider an explicit unlimited value
or `Option<NonZeroU64>` instead of the special value `0`. Rename `deep_history` to
`history_depth`. Choose defaults deliberately when designing the convenience API.

Relevant sources: [task configuration](crates/tools-core/src/monitor/task/config.rs),
[attempt configuration](crates/tools-core/src/polling/config.rs), and
[consumer example](crates/tools-core/examples/monitor.rs).

### 4. Explain command acknowledgements and support waiting for completion

Successful `start_task().await` confirms initiation, not successful scenario preparation.
Pause is also applied asynchronously. Returning `Result<(), _>` does not explain when a
consumer can rely on the requested state.

Document the acknowledgement boundary for every command in rustdoc. Consider helpers
that wait for readiness or completion of a particular execution generation, so consumers
do not each implement event filtering and race handling.

Relevant source: [application handle](crates/tools-core/src/monitor/runtime/app.rs).

### 5. Demonstrate subscription recovery

`subscribe_with_snapshot()` provides a useful consistent observation boundary. However,
the canonical example uses `while let Ok(...)`, which silently ends observation on
`Lagged` just as it does on channel closure.

Handle lag separately in the example and obtain a fresh consistent snapshot and
subscription to recover. A library-owned receiver wrapper can follow if justified by
consumer experience; a correct example is useful immediately.

Relevant sources: [consumer example](crates/tools-core/examples/monitor.rs) and
[subscription](crates/tools-core/src/monitor/runtime/subscription.rs).

### 6. Make restart behavior configurable

`AppConfig` accepts a `RestartPolicy`, but the policy has private fields and no public
configuration constructor. Consumers can only obtain the default policy.

Support disabling restarts and configuring delays; consider a restart-attempt limit.
A finite probe with a persistent preparation failure can otherwise keep restarting
without reaching its execution limit.

Relevant sources: [restart policy](crates/tools-core/src/monitor/runtime/restart_policy.rs)
and [task configuration](crates/tools-core/src/monitor/task/config.rs).

### 7. State the extension boundaries explicitly

Consumers can supply numeric OIDs and custom GET parsers. However, profile metadata and
alias catalogs are not publicly discoverable, and the monitor accepts only the built-in
`ScenarioSpec` variants.

Expose a read-only catalog of profile capabilities. Before designing a scenario extension
contract, decide whether consumers must be able to supply implementations from another
crate or whether new implementations belong inside `tools-core`. Avoid introducing a
universal trait before that requirement is settled.

Relevant sources: [profiles](crates/tools-core/src/snmp/profiles.rs) and
[scenario specifications](crates/tools-core/src/monitor/scenario/spec.rs).

### 8. Put consumer guarantees in rustdoc and useful examples

`ARCHITECTURE.md` explains behavior that consumers need directly at API entry points:
the first request waits a full interval, the execution limit includes `NoResponse`, local
history stores previous results separately from the latest result, and shutdown can wait
for an unread history channel.

Document these guarantees next to the relevant methods and types. Replace the README's
empty-monitor introduction with a minimal useful poll that obtains a result, followed by
examples of task management and subscription recovery.

Relevant sources: [crate README](crates/tools-core/README.md),
[architecture](ARCHITECTURE.md), and [consumer example](crates/tools-core/examples/monitor.rs).

## Suggested first increment

Start with explicit decoding failures, independent execution of prepared requests, and a
complete consumer example. These changes provide immediate value and let real consumer
code guide the next API decisions.

## Design references

- [Rust API Guidelines: expressive argument types](https://rust-lang.github.io/api-guidelines/type-safety.html#arguments-convey-meaning-through-types-not-bool-or-option-c-custom-type).
- [Rust API Guidelines: documentation and examples](https://rust-lang.github.io/api-guidelines/documentation.html).
