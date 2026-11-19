# TODO

Working backlog for repository hygiene and the public API contract.

Decisions made:
- Public documentation is written in **English** (READMEs, `ARCHITECTURE.md`, crate
  metadata descriptions). Local/private notes may stay in Russian.
- Private notes live in the gitignored `notes/` folder; public docs live at the repo root.
- crates.io publishing is not planned; crate metadata (`repository`, `description`,
  `license`) is intentionally out of scope for now.

## First impression

- [x] Add root `README.md`: what the project is, the three crates and their status
      (`tools-core` main library, `tctl` CLI, `net-monitor` frozen), build/run commands
      (`mise run check/test/clippy`), license, and "active development" status.
- [x] Add `README.md` for `tctl` (`crates/tools-cli`) with real command examples
      (SCN conversion, `poll`).
- [x] Add a one-liner `README.md` for `net-monitor`: frozen, superseded by `tools-core`.

## Documentation consistency

- [x] Keep a single `ARCHITECTURE.md`; remove the `docs/ARCHITECTURE.md` duplicate.
- [x] Fix intra-doc links once `TODO.md` and READMEs exist (re-check `ARCHITECTURE.md`).
- [x] Translate `ARCHITECTURE.md` to English.

## Structure cleanup

- [x] Split `docs/` into public documentation vs private notes; move private files into a
      gitignored `notes/` and `cross-build-win7.md` to the repo root.
- [x] Consolidate `.gitignore`: one language for comments, gather personal excludes into
      one block.

## Metadata & tooling

- [x] Fix `mise run` (`[tasks.run]`): use `cargo run -p tctl`; drop the broken
      `example-traceroute` task (missing `test_traceroute` example).
- [x] Add `--locked` to CI build steps.

## Nice-to-have

- [x] Add `CHANGELOG.md`.
- [ ] Add `CONTRIBUTING.md` (incl. conventional-commit convention).
- [ ] Add `.github/PULL_REQUEST_TEMPLATE.md` and issue templates.

## Public API (consumer-facing contract)

Goal: an unknown consumer can use `tools-core` from `cargo doc` + examples alone.
Each item is a small, checkable step, ordered by consumer impact.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the current monitor contract. Items that require
a design decision remain proposals rather than accepted API changes.

### Ergonomics (most visible)

- [x] Flatten scenario construction: `ScenarioSpec::snmp_get` / `snmp_set`.
- [x] Own schedule validation: `PollSchedule::fixed_delay` / `fixed_rate` returning `Result`.
- [x] Canonical consumer example `examples/monitor.rs` + doctest on `Application`.
- [ ] Unify `Scn` trim rule: text constructors (`from_str`, `from_dotted`, `from_scn_index`,
      `from_utc_index`) trim whitespace; `from_bytes` stays exact. Today `from_dotted` does not trim.
- [ ] Remove dead `ScnError::InvalidPrefix` (declared and mentioned in docs, never returned).
- [ ] Add `Serialize`/`Deserialize` for `Scn`.
- [ ] Replace positional magic numbers with named/builder args: `TaskConfig` (`limit`,
      `deep_history`) and `AttemptConfig` (`timeout`, `retries`, `retry_delay`).
- [ ] Simplify observation to one obvious path (snapshot + events); resolve the `Application`
      vs `ApplicationHandle` two-phase asymmetry of `subscribe_with_snapshot`.
- [ ] Wrap `broadcast::Receiver` in a library-owned subscription type (hide `Lagged`/re-sync).
- [ ] Add a flat accessor for a task's last result (avoid `view.result.outcome` → enum →
      `Response` → payload nesting for the common "did it succeed / what value" case).
- [ ] Decide and show the error story: `tools_core::Error` umbrella vs `anyhow` in the example.
- [ ] Make `RestartPolicy` configurable; give `AppConfig` a builder or private fields.

### Forward compatibility

- [ ] `#[non_exhaustive]` on `ScenarioSpec`, `TaskStatus`, and root `error::Error`.
- [ ] Decide `#[non_exhaustive]` on `Response<T>` and `OutcomeStatus`.

### Internal leaks / dead surface

- [ ] `pub(crate)` on `Health::{record, record_error, clear_error}` and `ExecutionMetrics::record`.
- [ ] Decide `Pollable`: public extension point (drop `async_trait`, document) or seal it.
- [ ] Remove or internalize orphan `polling::Metrics` (produced/consumed nowhere).
- [ ] Move `DT_FMT` / `DT_FMT_WITH_MICROSECONDS` from core to `tctl` (presentation).
- [ ] Unify `Duration`: `RestartPolicy` uses `std::time::Duration`, the rest `tokio::time::Duration`.
- [ ] Make `TaskId` opaque like `ApplicationId` (or document the transparency deliberately).
- [ ] Drop `Clone` from the error tree (removes the `Arc<JoinError>` workaround); drop
      `PartialEq`/`Eq` on errors where unnecessary.

### Documentation & guarantees

- [ ] Drive `missing_docs` from 553 to 0 (doc + doctest on public value types).
- [ ] `static_assertions` for `Send + Sync + Unpin + 'static` on `ApplicationHandle`,
      `MonitorSubscription`, `RunningApplication`.
- [ ] `cargo semver-checks` in CI (or run manually before a release).
