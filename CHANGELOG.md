# Changelog

All notable changes to this project are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). The project is
pre-1.0 and in active development: entries summarize user-facing changes, while many
internal refactors are not listed individually.

## [Unreleased]

### Added
- Root `README.md` and per-crate READMEs (`tools-core`, `tctl`, `net-monitor`).
- `CHANGELOG.md`.

### Changed
- `ARCHITECTURE.md` rewritten in English and synced with the current code (scenario
  `impls/` module, mise tasks).
- Public documentation consolidated at the repo root; private notes moved to a gitignored
  `notes/` folder.
- CI build steps now use `--locked` for reproducible builds.

### Fixed
- `mise run run` now runs the `tctl` binary (`cargo run -p tctl`) instead of failing to
  pick a package.
- Removed the broken `example-traceroute` mise task (missing example) and the unused
  `ENV_FILE` variable from the mise config.

## [0.2.0] - 2026-11-11

### Added
- `tools-core` monitor: a reusable network monitor with typed scenarios (repeating SNMP
  GET/SET), fixed-rate/fixed-delay schedules, orchestrator, restart policy, health and
  execution metrics, snapshots, events, and subscriptions.
- `tools-core` SNMP: device profiles and an OID registry with OID resolution, SET support,
  and parsers for Potok UG405, SWARCO, and STCIP.
- `tools-core` core utilities: SCN conversion (`Scn`), `potok_log` parsing, and coordination
  domain types (`Phase`, `Cycle`, `Epoch`).
- `tctl`: poll dashboard (`classic`/`compact`), `to-scn`/`from-scn` commands, JSONL history
  output, and file logging.
