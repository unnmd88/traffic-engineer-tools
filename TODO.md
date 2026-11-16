# TODO

Working backlog for repository and project hygiene (no code-logic changes).

Decisions made:
- Public documentation is written in **English** (READMEs, `ARCHITECTURE.md`, crate
  metadata descriptions). Local/private notes may stay in Russian.
- `docs/` cleanup is a **separate iteration** (see the last section).

## First impression

- [ ] Add root `README.md`: what the project is, the three crates and their status
      (`tools-core` main library, `tctl` CLI, `net-monitor` frozen), build/run commands
      (`mise run check/test/clippy`), license, and "active development" status.
- [ ] Add `README.md` for `tctl` (`crates/tools-cli`) with real command examples
      (SCN conversion, `poll`).
- [ ] Add a one-liner `README.md` for `net-monitor`: frozen, superseded by `tools-core`.

## Documentation consistency

- [ ] Keep a single `ARCHITECTURE.md`; remove or mark `docs/ARCHITECTURE.md` as legacy
      (deferred — see last section).
- [ ] Fix intra-doc links once `TODO.md` and READMEs exist (re-check `ARCHITECTURE.md`).
- [ ] Translate public docs to English: root README, crate READMEs, `ARCHITECTURE.md`,
      and non-English comments in tracked tooling files (`.gitignore`, `.mise.toml`,
      `.cargo/config.toml`).
- [ ] Align crate `description` fields to English.

## Structure cleanup (separate iteration)

- [ ] Split `docs/` into public documentation vs private notes/dumps; move private files
      (`arch_new.txt`, `coordination_mode.txt`, `prompt.txt`, `обсудить.txt`, MIB dumps,
      `potok_backup_1.5.15/`, `monitor_dispatcher.png`, etc.) out of the repo or into a
      gitignored `notes/`.
- [ ] Consolidate `.gitignore`: one language for comments, gather personal excludes into
      one block.

## Metadata & tooling

- [ ] Add `repository = "https://github.com/unnmd88/traffic-engineer-tools"` to
      `[workspace.package]` and reference it from every crate.
- [ ] Align crate manifests: add `description` to `tctl`; add `license`, `[lints]
      workspace`, and `edition.workspace` to `net-monitor`.
- [ ] Fix `mise run` (`[tasks.run]`): use `cargo run -p tctl` (or set `default-members`);
      drop/fix the broken `example-traceroute` task (missing `test_traceroute` example).
- [ ] Add `--locked` to CI build steps; consider a doctest step.

## Nice-to-have

- [ ] Add `CHANGELOG.md`.
- [ ] Add `CONTRIBUTING.md` (incl. conventional-commit convention).
- [ ] Add `.github/PULL_REQUEST_TEMPLATE.md` and issue templates.
