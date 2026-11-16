# TODO

Working backlog for repository and project hygiene (no code-logic changes).

Decisions made:
- Public documentation is written in **English** (READMEs, `ARCHITECTURE.md`, crate
  metadata descriptions). Local/private notes may stay in Russian.
- Private notes live in the gitignored `notes/` folder; public docs live at the repo root.

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
- [ ] Align crate `description` fields to English.

## Structure cleanup

- [x] Split `docs/` into public documentation vs private notes; move private files into a
      gitignored `notes/` and `cross-build-win7.md` to the repo root.
- [x] Consolidate `.gitignore`: one language for comments, gather personal excludes into
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
