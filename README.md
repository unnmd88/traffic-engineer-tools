# traffic-engineer-tools

Tooling for road-traffic management (traffic control systems): a Rust workspace with a
reusable core library and ready-to-use applications on top.

> **Status:** active development — moving from prototype to production. The public API
> may still change.

## What it is

The domain is road-traffic organization (ОДД): devices on the street-road network —
traffic controllers, detectors, switches, and other peripherals — speaking different
protocols (SNMP, HTTP, Modbus, …).

The main product is the `tools-core` library: small building blocks plus ready-made
solutions (SCN calculations, a network *Monitor* that polls and manages devices by
user-defined scenarios, log parsers, …). Default applications are built on top of it:
the `tctl` CLI and (planned) a web app.

## Workspace

| Crate | Path | Purpose |
|---|---|---|
| `tools-core` | `crates/tools-core` | Reusable core library, no UI. The main product. |
| `tctl` | `crates/tools-cli` | CLI: real-time monitor and other ready-to-use cases. |
| `net-monitor` | `crates/net-monitor` | Standalone network-device testing service. **Frozen** — its functionality is being merged into `tools-core`. |

## Getting started

Prerequisites: a recent stable Rust toolchain (the workspace uses edition 2024) and,
optionally, [mise](https://mise.jdx.dev/) for the task shortcuts.

```sh
mise run check    # compile the whole workspace
mise run test     # run all tests
mise run clippy   # lint
```

Or plain cargo:

```sh
cargo check --workspace
cargo test --workspace
cargo clippy --workspace
```

## CLI: `tctl`

Build and run:

```sh
cargo run -p tctl -- <command>
```

Commands:

```sh
# Convert an ASCII SCN string to its UTC index
cargo run -p tctl -- to-scn CO101

# Convert a UTC index back to SCN
cargo run -p tctl -- from-scn .1.3.67.79.49.48.49

# Run the monitor over a poll configuration (see crates/tools-cli/examples/poll.yaml)
cargo run -p tctl -- poll --config crates/tools-cli/examples/poll.yaml
```

The SCN commands accept `--output human|json|pretty-json`; `poll` accepts
`--view classic|compact`.

## Library: `tools-core`

Build on the building blocks. Quick SCN example:

```rust
use tools_core::Scn;

let scn = Scn::from_str("ABC").unwrap();
assert_eq!(scn.to_dotted(), "65.66.67");
assert_eq!(scn.to_utc_index(), ".1.3.65.66.67");
```

See the crate docs (`cargo doc -p tools-core --open`) and
[`crates/tools-core/README.md`](crates/tools-core/README.md) for more.

## Documentation

- [`ARCHITECTURE.md`](ARCHITECTURE.md) — component map and responsibilities.
- [`docs/cross-build-win7.md`](docs/cross-build-win7.md) — cross-compiling for
  Windows 7 / Server 2008 R2.
- [`TODO.md`](TODO.md) — current backlog.

## License

[MIT](LICENSE).
