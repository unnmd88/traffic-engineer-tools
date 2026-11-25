# tctl

CLI for `tools-core`: a real-time network monitor and other ready-to-use cases.

## Build

```sh
cargo build -p tctl
```

The binary is produced at `target/debug/tctl`. You can also run it without an explicit
build step:

```sh
cargo run -p tctl -- <command>
```

## Commands

### `poll` — run the monitor

Polls devices according to a YAML configuration of tasks.

```sh
tctl poll --config examples/poll.yaml
tctl poll --config examples/poll.yaml --view compact
```

`--view` selects the dashboard: `classic` (default) or `compact`.

Each task is a repeating query (e.g. an SNMP GET) with an interval, attempt policy, and
target. A `history_sink` path lets the CLI record monitor events as JSONL. If its writer
lags behind the event stream, polling continues and the CLI reports an incomplete journal
when it exits. The optional root setting `ordered_queue_capacity` sets the number
of command and scenario report slots, with a default of 4096. See
[`examples/poll.yaml`](examples/poll.yaml) for a minimal configuration.

### `to-scn` / `from-scn` — SCN conversion

`to-scn` converts an ASCII SCN string to its UTC index; `from-scn` parses a UTC index
back to SCN.

```sh
tctl to-scn CO101
tctl from-scn .1.3.67.79.49.48.49
```

Both accept `--output human|json|pretty-json` (default `human`).

## License

MIT — see the [workspace license](../../LICENSE).
