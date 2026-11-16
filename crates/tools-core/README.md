# tools-core

Library for road-traffic management (ОДД) tasks.

## Ready-made features

- [`scn`] — SCN conversion (ASCII ↔ dotted ↔ scn-index ↔ utc-index);
- [`monitor`] — polling and controlling traffic controllers via scenarios.

## Usage

### SCN conversion

```rust
use tools_core::Scn;

let s = Scn::from_str("ABC").unwrap();
assert_eq!(s.to_dotted(), "65.66.67");
assert_eq!(s.to_utc_index(), ".1.3.65.66.67");

let parsed = Scn::from_utc_index(".1.3.65.66.67").unwrap();
assert_eq!(parsed, s);
```

### Monitor

```rust
use tools_core::monitor::{AppConfig, Application};

let app = Application::new(AppConfig::default(), vec![]);
assert!(app.task_ids().is_empty());
```

A full async launch example — `examples/monitor.rs`.
