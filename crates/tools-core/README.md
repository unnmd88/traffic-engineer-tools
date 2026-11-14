# tools-core

Библиотека для задач организации дорожного движения (ОДД).

## Готовые фичи

- [`scn`] — конвертация SCN (ASCII ↔ dotted ↔ scn-index ↔ utc-index);
- [`monitor`] — опрос и управление ДК по сценариям.

## Usage

### Конвертация SCN

```rust
use tools_core::Scn;

let s = Scn::from_str("ABC").unwrap();
assert_eq!(s.to_dotted(), "65.66.67");
assert_eq!(s.to_utc_index(), ".1.3.65.66.67");

let parsed = Scn::from_utc_index(".1.3.65.66.67").unwrap();
assert_eq!(parsed, s);
```

### Монитор

```rust
use tools_core::monitor::{AppConfig, Application};

let app = Application::new(AppConfig::default(), vec![]);
assert!(app.task_ids().is_empty());
```

Полный асинхронный пример запуска — `examples/monitor.rs`.
