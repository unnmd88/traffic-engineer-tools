# Кросс-сборка под Windows 7 / Server 2008 R2

Таргет `x86_64-win7-windows-msvc` — встроенный в rustc (tier 3). Его `std` не
распространяется через rustup, поэтому собирается из исходников через
`build-std` (нужен nightly + компонент `rust-src`).

## Требования

- nightly toolchain и компонент `rust-src`:
  `rustup toolchain install nightly --component rust-src`
- MSVC-линкер `lld-link` (на Arch — пакет `clang`).
- SDK Windows, распакованный через `xwin`. По умолчанию скрипт линковки ищет его в
  `~/winsdk`; путь переопределяется переменной окружения `XWIN_SDK`.

## Сборка

```sh
mise run cbr-win7
```

Результат: `target/x86_64-win7-windows-msvc/release/…`.

Сборка запускается из корня проекта (относительный путь к `scripts/xwin-lld-link.sh`).
