# EXEC = docker exec -it


# ?= - можно переопределить ✅
# = - нельзя переопределить ❌
# := - нельзя переопределить ❌

#.PHONY: app
#up:
#	${DC} -f ${APP_FILE} ${ENV} up --build -d

ENV_FILE ?= .env.dev
TARGET := x86_64-unknown-linux-gnu


# TEST SECTION

.PHONY: test
test:
	cargo test --target x86_64-unknown-linux-gnu -- --nocapture

.PHONY: test-core
test-core:
	cargo test --target x86_64-unknown-linux-gnu -p tools-core -- --nocapture

.PHONY: check
check:
	cargo check --target x86_64-unknown-linux-gnu

.PHONY: cargo-fix
cargo-fix:
	cargo fix --allow-dirty --target $(TARGET)

.PHONY: clippy
clippy:
	cargo clippy --target $(TARGET)

.PHONY: clippy-fix
clippy-fix:
	cargo clippy --fix --allow-dirty --target $(TARGET)




.PHONY: check-core
check-core:
	cargo check -p tools-core --target x86_64-unknown-linux-gnu

.PHONY: run
run:
	ENV_FILE=$(ENV_FILE) cargo run --target x86_64-unknown-linux-gnu

# BUILD RELEASE SECTION

.PHONY: cbr
cbr:
	cargo build --release --target x86_64-unknown-linux-gnu

.PHONY: cbr-win10
cbr-win10:
	cargo build --release --target x86_64-pc-windows-gnu

.PHONY: cbr-win7
cbr-win7:
	cargo +nightly build --release

.PHONY: cbr-net-monitor
cbr-net-monitor:
	cargo build --release -p net-monitor --target x86_64-unknown-linux-gnu

.PHONY: cbr-tctl
cbr-tctl:
	cargo build --release -p tctl --target x86_64-unknown-linux-gnu

.PHONY: cbr-win10-net-monitor
cbr-win10-net-monitor:
	cargo build --release -p net-monitor --target x86_64-pc-windows-gnu

.PHONY: cbr-win7-net-monitor
cbr-win7-net-monitor:
	cargo +nightly build --release -p net-monitor

.PHONY: objdump
objdump:
	objdump -p target/x86_64-win7-windows-msvc/release/traffic-api.exe | grep "DLL Name"

# EXAMPLES SECTION

.PHONY: example-traceroute
example-traceroute:
	cargo build --target x86_64-unknown-linux-gnu --example test_traceroute
	sudo setcap cap_net_raw+ep target/x86_64-unknown-linux-gnu/debug/examples/test_traceroute
	./target/x86_64-unknown-linux-gnu/debug/examples/test_traceroute

.PHONY: snmp-example
snmp-example:
	cargo run --target x86_64-unknown-linux-gnu --example snmp-example

.PHONY: scn-example
scn-example:
	cargo run --target x86_64-unknown-linux-gnu --example ascii-scn
