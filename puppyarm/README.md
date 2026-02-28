# puppyarm

Small CLI example that uses `robot_utils` as a client library to connect to a Feetech/ST servo bus.

## Build

```bash
cargo build -p puppyarm
```

## Usage

Start WGUI control panel:

```bash
cargo run -p puppyarm ui
```

That uses `0.0.0.0:12348`.
- If a serial port is detected, it auto-selects it.
- If no serial port is detected, open the UI and click `Start Virtual Bus`.

Then open the WGUI client and connect to `ws://<host>:12348`.

To force a specific port:

```bash
cargo run -p puppyarm -- --port /dev/ttyUSB0 ui --bind 0.0.0.0:12348
```

PuppyArm is hardcoded to 4 servos (IDs `1..4`).

Scan fixed IDs 1..4:

```bash
cargo run -p puppyarm -- --port /dev/ttyUSB0 --baud 1000000 scan
```

Read one servo:

```bash
cargo run -p puppyarm -- --port /dev/ttyUSB0 read --id 1
```

Move one servo and read back state:

```bash
cargo run -p puppyarm -- --port /dev/ttyUSB0 move --id 1 --position 2048 --speed 400
```
