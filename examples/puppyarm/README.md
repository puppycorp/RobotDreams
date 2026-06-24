# PuppyArm

This example owns the demo PuppyArm model at:

```text
examples/puppyarm/model/robotdreams.json
examples/puppyarm/model/final/urdf/final.urdf
```

Run the RobotDreams workbench with auto-discovery:

```bash
cargo run
```

Or point at the example project/model explicitly:

```bash
cargo run -- examples/puppyarm/robotdreams.example.json
cargo run -- examples/puppyarm/model/robotdreams.json
cargo run -- examples/puppyarm/model/final/urdf/final.urdf
```

This is not a Rust package. PuppyArm is treated as demo data loaded into RobotDreams, not as a separate app crate.
