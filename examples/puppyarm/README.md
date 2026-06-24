# PuppyArm

This example owns the demo PuppyArm model at:

```text
examples/puppyarm/model/robotdreams.json
examples/puppyarm/model/final/urdf/final.urdf
```

Run the URDF viewer with auto-discovery:

```bash
cargo run -- urdf-view
```

Or point at the model explicitly:

```bash
cargo run -- urdf-view examples/puppyarm/model/final/urdf/final.urdf
```

This is not a Rust package. PuppyArm is treated as demo data loaded into RobotDreams, not as a separate app crate.
