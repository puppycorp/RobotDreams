# PuppyArm

This example owns a git-friendly RobotDreams project manifest and the demo PuppyArm model at:

```text
examples/puppyarm/project.json
examples/puppyarm/model/robotdreams.json
examples/puppyarm/model/final/urdf/final.urdf
```

Run the RobotDreams workbench with auto-discovery:

```bash
cargo run
```

Or point at the project/model explicitly:

```bash
cargo run -- examples/puppyarm/project.json
cargo run -- examples/puppyarm/model/robotdreams.json
cargo run -- examples/puppyarm/model/final/urdf/final.urdf
```

Inside a RobotDreams project folder, the preferred launch form is:

```bash
cargo run -- ./project.json
```

This is not a Rust package. PuppyArm is treated as demo data loaded into RobotDreams, not as a separate app crate.
