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

Use `robots[].jointNames` in `project.json` to give imported URDF joints readable
names without editing the URDF. The project follows Puppybot's logical servo
order: `1=yaw`, `2=shoulder`, `3=elbow`, and `4=wrist`; these map to generated
URDF joints `revolute_2_1`, `revolute_1_2`, `revolute_1_3`, and
`revolute_1_1`.

This is not a Rust package. PuppyArm is treated as demo data loaded into RobotDreams, not as a separate app crate.
