# Examples

Examples show how to use RobotDreams with demo models, reusable assets, and scene projects.

Reusable robot data lives under `examples/assets/robots/<robot>/`. Each robot asset should include a `robotdreams.json` model profile and exported model files such as URDF, meshes, and launch metadata.

Scene examples live under `examples/scenes/<scene>/` and should reference robot assets by relative path from their `project.json`.

Current primary scene:

```bash
cargo run -- examples/scenes/puppybot-bin-ball/project.json
```

The crate-root `project.json` is a convenience copy of that PuppyBot bin-and-ball scene.
