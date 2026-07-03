# Examples

Examples show how to use RobotDreams with demo models, reusable assets, and scene projects.

Reusable RobotDreams demo robot data lives under project-specific example directories such as `examples/puppyarm/`. Product robot models and scenes should live in the owning product repository.

Scene examples live under `examples/scenes/<scene>/` and should reference robot assets by relative path from their `project.json`.

Current bundled demo:

```bash
cargo run -- examples/puppyarm/project.json
```

The canonical PuppyBot RobotDreams scene lives in the PuppyBot repository:

```bash
cargo run -p robotdreams -- open ../PuppyBot/robotdreams/project.json
```
