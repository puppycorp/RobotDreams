use std::fs;
use std::path::{Path, PathBuf};

fn rust_sources(root: &Path) -> Vec<PathBuf> {
    fn visit(path: &Path, sources: &mut Vec<PathBuf>) {
        for entry in fs::read_dir(path).expect("read RobotDreams source directory") {
            let path = entry.expect("read RobotDreams source entry").path();
            if path.is_dir() {
                visit(&path, sources);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                sources.push(path);
            }
        }
    }

    let mut sources = Vec::new();
    visit(root, &mut sources);
    sources.sort();
    sources
}

#[test]
fn direct_solver_dependency_and_imports_are_test_only() {
    let core_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let manifest = fs::read_to_string(core_root.join("Cargo.toml")).expect("read core manifest");
    let (production, dev) = manifest
        .split_once("[dev-dependencies]")
        .expect("core manifest has a dev dependency section");
    assert!(
        !production
            .lines()
            .any(|line| line.trim_start().starts_with("rapier3d")),
        "Rapier must not be a RobotDreams production dependency"
    );
    assert!(
        dev.lines()
            .any(|line| line.trim_start().starts_with("rapier3d")),
        "the test oracle must declare its direct solver as a dev dependency"
    );

    let oracle = core_root.join("src/scene_physics.rs");
    let mut forbidden = Vec::new();
    for source in rust_sources(&core_root.join("src")) {
        if source == oracle {
            continue;
        }
        let contents = fs::read_to_string(&source).expect("read RobotDreams Rust source");
        if contents.contains("rapier3d") {
            forbidden.push(
                source
                    .strip_prefix(core_root)
                    .expect("source is below core root")
                    .display()
                    .to_string(),
            );
        }
    }
    assert!(
        forbidden.is_empty(),
        "direct solver imports escaped the test-only oracle: {forbidden:#?}"
    );

    let crate_root = fs::read_to_string(core_root.join("src/lib.rs")).expect("read crate root");
    assert!(
        crate_root.contains("#[cfg(test)]\nmod scene_physics;"),
        "the direct solver oracle module must remain cfg(test)"
    );
}
