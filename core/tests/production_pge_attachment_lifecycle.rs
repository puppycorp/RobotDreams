use std::path::Path;

use robotdreams_core::RobotDreams;

fn puppybot_project_path() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("../PuppyBot/robotdreams/project.json")
}

#[test]
fn production_pge_publishes_dynamic_attachment_and_release_immediately() {
    let mut dreams = RobotDreams::open(puppybot_project_path()).unwrap();
    // Commit an initial PGE frame first: this reproduces the stale-frame path
    // that hid a successful attachment from public state reads.
    dreams.advance_seconds(1.0 / 120.0);

    assert!(
        dreams
            .try_attach_scene_object_to_tcp("ball", "puppybot", 1.0, [0.0; 3])
            .unwrap()
    );
    assert_eq!(
        dreams
            .scene_object_state("ball")
            .unwrap()
            .attachment
            .as_ref()
            .map(|attachment| attachment.frame_name.as_str()),
        Some("tcp")
    );

    let attached_position = dreams.scene_object_state("ball").unwrap().position;
    dreams.set_joint_angle("yaw", 0.2).unwrap();
    assert_ne!(
        dreams.scene_object_state("ball").unwrap().position,
        attached_position
    );

    dreams.detach_scene_object("ball").unwrap();
    assert!(
        dreams
            .scene_object_state("ball")
            .unwrap()
            .attachment
            .is_none()
    );
    let release_z = dreams.scene_object_state("ball").unwrap().position[2];
    dreams.advance_seconds(1.0 / 120.0);
    let released = dreams.scene_object_state("ball").unwrap();
    assert!(released.position[2] < release_z);
    assert!(released.velocity_mps[2] < 0.0);
}
