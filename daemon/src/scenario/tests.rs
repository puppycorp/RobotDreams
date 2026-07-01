use super::{
    Action, Condition, Pose, Scenario, ScenarioRuntime, ScenarioStatus, ScenarioStep,
    ScenarioWorld, Volume, VolumeShape,
};

fn ball_to_bin_scenario() -> Scenario {
    Scenario {
        id: "puppybot-ball-to-bin".to_string(),
        name: "PuppyBot Ball To Bin".to_string(),
        robot_id: "puppyarm".to_string(),
        frames: Vec::new(),
        volumes: vec![
            Volume {
                id: "bin_drop_zone".to_string(),
                shape: VolumeShape::Box {
                    center: [0.38, 0.24, -0.28],
                    size: [0.28, 0.24, 0.28],
                },
            },
            Volume {
                id: "bin_success_volume".to_string(),
                shape: VolumeShape::Box {
                    center: [0.38, 0.08, -0.28],
                    size: [0.24, 0.20, 0.24],
                },
            },
        ],
        steps: vec![
            ScenarioStep {
                id: "attach-ball".to_string(),
                when: Condition::All {
                    conditions: vec![
                        Condition::FrameNearObject {
                            frame: "tool".to_string(),
                            object: "ball".to_string(),
                            distance_m: 0.03,
                        },
                        Condition::ServoAtOrPast {
                            id: 4,
                            position: 2600,
                        },
                    ],
                },
                actions: vec![Action::AttachObject {
                    object: "ball".to_string(),
                    frame: "tool".to_string(),
                }],
                once: true,
            },
            ScenarioStep {
                id: "release-ball".to_string(),
                when: Condition::All {
                    conditions: vec![
                        Condition::ObjectAttached {
                            object: "ball".to_string(),
                        },
                        Condition::FrameInsideVolume {
                            frame: "tool".to_string(),
                            volume: "bin_drop_zone".to_string(),
                        },
                        Condition::ServoAtOrBefore {
                            id: 4,
                            position: 2100,
                        },
                    ],
                },
                actions: vec![
                    Action::DetachObject {
                        object: "ball".to_string(),
                    },
                    Action::MoveObjectToVolumeCenter {
                        object: "ball".to_string(),
                        volume: "bin_success_volume".to_string(),
                    },
                ],
                once: true,
            },
        ],
        success: Condition::All {
            conditions: vec![
                Condition::ObjectDetached {
                    object: "ball".to_string(),
                },
                Condition::ObjectInsideVolume {
                    object: "ball".to_string(),
                    volume: "bin_success_volume".to_string(),
                },
            ],
        },
    }
}

fn world_with_ball() -> ScenarioWorld {
    let mut world = ScenarioWorld::default();
    world.set_frame_pose("tool", Pose::new([-0.18, 0.055, -0.34]));
    world.set_object_pose("ball", Pose::new([-0.18, 0.055, -0.34]));
    world.set_servo_position(4, 2048);
    world
}

#[test]
fn attaches_object_when_frame_is_near_and_servo_is_closed() {
    let scenario = ball_to_bin_scenario();
    let mut runtime = ScenarioRuntime::new(scenario);
    let mut world = world_with_ball();

    let report = runtime.tick(&mut world, 0.016);
    assert_eq!(report.status, ScenarioStatus::Running);
    assert!(report.events.is_empty());
    assert_eq!(world.object_attached_to("ball"), None);

    world.set_servo_position(4, 2700);
    let report = runtime.tick(&mut world, 0.016);
    assert_eq!(report.status, ScenarioStatus::Running);
    assert_eq!(report.events[0].step_id, "attach-ball");
    assert_eq!(world.object_attached_to("ball"), Some("tool"));
}

#[test]
fn attached_object_tracks_frame_pose() {
    let scenario = ball_to_bin_scenario();
    let mut runtime = ScenarioRuntime::new(scenario);
    let mut world = world_with_ball();

    world.set_servo_position(4, 2700);
    runtime.tick(&mut world, 0.016);
    world.set_frame_pose("tool", Pose::new([0.38, 0.24, -0.28]));
    runtime.tick(&mut world, 0.016);

    assert_eq!(
        world.object_pose("ball").map(|pose| pose.position),
        Some([0.38, 0.24, -0.28])
    );
}

#[test]
fn release_moves_object_to_success_volume_and_completes() {
    let scenario = ball_to_bin_scenario();
    let mut runtime = ScenarioRuntime::new(scenario);
    let mut world = world_with_ball();

    world.set_servo_position(4, 2700);
    runtime.tick(&mut world, 0.016);
    world.set_frame_pose("tool", Pose::new([0.38, 0.24, -0.28]));
    world.set_servo_position(4, 2000);

    let report = runtime.tick(&mut world, 0.016);
    assert_eq!(report.status, ScenarioStatus::Complete);
    assert_eq!(world.object_attached_to("ball"), None);
    assert_eq!(
        world.object_pose("ball").map(|pose| pose.position),
        Some([0.38, 0.08, -0.28])
    );
}

#[test]
fn once_steps_do_not_repeat_after_triggering() {
    let scenario = ball_to_bin_scenario();
    let mut runtime = ScenarioRuntime::new(scenario);
    let mut world = world_with_ball();

    world.set_servo_position(4, 2700);
    let first = runtime.tick(&mut world, 0.016);
    let second = runtime.tick(&mut world, 0.016);

    assert_eq!(first.events.len(), 1);
    assert!(second.events.is_empty());
    assert!(runtime.triggered_steps().contains("attach-ball"));
}

#[test]
fn missing_action_target_fails_scenario() {
    let scenario = Scenario {
        id: "bad-scenario".to_string(),
        name: "Bad Scenario".to_string(),
        robot_id: "puppyarm".to_string(),
        frames: Vec::new(),
        volumes: Vec::new(),
        steps: vec![ScenarioStep {
            id: "missing-object".to_string(),
            when: Condition::Always,
            actions: vec![Action::AttachObject {
                object: "missing".to_string(),
                frame: "tool".to_string(),
            }],
            once: true,
        }],
        success: Condition::Always,
    };
    let mut runtime = ScenarioRuntime::new(scenario);
    let mut world = ScenarioWorld::default();

    let report = runtime.tick(&mut world, 0.016);

    assert_eq!(report.status, ScenarioStatus::Failed);
    assert_eq!(
        report.failure_reason,
        Some("unknown object missing".to_string())
    );
}
