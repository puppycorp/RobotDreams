use super::{
    Action, BallToBinObservation, BallToBinScenarioSession, Condition, Pose, Scenario,
    ScenarioProgress, ScenarioRuntime, ScenarioStatus, ScenarioStep, ScenarioWorld,
    ball_to_bin_scenario, ball_to_bin_world, evaluate_ball_to_bin_state, run_ball_to_bin_smoke,
    world::PressureSensorState,
};

#[test]
fn attaches_object_when_frame_is_near_and_servo_is_closed() {
    let scenario = ball_to_bin_scenario();
    let mut runtime = ScenarioRuntime::new(scenario);
    let mut world = ball_to_bin_world();

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
    let mut world = ball_to_bin_world();

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
    let mut world = ball_to_bin_world();

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
    assert_eq!(
        world.pressure_sensor("bin_pressure"),
        Some(PressureSensorState::new(true, 1.0))
    );
}

#[test]
fn once_steps_do_not_repeat_after_triggering() {
    let scenario = ball_to_bin_scenario();
    let mut runtime = ScenarioRuntime::new(scenario);
    let mut world = ball_to_bin_world();

    world.set_servo_position(4, 2700);
    let first = runtime.tick(&mut world, 0.016);
    let second = runtime.tick(&mut world, 0.016);

    assert_eq!(first.events.len(), 1);
    assert!(second.events.is_empty());
    assert!(runtime.triggered_steps().contains("attach-ball"));
}

#[test]
fn smoke_report_completes_ball_to_bin_scenario() {
    let report = run_ball_to_bin_smoke();

    assert!(report.success);
    assert_eq!(report.status, ScenarioStatus::Complete);
    assert_eq!(report.state.progress, ScenarioProgress::Complete);
    assert_eq!(
        report.triggered_steps,
        vec!["attach-ball".to_string(), "release-ball".to_string()]
    );
    assert_eq!(
        report.world.object_pose("ball").map(|pose| pose.position),
        Some([0.38, 0.08, -0.28])
    );
    assert_eq!(
        report.sensors.get("bin_pressure"),
        Some(&PressureSensorState::new(true, 1.0))
    );
}

#[test]
fn scenario_json_accepts_camel_case_distance_field() {
    let raw = r#"{
        "id": "camel-case-distance",
        "name": "Camel Case Distance",
        "robotId": "puppyarm",
        "steps": [
            {
                "id": "attach",
                "when": {
                    "type": "frameNearObject",
                    "frame": "tool",
                    "object": "ball",
                    "distanceM": 0.03
                }
            }
        ],
        "success": { "type": "always" }
    }"#;

    let scenario: Scenario = serde_json::from_str(raw).expect("scenario json");

    assert!(matches!(
        scenario.steps[0].when,
        Condition::FrameNearObject {
            distance_m,
            ..
        } if distance_m == 0.03
    ));
}

#[test]
fn state_report_tracks_ball_to_bin_progress() {
    let scenario = ball_to_bin_scenario();
    let mut runtime = ScenarioRuntime::new(scenario.clone());
    let mut world = ball_to_bin_world();

    let initial = evaluate_ball_to_bin_state(&scenario, &runtime, &world);
    assert_eq!(initial.progress, ScenarioProgress::SeekingBall);

    world.set_servo_position(4, 2700);
    runtime.tick(&mut world, 0.016);
    let grasped = evaluate_ball_to_bin_state(&scenario, &runtime, &world);
    assert_eq!(grasped.progress, ScenarioProgress::Grasped);
    assert_eq!(
        grasped
            .objects
            .get("ball")
            .and_then(|object| object.attached_to.as_deref()),
        Some("tool")
    );

    world.set_frame_pose("tool", Pose::new([0.38, 0.24, -0.28]));
    runtime.tick(&mut world, 0.016);
    let carrying = evaluate_ball_to_bin_state(&scenario, &runtime, &world);
    assert_eq!(carrying.progress, ScenarioProgress::Carrying);

    world.set_servo_position(4, 2000);
    runtime.tick(&mut world, 0.016);
    let complete = evaluate_ball_to_bin_state(&scenario, &runtime, &world);
    assert_eq!(complete.progress, ScenarioProgress::Complete);
    assert_eq!(complete.status, ScenarioStatus::Complete);
    assert_eq!(
        complete.sensors.get("bin_pressure"),
        Some(&PressureSensorState::new(true, 1.0))
    );
}

#[test]
fn session_observations_advance_ball_to_bin_progress() {
    let mut session = BallToBinScenarioSession::new();

    assert_eq!(session.state().progress, ScenarioProgress::SeekingBall);

    let grasped = session
        .observe(BallToBinObservation {
            tool_position: Some([-0.18, 0.055, -0.34]),
            gripper_tick: Some(2700),
            ..BallToBinObservation::default()
        })
        .expect("grasp observation");
    assert_eq!(grasped.progress, ScenarioProgress::Grasped);

    let carrying = session
        .observe(BallToBinObservation {
            tool_position: Some([0.38, 0.24, -0.28]),
            gripper_tick: Some(2700),
            ..BallToBinObservation::default()
        })
        .expect("carry observation");
    assert_eq!(carrying.progress, ScenarioProgress::Carrying);

    let complete = session
        .observe(BallToBinObservation {
            tool_position: Some([0.38, 0.24, -0.28]),
            gripper_tick: Some(2000),
            ..BallToBinObservation::default()
        })
        .expect("release observation");
    assert_eq!(complete.progress, ScenarioProgress::Complete);
    assert_eq!(complete.status, ScenarioStatus::Complete);
}

#[test]
fn pressure_sensor_conditions_reflect_world_state() {
    let scenario = Scenario {
        id: "pressure-sensor".to_string(),
        name: "Pressure Sensor".to_string(),
        robot_id: "puppyarm".to_string(),
        frames: Vec::new(),
        volumes: Vec::new(),
        steps: Vec::new(),
        success: Condition::All {
            conditions: vec![
                Condition::PressureSensorPressed {
                    sensor: "bin_pressure".to_string(),
                },
                Condition::PressureSensorAtOrAbove {
                    sensor: "bin_pressure".to_string(),
                    pressure: 0.5,
                },
            ],
        },
    };
    let mut runtime = ScenarioRuntime::new(scenario);
    let mut world = ScenarioWorld::default();
    world.set_pressure_sensor("bin_pressure", PressureSensorState::new(true, 0.75));

    let report = runtime.tick(&mut world, 0.016);

    assert_eq!(report.status, ScenarioStatus::Complete);
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
