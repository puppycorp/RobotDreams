use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::{
    Action, Condition, Pose, Scenario, ScenarioRuntime, ScenarioStatus, ScenarioStep,
    ScenarioTickReport, ScenarioWorld, Vec3, Volume, VolumeShape, volume_contains,
    world::PressureSensorState,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum ScenarioProgress {
    SeekingBall,
    Grasped,
    Carrying,
    Released,
    PressureDetected,
    Complete,
    Failed,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ScenarioObjectReport {
    pub(crate) position: Vec3,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) attached_to: Option<String>,
    #[serde(default)]
    pub(crate) inside_volumes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ScenarioStateReport {
    pub(crate) scenario_id: String,
    pub(crate) status: ScenarioStatus,
    pub(crate) progress: ScenarioProgress,
    pub(crate) success: bool,
    pub(crate) triggered_steps: Vec<String>,
    pub(crate) objects: HashMap<String, ScenarioObjectReport>,
    pub(crate) sensors: HashMap<String, PressureSensorState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) failure_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ScenarioSmokeReport {
    pub(crate) state: ScenarioStateReport,
    pub(crate) scenario_id: String,
    pub(crate) status: ScenarioStatus,
    pub(crate) success: bool,
    pub(crate) triggered_steps: Vec<String>,
    pub(crate) tick_reports: Vec<ScenarioTickReport>,
    pub(crate) sensors: HashMap<String, PressureSensorState>,
    pub(crate) world: ScenarioWorld,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BallToBinObservation {
    #[serde(default)]
    pub(crate) tool_position: Option<Vec3>,
    #[serde(default)]
    pub(crate) ball_position: Option<Vec3>,
    #[serde(default)]
    pub(crate) gripper_tick: Option<i16>,
    #[serde(default, alias = "attached")]
    pub(crate) ball_attached: Option<bool>,
    #[serde(default, alias = "pressed")]
    pub(crate) bin_pressure_pressed: Option<bool>,
    #[serde(default, alias = "pressure")]
    pub(crate) bin_pressure: Option<f32>,
}

#[derive(Debug, Clone)]
pub(crate) struct BallToBinScenarioSession {
    scenario: Scenario,
    initial_world: ScenarioWorld,
    runtime: ScenarioRuntime,
    world: ScenarioWorld,
}

impl BallToBinScenarioSession {
    pub(crate) fn new() -> Self {
        Self::with_world(ball_to_bin_scenario(), ball_to_bin_world())
    }

    pub(crate) fn from_scenario(scenario: Scenario) -> Self {
        Self::with_world(scenario, ScenarioWorld::default())
    }

    pub(crate) fn with_world(scenario: Scenario, world: ScenarioWorld) -> Self {
        let runtime = ScenarioRuntime::new(scenario.clone());
        Self {
            scenario,
            initial_world: world.clone(),
            runtime,
            world,
        }
    }

    pub(crate) fn reset(&mut self) -> ScenarioStateReport {
        self.runtime = ScenarioRuntime::new(self.scenario.clone());
        self.world = self.initial_world.clone();
        self.state()
    }

    pub(crate) fn state(&self) -> ScenarioStateReport {
        evaluate_ball_to_bin_state(&self.scenario, &self.runtime, &self.world)
    }

    pub(crate) fn observe(
        &mut self,
        observation: BallToBinObservation,
    ) -> Result<ScenarioStateReport, String> {
        if let Some(position) = observation.tool_position {
            self.world.set_frame_pose("tool", Pose::new(position));
        }
        if let Some(position) = observation.ball_position {
            self.world.set_object_pose("ball", Pose::new(position));
        }
        if let Some(gripper_tick) = observation.gripper_tick {
            self.world.set_servo_position(4, gripper_tick);
        }
        if let Some(attached) = observation.ball_attached {
            if attached {
                self.world.attach_object("ball", "tool".to_string())?;
            } else {
                self.world.detach_object("ball")?;
            }
        }
        if observation.bin_pressure_pressed.is_some() || observation.bin_pressure.is_some() {
            let pressure = observation.bin_pressure.unwrap_or_else(|| {
                if observation.bin_pressure_pressed.unwrap_or(false) {
                    1.0
                } else {
                    0.0
                }
            });
            let pressed = observation.bin_pressure_pressed.unwrap_or(pressure >= 0.5);
            self.world
                .set_pressure_sensor("bin_pressure", PressureSensorState::new(pressed, pressure));
        }

        self.runtime.tick(&mut self.world, 0.016);
        Ok(self.state())
    }
}

pub(crate) fn load_scenario_json(path: &std::path::Path) -> Result<Scenario, String> {
    let raw = std::fs::read_to_string(path)
        .map_err(|err| format!("failed to read scenario {}: {err}", path.display()))?;
    serde_json::from_str(&raw)
        .map_err(|err| format!("failed to parse scenario {}: {err}", path.display()))
}

impl Default for BallToBinScenarioSession {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) fn ball_to_bin_scenario() -> Scenario {
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
                    Action::SetPressureSensor {
                        sensor: "bin_pressure".to_string(),
                        pressed: true,
                        pressure: 1.0,
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
                Condition::PressureSensorPressed {
                    sensor: "bin_pressure".to_string(),
                },
            ],
        },
    }
}

pub(crate) fn ball_to_bin_world() -> ScenarioWorld {
    let mut world = ScenarioWorld::default();
    world.set_frame_pose("tool", Pose::new([-0.18, 0.055, -0.34]));
    world.set_object_pose("ball", Pose::new([-0.18, 0.055, -0.34]));
    world.set_servo_position(4, 2048);
    world.set_pressure_sensor("bin_pressure", PressureSensorState::new(false, 0.0));
    world
}

fn sorted_triggered_steps(runtime: &ScenarioRuntime) -> Vec<String> {
    let mut triggered_steps = runtime
        .triggered_steps()
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    triggered_steps.sort();
    triggered_steps
}

fn object_reports(
    scenario: &Scenario,
    world: &ScenarioWorld,
) -> HashMap<String, ScenarioObjectReport> {
    world
        .objects
        .iter()
        .map(|(id, object)| {
            let inside_volumes = scenario
                .volumes
                .iter()
                .filter(|volume| volume_contains(volume, object.pose.position))
                .map(|volume| volume.id.clone())
                .collect::<Vec<_>>();
            (
                id.clone(),
                ScenarioObjectReport {
                    position: object.pose.position,
                    attached_to: object
                        .attached_to
                        .as_ref()
                        .map(|attachment| attachment.frame.clone()),
                    inside_volumes,
                },
            )
        })
        .collect()
}

fn scenario_progress(
    scenario: &Scenario,
    runtime: &ScenarioRuntime,
    world: &ScenarioWorld,
) -> ScenarioProgress {
    if runtime.status() == &ScenarioStatus::Failed {
        return ScenarioProgress::Failed;
    }
    if runtime.status() == &ScenarioStatus::Complete {
        return ScenarioProgress::Complete;
    }
    if world
        .pressure_sensor("bin_pressure")
        .map(|sensor| sensor.pressed)
        .unwrap_or(false)
    {
        return ScenarioProgress::PressureDetected;
    }
    if world
        .object_pose("ball")
        .map(|pose| {
            scenario
                .volumes
                .iter()
                .find(|volume| volume.id == "bin_success_volume")
                .map(|volume| volume_contains(volume, pose.position))
                .unwrap_or(false)
        })
        .unwrap_or(false)
    {
        return ScenarioProgress::Released;
    }
    if world.object_attached_to("ball").is_some()
        && world
            .frame_pose("tool")
            .map(|pose| {
                scenario
                    .volumes
                    .iter()
                    .find(|volume| volume.id == "bin_drop_zone")
                    .map(|volume| volume_contains(volume, pose.position))
                    .unwrap_or(false)
            })
            .unwrap_or(false)
    {
        return ScenarioProgress::Carrying;
    }
    if world.object_attached_to("ball").is_some() {
        return ScenarioProgress::Grasped;
    }
    ScenarioProgress::SeekingBall
}

pub(crate) fn evaluate_ball_to_bin_state(
    scenario: &Scenario,
    runtime: &ScenarioRuntime,
    world: &ScenarioWorld,
) -> ScenarioStateReport {
    let status = runtime.status().clone();
    ScenarioStateReport {
        scenario_id: scenario.id.clone(),
        status: status.clone(),
        progress: scenario_progress(scenario, runtime, world),
        success: status == ScenarioStatus::Complete,
        triggered_steps: sorted_triggered_steps(runtime),
        objects: object_reports(scenario, world),
        sensors: world.pressure_sensors.clone(),
        failure_reason: runtime.failure_reason().map(ToString::to_string),
    }
}

pub(crate) fn run_ball_to_bin_smoke() -> ScenarioSmokeReport {
    let scenario = ball_to_bin_scenario();
    let scenario_id = scenario.id.clone();
    let mut runtime = ScenarioRuntime::new(scenario);
    let mut world = ball_to_bin_world();
    let mut tick_reports = Vec::new();

    tick_reports.push(runtime.tick(&mut world, 0.016));
    world.set_servo_position(4, 2700);
    tick_reports.push(runtime.tick(&mut world, 0.016));
    world.set_frame_pose("tool", Pose::new([0.38, 0.24, -0.28]));
    tick_reports.push(runtime.tick(&mut world, 0.016));
    world.set_servo_position(4, 2000);
    tick_reports.push(runtime.tick(&mut world, 0.016));

    let state = evaluate_ball_to_bin_state(runtime.scenario(), &runtime, &world);
    let triggered_steps = state.triggered_steps.clone();
    let status = runtime.status().clone();
    let success = status == ScenarioStatus::Complete;

    ScenarioSmokeReport {
        state,
        scenario_id,
        status,
        success,
        triggered_steps,
        tick_reports,
        sensors: world.pressure_sensors.clone(),
        world,
    }
}

pub(crate) fn run_ball_to_bin_smoke_for_scenario(scenario: Scenario) -> ScenarioSmokeReport {
    let scenario_id = scenario.id.clone();
    let mut session = BallToBinScenarioSession::from_scenario(scenario);

    let _ = session.observe(BallToBinObservation {
        tool_position: Some([-0.18, 0.055, -0.34]),
        ball_position: Some([-0.18, 0.055, -0.34]),
        gripper_tick: Some(2700),
        ..BallToBinObservation::default()
    });
    let _ = session.observe(BallToBinObservation {
        tool_position: Some([0.38, 0.24, -0.28]),
        gripper_tick: Some(2700),
        ..BallToBinObservation::default()
    });
    let _ = session.observe(BallToBinObservation {
        tool_position: Some([0.38, 0.24, -0.28]),
        gripper_tick: Some(2000),
        ..BallToBinObservation::default()
    });

    let state = session.state();
    let status = state.status.clone();
    let success = status == ScenarioStatus::Complete;

    ScenarioSmokeReport {
        scenario_id,
        status,
        success,
        triggered_steps: state.triggered_steps.clone(),
        tick_reports: Vec::new(),
        sensors: state.sensors.clone(),
        world: session.world.clone(),
        state,
    }
}

pub(crate) fn run_ball_to_bin_state() -> ScenarioStateReport {
    run_ball_to_bin_smoke().state
}
