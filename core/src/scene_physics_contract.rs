//! Solver-neutral RobotDreams scene-physics semantics.
//!
//! This module owns product state shapes and deterministic policy only. It
//! must not import a physics backend; both the production PGE adapter and the
//! test-only direct Rapier oracle consume these contracts.

use pge_core as pge;

use crate::calibration::{
    CalibrationRecord, MeasurementProvenance, apply_calibration_to_vehicle_profile,
    load_calibration,
};
use crate::project::{
    ProjectConfig, ProjectSceneColliderGeometry, ProjectSceneTriggerConfig,
    ProjectVehicleColliderConfig, ProjectVehiclePhysicsConfig,
};

/// Loads the reviewed generated vehicle-collider interchange. This is project
/// authoring I/O, not a physics-backend operation, so both PGE authority and
/// the test oracle consume the same accepted geometry.
pub(crate) fn generated_vehicle_colliders(
    project: &ProjectConfig,
    vehicle: &ProjectVehiclePhysicsConfig,
) -> Result<Vec<ProjectVehicleColliderConfig>, Box<dyn std::error::Error>> {
    let Some(profile) = &vehicle.collision_profile else {
        return Ok(Vec::new());
    };
    let path = project.base_dir.join(profile);
    let source = std::fs::read_to_string(&path).map_err(|error| {
        format!(
            "vehicle collision profile '{}' could not be read: {error}",
            path.display()
        )
    })?;
    let value: serde_json::Value = serde_json::from_str(&source).map_err(|error| {
        format!(
            "vehicle collision profile '{}' is not valid JSON: {error}",
            path.display()
        )
    })?;
    let colliders = if let Some(colliders) =
        value.get("colliders").and_then(serde_json::Value::as_array)
    {
        colliders.clone()
    } else if let Some(parts) = value
        .get("compounds")
        .and_then(serde_json::Value::as_array)
        .and_then(|compounds| compounds.first())
        .and_then(|compound| compound.get("parts"))
        .and_then(serde_json::Value::as_array)
    {
        parts.iter().map(|part| serde_json::json!({
            "shape": "box", "size": part.get("size").cloned().unwrap_or(serde_json::Value::Null),
            "offset": part.get("center").cloned().unwrap_or(serde_json::Value::Null),
        })).collect()
    } else {
        return Err(format!("vehicle collision profile '{}' must contain reviewed colliders or PGE compounds[0].parts", path.display()).into());
    };
    colliders
        .iter()
        .map(|collider| {
            let shape = collider
                .get("shape")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| "collider has no string 'shape'".to_string())?;
            let numbers = |name: &str, count: usize| -> Result<Vec<f32>, String> {
                let values = collider
                    .get(name)
                    .and_then(serde_json::Value::as_array)
                    .ok_or_else(|| format!("collider '{shape}' has no '{name}' array"))?;
                if values.len() != count {
                    return Err(format!(
                        "collider '{shape}' '{name}' must have {count} entries"
                    ));
                }
                values
                    .iter()
                    .map(|value| {
                        value
                            .as_f64()
                            .filter(|value| value.is_finite())
                            .map(|value| value as f32)
                            .ok_or_else(|| format!("collider '{shape}' '{name}' must be finite"))
                    })
                    .collect()
            };
            let geometry = match shape {
                "box" => {
                    let size = numbers("size", 3)?;
                    ProjectSceneColliderGeometry::Box {
                        size: [size[0], size[1], size[2]],
                    }
                }
                "sphere" => ProjectSceneColliderGeometry::Sphere {
                    radius: collider
                        .get("radius")
                        .and_then(serde_json::Value::as_f64)
                        .filter(|value| value.is_finite() && *value > 0.0)
                        .ok_or_else(|| "sphere collider needs positive 'radius'".to_string())?
                        as f32,
                },
                "cylinder" => ProjectSceneColliderGeometry::Cylinder {
                    radius: collider
                        .get("radius")
                        .and_then(serde_json::Value::as_f64)
                        .filter(|value| value.is_finite() && *value > 0.0)
                        .ok_or_else(|| "cylinder collider needs positive 'radius'".to_string())?
                        as f32,
                    height: collider
                        .get("height")
                        .and_then(serde_json::Value::as_f64)
                        .filter(|value| value.is_finite() && *value > 0.0)
                        .ok_or_else(|| "cylinder collider needs positive 'height'".to_string())?
                        as f32,
                },
                _ => return Err(format!("unsupported vehicle collider shape '{shape}'")),
            };
            let offset = collider
                .get("offset")
                .map(|_| numbers("offset", 3))
                .transpose()?
                .unwrap_or_else(|| vec![0.0; 3]);
            let rotation = collider
                .get("rotation")
                .map(|_| numbers("rotation", 3))
                .transpose()?
                .unwrap_or_else(|| vec![0.0; 3]);
            Ok(ProjectVehicleColliderConfig {
                geometry,
                offset: [offset[0], offset[1], offset[2]],
                rotation: [rotation[0], rotation[1], rotation[2]],
            })
        })
        .collect::<Result<Vec<_>, String>>()
        .map_err(Into::into)
}

/// Loads the optional project-level measurement artifact once for all vehicle
/// profiles. This is RobotDreams authoring semantics, not solver behavior.
pub(crate) fn load_project_calibration(
    project: &ProjectConfig,
) -> Result<Option<CalibrationRecord>, Box<dyn std::error::Error>> {
    project
        .calibration_record_path
        .as_ref()
        .map(|path| {
            load_calibration(project.base_dir.join(path)).map_err(|error| {
                format!("project calibrationRecord '{path}' could not be loaded: {error}")
            })
        })
        .transpose()
        .map_err(Into::into)
}

/// Applies the optional measured artifact to one authored vehicle profile and
/// returns the explicit publication record that must accompany the solved
/// physics state. Both solver implementations consume this exact mapping.
pub(crate) fn calibrated_vehicle_profile(
    robot_id: &str,
    authored_vehicle: &ProjectVehiclePhysicsConfig,
    calibration: Option<&CalibrationRecord>,
) -> Result<
    (ProjectVehiclePhysicsConfig, Option<AppliedCalibrationState>),
    Box<dyn std::error::Error>,
> {
    let calibrated = calibration
        .map(|record| apply_calibration_to_vehicle_profile(authored_vehicle, record))
        .transpose()
        .map_err(|error| format!("project calibrationRecord is invalid: {error}"))?;
    let applied = calibrated
        .as_ref()
        .map(|calibrated| AppliedCalibrationState {
            robot_id: robot_id.to_string(),
            hardware_revision: calibrated.hardware_revision.clone(),
            provenance: calibrated.provenance.clone(),
        });
    Ok((
        calibrated
            .map(|calibrated| calibrated.vehicle)
            .unwrap_or_else(|| authored_vehicle.clone()),
        applied,
    ))
}

#[derive(Clone, Debug, PartialEq)]
pub struct SceneObjectAttachment {
    pub robot_id: String,
    pub frame_name: String,
    pub offset_m: [f32; 3],
    pub rotation_offset_rpy: [f32; 3],
}

#[derive(Clone, Debug, PartialEq)]
pub struct SceneObjectState {
    pub id: String,
    pub position: [f32; 3],
    pub rotation: [f32; 3],
    pub velocity_mps: [f32; 3],
    pub angular_velocity_rps: [f32; 3],
    pub dynamic: bool,
    pub attachment: Option<SceneObjectAttachment>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SceneTriggerState {
    pub id: String,
    pub object_id: String,
    pub inside: bool,
    pub entered: bool,
    pub entered_at_sec: Option<f64>,
    pub entry_count: u64,
    pub settled: bool,
    pub triggered: bool,
    pub triggered_at_sec: Option<f64>,
    pub settled_time_sec: f32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VehiclePhysicsState {
    pub robot_id: String,
    pub position: [f32; 3],
    pub rotation: [f32; 3],
    pub velocity_mps: [f32; 3],
    pub angular_velocity_rps: [f32; 3],
    pub actuator: VehicleActuatorState,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AppliedCalibrationState {
    pub robot_id: String,
    pub hardware_revision: String,
    pub provenance: MeasurementProvenance,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VehicleActuatorState {
    pub left_command: f32,
    pub right_command: f32,
    pub left_drive_force_n: f32,
    pub right_drive_force_n: f32,
    pub braking: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RobotLinkContact {
    pub robot_id: String,
    pub link_name: String,
    pub object_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct KinematicColliderMotionConfig {
    pub maximum_linear_speed_mps: f32,
    pub maximum_angular_speed_rps: f32,
    pub maximum_substep_seconds: f32,
}

impl Default for KinematicColliderMotionConfig {
    fn default() -> Self {
        Self {
            maximum_linear_speed_mps: 0.20,
            maximum_angular_speed_rps: 2.0,
            maximum_substep_seconds: 1.0 / 120.0,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct VehicleDriveCommand {
    pub left_command: f32,
    pub right_command: f32,
    pub brake: bool,
    pub steering_target_rad: f32,
}

/// Solver-neutral vehicle state sampled immediately before a force plan is
/// produced. Grouping these co-evolving values keeps both solver adapters on
/// one semantic input contract.
#[derive(Clone, Copy, Debug)]
pub(crate) struct VehiclePhysicsKinematics {
    pub position: [f32; 3],
    pub yaw: f32,
    pub velocity_mps: [f32; 3],
    pub yaw_rate_rps: f32,
    pub dt: f32,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum VehiclePhysicsForceCommand {
    AtPoint {
        force_n: [f32; 3],
        point_world_m: [f32; 3],
    },
    AtCenter {
        force_n: [f32; 3],
    },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct VehiclePhysicsForcePlan {
    pub steering_angle_rad: f32,
    pub forces: Vec<VehiclePhysicsForceCommand>,
    pub actuator: VehicleActuatorState,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct LivePhysicsColliderDebugPart {
    pub geometry: ProjectSceneColliderGeometry,
    pub local_transform: pge::Transform,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct LivePhysicsColliderDebugEntry {
    pub id: String,
    pub category: &'static str,
    pub color: [f32; 4],
    pub transform: pge::Transform,
    pub parts: Vec<LivePhysicsColliderDebugPart>,
}

pub(crate) fn pge_transform(translation: [f32; 3], rotation: [f32; 3]) -> pge::Transform {
    pge::Transform {
        translation,
        rotation,
        rotation_matrix: None,
    }
}

pub(crate) fn plan_vehicle_forces(
    config: &ProjectVehiclePhysicsConfig,
    current_steering_angle_rad: f32,
    command: VehicleDriveCommand,
    kinematics: VehiclePhysicsKinematics,
) -> VehiclePhysicsForcePlan {
    let VehiclePhysicsKinematics {
        position,
        yaw,
        velocity_mps,
        yaw_rate_rps,
        dt,
    } = kinematics;
    let response = config.steering_response_deg_per_sec.to_radians() * dt;
    let steering_error = command.steering_target_rad - current_steering_angle_rad;
    let steering_angle_rad = current_steering_angle_rad + steering_error.clamp(-response, response);
    let forward = [yaw.cos(), yaw.sin(), 0.0];
    let lateral = [-yaw.sin(), yaw.cos(), 0.0];
    let dot = |left: [f32; 3], right: [f32; 3]| {
        left[0] * right[0] + left[1] * right[1] + left[2] * right[2]
    };
    let current_forward_speed = dot(velocity_mps, forward);
    let current_lateral_speed = dot(velocity_mps, lateral);
    let left_command = command.left_command.clamp(-1.0, 1.0);
    let right_command = command.right_command.clamp(-1.0, 1.0);
    let motor = &config.motor;
    let motor_angular_speed = current_forward_speed / motor.wheel_radius_m * motor.gear_ratio;
    let no_load_rad_s = motor.no_load_rpm * std::f32::consts::TAU / 60.0;
    let wheel_force = |command: f32| {
        if command.abs() <= 1.0e-4 {
            return 0.0;
        }
        let torque = motor.stall_torque_nm
            * (command - motor_angular_speed / no_load_rad_s).clamp(-1.0, 1.0);
        (torque * motor.gear_ratio / motor.wheel_radius_m)
            .clamp(-config.max_drive_force_n, config.max_drive_force_n)
    };
    let left_force = wheel_force(left_command);
    let right_force = wheel_force(right_command);
    let add_scaled = |origin: [f32; 3],
                      first: [f32; 3],
                      first_scale: f32,
                      second: [f32; 3],
                      second_scale: f32| {
        std::array::from_fn(|axis| {
            origin[axis] + first[axis] * first_scale + second[axis] * second_scale
        })
    };
    let rear_offset = -config.wheelbase_m * 0.5;
    let half_track = config.track_width_m * 0.5;
    let mut forces = vec![
        VehiclePhysicsForceCommand::AtPoint {
            force_n: forward.map(|component| component * left_force),
            point_world_m: add_scaled(position, forward, rear_offset, lateral, half_track),
        },
        VehiclePhysicsForceCommand::AtPoint {
            force_n: forward.map(|component| component * right_force),
            point_world_m: add_scaled(position, forward, rear_offset, lateral, -half_track),
        },
    ];
    let axle_offset = config.wheelbase_m * 0.5;
    let bicycle_steering_angle_rad = -steering_angle_rad;
    let front_slip_mps = current_lateral_speed + axle_offset * yaw_rate_rps
        - current_forward_speed * bicycle_steering_angle_rad.tan();
    let rear_slip_mps = current_lateral_speed - axle_offset * yaw_rate_rps;
    let axle_stiffness = config.lateral_grip_n_per_mps * 0.5;
    let front_force_n = (-axle_stiffness * front_slip_mps)
        .clamp(-config.max_drive_force_n, config.max_drive_force_n);
    let rear_force_n = (-axle_stiffness * rear_slip_mps)
        .clamp(-config.max_drive_force_n, config.max_drive_force_n);
    forces.push(VehiclePhysicsForceCommand::AtPoint {
        force_n: lateral.map(|component| component * front_force_n),
        point_world_m: add_scaled(position, forward, axle_offset, lateral, 0.0),
    });
    forces.push(VehiclePhysicsForceCommand::AtPoint {
        force_n: lateral.map(|component| component * rear_force_n),
        point_world_m: add_scaled(position, forward, -axle_offset, lateral, 0.0),
    });
    let resistance = if command.brake {
        motor.brake_torque_nm * motor.gear_ratio / motor.wheel_radius_m
    } else {
        motor.rolling_resistance_n
    };
    if current_forward_speed.abs() > 1.0e-4 {
        forces.push(VehiclePhysicsForceCommand::AtCenter {
            force_n: forward
                .map(|component| -component * resistance * current_forward_speed.signum()),
        });
    }
    VehiclePhysicsForcePlan {
        steering_angle_rad,
        forces,
        actuator: VehicleActuatorState {
            left_command,
            right_command,
            left_drive_force_n: left_force,
            right_drive_force_n: right_force,
            braking: command.brake,
        },
    }
}

pub(crate) fn apply_scene_trigger_rule(
    state: &mut SceneTriggerState,
    config: &ProjectSceneTriggerConfig,
    object: &SceneObjectState,
    dt: f32,
    clock_sec: f64,
) {
    let inside = object.dynamic
        && object.attachment.is_none()
        && (0..3).all(|axis| {
            (object.position[axis] - config.position[axis]).abs() <= config.size[axis] * 0.5
        });
    if inside && !state.inside {
        state.entered = true;
        state.entered_at_sec.get_or_insert(clock_sec);
        state.entry_count += 1;
    }
    state.inside = inside;
    let speed = object
        .velocity_mps
        .iter()
        .map(|component| component * component)
        .sum::<f32>()
        .sqrt();
    if inside && speed <= config.settle_speed_mps {
        state.settled_time_sec += dt;
        if state.settled_time_sec >= config.settle_time_sec {
            state.settled = true;
            state.triggered = true;
            state.triggered_at_sec.get_or_insert(clock_sec);
        }
    } else {
        state.settled_time_sec = 0.0;
        state.settled = false;
    }
}
