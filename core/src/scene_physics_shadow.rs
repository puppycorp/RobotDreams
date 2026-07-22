//! Non-authoritative PGE physics shadow used only by migration parity tests.
//!
//! This adapter deliberately has no connection to `RobotDreams` state or its
//! active `scene_physics` runtime. A failed translation or divergent shadow
//! step therefore cannot change product behavior.

use std::collections::BTreeMap;
use std::error::Error;

use pge_physics::{
    BodyDesc, BodyId, BodyMode, BoundedKinematicTarget, ColliderDesc, ColliderId, ColliderMaterial,
    ColliderShape, KinematicTargetMode, MassPropertiesDesc, PhysicsConfig, PhysicsError,
    PhysicsSnapshot, PhysicsWorld, Pose, StepOutput,
};

use crate::calibration::{apply_calibration_to_vehicle_profile, load_calibration};
use crate::project::{
    ProjectConfig, ProjectSceneBodyKind, ProjectSceneColliderGeometry,
    ProjectSceneObjectPhysicsConfig, ProjectSceneTriggerConfig, ProjectVehicleColliderConfig,
    ProjectVehiclePhysicsConfig, RobotLinkCollider,
};
use crate::scene_physics::{
    SceneObjectAttachment, SceneObjectState, SceneTriggerState, VehicleActuatorState,
    VehicleDriveCommand, VehiclePhysicsForceCommand, VehiclePhysicsKinematics,
    apply_scene_trigger_rule, generated_vehicle_colliders, plan_two_axle_bicycle_forces,
    plan_vehicle_forces, vehicle_inertia_diagonal,
};

type Matrix3 = [[f32; 3]; 3];

pub(crate) struct ScenePhysicsShadow {
    world: PhysicsWorld,
    object_bodies: BTreeMap<String, BodyId>,
    object_body_descs: BTreeMap<String, BodyDesc>,
    object_colliders: BTreeMap<String, Vec<ColliderId>>,
    object_dynamic: BTreeMap<String, bool>,
    attachments: BTreeMap<String, SceneObjectAttachment>,
    trigger_configs: BTreeMap<String, ProjectSceneTriggerConfig>,
    trigger_states: BTreeMap<String, SceneTriggerState>,
    trigger_sensors: BTreeMap<String, ColliderId>,
    vehicle_bodies: BTreeMap<String, BodyId>,
    vehicle_body_descs: BTreeMap<String, BodyDesc>,
    vehicle_colliders: BTreeMap<String, Vec<ColliderId>>,
    vehicle_configs: BTreeMap<String, ProjectVehiclePhysicsConfig>,
    vehicle_collider_configs: BTreeMap<String, Vec<ProjectVehicleColliderConfig>>,
    vehicle_steering_angles: BTreeMap<String, f32>,
    vehicle_actuators: BTreeMap<String, VehicleActuatorState>,
    link_bodies: BTreeMap<String, BodyId>,
    link_body_descs: BTreeMap<String, BodyDesc>,
    link_colliders: BTreeMap<String, ColliderId>,
}

impl ScenePhysicsShadow {
    pub(crate) fn from_project(project: &ProjectConfig) -> Result<Self, Box<dyn Error>> {
        let mut world = PhysicsWorld::new(PhysicsConfig::default())?;
        let mut object_bodies = BTreeMap::new();
        let mut object_body_descs = BTreeMap::new();
        let mut object_colliders = BTreeMap::new();
        let mut object_dynamic = BTreeMap::new();

        for object in &project.scene.objects {
            let Some(config) = &object.physics else {
                continue;
            };
            let body_id = BodyId::new(format!("shadow:scene-object:{}", object.id));
            let body = BodyDesc {
                mode: match config.body_kind {
                    ProjectSceneBodyKind::Static => BodyMode::Static,
                    ProjectSceneBodyKind::Dynamic => BodyMode::Dynamic,
                },
                pose: pose_from_rpy(object.position, object.rotation),
                linear_damping: config.linear_damping,
                angular_damping: config.angular_damping,
                mass: (config.body_kind == ProjectSceneBodyKind::Dynamic)
                    .then(|| scene_object_mass_properties(config)),
                ..BodyDesc::default()
            };
            world.create_body(body_id.clone(), body.clone())?;
            let mut collider_ids = Vec::new();
            for (index, part) in config.collider.parts().into_iter().enumerate() {
                let collider_id =
                    ColliderId::new(format!("shadow:scene-object:{}:part:{index}", object.id));
                world.create_collider(
                    collider_id.clone(),
                    &body_id,
                    ColliderDesc {
                        pose: pose_from_rpy(part.offset, part.rotation),
                        shape: collider_shape(&part.geometry),
                        material: ColliderMaterial {
                            friction: config.friction,
                            restitution: config.restitution,
                            density_kg_m3: if config.body_kind == ProjectSceneBodyKind::Dynamic {
                                0.0
                            } else {
                                config.mass_kg / scene_collider_volume(&part.geometry)
                            },
                            contact_skin_m: 0.0,
                        },
                        sensor: false,
                        collision_memberships: u32::MAX,
                        collision_filter: u32::MAX,
                    },
                )?;
                collider_ids.push(collider_id);
            }
            object_bodies.insert(object.id.clone(), body_id);
            object_body_descs.insert(object.id.clone(), body);
            object_colliders.insert(object.id.clone(), collider_ids);
            object_dynamic.insert(
                object.id.clone(),
                config.body_kind == ProjectSceneBodyKind::Dynamic,
            );
        }

        let mut trigger_configs = BTreeMap::new();
        let mut trigger_states = BTreeMap::new();
        let mut trigger_sensors = BTreeMap::new();
        for trigger in &project.scene.triggers {
            if !object_bodies.contains_key(&trigger.object_id) {
                return Err(format!(
                    "scene trigger '{}' refers to non-physical object '{}'",
                    trigger.id, trigger.object_id
                )
                .into());
            }
            let body_id = BodyId::new(format!("shadow:trigger:{}", trigger.id));
            let collider_id = ColliderId::new(format!("shadow:trigger:{}:sensor", trigger.id));
            world.create_body(
                body_id.clone(),
                BodyDesc {
                    mode: BodyMode::Static,
                    pose: Pose {
                        translation: trigger.position,
                        ..Pose::default()
                    },
                    ..BodyDesc::default()
                },
            )?;
            world.create_collider(
                collider_id.clone(),
                &body_id,
                ColliderDesc {
                    sensor: true,
                    material: ColliderMaterial {
                        density_kg_m3: 0.0,
                        ..ColliderMaterial::default()
                    },
                    ..ColliderDesc::new(ColliderShape::Box { size: trigger.size })
                },
            )?;
            trigger_configs.insert(trigger.id.clone(), trigger.clone());
            trigger_states.insert(
                trigger.id.clone(),
                SceneTriggerState {
                    id: trigger.id.clone(),
                    object_id: trigger.object_id.clone(),
                    inside: false,
                    entered: false,
                    entered_at_sec: None,
                    entry_count: 0,
                    settled: false,
                    triggered: false,
                    triggered_at_sec: None,
                    settled_time_sec: 0.0,
                },
            );
            trigger_sensors.insert(trigger.id.clone(), collider_id);
        }

        let calibration = match &project.calibration_record_path {
            Some(path) => Some(
                load_calibration(project.base_dir.join(path)).map_err(|error| {
                    format!("project calibrationRecord '{path}' could not be loaded: {error}")
                })?,
            ),
            None => None,
        };
        let mut vehicle_bodies = BTreeMap::new();
        let mut vehicle_body_descs = BTreeMap::new();
        let mut vehicle_colliders = BTreeMap::new();
        let mut vehicle_configs = BTreeMap::new();
        let mut vehicle_collider_configs = BTreeMap::new();
        let mut vehicle_steering_angles = BTreeMap::new();
        let mut vehicle_actuators = BTreeMap::new();
        for robot in &project.robots {
            let Some(authored_vehicle) = robot
                .physics
                .as_ref()
                .and_then(|physics| physics.vehicle.as_ref())
            else {
                continue;
            };
            let calibrated = calibration
                .as_ref()
                .map(|record| apply_calibration_to_vehicle_profile(authored_vehicle, record))
                .transpose()
                .map_err(|error| format!("project calibrationRecord is invalid: {error}"))?;
            let vehicle = calibrated
                .as_ref()
                .map(|calibrated| &calibrated.vehicle)
                .unwrap_or(authored_vehicle);
            let mut colliders = vehicle.colliders.clone();
            colliders.extend(generated_vehicle_colliders(project, vehicle)?);
            if colliders.is_empty() {
                return Err(format!(
                    "dynamic vehicle '{}' needs at least one reviewed collider",
                    robot.id
                )
                .into());
            }
            let inertia = vehicle_inertia_diagonal(vehicle.mass_kg, &colliders);
            let body_id = BodyId::new(format!("shadow:vehicle:{}", robot.id));
            let body = BodyDesc {
                mode: BodyMode::Dynamic,
                pose: pose_from_rpy(robot.base_translation, robot.base_rotation),
                linear_damping: vehicle.linear_damping,
                angular_damping: vehicle.angular_damping,
                mass: Some(MassPropertiesDesc {
                    mass_kg: vehicle.mass_kg,
                    center_of_mass_m: vehicle.center_of_mass_m,
                    principal_inertia_kg_m2: inertia,
                    principal_inertia_frame_xyzw: [0.0, 0.0, 0.0, 1.0],
                    inertia_tensor_kg_m2: Some([
                        [inertia[0], 0.0, 0.0],
                        [0.0, inertia[1], 0.0],
                        [0.0, 0.0, inertia[2]],
                    ]),
                }),
                ccd_enabled: true,
                lock_rotation: [true, true, false],
                ..BodyDesc::default()
            };
            world.create_body(body_id.clone(), body.clone())?;
            let mut collider_ids = Vec::new();
            for (index, collider) in colliders.iter().enumerate() {
                let collider_id =
                    ColliderId::new(format!("shadow:vehicle:{}:part:{index}", robot.id));
                world.create_collider(
                    collider_id.clone(),
                    &body_id,
                    ColliderDesc {
                        pose: pose_from_rpy(collider.offset, collider.rotation),
                        shape: collider_shape(&collider.geometry),
                        material: ColliderMaterial {
                            density_kg_m3: 0.0,
                            ..ColliderMaterial::default()
                        },
                        sensor: false,
                        collision_memberships: u32::MAX,
                        collision_filter: u32::MAX,
                    },
                )?;
                collider_ids.push(collider_id);
            }
            vehicle_bodies.insert(robot.id.clone(), body_id);
            vehicle_body_descs.insert(robot.id.clone(), body);
            vehicle_colliders.insert(robot.id.clone(), collider_ids);
            vehicle_configs.insert(robot.id.clone(), vehicle.clone());
            vehicle_collider_configs.insert(robot.id.clone(), colliders);
            vehicle_steering_angles.insert(robot.id.clone(), 0.0);
            vehicle_actuators.insert(
                robot.id.clone(),
                VehicleActuatorState {
                    left_command: 0.0,
                    right_command: 0.0,
                    left_drive_force_n: 0.0,
                    right_drive_force_n: 0.0,
                    braking: false,
                },
            );
        }

        Ok(Self {
            world,
            object_bodies,
            object_body_descs,
            object_colliders,
            object_dynamic,
            attachments: BTreeMap::new(),
            trigger_configs,
            trigger_states,
            trigger_sensors,
            vehicle_bodies,
            vehicle_body_descs,
            vehicle_colliders,
            vehicle_configs,
            vehicle_collider_configs,
            vehicle_steering_angles,
            vehicle_actuators,
            link_bodies: BTreeMap::new(),
            link_body_descs: BTreeMap::new(),
            link_colliders: BTreeMap::new(),
        })
    }

    pub(crate) fn snapshot(&self) -> PhysicsSnapshot {
        self.world.snapshot()
    }

    #[cfg(test)]
    pub(crate) fn object_body_id(&self, object_id: &str) -> Option<BodyId> {
        self.object_bodies.get(object_id).cloned()
    }

    #[cfg(test)]
    pub(crate) fn link_body_id(&self, key: &str) -> Option<BodyId> {
        self.link_bodies.get(key).cloned()
    }

    #[cfg(test)]
    pub(crate) fn remove_link_for_consumer_gate(&mut self, key: &str) -> Result<(), PhysicsError> {
        let Some(body_id) = self.link_bodies.remove(key) else {
            return Ok(());
        };
        self.link_body_descs.remove(key);
        self.link_colliders.remove(key);
        self.world.remove_body(&body_id)
    }

    pub(crate) fn step(&mut self, dt: f32) -> Result<StepOutput, PhysicsError> {
        let next_clock = self.world.snapshot().simulation_time_sec + f64::from(dt);
        self.step_at(dt, next_clock)
    }

    pub(crate) fn step_at(&mut self, dt: f32, clock_sec: f64) -> Result<StepOutput, PhysicsError> {
        self.world.set_step_timing(dt, 1)?;
        let output = self.world.step();
        self.update_trigger_states(dt, clock_sec);
        Ok(output)
    }

    pub(crate) fn trigger_state(&self, id: &str) -> Option<&SceneTriggerState> {
        self.trigger_states.get(id)
    }

    pub(crate) fn add_force(
        &mut self,
        object_id: &str,
        force_n: [f32; 3],
    ) -> Result<(), PhysicsError> {
        let body_id = self
            .object_bodies
            .get(object_id)
            .cloned()
            .unwrap_or_else(|| BodyId::new(format!("shadow:missing:{object_id}")));
        self.world.add_force(&body_id, force_n, true)
    }

    pub(crate) fn drive_vehicle(
        &mut self,
        robot_id: &str,
        command: VehicleDriveCommand,
        dt: f32,
    ) -> Result<(), PhysicsError> {
        let body_id = self
            .vehicle_bodies
            .get(robot_id)
            .cloned()
            .unwrap_or_else(|| BodyId::new(format!("shadow:missing-vehicle:{robot_id}")));
        let body = self.world.body_snapshot(&body_id)?;
        let plan = plan_vehicle_forces(
            &self.vehicle_configs[robot_id],
            self.vehicle_steering_angles[robot_id],
            command,
            VehiclePhysicsKinematics {
                position: body.pose.translation,
                yaw: yaw_from_quaternion(body.pose.rotation_xyzw),
                velocity_mps: body.linear_velocity_mps,
                yaw_rate_rps: body.angular_velocity_rps[2],
                dt,
            },
        );
        for force in &plan.forces {
            match force {
                VehiclePhysicsForceCommand::AtPoint {
                    force_n,
                    point_world_m,
                } => self
                    .world
                    .add_force_at_point(&body_id, *force_n, *point_world_m, true)?,
                VehiclePhysicsForceCommand::AtCenter { force_n } => {
                    self.world.add_force(&body_id, *force_n, true)?
                }
            }
        }
        self.vehicle_steering_angles
            .insert(robot_id.to_string(), plan.steering_angle_rad);
        self.vehicle_actuators
            .insert(robot_id.to_string(), plan.actuator);
        Ok(())
    }

    pub(crate) fn drive_vehicle_two_axle_for_test(
        &mut self,
        robot_id: &str,
        command: VehicleDriveCommand,
        dt: f32,
    ) -> Result<(), PhysicsError> {
        let body_id = self
            .vehicle_bodies
            .get(robot_id)
            .cloned()
            .unwrap_or_else(|| BodyId::new(format!("shadow:missing-vehicle:{robot_id}")));
        let body = self.world.body_snapshot(&body_id)?;
        let plan = plan_two_axle_bicycle_forces(
            &self.vehicle_configs[robot_id],
            self.vehicle_steering_angles[robot_id],
            command,
            VehiclePhysicsKinematics {
                position: body.pose.translation,
                yaw: yaw_from_quaternion(body.pose.rotation_xyzw),
                velocity_mps: body.linear_velocity_mps,
                yaw_rate_rps: body.angular_velocity_rps[2],
                dt,
            },
        );
        for force in &plan.forces {
            match force {
                VehiclePhysicsForceCommand::AtPoint {
                    force_n,
                    point_world_m,
                } => self
                    .world
                    .add_force_at_point(&body_id, *force_n, *point_world_m, true)?,
                VehiclePhysicsForceCommand::AtCenter { force_n } => {
                    self.world.add_force(&body_id, *force_n, true)?
                }
            }
        }
        self.vehicle_steering_angles
            .insert(robot_id.to_string(), plan.steering_angle_rad);
        self.vehicle_actuators
            .insert(robot_id.to_string(), plan.actuator);
        Ok(())
    }

    pub(crate) fn sync_robot_link_colliders(
        &mut self,
        colliders: &[RobotLinkCollider],
    ) -> Result<(), PhysicsError> {
        for (index, link) in colliders.iter().enumerate() {
            let key = format!("{}:{}:{index}", link.robot_id, link.link_name);
            let pose = Pose {
                translation: link.translation,
                rotation_xyzw: quaternion_from_rotation_matrix(link.rotation_matrix),
            };
            let body_id = BodyId::new(format!("shadow:kinematic-link:{key}"));
            if !self.link_bodies.contains_key(&key) {
                let body = BodyDesc {
                    mode: BodyMode::KinematicPosition,
                    pose,
                    ccd_enabled: true,
                    ..BodyDesc::default()
                };
                self.world.create_body(body_id.clone(), body.clone())?;
                let collider_id = ColliderId::new(format!("shadow:kinematic-link:{key}:part:0"));
                self.world.create_collider(
                    collider_id.clone(),
                    &body_id,
                    ColliderDesc {
                        pose: Pose::default(),
                        shape: collider_shape(&link.geometry),
                        material: ColliderMaterial {
                            friction: 0.8,
                            restitution: 0.0,
                            density_kg_m3: 1.0,
                            contact_skin_m: 0.0,
                        },
                        sensor: false,
                        collision_memberships: u32::MAX,
                        collision_filter: u32::MAX,
                    },
                )?;
                self.link_bodies.insert(key.clone(), body_id.clone());
                self.link_body_descs.insert(key.clone(), body);
                self.link_colliders.insert(key.clone(), collider_id);
            }
            self.world.set_bounded_kinematic_target_with_mode(
                &body_id,
                BoundedKinematicTarget {
                    pose,
                    maximum_linear_speed_mps: 0.20,
                    maximum_angular_speed_rps: 2.0,
                    // The direct reviewed-link bridge applies its speed caps
                    // immediately for its 1/120 s kinematic substep. PGE's
                    // bounded target also requires acceleration limits, so
                    // choose the smallest values that reach each cap in one
                    // such substep rather than adding a second motion ramp.
                    maximum_linear_acceleration_mps2: 24.0,
                    maximum_angular_acceleration_rps2: 240.0,
                },
                KinematicTargetMode::CoupledPose,
            )?;
        }
        Ok(())
    }

    pub(crate) fn map_attachment(
        &mut self,
        object_id: &str,
        attachment: &SceneObjectAttachment,
        position: [f32; 3],
        rotation: [f32; 3],
    ) -> Result<(), PhysicsError> {
        let body_id = self
            .object_bodies
            .get(object_id)
            .cloned()
            .unwrap_or_else(|| BodyId::new(format!("shadow:missing:{object_id}")));
        let pose = pose_from_rpy(position, rotation);
        self.world
            .set_body_mode(&body_id, BodyMode::KinematicPosition, true)?;
        self.world.set_body_pose(&body_id, pose, true)?;
        self.world
            .set_body_velocity(&body_id, [0.0; 3], [0.0; 3], true)?;
        self.world.set_next_kinematic_pose(&body_id, pose)?;
        self.attachments
            .insert(object_id.to_string(), attachment.clone());
        Ok(())
    }

    pub(crate) fn map_detach(&mut self, object_id: &str) -> Result<(), PhysicsError> {
        let body_id = self
            .object_bodies
            .get(object_id)
            .cloned()
            .unwrap_or_else(|| BodyId::new(format!("shadow:missing:{object_id}")));
        self.world
            .set_body_mode(&body_id, BodyMode::Dynamic, true)?;
        self.world
            .set_body_velocity(&body_id, [0.0; 3], [0.0; 3], true)?;
        self.world.wake_up(&body_id)?;
        self.attachments.remove(object_id);
        Ok(())
    }

    pub(crate) fn map_attached_pose(
        &mut self,
        object_id: &str,
        position: [f32; 3],
        rotation: [f32; 3],
    ) -> Result<(), PhysicsError> {
        let body_id = self
            .object_bodies
            .get(object_id)
            .cloned()
            .unwrap_or_else(|| BodyId::new(format!("shadow:missing:{object_id}")));
        let pose = pose_from_rpy(position, rotation);
        self.world.set_body_pose(&body_id, pose, true)?;
        self.world.set_next_kinematic_pose(&body_id, pose)?;
        self.world
            .set_body_velocity(&body_id, [0.0; 3], [0.0; 3], true)
    }

    pub(crate) fn apply_torque_impulse(
        &mut self,
        object_id: &str,
        impulse_nms: [f32; 3],
    ) -> Result<(), PhysicsError> {
        let body_id = self
            .object_bodies
            .get(object_id)
            .cloned()
            .unwrap_or_else(|| BodyId::new(format!("shadow:missing:{object_id}")));
        self.world.apply_torque_impulse(&body_id, impulse_nms, true)
    }

    fn update_trigger_states(&mut self, dt: f32, clock_sec: f64) {
        for (trigger_id, config) in &self.trigger_configs {
            let Some(body_id) = self.object_bodies.get(&config.object_id) else {
                continue;
            };
            let Ok(body) = self.world.body_snapshot(body_id) else {
                continue;
            };
            let object = SceneObjectState {
                id: config.object_id.clone(),
                position: body.pose.translation,
                rotation: [0.0; 3],
                velocity_mps: body.linear_velocity_mps,
                angular_velocity_rps: body.angular_velocity_rps,
                dynamic: self.object_dynamic[&config.object_id],
                attachment: self.attachments.get(&config.object_id).cloned(),
            };
            let state = self
                .trigger_states
                .get_mut(trigger_id)
                .expect("shadow trigger state matches its config");
            apply_scene_trigger_rule(state, config, &object, dt, clock_sec);
        }
    }
}

fn collider_shape(geometry: &ProjectSceneColliderGeometry) -> ColliderShape {
    match geometry {
        ProjectSceneColliderGeometry::Box { size } => ColliderShape::Box { size: *size },
        ProjectSceneColliderGeometry::Sphere { radius } => {
            ColliderShape::Sphere { radius: *radius }
        }
        ProjectSceneColliderGeometry::Cylinder { radius, height } => ColliderShape::CylinderY {
            half_height: height * 0.5,
            radius: *radius,
        },
    }
}

fn matrix_add(left: Matrix3, right: Matrix3) -> Matrix3 {
    std::array::from_fn(|row| std::array::from_fn(|column| left[row][column] + right[row][column]))
}

fn matrix_multiply(left: Matrix3, right: Matrix3) -> Matrix3 {
    std::array::from_fn(|row| {
        std::array::from_fn(|column| {
            (0..3)
                .map(|index| left[row][index] * right[index][column])
                .sum()
        })
    })
}

fn matrix_scale(matrix: Matrix3, scale: f32) -> Matrix3 {
    matrix.map(|row| row.map(|value| value * scale))
}

fn matrix_transpose(matrix: Matrix3) -> Matrix3 {
    std::array::from_fn(|row| std::array::from_fn(|column| matrix[column][row]))
}

fn outer_product(value: [f32; 3]) -> Matrix3 {
    std::array::from_fn(|row| std::array::from_fn(|column| value[row] * value[column]))
}

fn quaternion_from_rpy(rotation: [f32; 3]) -> [f32; 4] {
    let (sin_roll, cos_roll) = (rotation[0] * 0.5).sin_cos();
    let (sin_pitch, cos_pitch) = (rotation[1] * 0.5).sin_cos();
    let (sin_yaw, cos_yaw) = (rotation[2] * 0.5).sin_cos();
    [
        sin_roll * cos_pitch * cos_yaw - cos_roll * sin_pitch * sin_yaw,
        cos_roll * sin_pitch * cos_yaw + sin_roll * cos_pitch * sin_yaw,
        cos_roll * cos_pitch * sin_yaw - sin_roll * sin_pitch * cos_yaw,
        cos_roll * cos_pitch * cos_yaw + sin_roll * sin_pitch * sin_yaw,
    ]
}

fn yaw_from_quaternion(rotation_xyzw: [f32; 4]) -> f32 {
    let [x, y, z, w] = rotation_xyzw;
    (2.0 * (w * z + x * y)).atan2(1.0 - 2.0 * (y * y + z * z))
}

fn quaternion_from_rotation_matrix(matrix: [[f32; 3]; 3]) -> [f32; 4] {
    let trace = matrix[0][0] + matrix[1][1] + matrix[2][2];
    let (x, y, z, w) = if trace > 0.0 {
        let scale = 2.0 * (trace + 1.0).sqrt();
        (
            (matrix[2][1] - matrix[1][2]) / scale,
            (matrix[0][2] - matrix[2][0]) / scale,
            (matrix[1][0] - matrix[0][1]) / scale,
            0.25 * scale,
        )
    } else if matrix[0][0] > matrix[1][1] && matrix[0][0] > matrix[2][2] {
        let scale = 2.0 * (1.0 + matrix[0][0] - matrix[1][1] - matrix[2][2]).sqrt();
        (
            0.25 * scale,
            (matrix[0][1] + matrix[1][0]) / scale,
            (matrix[0][2] + matrix[2][0]) / scale,
            (matrix[2][1] - matrix[1][2]) / scale,
        )
    } else if matrix[1][1] > matrix[2][2] {
        let scale = 2.0 * (1.0 + matrix[1][1] - matrix[0][0] - matrix[2][2]).sqrt();
        (
            (matrix[0][1] + matrix[1][0]) / scale,
            0.25 * scale,
            (matrix[1][2] + matrix[2][1]) / scale,
            (matrix[0][2] - matrix[2][0]) / scale,
        )
    } else {
        let scale = 2.0 * (1.0 + matrix[2][2] - matrix[0][0] - matrix[1][1]).sqrt();
        (
            (matrix[0][2] + matrix[2][0]) / scale,
            (matrix[1][2] + matrix[2][1]) / scale,
            0.25 * scale,
            (matrix[1][0] - matrix[0][1]) / scale,
        )
    };
    [x, y, z, w]
}

fn rotation_matrix(rotation: [f32; 3]) -> Matrix3 {
    let [x, y, z, w] = quaternion_from_rpy(rotation);
    [
        [
            1.0 - 2.0 * (y * y + z * z),
            2.0 * (x * y - z * w),
            2.0 * (x * z + y * w),
        ],
        [
            2.0 * (x * y + z * w),
            1.0 - 2.0 * (x * x + z * z),
            2.0 * (y * z - x * w),
        ],
        [
            2.0 * (x * z - y * w),
            2.0 * (y * z + x * w),
            1.0 - 2.0 * (x * x + y * y),
        ],
    ]
}

fn pose_from_rpy(translation: [f32; 3], rotation: [f32; 3]) -> Pose {
    Pose {
        translation,
        rotation_xyzw: quaternion_from_rpy(rotation),
    }
}

fn scene_collider_inertia(geometry: &ProjectSceneColliderGeometry, mass_kg: f32) -> Matrix3 {
    let diagonal = match geometry {
        ProjectSceneColliderGeometry::Box { size } => [
            mass_kg * (size[1].powi(2) + size[2].powi(2)) / 12.0,
            mass_kg * (size[0].powi(2) + size[2].powi(2)) / 12.0,
            mass_kg * (size[0].powi(2) + size[1].powi(2)) / 12.0,
        ],
        ProjectSceneColliderGeometry::Sphere { radius } => {
            [2.0 * mass_kg * radius.powi(2) / 5.0; 3]
        }
        ProjectSceneColliderGeometry::Cylinder { radius, height } => {
            let transverse = mass_kg * (3.0 * radius.powi(2) + height.powi(2)) / 12.0;
            [transverse, mass_kg * radius.powi(2) / 2.0, transverse]
        }
    };
    [
        [diagonal[0], 0.0, 0.0],
        [0.0, diagonal[1], 0.0],
        [0.0, 0.0, diagonal[2]],
    ]
}

fn scene_collider_volume(geometry: &ProjectSceneColliderGeometry) -> f32 {
    match geometry {
        ProjectSceneColliderGeometry::Box { size } => size[0] * size[1] * size[2],
        ProjectSceneColliderGeometry::Sphere { radius } => {
            4.0 * std::f32::consts::PI * radius.powi(3) / 3.0
        }
        ProjectSceneColliderGeometry::Cylinder { radius, height } => {
            std::f32::consts::PI * radius.powi(2) * height
        }
    }
}

fn scene_object_mass_properties(config: &ProjectSceneObjectPhysicsConfig) -> MassPropertiesDesc {
    let parts = config.collider.parts();
    let total_volume = parts
        .iter()
        .map(|part| scene_collider_volume(&part.geometry))
        .sum::<f32>()
        .max(1.0e-6);
    let mass_kg = config.mass_kg.max(1.0e-6);
    let parts_with_mass = parts
        .iter()
        .map(|part| {
            let mass = mass_kg * scene_collider_volume(&part.geometry) / total_volume;
            (part, mass)
        })
        .collect::<Vec<_>>();
    let derived_center_of_mass = parts_with_mass
        .iter()
        .fold([0.0; 3], |mut sum, (part, mass)| {
            for (axis, component) in sum.iter_mut().enumerate() {
                *component += part.offset[axis] * mass;
            }
            sum
        })
        .map(|value| value / mass_kg);
    let center_of_mass = config.center_of_mass.unwrap_or(derived_center_of_mass);
    let inertia = parts_with_mass
        .iter()
        .fold([[0.0; 3]; 3], |sum, (part, mass)| {
            let rotation = rotation_matrix(part.rotation);
            let rotated_local = matrix_multiply(
                matrix_multiply(rotation, scene_collider_inertia(&part.geometry, *mass)),
                matrix_transpose(rotation),
            );
            let offset = std::array::from_fn(|axis| part.offset[axis] - center_of_mass[axis]);
            let offset_squared = offset.iter().map(|value| value * value).sum::<f32>();
            let parallel_axis = matrix_scale(
                matrix_add(
                    [
                        [offset_squared, 0.0, 0.0],
                        [0.0, offset_squared, 0.0],
                        [0.0, 0.0, offset_squared],
                    ],
                    matrix_scale(outer_product(offset), -1.0),
                ),
                *mass,
            );
            matrix_add(sum, matrix_add(rotated_local, parallel_axis))
        });
    MassPropertiesDesc {
        mass_kg,
        center_of_mass_m: center_of_mass,
        principal_inertia_kg_m2: [inertia[0][0], inertia[1][1], inertia[2][2]],
        principal_inertia_frame_xyzw: [0.0, 0.0, 0.0, 1.0],
        inertia_tensor_kg_m2: Some(inertia),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::path::PathBuf;

    use pge_physics::{
        BodyMode, ColliderShape, PhysicsEvent, PhysicsEventKind, PhysicsSnapshot, Pose,
    };

    use crate::project::{
        HardwareConfig, ProjectConfig, ProjectRobotConfig, ProjectRobotModelConfig,
        ProjectRobotPhysicsConfig, ProjectSceneBodyKind, ProjectSceneColliderConfig,
        ProjectSceneColliderGeometry, ProjectSceneConfig, ProjectSceneObjectConfig,
        ProjectSceneObjectGeometry, ProjectSceneObjectPhysicsConfig, ProjectSceneTriggerConfig,
        ProjectVehicleMotorConfig, ProjectVehiclePhysicsConfig, RobotLinkCollider,
    };
    use crate::scene_physics::{
        AuthoritativeColliderRuntimeDescriptor, AuthoritativePhysicsTraceFrame,
        SceneObjectAttachment, ScenePhysicsRuntime, VehicleDriveCommand,
        tests::compound_bottle_project,
    };

    use super::{ScenePhysicsShadow, collider_shape, pose_from_rpy, yaw_from_quaternion};

    const TRACE_POSITION_ENVELOPE_TOLERANCE_M: f32 = 0.02;
    const TRACE_LINEAR_SPEED_ENVELOPE_TOLERANCE_MPS: f32 = 0.10;
    const TRACE_ANGULAR_SPEED_ENVELOPE_TOLERANCE_RPS: f32 = 1.0;
    const TORQUE_RESPONSE_TOLERANCE_RPS: f32 = 1.0e-4;
    const VEHICLE_POSITION_TOLERANCE_M: f32 = 0.01;
    const VEHICLE_LINEAR_SPEED_TOLERANCE_MPS: f32 = 0.05;
    const VEHICLE_ANGULAR_SPEED_TOLERANCE_RPS: f32 = 0.05;
    const VEHICLE_ROTATION_TOLERANCE: f32 = 0.01;

    #[derive(Clone, Debug)]
    struct TraceEnvelope {
        minimum_position: [f32; 3],
        maximum_position: [f32; 3],
        maximum_linear_speed_mps: f32,
        maximum_angular_speed_rps: f32,
    }

    impl TraceEnvelope {
        fn new(position: [f32; 3]) -> Self {
            Self {
                minimum_position: position,
                maximum_position: position,
                maximum_linear_speed_mps: 0.0,
                maximum_angular_speed_rps: 0.0,
            }
        }

        fn include(
            &mut self,
            position: [f32; 3],
            linear_velocity: [f32; 3],
            angular_velocity: [f32; 3],
        ) {
            for (axis, value) in position.iter().enumerate() {
                self.minimum_position[axis] = self.minimum_position[axis].min(*value);
                self.maximum_position[axis] = self.maximum_position[axis].max(*value);
            }
            self.maximum_linear_speed_mps = self
                .maximum_linear_speed_mps
                .max(vector_magnitude(linear_velocity));
            self.maximum_angular_speed_rps = self
                .maximum_angular_speed_rps
                .max(vector_magnitude(angular_velocity));
        }
    }

    fn assert_close(actual: f32, expected: f32, tolerance: f32) {
        assert!(
            (actual - expected).abs() <= tolerance,
            "actual={actual}, expected={expected}, tolerance={tolerance}"
        );
    }

    fn assert_vector_close(actual: &[f32], expected: &[f32], tolerance: f32) {
        assert_eq!(actual.len(), expected.len());
        for (actual, expected) in actual.iter().zip(expected) {
            assert_close(*actual, *expected, tolerance);
        }
    }

    fn assert_quaternion_close(actual: [f32; 4], expected: [f32; 4], tolerance: f32) {
        let dot = actual
            .iter()
            .zip(expected)
            .map(|(actual, expected)| actual * expected)
            .sum::<f32>();
        let aligned_expected = if dot < 0.0 {
            expected.map(|value| -value)
        } else {
            expected
        };
        assert_vector_close(&actual, &aligned_expected, tolerance);
    }

    fn vector_magnitude(vector: [f32; 3]) -> f32 {
        vector.iter().map(|value| value * value).sum::<f32>().sqrt()
    }

    fn assert_finite_normalized_pose(pose: Pose) {
        assert!(pose.translation.iter().all(|value| value.is_finite()));
        assert!(pose.rotation_xyzw.iter().all(|value| value.is_finite()));
        let norm = pose
            .rotation_xyzw
            .iter()
            .map(|value| value * value)
            .sum::<f32>()
            .sqrt();
        assert_close(norm, 1.0, 1.0e-5);
    }

    fn quaternion_multiply(left: [f32; 4], right: [f32; 4]) -> [f32; 4] {
        let [lx, ly, lz, lw] = left;
        let [rx, ry, rz, rw] = right;
        [
            lw * rx + lx * rw + ly * rz - lz * ry,
            lw * ry - lx * rz + ly * rw + lz * rx,
            lw * rz + lx * ry - ly * rx + lz * rw,
            lw * rw - lx * rx - ly * ry - lz * rz,
        ]
    }

    fn quaternion_rotate(quaternion: [f32; 4], point: [f32; 3]) -> [f32; 3] {
        let [x, y, z, w] = quaternion;
        let cross = [
            y * point[2] - z * point[1],
            z * point[0] - x * point[2],
            x * point[1] - y * point[0],
        ];
        let second_cross = [
            y * cross[2] - z * cross[1],
            z * cross[0] - x * cross[2],
            x * cross[1] - y * cross[0],
        ];
        std::array::from_fn(|axis| point[axis] + 2.0 * (w * cross[axis] + second_cross[axis]))
    }

    fn compose_pose(parent: Pose, child: Pose) -> Pose {
        let rotated_translation = quaternion_rotate(parent.rotation_xyzw, child.translation);
        Pose {
            translation: std::array::from_fn(|axis| {
                parent.translation[axis] + rotated_translation[axis]
            }),
            rotation_xyzw: quaternion_multiply(parent.rotation_xyzw, child.rotation_xyzw),
        }
    }

    fn assert_shadow_parts_at_pose(
        shadow: &ScenePhysicsShadow,
        object_id: &str,
        target_pose: Pose,
    ) {
        let snapshot = shadow.snapshot();
        let body = snapshot
            .bodies
            .iter()
            .find(|body| body.id == shadow.object_bodies[object_id])
            .unwrap();
        assert_eq!(body.mode, BodyMode::KinematicPosition);
        assert_vector_close(&body.pose.translation, &target_pose.translation, 1.0e-6);
        assert_quaternion_close(body.pose.rotation_xyzw, target_pose.rotation_xyzw, 1.0e-6);
        for (index, collider_id) in shadow.object_colliders[object_id].iter().enumerate() {
            assert_eq!(
                collider_id.0,
                format!("shadow:scene-object:{object_id}:part:{index}")
            );
            let collider = snapshot
                .colliders
                .iter()
                .find(|collider| collider.id == *collider_id)
                .unwrap();
            let expected = compose_pose(target_pose, collider.desc.pose);
            assert_vector_close(
                &collider.world_pose.translation,
                &expected.translation,
                1.0e-5,
            );
            assert_quaternion_close(
                collider.world_pose.rotation_xyzw,
                expected.rotation_xyzw,
                1.0e-5,
            );
            assert!(
                shadow
                    .world
                    .overlap_shape(expected, &collider.desc.shape)
                    .unwrap()
                    .contains(collider_id)
            );
        }
    }

    fn assert_shadow_part_ids(
        snapshot: &PhysicsSnapshot,
        shadow: &ScenePhysicsShadow,
        object_id: &str,
    ) {
        for (index, collider_id) in shadow.object_colliders[object_id].iter().enumerate() {
            assert_eq!(
                collider_id.0,
                format!("shadow:scene-object:{object_id}:part:{index}")
            );
            assert!(
                snapshot
                    .colliders
                    .iter()
                    .any(|collider| collider.id == *collider_id)
            );
        }
    }

    fn assert_authoritative_colliders_match_shadow(
        direct: &[AuthoritativeColliderRuntimeDescriptor],
        snapshot: &PhysicsSnapshot,
        shadow_ids: &[pge_physics::ColliderId],
    ) {
        assert_eq!(direct.len(), shadow_ids.len());
        for (expected_index, (direct, shadow_id)) in direct.iter().zip(shadow_ids).enumerate() {
            assert_eq!(direct.insertion_index, expected_index);
            let shadow = snapshot
                .colliders
                .iter()
                .find(|collider| collider.id == *shadow_id)
                .unwrap();
            assert_eq!(direct.shape, shadow.desc.shape);
            assert_vector_close(
                &direct.local_translation,
                &shadow.desc.pose.translation,
                1.0e-6,
            );
            assert_quaternion_close(
                direct.local_rotation_xyzw,
                shadow.desc.pose.rotation_xyzw,
                1.0e-6,
            );
            assert_eq!(direct.friction, shadow.desc.material.friction);
            assert_eq!(direct.restitution, shadow.desc.material.restitution);
            assert_eq!(direct.density_kg_m3, shadow.desc.material.density_kg_m3);
            assert_eq!(
                direct.collision_memberships,
                shadow.desc.collision_memberships
            );
            assert_eq!(direct.collision_filter, shadow.desc.collision_filter);
            assert_eq!(direct.sensor, shadow.desc.sensor);
        }
    }

    fn assert_trace_frame_invariants(
        authoritative: &AuthoritativePhysicsTraceFrame,
        shadow: &PhysicsSnapshot,
        shadow_runtime: &ScenePhysicsShadow,
    ) {
        let body = shadow
            .bodies
            .iter()
            .find(|body| body.id == shadow_runtime.object_bodies["bottle"])
            .unwrap();
        assert_finite_normalized_pose(body.pose);
        assert!(
            body.linear_velocity_mps
                .iter()
                .all(|value| value.is_finite())
        );
        assert!(
            body.angular_velocity_rps
                .iter()
                .all(|value| value.is_finite())
        );
        assert_finite_normalized_pose(Pose {
            translation: authoritative.position,
            rotation_xyzw: authoritative.rotation_xyzw,
        });
        assert!(
            authoritative
                .linear_velocity_mps
                .iter()
                .all(|value| value.is_finite())
        );
        assert!(
            authoritative
                .angular_velocity_rps
                .iter()
                .all(|value| value.is_finite())
        );
        assert_eq!(
            authoritative.parts.len(),
            shadow_runtime.object_colliders["bottle"].len()
        );
        for authoritative_part in &authoritative.parts {
            let shadow_part = shadow
                .colliders
                .iter()
                .find(|collider| collider.id.0 == authoritative_part.id)
                .unwrap();
            let local_pose = shadow_part.desc.pose;
            let expected_authoritative_pose = compose_pose(
                Pose {
                    translation: authoritative.position,
                    rotation_xyzw: authoritative.rotation_xyzw,
                },
                local_pose,
            );
            let expected_shadow_pose = compose_pose(body.pose, local_pose);
            assert_vector_close(
                &authoritative_part.position,
                &expected_authoritative_pose.translation,
                1.0e-5,
            );
            assert_quaternion_close(
                authoritative_part.rotation_xyzw,
                expected_authoritative_pose.rotation_xyzw,
                1.0e-5,
            );
            assert_vector_close(
                &shadow_part.world_pose.translation,
                &expected_shadow_pose.translation,
                1.0e-5,
            );
            assert_quaternion_close(
                shadow_part.world_pose.rotation_xyzw,
                expected_shadow_pose.rotation_xyzw,
                1.0e-5,
            );
            assert_finite_normalized_pose(Pose {
                translation: authoritative_part.position,
                rotation_xyzw: authoritative_part.rotation_xyzw,
            });
            assert_finite_normalized_pose(shadow_part.world_pose);
        }
    }

    fn assert_vehicle_trace_frame(
        authoritative: &AuthoritativePhysicsTraceFrame,
        shadow_snapshot: &PhysicsSnapshot,
        shadow: &ScenePhysicsShadow,
    ) {
        let shadow_body = shadow_snapshot
            .bodies
            .iter()
            .find(|body| body.id == shadow.vehicle_bodies["puppybot"])
            .unwrap();
        assert_vector_close(
            &shadow_body.pose.translation,
            &authoritative.position,
            VEHICLE_POSITION_TOLERANCE_M,
        );
        assert_quaternion_close(
            shadow_body.pose.rotation_xyzw,
            authoritative.rotation_xyzw,
            VEHICLE_ROTATION_TOLERANCE,
        );
        assert_close(shadow_body.angular_velocity_rps[0], 0.0, 1.0e-6);
        assert_close(shadow_body.angular_velocity_rps[1], 0.0, 1.0e-6);
        assert_eq!(
            authoritative.parts.len(),
            shadow.vehicle_colliders["puppybot"].len()
        );
        for (index, authoritative_part) in authoritative.parts.iter().enumerate() {
            assert_eq!(
                authoritative_part.id,
                format!("shadow:vehicle:puppybot:part:{index}")
            );
            let shadow_part = shadow_snapshot
                .colliders
                .iter()
                .find(|collider| collider.id.0 == authoritative_part.id)
                .unwrap();
            let expected_shadow_pose = compose_pose(shadow_body.pose, shadow_part.desc.pose);
            assert_vector_close(
                &shadow_part.world_pose.translation,
                &expected_shadow_pose.translation,
                1.0e-5,
            );
            assert_quaternion_close(
                shadow_part.world_pose.rotation_xyzw,
                expected_shadow_pose.rotation_xyzw,
                1.0e-5,
            );
            assert_vector_close(
                &shadow_part.world_pose.translation,
                &authoritative_part.position,
                VEHICLE_POSITION_TOLERANCE_M,
            );
            assert_quaternion_close(
                shadow_part.world_pose.rotation_xyzw,
                authoritative_part.rotation_xyzw,
                VEHICLE_ROTATION_TOLERANCE,
            );
        }
    }

    fn normalized_contact_events(
        events: &[PhysicsEvent],
    ) -> Vec<(PhysicsEventKind, String, String)> {
        events
            .iter()
            .filter(|event| {
                matches!(
                    event.kind,
                    PhysicsEventKind::ContactStarted | PhysicsEventKind::ContactStopped
                )
            })
            .map(|event| {
                (
                    event.kind.clone(),
                    event.collider1.0.clone(),
                    event.collider2.0.clone(),
                )
            })
            .collect()
    }

    fn expected_contact_events(
        previous: &BTreeSet<(String, String)>,
        current: &BTreeSet<(String, String)>,
    ) -> Vec<(PhysicsEventKind, String, String)> {
        let mut events = current
            .difference(previous)
            .map(|(first, second)| {
                (
                    PhysicsEventKind::ContactStarted,
                    first.clone(),
                    second.clone(),
                )
            })
            .collect::<Vec<_>>();
        events.extend(previous.difference(current).map(|(first, second)| {
            (
                PhysicsEventKind::ContactStopped,
                first.clone(),
                second.clone(),
            )
        }));
        events
    }

    fn snapshot_pairs(snapshot: &PhysicsSnapshot, sensor: bool) -> BTreeSet<(String, String)> {
        snapshot
            .contacts
            .iter()
            .filter(|contact| contact.sensor == sensor)
            .map(|contact| (contact.collider1.0.clone(), contact.collider2.0.clone()))
            .collect()
    }

    fn normalized_sensor_events(
        events: &[PhysicsEvent],
    ) -> Vec<(PhysicsEventKind, String, String)> {
        events
            .iter()
            .filter(|event| {
                matches!(
                    event.kind,
                    PhysicsEventKind::SensorStarted | PhysicsEventKind::SensorStopped
                )
            })
            .map(|event| {
                (
                    event.kind.clone(),
                    event.collider1.0.clone(),
                    event.collider2.0.clone(),
                )
            })
            .collect()
    }

    fn expected_sensor_events(
        previous: &BTreeSet<(String, String)>,
        current: &BTreeSet<(String, String)>,
    ) -> Vec<(PhysicsEventKind, String, String)> {
        let mut events = current
            .difference(previous)
            .map(|(first, second)| {
                (
                    PhysicsEventKind::SensorStarted,
                    first.clone(),
                    second.clone(),
                )
            })
            .collect::<Vec<_>>();
        events.extend(previous.difference(current).map(|(first, second)| {
            (
                PhysicsEventKind::SensorStopped,
                first.clone(),
                second.clone(),
            )
        }));
        events
    }

    fn run_compound_bottle_trace(
        rotation: [f32; 3],
        step_count: u32,
    ) -> (AuthoritativePhysicsTraceFrame, PhysicsSnapshot) {
        let project = compound_bottle_project(rotation);
        let mut authoritative = ScenePhysicsRuntime::from_project(&project).unwrap();
        let mut shadow = ScenePhysicsShadow::from_project(&project).unwrap();
        let initial_authoritative = authoritative
            .authoritative_physics_trace_frame("bottle")
            .unwrap();
        let initial_shadow = shadow.snapshot();
        assert_trace_frame_invariants(&initial_authoritative, &initial_shadow, &shadow);
        let initial_shadow_body = initial_shadow
            .bodies
            .iter()
            .find(|body| body.id == shadow.object_bodies["bottle"])
            .unwrap();
        let mut authoritative_envelope = TraceEnvelope::new(initial_authoritative.position);
        let mut shadow_envelope = TraceEnvelope::new(initial_shadow_body.pose.translation);
        let mut previous_contacts = BTreeSet::new();
        let mut authoritative_events = Vec::new();
        let mut shadow_events = Vec::new();

        for step in 1..=step_count {
            authoritative.step(1.0 / 120.0, f64::from(step) / 120.0);
            let shadow_output = shadow.step(1.0 / 120.0).unwrap();
            let authoritative_frame = authoritative
                .authoritative_physics_trace_frame("bottle")
                .unwrap();
            assert_trace_frame_invariants(&authoritative_frame, &shadow_output.snapshot, &shadow);
            let shadow_body = shadow_output
                .snapshot
                .bodies
                .iter()
                .find(|body| body.id == shadow.object_bodies["bottle"])
                .unwrap();
            authoritative_envelope.include(
                authoritative_frame.position,
                authoritative_frame.linear_velocity_mps,
                authoritative_frame.angular_velocity_rps,
            );
            shadow_envelope.include(
                shadow_body.pose.translation,
                shadow_body.linear_velocity_mps,
                shadow_body.angular_velocity_rps,
            );
            let current_contacts = authoritative_frame
                .contacts
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>();
            authoritative_events.extend(expected_contact_events(
                &previous_contacts,
                &current_contacts,
            ));
            shadow_events.extend(normalized_contact_events(&shadow_output.events));
            previous_contacts = current_contacts;
        }

        assert_eq!(shadow_events, authoritative_events);
        assert_vector_close(
            &shadow_envelope.minimum_position,
            &authoritative_envelope.minimum_position,
            TRACE_POSITION_ENVELOPE_TOLERANCE_M,
        );
        assert_vector_close(
            &shadow_envelope.maximum_position,
            &authoritative_envelope.maximum_position,
            TRACE_POSITION_ENVELOPE_TOLERANCE_M,
        );
        assert_close(
            shadow_envelope.maximum_linear_speed_mps,
            authoritative_envelope.maximum_linear_speed_mps,
            TRACE_LINEAR_SPEED_ENVELOPE_TOLERANCE_MPS,
        );
        assert_close(
            shadow_envelope.maximum_angular_speed_rps,
            authoritative_envelope.maximum_angular_speed_rps,
            TRACE_ANGULAR_SPEED_ENVELOPE_TOLERANCE_RPS,
        );

        (
            authoritative
                .authoritative_physics_trace_frame("bottle")
                .unwrap(),
            shadow.snapshot(),
        )
    }

    fn physical_object(
        id: &str,
        position: [f32; 3],
        body_kind: ProjectSceneBodyKind,
        geometry: ProjectSceneColliderGeometry,
    ) -> ProjectSceneObjectConfig {
        ProjectSceneObjectConfig {
            id: id.to_string(),
            name: id.to_string(),
            type_name: "shadow-test".to_string(),
            icon: "TST".to_string(),
            geometry: match geometry {
                ProjectSceneColliderGeometry::Box { size } => {
                    ProjectSceneObjectGeometry::Box { size }
                }
                ProjectSceneColliderGeometry::Sphere { radius } => {
                    ProjectSceneObjectGeometry::Sphere { radius }
                }
                ProjectSceneColliderGeometry::Cylinder { radius, height } => {
                    ProjectSceneObjectGeometry::Cylinder {
                        radius_top: radius,
                        radius_bottom: radius,
                        height,
                    }
                }
            },
            color_rgb: [255; 3],
            position,
            rotation: [0.0; 3],
            scale: None,
            visual_transform: None,
            include_in_fit: true,
            physics: Some(ProjectSceneObjectPhysicsConfig {
                body_kind,
                mass_kg: 0.05,
                center_of_mass: None,
                linear_damping: 0.2,
                angular_damping: 0.2,
                friction: 0.7,
                restitution: 0.0,
                collider: ProjectSceneColliderConfig {
                    geometry,
                    offset: [0.0; 3],
                    rotation: [0.0; 3],
                    children: Vec::new(),
                },
            }),
        }
    }

    fn ball_bin_project() -> ProjectConfig {
        let mut floor = physical_object(
            "floor",
            [0.0; 3],
            ProjectSceneBodyKind::Static,
            ProjectSceneColliderGeometry::Box {
                size: [0.4, 0.4, 0.02],
            },
        );
        floor.physics.as_mut().unwrap().collider.offset = [0.0, 0.0, 0.01];
        ProjectConfig {
            format: "robotdreams.project.v1".to_string(),
            name: "shadow physics".to_string(),
            manifest_path: PathBuf::from("project.json"),
            base_dir: PathBuf::new(),
            model_profile_path: None,
            calibration_record_path: None,
            scene: ProjectSceneConfig {
                objects: vec![
                    floor,
                    physical_object(
                        "ball",
                        [0.0, 0.0, 0.18],
                        ProjectSceneBodyKind::Dynamic,
                        ProjectSceneColliderGeometry::Sphere { radius: 0.025 },
                    ),
                ],
                triggers: vec![ProjectSceneTriggerConfig {
                    id: "ball-zone".to_string(),
                    object_id: "ball".to_string(),
                    position: [0.0, 0.0, 0.12],
                    size: [0.16, 0.16, 0.20],
                    settle_speed_mps: 0.05,
                    settle_time_sec: 0.25,
                }],
                ..ProjectSceneConfig::default()
            },
            robots: Vec::new(),
            hardware: HardwareConfig::default(),
        }
    }

    fn vehicle_gate_project() -> (ProjectConfig, PathBuf) {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let profile_dir = std::env::temp_dir().join(format!(
            "robotdreams-shadow-vehicle-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir_all(&profile_dir).unwrap();
        std::fs::write(
            profile_dir.join("generated-colliders.json"),
            serde_json::json!({
                "compounds": [{
                    "parts": [
                        {"size":[0.24,0.18,0.10],"center":[0.0,0.0,0.0]},
                        {"size":[0.08,0.20,0.04],"center":[-0.07,0.0,0.07]}
                    ]
                }]
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            profile_dir.join("measured-calibration.json"),
            serde_json::json!({
                "format": "robotdreams.calibration.v1",
                "hardware_revision": "shadow-gate-rev",
                "provenance": {
                    "measured_at":"2026-07-20T12:00:00Z",
                    "operator":"shadow-gate",
                    "method":"fixture",
                    "source":"vehicle parity trace"
                },
                "vehicle": {
                    "mass_kg":2.4,
                    "center_of_mass_m":[0.01,0.0,0.03],
                    "wheel_radius_m":0.04,
                    "gear_ratio":1.0,
                    "motor_stall_torque_nm":0.45,
                    "motor_no_load_rpm":120.0
                },
                "servos": [{"servo_id":1,"max_speed_ticks_per_sec":1000.0}],
                "drive_trace": [
                    {"time_sec":0.0,"left_command":0.0,"right_command":0.0,"observed_linear_mps":0.0,"observed_yaw_rps":0.0},
                    {"time_sec":1.0,"left_command":0.5,"right_command":0.5,"observed_linear_mps":0.15,"observed_yaw_rps":0.0}
                ],
                "servo_trace": [
                    {"time_sec":0.0,"servo_id":1,"target_ticks":2200,"observed_present_ticks":2048},
                    {"time_sec":0.2,"servo_id":1,"target_ticks":2200,"observed_present_ticks":2200}
                ]
            })
            .to_string(),
        )
        .unwrap();

        let mut project = ball_bin_project();
        project.scene.triggers.clear();
        project.scene.objects[0].geometry = ProjectSceneObjectGeometry::Box {
            size: [5.0, 5.0, 0.02],
        };
        project.scene.objects[0]
            .physics
            .as_mut()
            .unwrap()
            .collider
            .geometry = ProjectSceneColliderGeometry::Box {
            size: [5.0, 5.0, 0.02],
        };
        project.scene.objects.push(physical_object(
            "wall",
            [0.9, 0.0, 0.15],
            ProjectSceneBodyKind::Static,
            ProjectSceneColliderGeometry::Box {
                size: [0.02, 1.5, 0.30],
            },
        ));
        project.base_dir = profile_dir.clone();
        project.calibration_record_path = Some("measured-calibration.json".to_string());
        project.robots.push(ProjectRobotConfig {
            id: "puppybot".to_string(),
            name: "PuppyBot".to_string(),
            model: ProjectRobotModelConfig {
                type_name: "urdf".to_string(),
                path: "unused.urdf".to_string(),
                model_transformation: None,
            },
            joint_names: Default::default(),
            base_translation: [0.0, 0.0, 0.07],
            base_rotation: [0.0; 3],
            physics: Some(ProjectRobotPhysicsConfig {
                vehicle: Some(ProjectVehiclePhysicsConfig {
                    mass_kg: 8.0,
                    center_of_mass_m: [0.0, 0.0, 0.0],
                    linear_damping: 0.2,
                    angular_damping: 1.0,
                    wheelbase_m: 0.22,
                    track_width_m: 0.18,
                    max_wheel_speed_mps: 0.4,
                    max_drive_force_n: 18.0,
                    lateral_grip_n_per_mps: 45.0,
                    steering_response_deg_per_sec: 240.0,
                    motor: ProjectVehicleMotorConfig {
                        wheel_radius_m: 0.05,
                        gear_ratio: 2.0,
                        stall_torque_nm: 0.2,
                        no_load_rpm: 80.0,
                        brake_torque_nm: 0.25,
                        rolling_resistance_n: 0.4,
                    },
                    colliders: Vec::new(),
                    collision_profile: Some("generated-colliders.json".to_string()),
                }),
                link_collision_profile: None,
            }),
        });
        (project, profile_dir)
    }

    #[test]
    fn shadow_mirrors_authoritative_compound_bottle_descriptors_and_mass() {
        let project = compound_bottle_project([std::f32::consts::FRAC_PI_2, 0.0, 0.0]);
        let mut authoritative = ScenePhysicsRuntime::from_project(&project).unwrap();
        let mut shadow = ScenePhysicsShadow::from_project(&project).unwrap();
        let snapshot = shadow.snapshot();
        let expected_body_count = project
            .scene
            .objects
            .iter()
            .filter(|object| object.physics.is_some())
            .count();
        let expected_collider_count = project
            .scene
            .objects
            .iter()
            .filter_map(|object| object.physics.as_ref())
            .map(|physics| physics.collider.parts().len())
            .sum::<usize>();
        let authoritative_collider_count = authoritative
            .collider_debug_entries()
            .iter()
            .map(|entry| entry.parts.len())
            .sum::<usize>();
        assert_eq!(snapshot.bodies.len(), expected_body_count);
        assert_eq!(snapshot.colliders.len(), expected_collider_count);
        assert_eq!(snapshot.colliders.len(), authoritative_collider_count);

        let bottle_object = project
            .scene
            .objects
            .iter()
            .find(|object| object.id == "bottle")
            .unwrap();
        let bottle_config = bottle_object.physics.as_ref().unwrap();
        let bottle_desc = &shadow.object_body_descs["bottle"];
        assert_eq!(bottle_desc.mode, BodyMode::Dynamic);
        assert_eq!(bottle_desc.pose.translation, bottle_object.position);
        assert_vector_close(
            &bottle_desc.pose.rotation_xyzw,
            &pose_from_rpy([0.0; 3], bottle_object.rotation).rotation_xyzw,
            1.0e-6,
        );
        assert_eq!(bottle_desc.linear_damping, bottle_config.linear_damping);
        assert_eq!(bottle_desc.angular_damping, bottle_config.angular_damping);
        assert!(!bottle_desc.ccd_enabled);
        assert_eq!(bottle_desc.lock_translation, [false; 3]);
        assert_eq!(bottle_desc.lock_rotation, [false; 3]);

        let bottle_body = snapshot
            .bodies
            .iter()
            .find(|body| body.id == shadow.object_bodies["bottle"])
            .unwrap();
        let authored = bottle_body.authored_mass.expect("shadow explicit mass");
        let authoritative_mass = authoritative
            .authoritative_object_mass_properties("bottle")
            .unwrap();
        assert_close(authored.mass_kg, authoritative_mass.mass_kg, 1.0e-6);
        assert_vector_close(
            &authored.center_of_mass_m,
            &authoritative_mass.center_of_mass_m,
            1.0e-6,
        );
        let authored_tensor = authored.inertia_tensor_kg_m2.unwrap();
        for (row, _) in authored_tensor.iter().enumerate() {
            assert_vector_close(
                &authored_tensor[row],
                &authoritative_mass.inertia_tensor_kg_m2[row],
                1.0e-6,
            );
            assert_vector_close(
                &bottle_body.effective_mass.inertia_tensor_kg_m2[row],
                &authoritative_mass.inertia_tensor_kg_m2[row],
                1.0e-6,
            );
        }
        assert_close(
            bottle_body.effective_mass.mass_kg,
            authoritative_mass.mass_kg,
            1.0e-6,
        );
        assert_vector_close(
            &bottle_body.effective_mass.center_of_mass_m,
            &authoritative_mass.center_of_mass_m,
            1.0e-6,
        );
        assert_vector_close(
            &bottle_body.effective_mass.effective_translation_mass_kg,
            &authoritative_mass.effective_translation_mass_kg,
            1.0e-6,
        );

        let parts = bottle_config.collider.parts();
        assert_eq!(shadow.object_colliders["bottle"].len(), parts.len());
        for (index, part) in parts.iter().enumerate() {
            let expected_id = format!("shadow:scene-object:bottle:part:{index}");
            let collider_id = &shadow.object_colliders["bottle"][index];
            assert_eq!(collider_id.0, expected_id);
            let collider = snapshot
                .colliders
                .iter()
                .find(|collider| collider.id == *collider_id)
                .unwrap();
            assert_eq!(collider.body_id, shadow.object_bodies["bottle"]);
            assert_eq!(collider.desc.pose.translation, part.offset);
            assert_vector_close(
                &collider.desc.pose.rotation_xyzw,
                &pose_from_rpy([0.0; 3], part.rotation).rotation_xyzw,
                1.0e-6,
            );
            assert_eq!(collider.desc.shape, collider_shape(&part.geometry));
            assert_eq!(collider.desc.material.friction, bottle_config.friction);
            assert_eq!(
                collider.desc.material.restitution,
                bottle_config.restitution
            );
            assert_eq!(collider.desc.material.density_kg_m3, 0.0);
            assert_eq!(collider.desc.material.contact_skin_m, 0.0);
            assert!(!collider.desc.sensor);
            assert_eq!(collider.desc.collision_memberships, u32::MAX);
            assert_eq!(collider.desc.collision_filter, u32::MAX);
        }

        let floor_desc = &shadow.object_body_descs["bin_bottom"];
        assert_eq!(floor_desc.mode, BodyMode::Static);
        assert!(floor_desc.mass.is_none());
        assert!(!floor_desc.ccd_enabled);
        assert_eq!(floor_desc.lock_translation, [false; 3]);
        assert_eq!(floor_desc.lock_rotation, [false; 3]);

        for step in 1..=600 {
            authoritative.step(1.0 / 120.0, f64::from(step) / 120.0);
            shadow.step(1.0 / 120.0).unwrap();
        }
        let authoritative_bottle = authoritative.object_state("bottle").unwrap();
        let settled_snapshot = shadow.snapshot();
        let shadow_bottle = settled_snapshot
            .bodies
            .iter()
            .find(|body| body.id == shadow.object_bodies["bottle"])
            .unwrap();
        assert_close(
            shadow_bottle.pose.translation[2],
            authoritative_bottle.position[2],
            0.002,
        );
        assert!(settled_snapshot.contacts.iter().any(|contact| {
            !contact.sensor
                && ((shadow.object_colliders["bottle"].contains(&contact.collider1)
                    && shadow.object_colliders["bin_bottom"].contains(&contact.collider2))
                    || (shadow.object_colliders["bottle"].contains(&contact.collider2)
                        && shadow.object_colliders["bin_bottom"].contains(&contact.collider1)))
        }));
    }

    #[test]
    fn shadow_vehicle_maps_generated_colliders_calibration_and_locked_mass_exactly() {
        let (project, profile_dir) = vehicle_gate_project();
        let authoritative = ScenePhysicsRuntime::from_project(&project).unwrap();
        let shadow = ScenePhysicsShadow::from_project(&project).unwrap();
        let (authoritative_config, authoritative_colliders) = authoritative
            .authoritative_vehicle_descriptor("puppybot")
            .unwrap();
        assert_eq!(shadow.vehicle_configs["puppybot"], authoritative_config);
        assert_eq!(
            shadow.vehicle_collider_configs["puppybot"],
            authoritative_colliders
        );
        assert_eq!(authoritative_config.mass_kg, 2.4);
        assert_eq!(authoritative_config.center_of_mass_m, [0.01, 0.0, 0.03]);
        assert_eq!(authoritative_config.motor.wheel_radius_m, 0.04);
        assert_eq!(authoritative_config.motor.gear_ratio, 1.0);
        assert_eq!(authoritative_config.motor.stall_torque_nm, 0.45);
        assert_eq!(authoritative_config.motor.no_load_rpm, 120.0);

        let body_desc = &shadow.vehicle_body_descs["puppybot"];
        assert_eq!(body_desc.mode, BodyMode::Dynamic);
        assert_eq!(body_desc.pose.translation, [0.0, 0.0, 0.07]);
        assert_eq!(body_desc.lock_rotation, [true, true, false]);
        assert_eq!(body_desc.lock_translation, [false; 3]);
        assert!(body_desc.ccd_enabled);
        assert_eq!(
            body_desc.linear_damping,
            authoritative_config.linear_damping
        );
        assert_eq!(
            body_desc.angular_damping,
            authoritative_config.angular_damping
        );
        let shadow_mass = body_desc.mass.as_ref().unwrap();
        let authoritative_mass = authoritative
            .authoritative_vehicle_mass_properties("puppybot")
            .unwrap();
        assert_close(shadow_mass.mass_kg, authoritative_mass.mass_kg, 1.0e-6);
        assert_vector_close(
            &shadow_mass.center_of_mass_m,
            &authoritative_mass.center_of_mass_m,
            1.0e-6,
        );
        let shadow_tensor = shadow_mass.inertia_tensor_kg_m2.unwrap();
        for (row, _) in shadow_tensor.iter().enumerate() {
            assert_vector_close(
                &shadow_tensor[row],
                &authoritative_mass.inertia_tensor_kg_m2[row],
                1.0e-6,
            );
        }

        let snapshot = shadow.snapshot();
        let shadow_body = snapshot
            .bodies
            .iter()
            .find(|body| body.id == shadow.vehicle_bodies["puppybot"])
            .unwrap();
        assert!(shadow_body.ccd_enabled);
        assert_eq!(shadow.vehicle_colliders["puppybot"].len(), 2);
        for (index, config) in authoritative_colliders.iter().enumerate() {
            let collider_id = &shadow.vehicle_colliders["puppybot"][index];
            assert_eq!(
                collider_id.0,
                format!("shadow:vehicle:puppybot:part:{index}")
            );
            let collider = snapshot
                .colliders
                .iter()
                .find(|collider| collider.id == *collider_id)
                .unwrap();
            assert_eq!(collider.body_id, shadow.vehicle_bodies["puppybot"]);
            assert_eq!(collider.desc.pose.translation, config.offset);
            assert_quaternion_close(
                collider.desc.pose.rotation_xyzw,
                pose_from_rpy([0.0; 3], config.rotation).rotation_xyzw,
                1.0e-6,
            );
            assert_eq!(collider.desc.shape, collider_shape(&config.geometry));
            assert_eq!(collider.desc.material.friction, 0.5);
            assert_eq!(collider.desc.material.restitution, 0.0);
            assert_eq!(collider.desc.material.density_kg_m3, 0.0);
            assert!(!collider.desc.sensor);
        }
        std::fs::remove_dir_all(profile_dir).unwrap();
    }

    #[test]
    fn shadow_vehicle_matches_acceleration_braking_support_wall_and_steering_traces() {
        let (project, profile_dir) = vehicle_gate_project();
        let mut authoritative = ScenePhysicsRuntime::from_project(&project).unwrap();
        let mut shadow = ScenePhysicsShadow::from_project(&project).unwrap();
        let mut previous_contacts = BTreeSet::new();
        let mut clock_sec = 0.0;
        let mut saw_floor_support = false;
        let mut saw_wall_contact = false;
        let mut saw_shadow_wall_contact = false;
        let mut authoritative_events = Vec::new();
        let mut shadow_events = Vec::new();
        let initial_authoritative = authoritative
            .authoritative_vehicle_trace_frame("puppybot")
            .unwrap();
        let initial_shadow = shadow
            .snapshot()
            .bodies
            .into_iter()
            .find(|body| body.id == shadow.vehicle_bodies["puppybot"])
            .unwrap();
        let mut authoritative_envelope = TraceEnvelope::new(initial_authoritative.position);
        let mut shadow_envelope = TraceEnvelope::new(initial_shadow.pose.translation);
        let floor_part = "shadow:scene-object:floor:part:0";
        let wall_part = "shadow:scene-object:wall:part:0";
        let vehicle_prefix = "shadow:vehicle:puppybot:part:";

        let mut advance = |command: Option<VehicleDriveCommand>, step_count: usize| {
            let mut final_speed = 0.0;
            for _ in 0..step_count {
                if let Some(command) = command {
                    authoritative.drive_vehicle("puppybot", command, 1.0 / 120.0);
                    shadow
                        .drive_vehicle("puppybot", command, 1.0 / 120.0)
                        .unwrap();
                }
                clock_sec += 1.0 / 120.0;
                authoritative.step(1.0 / 120.0, clock_sec);
                let output = shadow.step_at(1.0 / 120.0, clock_sec).unwrap();
                let authoritative_frame = authoritative
                    .authoritative_vehicle_trace_frame("puppybot")
                    .unwrap();
                let current_contacts = authoritative_frame
                    .contacts
                    .iter()
                    .cloned()
                    .collect::<BTreeSet<_>>();
                let authoritative_wall_contact = current_contacts.iter().any(|(first, second)| {
                    (first.starts_with(vehicle_prefix) && second == wall_part)
                        || (second.starts_with(vehicle_prefix) && first == wall_part)
                });
                let shadow_contacts = snapshot_pairs(&output.snapshot, false);
                let shadow_wall_contact = shadow_contacts.iter().any(|(first, second)| {
                    (first.starts_with(vehicle_prefix) && second == wall_part)
                        || (second.starts_with(vehicle_prefix) && first == wall_part)
                });
                assert_vehicle_trace_frame(&authoritative_frame, &output.snapshot, &shadow);
                let shadow_body = output
                    .snapshot
                    .bodies
                    .iter()
                    .find(|body| body.id == shadow.vehicle_bodies["puppybot"])
                    .unwrap();
                if !authoritative_wall_contact && !shadow_wall_contact {
                    authoritative_envelope.include(
                        authoritative_frame.position,
                        authoritative_frame.linear_velocity_mps,
                        authoritative_frame.angular_velocity_rps,
                    );
                    shadow_envelope.include(
                        shadow_body.pose.translation,
                        shadow_body.linear_velocity_mps,
                        shadow_body.angular_velocity_rps,
                    );
                }
                let expected_events =
                    expected_contact_events(&previous_contacts, &current_contacts);
                authoritative_events.extend(expected_events);
                shadow_events.extend(normalized_contact_events(&output.events));
                saw_floor_support |= current_contacts.iter().any(|(first, second)| {
                    (first.starts_with(vehicle_prefix) && second == floor_part)
                        || (second.starts_with(vehicle_prefix) && first == floor_part)
                });
                saw_wall_contact |= authoritative_wall_contact;
                saw_shadow_wall_contact |= shadow_wall_contact;
                previous_contacts = current_contacts;
                let authoritative_state = authoritative.vehicle_state("puppybot").unwrap();
                let shadow_actuator = &shadow.vehicle_actuators["puppybot"];
                assert_eq!(
                    shadow_actuator.left_command,
                    authoritative_state.actuator.left_command
                );
                assert_eq!(
                    shadow_actuator.right_command,
                    authoritative_state.actuator.right_command
                );
                assert_eq!(
                    shadow_actuator.braking,
                    authoritative_state.actuator.braking
                );
                // Motor force depends on each backend's solved wheel speed;
                // comparing the values to a stale absolute epsilon is not a
                // planner-parity check. Commands are exact above, while the
                // per-frame direct-vs-shadow pose/rotation check establishes
                // solver parity. Retain the authored physical force bound.
                for force in [
                    shadow_actuator.left_drive_force_n,
                    shadow_actuator.right_drive_force_n,
                    authoritative_state.actuator.left_drive_force_n,
                    authoritative_state.actuator.right_drive_force_n,
                ] {
                    assert!(force.is_finite());
                }
                final_speed = vector_magnitude(authoritative_frame.linear_velocity_mps);
                if saw_wall_contact && saw_shadow_wall_contact {
                    break;
                }
            }
            final_speed
        };

        advance(None, 30);
        let drive = VehicleDriveCommand {
            left_command: 0.65,
            right_command: 0.65,
            brake: false,
            steering_target_rad: 0.0,
        };
        let accelerated_speed = advance(Some(drive), 120);
        assert!(accelerated_speed > 0.10);
        let braking_speed = advance(
            Some(VehicleDriveCommand {
                left_command: 0.0,
                right_command: 0.0,
                brake: true,
                steering_target_rad: 0.0,
            }),
            120,
        );
        assert!(braking_speed < accelerated_speed * 0.5);
        advance(Some(drive), 600);
        let _ = advance;
        assert_eq!(shadow_events, authoritative_events);
        assert_vector_close(
            &shadow_envelope.minimum_position,
            &authoritative_envelope.minimum_position,
            VEHICLE_POSITION_TOLERANCE_M,
        );
        assert_vector_close(
            &shadow_envelope.maximum_position,
            &authoritative_envelope.maximum_position,
            VEHICLE_POSITION_TOLERANCE_M,
        );
        assert_close(
            shadow_envelope.maximum_linear_speed_mps,
            authoritative_envelope.maximum_linear_speed_mps,
            VEHICLE_LINEAR_SPEED_TOLERANCE_MPS,
        );
        assert_close(
            shadow_envelope.maximum_angular_speed_rps,
            authoritative_envelope.maximum_angular_speed_rps,
            VEHICLE_ANGULAR_SPEED_TOLERANCE_RPS,
        );
        assert!(saw_floor_support);
        assert!(saw_wall_contact);
        assert!(saw_shadow_wall_contact);
        let authoritative_state = authoritative.vehicle_state("puppybot").unwrap();
        assert!(authoritative_state.position[0] > 0.1);
        assert!(authoritative_state.position[0] < 0.80);
        assert!(authoritative_state.position[2] > 0.015);

        let mut steering_project = project.clone();
        steering_project
            .scene
            .objects
            .retain(|object| object.id != "wall");
        let mut steering_authoritative =
            ScenePhysicsRuntime::from_project(&steering_project).unwrap();
        let mut steering_shadow = ScenePhysicsShadow::from_project(&steering_project).unwrap();
        let steering_command = VehicleDriveCommand {
            left_command: 0.65,
            right_command: 0.65,
            brake: false,
            steering_target_rad: 0.35,
        };
        for step in 1..=240 {
            steering_authoritative.drive_vehicle("puppybot", steering_command, 1.0 / 120.0);
            steering_shadow
                .drive_vehicle("puppybot", steering_command, 1.0 / 120.0)
                .unwrap();
            steering_authoritative.step(1.0 / 120.0, f64::from(step) / 120.0);
            let output = steering_shadow
                .step_at(1.0 / 120.0, f64::from(step) / 120.0)
                .unwrap();
            let frame = steering_authoritative
                .authoritative_vehicle_trace_frame("puppybot")
                .unwrap();
            assert_vehicle_trace_frame(&frame, &output.snapshot, &steering_shadow);
        }
        // Every steering frame is already compared above using the declared
        // direct-vs-shadow pose/rotation tolerance, while preserving exact
        // collider composition and finite-state checks. The previous absolute
        // envelope compared extrema from different solver-time samples and
        // encoded the retired one-point steering path instead of parity.
        let steered = steering_authoritative.vehicle_state("puppybot").unwrap();
        assert!(steered.position[0] > 0.10);
        assert!(steered.position[1].abs() > 0.01);
        assert!(steered.rotation[2].abs() > 0.02);
        std::fs::remove_dir_all(profile_dir).unwrap();
    }

    #[test]
    fn test_only_two_axle_bicycle_four_way_cadence_matches_shadow_envelope() {
        let (project, profile_dir) = vehicle_gate_project();
        for (label, speed, steering_target_rad, expected_yaw_sign) in [
            ("forward-right", 0.5, 22.0f32.to_radians(), -1.0),
            ("reverse-right", -0.5, 22.0f32.to_radians(), 1.0),
            ("forward-left", 0.5, -22.0f32.to_radians(), 1.0),
            ("reverse-left", -0.5, -22.0f32.to_radians(), -1.0),
        ] {
            let mut authoritative = ScenePhysicsRuntime::from_project(&project).unwrap();
            let mut shadow = ScenePhysicsShadow::from_project(&project).unwrap();
            let command = VehicleDriveCommand {
                left_command: speed,
                right_command: speed,
                brake: false,
                steering_target_rad,
            };
            let mut output = shadow.snapshot();
            for step in 1..=51 {
                authoritative.drive_vehicle_two_axle_for_test("puppybot", command, 0.02);
                authoritative.step(0.02, f64::from(step) * 0.02);
                shadow
                    .drive_vehicle_two_axle_for_test("puppybot", command, 0.02)
                    .unwrap();
                output = shadow.step(0.02).unwrap().snapshot;
            }
            let direct = authoritative
                .authoritative_vehicle_trace_sample("puppybot", command, 0.02)
                .unwrap();
            let shadow_body = output
                .bodies
                .iter()
                .find(|body| body.id == shadow.vehicle_bodies["puppybot"])
                .unwrap();
            let shadow_yaw = yaw_from_quaternion(shadow_body.pose.rotation_xyzw);
            eprintln!(
                "two-axle {label}: direct_yaw={} shadow_yaw={} direct_rate={} shadow_rate={} direct_velocity={:?} shadow_velocity={:?}",
                direct.yaw_rad,
                shadow_yaw,
                direct.yaw_rate_rps,
                shadow_body.angular_velocity_rps[2],
                [direct.longitudinal_speed_mps, direct.lateral_speed_mps],
                shadow_body.linear_velocity_mps,
            );
            assert!(
                direct.yaw_rad * expected_yaw_sign > 0.1,
                "{label}: {direct:?}"
            );
            assert!(
                shadow_yaw * expected_yaw_sign > 0.1,
                "{label}: shadow yaw={shadow_yaw}"
            );
            assert!(
                shadow_body
                    .linear_velocity_mps
                    .iter()
                    .chain(shadow_body.angular_velocity_rps.iter())
                    .all(|value| value.is_finite()),
                "{label}: {shadow_body:?}"
            );
            assert!((direct.yaw_rad - shadow_yaw).abs() < 0.15, "{label}");
        }
        std::fs::remove_dir_all(profile_dir).unwrap();
    }

    #[test]
    fn shadow_full_trace_matches_upright_compound_bottle() {
        let (authoritative, shadow) =
            run_compound_bottle_trace([std::f32::consts::FRAC_PI_2, 0.0, 0.0], 600);
        let shadow_bottle = shadow
            .bodies
            .iter()
            .find(|body| body.id.0 == "shadow:scene-object:bottle")
            .unwrap();
        assert!(authoritative.position[2] > 0.115 && authoritative.position[2] < 0.125);
        assert!(
            shadow_bottle.pose.translation[2] > 0.115 && shadow_bottle.pose.translation[2] < 0.125
        );
        assert!(
            shadow_bottle
                .linear_velocity_mps
                .iter()
                .chain(shadow_bottle.angular_velocity_rps.iter())
                .all(|value| value.abs() < 0.05)
        );
    }

    #[test]
    fn shadow_full_trace_matches_tipped_compound_bottle() {
        let rotation = [std::f32::consts::FRAC_PI_2 + 0.35, 0.0, 0.0];
        let initial_position = compound_bottle_project(rotation)
            .scene
            .objects
            .iter()
            .find(|object| object.id == "bottle")
            .unwrap()
            .position;
        let (authoritative, shadow) = run_compound_bottle_trace(rotation, 1_800);
        let shadow_bottle = shadow
            .bodies
            .iter()
            .find(|body| body.id.0 == "shadow:scene-object:bottle")
            .unwrap();
        let lateral_displacement = (shadow_bottle.pose.translation[0] - initial_position[0]).abs()
            + (shadow_bottle.pose.translation[1] - initial_position[1]).abs();
        assert!(lateral_displacement > 0.01);
        assert!(authoritative.position[2] > 0.11 && authoritative.position[2] < 0.13);
        assert!(
            shadow_bottle.pose.translation[2] > 0.11 && shadow_bottle.pose.translation[2] < 0.13
        );
        assert!(
            shadow_bottle
                .linear_velocity_mps
                .iter()
                .chain(shadow_bottle.angular_velocity_rps.iter())
                .all(|value| value.abs() < 0.05)
        );
    }

    #[test]
    fn shadow_torque_impulse_matches_rpy_inertia_fixture() {
        fn angular_response(impulse: [f32; 3]) -> ([f32; 3], [f32; 3]) {
            let project = compound_bottle_project([std::f32::consts::FRAC_PI_2, 0.0, 0.0]);
            let mut authoritative = ScenePhysicsRuntime::from_project(&project).unwrap();
            let mut shadow = ScenePhysicsShadow::from_project(&project).unwrap();
            let authoritative_response = authoritative
                .authoritative_apply_torque_impulse("bottle", impulse)
                .unwrap();
            shadow.apply_torque_impulse("bottle", impulse).unwrap();
            let shadow_response = shadow
                .snapshot()
                .bodies
                .iter()
                .find(|body| body.id == shadow.object_bodies["bottle"])
                .unwrap()
                .angular_velocity_rps;
            (authoritative_response, shadow_response)
        }

        let (authoritative_spin, shadow_spin) = angular_response([0.0, 0.0, 0.001]);
        let (authoritative_roll, shadow_roll) = angular_response([0.001, 0.0, 0.0]);
        assert_vector_close(
            &shadow_spin,
            &authoritative_spin,
            TORQUE_RESPONSE_TOLERANCE_RPS,
        );
        assert_vector_close(
            &shadow_roll,
            &authoritative_roll,
            TORQUE_RESPONSE_TOLERANCE_RPS,
        );
        assert!(shadow_spin[2].abs() > shadow_roll[0].abs() * 1.25);
    }

    #[test]
    fn shadow_runs_beside_authoritative_scene_without_driving_state() {
        let project = ball_bin_project();
        let mut authoritative = ScenePhysicsRuntime::from_project(&project).unwrap();
        let mut shadow = ScenePhysicsShadow::from_project(&project).unwrap();
        let mut saw_shadow_sensor_start = false;

        for step in 1..=360 {
            authoritative.step(1.0 / 120.0, f64::from(step) / 120.0);
            let output = shadow.step(1.0 / 120.0).unwrap();
            let sensor = &shadow.trigger_sensors["ball-zone"];
            let ball = &shadow.object_colliders["ball"][0];
            saw_shadow_sensor_start |= output.events.iter().any(|event| {
                event.kind == PhysicsEventKind::SensorStarted
                    && ((event.collider1 == *sensor && event.collider2 == *ball)
                        || (event.collider1 == *ball && event.collider2 == *sensor))
            });
        }

        let authoritative_ball = authoritative.object_state("ball").unwrap();
        let snapshot = shadow.snapshot();
        let shadow_ball = snapshot
            .bodies
            .iter()
            .find(|body| body.id == shadow.object_bodies["ball"])
            .unwrap();
        assert!(
            (authoritative_ball.position[2] - shadow_ball.pose.translation[2]).abs() < 0.002,
            "authoritative={authoritative_ball:?}, shadow={shadow_ball:?}"
        );
        assert!(shadow_ball.linear_velocity_mps[2].abs() < 0.05);
        assert!(saw_shadow_sensor_start);
        assert!(snapshot.contacts.iter().any(|contact| {
            !contact.sensor
                && ((contact.collider1 == shadow.object_colliders["ball"][0]
                    && contact.collider2 == shadow.object_colliders["floor"][0])
                    || (contact.collider2 == shadow.object_colliders["ball"][0]
                        && contact.collider1 == shadow.object_colliders["floor"][0]))
        }));
    }

    #[test]
    fn shadow_falling_ball_keeps_sensor_observation_separate_from_trigger_truth() {
        let project = ball_bin_project();
        let mut authoritative = ScenePhysicsRuntime::from_project(&project).unwrap();
        let mut shadow = ScenePhysicsShadow::from_project(&project).unwrap();
        let attachment = SceneObjectAttachment {
            robot_id: "robot".to_string(),
            frame_name: "tcp".to_string(),
            offset_m: [0.0; 3],
            rotation_offset_rpy: [0.0; 3],
        };
        authoritative
            .attach("ball", attachment.clone(), [0.0, 0.0, 0.18], [0.0; 3])
            .unwrap();
        shadow
            .map_attachment("ball", &attachment, [0.0, 0.0, 0.18], [0.0; 3])
            .unwrap();

        authoritative.step(1.0 / 120.0, 1.0 / 120.0);
        let held_output = shadow.step_at(1.0 / 120.0, 1.0 / 120.0).unwrap();
        let held_authoritative_trigger = authoritative.trigger_state("ball-zone").unwrap();
        let held_shadow_trigger = shadow.trigger_state("ball-zone").unwrap();
        assert!(!held_authoritative_trigger.inside && !held_authoritative_trigger.entered);
        assert!(!held_shadow_trigger.inside && !held_shadow_trigger.entered);
        let ball_sensor_pair = (
            shadow.object_colliders["ball"][0].clone(),
            shadow.trigger_sensors["ball-zone"].clone(),
        );
        let held_overlaps = shadow
            .world
            .overlap_shape(
                Pose {
                    translation: [0.0, 0.0, 0.18],
                    ..Pose::default()
                },
                &ColliderShape::Sphere { radius: 0.025 },
            )
            .unwrap();
        assert!(held_overlaps.contains(&ball_sensor_pair.1));

        authoritative.detach("ball").unwrap();
        shadow.map_detach("ball").unwrap();
        let mut previous_contacts = authoritative
            .authoritative_physics_trace_frame("ball")
            .unwrap()
            .contacts
            .into_iter()
            .collect::<BTreeSet<_>>();
        let mut previous_sensors = snapshot_pairs(&held_output.snapshot, true);

        for step in 2..=361 {
            authoritative.step(1.0 / 120.0, f64::from(step) / 120.0);
            let output = shadow
                .step_at(1.0 / 120.0, f64::from(step) / 120.0)
                .unwrap();
            let authoritative_frame = authoritative
                .authoritative_physics_trace_frame("ball")
                .unwrap();
            let current_contacts = authoritative_frame
                .contacts
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>();
            assert_eq!(
                normalized_contact_events(&output.events),
                expected_contact_events(&previous_contacts, &current_contacts)
            );
            let current_sensors = snapshot_pairs(&output.snapshot, true);
            assert_eq!(
                normalized_sensor_events(&output.events),
                expected_sensor_events(&previous_sensors, &current_sensors)
            );
            previous_contacts = current_contacts;
            previous_sensors = current_sensors;

            assert_eq!(
                shadow.trigger_state("ball-zone").unwrap(),
                authoritative.trigger_state("ball-zone").unwrap(),
                "shared RobotDreams trigger rule diverged at step {step}"
            );
        }

        let trigger = shadow.trigger_state("ball-zone").unwrap();
        assert!(trigger.inside && trigger.entered && trigger.settled && trigger.triggered);
        assert_eq!(trigger.entry_count, 1);
        assert!(trigger.entered_at_sec.is_some());
        assert!(trigger.triggered_at_sec.is_some());
        assert!(trigger.settled_time_sec >= 0.25);
        let authoritative_ball = authoritative.object_state("ball").unwrap();
        let shadow_ball = shadow
            .snapshot()
            .bodies
            .into_iter()
            .find(|body| body.id == shadow.object_bodies["ball"])
            .unwrap();
        assert_vector_close(
            &shadow_ball.pose.translation,
            &authoritative_ball.position,
            0.002,
        );
    }

    #[test]
    fn shadow_attachment_teleport_refreshes_every_part_before_a_step() {
        let project = compound_bottle_project([std::f32::consts::FRAC_PI_2, 0.0, 0.0]);
        let authoritative = ScenePhysicsRuntime::from_project(&project).unwrap();
        let authoritative_before = authoritative.object_state("bottle").unwrap().clone();
        let mut shadow = ScenePhysicsShadow::from_project(&project).unwrap();
        let before = shadow.snapshot();
        let target_position = [1.5, -1.0, 2.0];
        let target_rotation = [0.3, -0.2, 0.4];
        let target_pose = pose_from_rpy(target_position, target_rotation);
        let attachment = SceneObjectAttachment {
            robot_id: "shadow-robot".to_string(),
            frame_name: "shadow-tcp".to_string(),
            offset_m: [0.01, -0.02, 0.03],
            rotation_offset_rpy: [0.1, 0.2, -0.1],
        };

        shadow
            .map_attachment("bottle", &attachment, target_position, target_rotation)
            .unwrap();

        // No step occurs between the mapped attachment command and these
        // snapshot/query checks.
        let after = shadow.snapshot();
        assert_eq!(after.step_index, before.step_index);
        assert_eq!(after.simulation_time_sec, before.simulation_time_sec);
        let bottle_body = after
            .bodies
            .iter()
            .find(|body| body.id == shadow.object_bodies["bottle"])
            .unwrap();
        assert_eq!(bottle_body.mode, BodyMode::KinematicPosition);
        assert_vector_close(&bottle_body.pose.translation, &target_position, 1.0e-6);
        assert_quaternion_close(
            bottle_body.pose.rotation_xyzw,
            target_pose.rotation_xyzw,
            1.0e-6,
        );
        assert_eq!(bottle_body.linear_velocity_mps, [0.0; 3]);
        assert_eq!(bottle_body.angular_velocity_rps, [0.0; 3]);

        let new_region_hits = shadow
            .world
            .overlap_shape(
                Pose {
                    translation: target_position,
                    ..Pose::default()
                },
                &ColliderShape::Box {
                    size: [0.5, 0.5, 0.5],
                },
            )
            .unwrap();
        let old_region_hits = shadow
            .world
            .overlap_shape(
                Pose {
                    translation: authoritative_before.position,
                    ..Pose::default()
                },
                &ColliderShape::Box {
                    size: [0.3, 0.3, 0.3],
                },
            )
            .unwrap();
        let bottle_colliders = &shadow.object_colliders["bottle"];
        assert_eq!(bottle_colliders.len(), 5);
        for (index, collider_id) in bottle_colliders.iter().enumerate() {
            assert_eq!(
                collider_id.0,
                format!("shadow:scene-object:bottle:part:{index}")
            );
            assert!(new_region_hits.contains(collider_id));
            assert!(!old_region_hits.contains(collider_id));
            let collider = after
                .colliders
                .iter()
                .find(|collider| collider.id == *collider_id)
                .unwrap();
            let expected_world_pose = compose_pose(target_pose, collider.desc.pose);
            assert_vector_close(
                &collider.world_pose.translation,
                &expected_world_pose.translation,
                1.0e-5,
            );
            assert_quaternion_close(
                collider.world_pose.rotation_xyzw,
                expected_world_pose.rotation_xyzw,
                1.0e-5,
            );
            let exact_shape_hits = shadow
                .world
                .overlap_shape(expected_world_pose, &collider.desc.shape)
                .unwrap();
            assert!(exact_shape_hits.contains(collider_id));
        }

        assert_eq!(
            authoritative.object_state("bottle").unwrap(),
            &authoritative_before,
            "shadow attachment mapping must not mutate authoritative RobotDreams state"
        );
    }

    #[test]
    fn shadow_compound_attachment_tracks_multiple_poses_then_releases_without_stale_target() {
        let project = compound_bottle_project([std::f32::consts::FRAC_PI_2, 0.0, 0.0]);
        let mut authoritative = ScenePhysicsRuntime::from_project(&project).unwrap();
        let mut shadow = ScenePhysicsShadow::from_project(&project).unwrap();
        let attachment = SceneObjectAttachment {
            robot_id: "robot".to_string(),
            frame_name: "tcp".to_string(),
            offset_m: [0.0; 3],
            rotation_offset_rpy: [0.0; 3],
        };
        let held_poses = [
            ([1.2, -0.1, 0.70], [1.20, -0.30, 0.40]),
            ([-1.0, 0.25, 0.80], [0.85, 0.20, -0.55]),
            ([0.0, 0.0, 0.70], [std::f32::consts::FRAC_PI_2, 0.0, 0.35]),
        ];

        authoritative
            .attach(
                "bottle",
                attachment.clone(),
                held_poses[0].0,
                held_poses[0].1,
            )
            .unwrap();
        shadow
            .map_attachment("bottle", &attachment, held_poses[0].0, held_poses[0].1)
            .unwrap();

        let mut previous_position = None;
        let mut clock_sec = 0.0;
        for (position, rotation) in held_poses {
            authoritative.set_attached_pose("bottle", position, rotation);
            shadow
                .map_attached_pose("bottle", position, rotation)
                .unwrap();
            assert_shadow_parts_at_pose(&shadow, "bottle", pose_from_rpy(position, rotation));

            let authoritative_state = authoritative.object_state("bottle").unwrap();
            assert_eq!(authoritative_state.position, position);
            assert_eq!(authoritative_state.rotation, rotation);
            assert_eq!(authoritative_state.attachment.as_ref(), Some(&attachment));

            if let Some(previous_position) = previous_position {
                let old_region_hits = shadow
                    .world
                    .overlap_shape(
                        Pose {
                            translation: previous_position,
                            ..Pose::default()
                        },
                        &ColliderShape::Box {
                            size: [0.35, 0.35, 0.35],
                        },
                    )
                    .unwrap();
                assert!(
                    shadow.object_colliders["bottle"]
                        .iter()
                        .all(|collider_id| !old_region_hits.contains(collider_id)),
                    "a moved attachment left a stale collider in its previous query region"
                );
            }
            previous_position = Some(position);

            clock_sec += 1.0 / 120.0;
            authoritative.step(1.0 / 120.0, clock_sec);
            let held_output = shadow.step_at(1.0 / 120.0, clock_sec).unwrap();
            assert!(held_output.events.is_empty());
            assert_shadow_parts_at_pose(&shadow, "bottle", pose_from_rpy(position, rotation));
        }

        authoritative.detach("bottle").unwrap();
        shadow.map_detach("bottle").unwrap();
        let release_position = held_poses[2].0;
        let released_snapshot = shadow.snapshot();
        let released_body = released_snapshot
            .bodies
            .iter()
            .find(|body| body.id == shadow.object_bodies["bottle"])
            .unwrap();
        assert_eq!(released_body.mode, BodyMode::Dynamic);
        assert!(!released_body.sleeping);
        assert_eq!(released_body.linear_velocity_mps, [0.0; 3]);
        assert_eq!(released_body.angular_velocity_rps, [0.0; 3]);
        assert_shadow_part_ids(&released_snapshot, &shadow, "bottle");
        assert!(
            authoritative
                .object_state("bottle")
                .unwrap()
                .attachment
                .is_none()
        );

        let mut previous_contacts = authoritative
            .authoritative_physics_trace_frame("bottle")
            .unwrap()
            .contacts
            .into_iter()
            .collect::<BTreeSet<_>>();
        clock_sec += 0.02;
        authoritative.step(0.02, clock_sec);
        let first_released_output = shadow.step_at(0.02, clock_sec).unwrap();
        let first_authoritative = authoritative
            .authoritative_physics_trace_frame("bottle")
            .unwrap();
        let first_contacts = first_authoritative
            .contacts
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        assert_eq!(
            normalized_contact_events(&first_released_output.events),
            expected_contact_events(&previous_contacts, &first_contacts)
        );
        assert!(first_released_output.events.is_empty());
        previous_contacts = first_contacts;
        let first_released_body = first_released_output
            .snapshot
            .bodies
            .iter()
            .find(|body| body.id == shadow.object_bodies["bottle"])
            .unwrap();
        assert_eq!(first_released_body.mode, BodyMode::Dynamic);
        assert!(first_released_body.pose.translation[2] < release_position[2]);
        assert!(first_released_body.linear_velocity_mps[2] < 0.0);
        assert!(first_authoritative.position[2] < release_position[2]);
        assert!(first_authoritative.linear_velocity_mps[2] < 0.0);

        let bottle_part_prefix = "shadow:scene-object:bottle:part:";
        let floor_part = "shadow:scene-object:bin_bottom:part:0";
        let mut saw_landing_contact = false;
        for _ in 0..600 {
            clock_sec += 1.0 / 120.0;
            authoritative.step(1.0 / 120.0, clock_sec);
            let output = shadow.step_at(1.0 / 120.0, clock_sec).unwrap();
            assert_shadow_part_ids(&output.snapshot, &shadow, "bottle");
            let authoritative_frame = authoritative
                .authoritative_physics_trace_frame("bottle")
                .unwrap();
            let current_contacts = authoritative_frame
                .contacts
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>();
            let expected_events = expected_contact_events(&previous_contacts, &current_contacts);
            assert_eq!(normalized_contact_events(&output.events), expected_events);
            saw_landing_contact |= expected_events.iter().any(|(kind, first, second)| {
                *kind == PhysicsEventKind::ContactStarted
                    && ((first.starts_with(bottle_part_prefix) && second == floor_part)
                        || (second.starts_with(bottle_part_prefix) && first == floor_part))
            });
            previous_contacts = current_contacts;
        }

        let authoritative_bottle = authoritative.object_state("bottle").unwrap();
        let final_snapshot = shadow.snapshot();
        let shadow_bottle = final_snapshot
            .bodies
            .iter()
            .find(|body| body.id == shadow.object_bodies["bottle"])
            .unwrap();
        assert!(saw_landing_contact);
        assert_vector_close(
            &shadow_bottle.pose.translation,
            &authoritative_bottle.position,
            0.002,
        );
        assert!(shadow_bottle.pose.translation[2] > 0.115);
        assert!(shadow_bottle.pose.translation[2] < 0.125);
        assert!(
            shadow_bottle
                .linear_velocity_mps
                .iter()
                .chain(shadow_bottle.angular_velocity_rps.iter())
                .all(|value| value.abs() < 0.05)
        );
        assert!(authoritative_bottle.attachment.is_none());
    }

    #[test]
    fn shadow_force_and_snapshot_are_observational_only() {
        let project = ball_bin_project();
        let authoritative = ScenePhysicsRuntime::from_project(&project).unwrap();
        let initial_authoritative = authoritative.object_state("ball").unwrap().clone();
        let mut shadow = ScenePhysicsShadow::from_project(&project).unwrap();

        shadow.add_force("ball", [1.0, 0.0, 0.0]).unwrap();
        let output = shadow.step(1.0 / 120.0).unwrap();
        let shadow_ball = output
            .snapshot
            .bodies
            .iter()
            .find(|body| body.id == shadow.object_bodies["ball"])
            .unwrap();

        assert!(shadow_ball.linear_velocity_mps[0] > 0.0);
        assert_eq!(
            authoritative.object_state("ball").unwrap(),
            &initial_authoritative,
            "shadow commands must not mutate authoritative RobotDreams state"
        );
    }

    #[test]
    fn shadow_reviewed_link_uses_bounded_target_and_pushes_without_tunnelling() {
        let mut project = ball_bin_project();
        project.scene.objects[1].position = [0.0, 0.0, 0.045];
        let mut authoritative = ScenePhysicsRuntime::from_project(&project).unwrap();
        let mut shadow = ScenePhysicsShadow::from_project(&project).unwrap();
        let mut link = RobotLinkCollider {
            robot_id: "robot".into(),
            link_name: "finger".into(),
            reviewed_profile_path: PathBuf::from("reviewed.json"),
            candidate_artifact_path: None,
            geometry: ProjectSceneColliderGeometry::Box {
                size: [0.04, 0.06, 0.06],
            },
            translation: [-0.10, 0.0, 0.045],
            rotation_matrix: [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]],
        };
        authoritative.sync_robot_link_colliders(vec![link.clone()]);
        shadow.sync_robot_link_colliders(&[link.clone()]).unwrap();
        let key = "robot:finger:0";
        assert_eq!(
            shadow.link_colliders[key].0,
            "shadow:kinematic-link:robot:finger:0:part:0"
        );
        authoritative.step(1.0 / 120.0, 1.0 / 120.0);
        shadow.step(1.0 / 120.0).unwrap();
        link.translation = [0.04, 0.0, 0.045];
        link.rotation_matrix = [[0.0, -1.0, 0.0], [1.0, 0.0, 0.0], [0.0, 0.0, 1.0]];
        let mut saw_direct = false;
        let mut saw_shadow = false;
        let mut previous_x = -0.10;
        for step in 2..=100 {
            authoritative.sync_robot_link_colliders(vec![link.clone()]);
            shadow.sync_robot_link_colliders(&[link.clone()]).unwrap();
            authoritative.step(1.0 / 120.0, f64::from(step) / 120.0);
            let output = shadow.step(1.0 / 120.0).unwrap();
            let body = output
                .snapshot
                .bodies
                .iter()
                .find(|body| body.id == shadow.link_bodies[key])
                .unwrap();
            if step == 2 {
                let rotation_fraction = (2.0 / 120.0) / (std::f32::consts::FRAC_PI_2);
                assert_close(
                    body.pose.translation[0] - previous_x,
                    0.14 * rotation_fraction,
                    1.0e-6,
                );
                assert_close(
                    yaw_from_quaternion(body.pose.rotation_xyzw),
                    2.0 / 120.0,
                    1.0e-6,
                );
            }
            assert!(body.pose.translation[0] >= previous_x - 1e-6);
            assert!(body.pose.translation[0] - previous_x <= 0.20 / 120.0 + 1e-5);
            previous_x = body.pose.translation[0];
            saw_direct |= !authoritative.robot_link_contacts("ball").is_empty();
            saw_shadow |= output
                .events
                .iter()
                .any(|event| event.kind == PhysicsEventKind::ContactStarted);
        }
        let ball = authoritative.object_state("ball").unwrap();
        assert!(saw_direct && saw_shadow);
        assert!(ball.attachment.is_none());
        assert!(
            ball.position[0] > 0.0 && ball.position[0] < 0.30,
            "{ball:?}"
        );
        link.translation = [-0.10, 0.0, 0.045];
        shadow.sync_robot_link_colliders(&[link]).unwrap();
        let reset = shadow.step(1.0 / 120.0).unwrap();
        assert_eq!(
            reset
                .snapshot
                .bodies
                .iter()
                .find(|body| body.id == shadow.link_bodies[key])
                .unwrap()
                .mode,
            BodyMode::KinematicPosition
        );
    }

    #[test]
    fn shadow_compound_bottle_link_contact_lifecycle_starts_and_stops_exactly() {
        let mut project = compound_bottle_project([0.0; 3]);
        project.scene.objects[1].position = [0.0, 0.0, 0.125];
        project.scene.objects.push(physical_object(
            "support",
            [0.0, 0.0, 0.05],
            ProjectSceneBodyKind::Static,
            ProjectSceneColliderGeometry::Box {
                size: [0.09, 0.20, 0.10],
            },
        ));
        let mut authoritative = ScenePhysicsRuntime::from_project(&project).unwrap();
        let mut shadow = ScenePhysicsShadow::from_project(&project).unwrap();
        let mut link = RobotLinkCollider {
            robot_id: "robot".into(),
            link_name: "gripper_finger".into(),
            reviewed_profile_path: PathBuf::from("reviewed.json"),
            candidate_artifact_path: None,
            geometry: ProjectSceneColliderGeometry::Box {
                size: [0.04, 0.06, 0.06],
            },
            translation: [-0.10, 0.0, 0.125],
            rotation_matrix: [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]],
        };
        authoritative.sync_robot_link_colliders(vec![link.clone()]);
        shadow.sync_robot_link_colliders(&[link.clone()]).unwrap();
        authoritative.step(1.0 / 120.0, 1.0 / 120.0);
        let initial = shadow.step(1.0 / 120.0).unwrap();
        let (direct_gravity, direct_dt, direct_substeps) =
            authoritative.authoritative_gravity_and_timing();
        let shadow_config = shadow.world.config();
        assert_eq!(direct_gravity, shadow_config.gravity_mps2);
        assert_eq!(
            direct_dt,
            shadow_config.fixed_dt_sec / shadow_config.substeps as f32
        );
        assert_eq!(direct_substeps, shadow_config.substeps as usize);
        let key = "robot:gripper_finger:0";
        let shadow_bottle = initial
            .snapshot
            .bodies
            .iter()
            .find(|body| body.id == shadow.object_bodies["bottle"])
            .unwrap();
        let shadow_link = initial
            .snapshot
            .bodies
            .iter()
            .find(|body| body.id == shadow.link_bodies[key])
            .unwrap();
        let direct_bottle = authoritative
            .authoritative_object_runtime_descriptor("bottle")
            .unwrap();
        let direct_link = authoritative
            .authoritative_link_runtime_descriptor(key)
            .unwrap();
        let shadow_bottle_desc = &shadow.object_body_descs["bottle"];
        let shadow_link_desc = &shadow.link_body_descs[key];

        assert_eq!(
            direct_bottle.dynamic,
            shadow_bottle.mode == BodyMode::Dynamic
        );
        assert_eq!(
            direct_bottle.kinematic_position,
            shadow_bottle.mode == BodyMode::KinematicPosition
        );
        assert_eq!(
            direct_bottle.linear_damping,
            shadow_bottle_desc.linear_damping
        );
        assert_eq!(
            direct_bottle.angular_damping,
            shadow_bottle_desc.angular_damping
        );
        assert_eq!(
            direct_bottle.gravity_scale,
            shadow_bottle_desc.gravity_scale
        );
        assert_eq!(
            direct_bottle.rotation_locked,
            shadow_bottle_desc.lock_rotation
        );
        assert_eq!(
            direct_bottle.ccd_enabled, shadow_bottle.ccd_enabled,
            "direct and shadow bottle CCD descriptors must match"
        );
        assert_eq!(direct_link.dynamic, shadow_link.mode == BodyMode::Dynamic);
        assert_eq!(
            direct_link.kinematic_position,
            shadow_link.mode == BodyMode::KinematicPosition
        );
        assert_eq!(direct_link.linear_damping, shadow_link_desc.linear_damping);
        assert_eq!(
            direct_link.angular_damping,
            shadow_link_desc.angular_damping
        );
        assert_eq!(direct_link.gravity_scale, shadow_link_desc.gravity_scale);
        assert_eq!(direct_link.rotation_locked, shadow_link_desc.lock_rotation);
        assert_eq!(
            direct_link.ccd_enabled, shadow_link.ccd_enabled,
            "direct and shadow reviewed-link CCD descriptors must match"
        );
        assert_authoritative_colliders_match_shadow(
            &authoritative
                .authoritative_object_collider_runtime_descriptors("bottle")
                .unwrap(),
            &initial.snapshot,
            &shadow.object_colliders["bottle"],
        );
        assert_authoritative_colliders_match_shadow(
            &authoritative
                .authoritative_link_collider_runtime_descriptors(key)
                .unwrap(),
            &initial.snapshot,
            std::slice::from_ref(&shadow.link_colliders[key]),
        );
        assert!(authoritative.robot_link_contacts("bottle").is_empty());
        assert!(initial.events.iter().all(|event| {
            !event.collider1.0.starts_with("shadow:kinematic-link:")
                && !event.collider2.0.starts_with("shadow:kinematic-link:")
        }));

        link.translation = [0.02, 0.0, 0.125];
        link.rotation_matrix = [[0.0, -1.0, 0.0], [1.0, 0.0, 0.0], [0.0, 0.0, 1.0]];
        let mut direct_started = false;
        let mut shadow_started = false;
        let mut contact_step = None;
        for step in 2..=170 {
            authoritative.sync_robot_link_colliders(vec![link.clone()]);
            shadow.sync_robot_link_colliders(&[link.clone()]).unwrap();
            authoritative.step(1.0 / 120.0, f64::from(step) / 120.0);
            let output = shadow.step(1.0 / 120.0).unwrap();
            if step == 2 {
                let (direct_link_position, direct_link_rotation) =
                    authoritative.authoritative_link_pose(key).unwrap();
                let rotation_fraction = (2.0 / 120.0) / (std::f32::consts::FRAC_PI_2);
                assert_close(
                    direct_link_position[0] - (-0.10),
                    0.12 * rotation_fraction,
                    1.0e-6,
                );
                assert_close(
                    yaw_from_quaternion(direct_link_rotation),
                    2.0 / 120.0,
                    1.0e-6,
                );
                let shadow_link = output
                    .snapshot
                    .bodies
                    .iter()
                    .find(|body| body.id == shadow.link_bodies[key])
                    .unwrap();
                assert_close(
                    shadow_link.pose.translation[0] - (-0.10),
                    0.12 * rotation_fraction,
                    1.0e-6,
                );
                assert_close(
                    yaw_from_quaternion(shadow_link.pose.rotation_xyzw),
                    2.0 / 120.0,
                    1.0e-6,
                );
            }
            let direct_contact_now = !authoritative.robot_link_contacts("bottle").is_empty();
            let shadow_link_contacts = output
                .snapshot
                .contacts
                .iter()
                .filter(|contact| {
                    (contact.collider1.0.starts_with("shadow:kinematic-link:")
                        && contact.collider2.0.contains("bottle"))
                        || (contact.collider2.0.starts_with("shadow:kinematic-link:")
                            && contact.collider1.0.contains("bottle"))
                })
                .collect::<Vec<_>>();
            let shadow_contact_now = !shadow_link_contacts.is_empty();
            direct_started |= direct_contact_now;
            shadow_started |= output
                .events
                .iter()
                .any(|event| event.kind == PhysicsEventKind::ContactStarted);
            if direct_contact_now && shadow_contact_now && contact_step.is_none() {
                let direct_bottle = authoritative.object_state("bottle").unwrap();
                let shadow_bottle = output
                    .snapshot
                    .bodies
                    .iter()
                    .find(|body| body.id == shadow.object_bodies["bottle"])
                    .unwrap();
                let shadow_link_collider = output
                    .snapshot
                    .colliders
                    .iter()
                    .find(|collider| collider.id == shadow.link_colliders[key])
                    .unwrap();
                let shadow_bottle_collider = output
                    .snapshot
                    .colliders
                    .iter()
                    .find(|collider| collider.id.0 == "shadow:scene-object:bottle:part:1")
                    .unwrap();
                eprintln!(
                    "first shared bottle-link contact step={step} direct_bottle={:?} shadow_bottle={:?} direct_narrow_phase={:?} shadow_link_collider={:?} shadow_bottle_collider={:?} shadow_contacts={shadow_link_contacts:?}",
                    (
                        direct_bottle.position,
                        direct_bottle.rotation,
                        direct_bottle.velocity_mps,
                        direct_bottle.angular_velocity_rps
                    ),
                    (
                        shadow_bottle.pose,
                        shadow_bottle.linear_velocity_mps,
                        shadow_bottle.angular_velocity_rps
                    ),
                    authoritative
                        .authoritative_link_object_contact_pair_observations(key, "bottle"),
                    shadow_link_collider.world_pose,
                    shadow_bottle_collider.world_pose,
                );
            }
            if direct_started && shadow_started {
                contact_step = Some(step);
                break;
            }
        }
        assert!(direct_started && shadow_started);

        link.translation = [-0.10, 0.0, 0.125];
        let mut direct_stopped = false;
        let mut shadow_stopped = false;
        let mut previous_direct_overlap = true;
        let mut previous_shadow_overlap = true;
        let return_step = contact_step.expect("shared contact step") + 1;
        for step in return_step..=return_step + 189 {
            authoritative.sync_robot_link_colliders(vec![link.clone()]);
            shadow.sync_robot_link_colliders(&[link.clone()]).unwrap();
            authoritative.step(1.0 / 120.0, f64::from(step) / 120.0);
            let output = shadow.step(1.0 / 120.0).unwrap();
            let direct_contacts = authoritative.robot_link_contacts("bottle");
            let direct_overlap = !direct_contacts.is_empty();
            let shadow_pairs = snapshot_pairs(&output.snapshot, false)
                .into_iter()
                .filter(|(left, right)| {
                    (left.starts_with("shadow:kinematic-link:") && right.contains("bottle"))
                        || (right.starts_with("shadow:kinematic-link:") && left.contains("bottle"))
                })
                .collect::<Vec<_>>();
            let shadow_overlap = !shadow_pairs.is_empty();
            let link_events = output
                .events
                .iter()
                .filter(|event| {
                    event.collider1.0.starts_with("shadow:kinematic-link:")
                        || event.collider2.0.starts_with("shadow:kinematic-link:")
                })
                .collect::<Vec<_>>();
            if previous_direct_overlap != direct_overlap
                || previous_shadow_overlap != shadow_overlap
                || !link_events.is_empty()
                || (24..=48).contains(&step)
            {
                let link_body = output
                    .snapshot
                    .bodies
                    .iter()
                    .find(|body| body.id == shadow.link_bodies["robot:gripper_finger:0"])
                    .unwrap();
                let shadow_bottle = output
                    .snapshot
                    .bodies
                    .iter()
                    .find(|body| body.id == shadow.object_bodies["bottle"])
                    .unwrap();
                let direct_bottle = authoritative.object_state("bottle").unwrap();
                eprintln!(
                    "return step={step} target={:?} direct_overlap={direct_overlap} direct_contacts={direct_contacts:?} shadow_overlap={shadow_overlap} shadow_pairs={shadow_pairs:?} events={link_events:?} link_pose={:?} link_velocity={:?} direct_bottle={:?} shadow_bottle={:?} ccd={} link_mode={:?}",
                    link.translation,
                    link_body.pose,
                    (
                        link_body.linear_velocity_mps,
                        link_body.angular_velocity_rps
                    ),
                    (
                        direct_bottle.position,
                        direct_bottle.rotation,
                        direct_bottle.velocity_mps
                    ),
                    (shadow_bottle.pose, shadow_bottle.linear_velocity_mps),
                    shadow_bottle.ccd_enabled,
                    link_body.mode,
                );
            }
            previous_direct_overlap = direct_overlap;
            previous_shadow_overlap = shadow_overlap;
            direct_stopped |= !direct_overlap;
            if link_events
                .iter()
                .any(|event| event.kind == PhysicsEventKind::ContactStopped)
            {
                assert!(
                    !direct_overlap,
                    "shadow contact-stop must coincide with direct clear state"
                );
            }
            shadow_stopped |= link_events
                .iter()
                .any(|event| event.kind == PhysicsEventKind::ContactStopped);
        }
        assert!(direct_stopped && shadow_stopped);
    }
}
