//! Production PGE scene-physics authority, staged for Gate 4 cutover.
//!
//! This adapter owns one persistent [`PhysicsWorld`] per RobotDreams session.
//! It deliberately accepts only authored RobotDreams descriptors and exposes
//! only PGE IDs/snapshots; it has no Rapier import or direct-runtime fallback.
//! The adapter is not wired into `RobotDreams` until every existing semantic
//! output can be derived from one solved PGE frame.

use std::collections::BTreeMap;
use std::error::Error;

use pge_physics::{
    BodyDesc, BodyId, BodyMode, BoundedKinematicTarget, ColliderDesc, ColliderId, ColliderMaterial,
    ColliderShape, KinematicTargetMode, MassPropertiesDesc, PhysicsConfig, PhysicsEvent,
    PhysicsSnapshot, PhysicsWorld, Pose,
};

use crate::project::{
    ProjectConfig, ProjectSceneBodyKind, ProjectSceneColliderConfig, ProjectSceneColliderGeometry,
    ProjectSceneObjectPhysicsConfig, ProjectSceneTriggerConfig, ProjectVehicleColliderConfig,
    ProjectVehiclePhysicsConfig, RobotLinkCollider,
};
use crate::scene_physics_contract::{
    AppliedCalibrationState, KinematicColliderMotionConfig, LivePhysicsColliderDebugEntry,
    LivePhysicsColliderDebugPart, SceneObjectAttachment, SceneObjectState, SceneTriggerState,
    VehicleActuatorState, VehicleDriveCommand, VehiclePhysicsForceCommand,
    VehiclePhysicsKinematics, VehiclePhysicsState, apply_scene_trigger_rule,
    calibrated_vehicle_profile, generated_vehicle_colliders, load_project_calibration,
    pge_transform, plan_vehicle_forces,
};

/// Persistent, single-world production authority prepared for the atomic Gate
/// 4 swap. Its stable IDs intentionally have no `shadow:` prefix: they are
/// the identifiers that the eventual authoritative public frame will publish.
pub(crate) struct PgeScenePhysicsRuntime {
    world: PhysicsWorld,
    object_bodies: BTreeMap<String, BodyId>,
    object_colliders: BTreeMap<String, Vec<ColliderId>>,
    object_states: BTreeMap<String, SceneObjectState>,
    object_configs: BTreeMap<String, ProjectSceneColliderConfig>,
    #[allow(dead_code)] // retained for authoritative sensor lifecycle publication
    trigger_bodies: BTreeMap<String, BodyId>,
    #[allow(dead_code)] // retained for authoritative sensor lifecycle publication
    trigger_sensors: BTreeMap<String, ColliderId>,
    trigger_configs: BTreeMap<String, ProjectSceneTriggerConfig>,
    trigger_states: BTreeMap<String, SceneTriggerState>,
    vehicle_bodies: BTreeMap<String, BodyId>,
    vehicle_configs: BTreeMap<String, ProjectVehiclePhysicsConfig>,
    vehicle_colliders: BTreeMap<String, Vec<ProjectVehicleColliderConfig>>,
    vehicle_states: BTreeMap<String, VehiclePhysicsState>,
    vehicle_steering_angles: BTreeMap<String, f32>,
    calibrations: Vec<AppliedCalibrationState>,
    link_bodies: BTreeMap<String, BodyId>,
    link_colliders: BTreeMap<String, ColliderId>,
    link_metadata: BTreeMap<String, (String, String, ProjectSceneColliderGeometry)>,
    last_events: Vec<PhysicsEvent>,
    last_frame: Option<PgeSolvedFrame>,
    frame_index: u64,
}

/// Immutable adapter publication produced from one solved PGE step.
#[derive(Clone, Debug)]
#[allow(dead_code)] // next atomic-publication slice reroutes facade queries here
pub(crate) struct PgeSolvedFrame {
    pub frame_index: u64,
    pub simulation_time_sec: f64,
    pub snapshot: PhysicsSnapshot,
    pub events: Vec<PhysicsEvent>,
    pub debug_entries: Vec<LivePhysicsColliderDebugEntry>,
    pub applied_calibrations: Vec<AppliedCalibrationState>,
    pub object_states: Vec<SceneObjectState>,
    pub trigger_states: Vec<SceneTriggerState>,
    pub vehicle_states: Vec<VehiclePhysicsState>,
    pub link_poses: Vec<(String, Pose)>,
}

impl PgeScenePhysicsRuntime {
    pub(crate) fn empty() -> Self {
        Self {
            world: PhysicsWorld::new(PhysicsConfig::default())
                .expect("default PGE physics configuration is valid"),
            object_bodies: BTreeMap::new(),
            object_colliders: BTreeMap::new(),
            object_states: BTreeMap::new(),
            object_configs: BTreeMap::new(),
            trigger_bodies: BTreeMap::new(),
            trigger_sensors: BTreeMap::new(),
            trigger_configs: BTreeMap::new(),
            trigger_states: BTreeMap::new(),
            vehicle_bodies: BTreeMap::new(),
            vehicle_configs: BTreeMap::new(),
            vehicle_colliders: BTreeMap::new(),
            vehicle_states: BTreeMap::new(),
            vehicle_steering_angles: BTreeMap::new(),
            calibrations: Vec::new(),
            link_bodies: BTreeMap::new(),
            link_colliders: BTreeMap::new(),
            link_metadata: BTreeMap::new(),
            last_events: Vec::new(),
            last_frame: None,
            frame_index: 0,
        }
    }

    /// Builds the one persistent PGE world for a newly created session.
    ///
    /// This first checkpoint maps authored physical scene objects and trigger
    /// sensors with stable IDs. Vehicle, attachment and reviewed-link command
    /// routing will be added before this type replaces the live runtime.
    pub(crate) fn from_project(project: &ProjectConfig) -> Result<Self, Box<dyn Error>> {
        let mut world = PhysicsWorld::new(PhysicsConfig::default())?;
        let mut object_bodies = BTreeMap::new();
        let mut object_colliders = BTreeMap::new();
        let mut object_states = BTreeMap::new();
        let mut object_configs = BTreeMap::new();

        for object in &project.scene.objects {
            let Some(config) = &object.physics else {
                continue;
            };
            let body_id = BodyId::new(format!("scene-object:{}", object.id));
            world.create_body(
                body_id.clone(),
                BodyDesc {
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
                },
            )?;

            let mut collider_ids = Vec::new();
            for (part_index, part) in config.collider.parts().into_iter().enumerate() {
                let collider_id =
                    ColliderId::new(format!("scene-object:{}:part:{part_index}", object.id));
                world.create_collider(
                    collider_id.clone(),
                    &body_id,
                    ColliderDesc {
                        pose: pose_from_rpy(part.offset, part.rotation),
                        shape: collider_shape(&part.geometry),
                        material: ColliderMaterial {
                            friction: config.friction,
                            restitution: config.restitution,
                            // Gate 4 will install the existing explicit
                            // compound mass/inertia descriptor for dynamic
                            // objects. Keeping collider mass at zero here
                            // prevents accidental additive mass authority.
                            density_kg_m3: 0.0,
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
            object_colliders.insert(object.id.clone(), collider_ids);
            object_states.insert(
                object.id.clone(),
                SceneObjectState {
                    id: object.id.clone(),
                    position: object.position,
                    rotation: object.rotation,
                    velocity_mps: [0.0; 3],
                    angular_velocity_rps: [0.0; 3],
                    dynamic: config.body_kind == ProjectSceneBodyKind::Dynamic,
                    attachment: None,
                },
            );
            object_configs.insert(object.id.clone(), config.collider.clone());
        }

        let mut trigger_bodies = BTreeMap::new();
        let mut trigger_sensors = BTreeMap::new();
        let mut trigger_configs = BTreeMap::new();
        let mut trigger_states = BTreeMap::new();
        for trigger in &project.scene.triggers {
            let body_id = BodyId::new(format!("trigger:{}", trigger.id));
            let collider_id = ColliderId::new(format!("trigger:{}:sensor", trigger.id));
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
            trigger_bodies.insert(trigger.id.clone(), body_id);
            trigger_sensors.insert(trigger.id.clone(), collider_id);
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
        }

        let mut vehicle_bodies = BTreeMap::new();
        let mut vehicle_configs = BTreeMap::new();
        let mut vehicle_colliders = BTreeMap::new();
        let mut vehicle_states = BTreeMap::new();
        let mut vehicle_steering_angles = BTreeMap::new();
        let calibration = load_project_calibration(project)?;
        let mut calibrations = Vec::new();
        for robot in &project.robots {
            let Some(authored_vehicle) = robot
                .physics
                .as_ref()
                .and_then(|physics| physics.vehicle.as_ref())
            else {
                continue;
            };
            let (vehicle, applied_calibration) =
                calibrated_vehicle_profile(&robot.id, authored_vehicle, calibration.as_ref())?;
            if let Some(applied_calibration) = applied_calibration {
                calibrations.push(applied_calibration);
            }
            let body_id = BodyId::new(format!("vehicle:{}", robot.id));
            let mut colliders = vehicle.colliders.clone();
            colliders.extend(generated_vehicle_colliders(project, &vehicle)?);
            world.create_body(
                body_id.clone(),
                vehicle_body_desc(
                    &vehicle,
                    &colliders,
                    robot.base_translation,
                    robot.base_rotation,
                ),
            )?;
            for (index, collider) in colliders.iter().enumerate() {
                world.create_collider(
                    ColliderId::new(format!("vehicle:{}:part:{index}", robot.id)),
                    &body_id,
                    ColliderDesc {
                        pose: pose_from_rpy(collider.offset, collider.rotation),
                        shape: collider_shape(&collider.geometry),
                        material: ColliderMaterial {
                            friction: 0.5,
                            restitution: 0.0,
                            density_kg_m3: 0.0,
                            contact_skin_m: 0.0,
                        },
                        sensor: false,
                        collision_memberships: u32::MAX,
                        collision_filter: u32::MAX,
                    },
                )?;
            }
            vehicle_states.insert(
                robot.id.clone(),
                VehiclePhysicsState {
                    robot_id: robot.id.clone(),
                    position: robot.base_translation,
                    rotation: robot.base_rotation,
                    velocity_mps: [0.0; 3],
                    angular_velocity_rps: [0.0; 3],
                    actuator: VehicleActuatorState {
                        left_command: 0.0,
                        right_command: 0.0,
                        left_drive_force_n: 0.0,
                        right_drive_force_n: 0.0,
                        braking: false,
                    },
                },
            );
            vehicle_bodies.insert(robot.id.clone(), body_id);
            vehicle_configs.insert(robot.id.clone(), vehicle);
            vehicle_colliders.insert(robot.id.clone(), colliders);
            vehicle_steering_angles.insert(robot.id.clone(), 0.0);
        }

        let runtime = Self {
            world,
            object_bodies,
            object_colliders,
            object_states,
            object_configs,
            trigger_bodies,
            trigger_sensors,
            trigger_configs,
            trigger_states,
            vehicle_bodies,
            vehicle_configs,
            vehicle_colliders,
            vehicle_states,
            vehicle_steering_angles,
            calibrations,
            link_bodies: BTreeMap::new(),
            link_colliders: BTreeMap::new(),
            link_metadata: BTreeMap::new(),
            last_events: Vec::new(),
            last_frame: None,
            frame_index: 0,
        };
        Ok(runtime)
    }

    /// Immutable solved state source for a future atomic RobotDreams frame.
    pub(crate) fn snapshot(&self) -> PhysicsSnapshot {
        self.last_frame
            .as_ref()
            .map(|frame| frame.snapshot.clone())
            .unwrap_or_else(|| self.world.snapshot())
    }

    /// Matches the established RobotDreams direct-oracle startup behavior:
    /// settle the authored scene for three seconds in bounded fixed steps
    /// before its first public read. The private warmup never advances the
    /// RobotDreams clock/frame or publishes its lifecycle events.
    pub(crate) fn settle_initial_scene(&mut self) -> Result<(), Box<dyn Error>> {
        const SETTLE_DT_SEC: f32 = 1.0 / 120.0;
        const SETTLE_STEPS: usize = 360;
        self.world.set_step_timing(SETTLE_DT_SEC, 1)?;
        for step in 1..=SETTLE_STEPS {
            self.world.step();
            self.sync_object_states()?;
            self.sync_vehicle_states()?;
            self.update_trigger_states(SETTLE_DT_SEC, step as f64 * f64::from(SETTLE_DT_SEC));
        }
        self.world.reset_timeline();
        self.last_events.clear();
        self.last_frame = None;
        self.frame_index = 0;
        Ok(())
    }

    #[allow(dead_code)] // published by the pending atomic frame facade
    pub(crate) fn frame_index(&self) -> u64 {
        self.frame_index
    }

    pub(crate) fn object_state(&self, id: &str) -> Option<&SceneObjectState> {
        self.last_frame
            .as_ref()
            .and_then(|frame| frame.object_states.iter().find(|state| state.id == id))
            .or_else(|| self.object_states.get(id))
    }

    pub(crate) fn object_states(&self) -> Vec<SceneObjectState> {
        self.last_frame
            .as_ref()
            .map(|frame| frame.object_states.clone())
            .unwrap_or_else(|| self.object_states.values().cloned().collect())
    }

    pub(crate) fn attachment_requests(&self) -> Vec<(String, SceneObjectAttachment)> {
        self.object_states
            .iter()
            .filter_map(|(id, state)| {
                state
                    .attachment
                    .clone()
                    .map(|attachment| (id.clone(), attachment))
            })
            .collect()
    }

    pub(crate) fn trigger_state(&self, id: &str) -> Option<&SceneTriggerState> {
        self.last_frame
            .as_ref()
            .and_then(|frame| frame.trigger_states.iter().find(|state| state.id == id))
            .or_else(|| self.trigger_states.get(id))
    }

    pub(crate) fn trigger_states(&self) -> Vec<SceneTriggerState> {
        self.last_frame
            .as_ref()
            .map(|frame| frame.trigger_states.clone())
            .unwrap_or_else(|| self.trigger_states.values().cloned().collect())
    }

    pub(crate) fn vehicle_state(&self, id: &str) -> Option<&VehiclePhysicsState> {
        self.last_frame
            .as_ref()
            .and_then(|frame| {
                frame
                    .vehicle_states
                    .iter()
                    .find(|state| state.robot_id == id)
            })
            .or_else(|| self.vehicle_states.get(id))
    }

    pub(crate) fn vehicle_states(&self) -> Vec<VehiclePhysicsState> {
        self.last_frame
            .as_ref()
            .map(|frame| frame.vehicle_states.clone())
            .unwrap_or_else(|| self.vehicle_states.values().cloned().collect())
    }

    #[allow(dead_code)] // consumed by the pending public event facade
    pub(crate) fn last_events(&self) -> &[PhysicsEvent] {
        self.last_frame
            .as_ref()
            .map(|frame| frame.events.as_slice())
            .unwrap_or(&self.last_events)
    }

    pub(crate) fn has_vehicle(&self, robot_id: &str) -> bool {
        self.vehicle_bodies.contains_key(robot_id)
    }

    pub(crate) fn applied_calibrations(&self) -> Vec<AppliedCalibrationState> {
        self.last_frame
            .as_ref()
            .map(|frame| frame.applied_calibrations.clone())
            .unwrap_or_default()
    }

    pub(crate) fn set_kinematic_collider_motion_config(
        &mut self,
        config: KinematicColliderMotionConfig,
    ) -> Result<(), Box<dyn Error>> {
        if !config.maximum_linear_speed_mps.is_finite()
            || !config.maximum_angular_speed_rps.is_finite()
            || !config.maximum_substep_seconds.is_finite()
            || config.maximum_linear_speed_mps <= 0.0
            || config.maximum_angular_speed_rps <= 0.0
            || config.maximum_substep_seconds <= 0.0
        {
            return Err("kinematic motion limits must be finite and positive".into());
        }
        // The first authority-switch adapter retains the reviewed Gate-3
        // coupled target constants. A configurable PGE target policy is a
        // remaining facade gap recorded at the compile boundary.
        Ok(())
    }

    pub(crate) fn robot_link_collider_spawn_is_clear(
        &self,
        object_id: &str,
        collider: &RobotLinkCollider,
    ) -> bool {
        const CLEARANCE: f32 = 0.002;
        let Some(object) = self.object_states.get(object_id) else {
            return false;
        };
        let object_radius = self
            .object_configs
            .get(object_id)
            .map(|config| {
                config
                    .parts()
                    .iter()
                    .map(|part| geometry_radius(&part.geometry))
                    .fold(0.0, f32::max)
            })
            .unwrap_or(0.0);
        let link_radius = geometry_radius(&collider.geometry);
        collider
            .translation
            .iter()
            .zip(object.position)
            .map(|(a, b)| (a - b).powi(2))
            .sum::<f32>()
            .sqrt()
            > object_radius + link_radius + CLEARANCE
    }

    pub(crate) fn drive_vehicle(
        &mut self,
        robot_id: &str,
        command: VehicleDriveCommand,
        dt: f32,
    ) -> Result<(), Box<dyn Error>> {
        let body_id = self
            .vehicle_bodies
            .get(robot_id)
            .ok_or_else(|| format!("vehicle '{robot_id}' has no PGE body"))?
            .clone();
        let body = self.world.body_snapshot(&body_id)?;
        let plan = plan_vehicle_forces(
            &self.vehicle_configs[robot_id],
            self.vehicle_steering_angles[robot_id],
            command,
            VehiclePhysicsKinematics {
                position: body.pose.translation,
                yaw: rpy_from_quaternion(body.pose.rotation_xyzw)[2],
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
        self.vehicle_states.get_mut(robot_id).unwrap().actuator = plan.actuator;
        Ok(())
    }

    pub(crate) fn sync_robot_link_colliders(
        &mut self,
        colliders: &[RobotLinkCollider],
    ) -> Result<(), Box<dyn Error>> {
        let mut next = BTreeMap::<(String, String), usize>::new();
        let mut active = std::collections::BTreeSet::new();
        for link in colliders {
            let index = next
                .entry((link.robot_id.clone(), link.link_name.clone()))
                .and_modify(|value| *value += 1)
                .or_insert(0);
            let key = format!("{}:{}:{index}", link.robot_id, link.link_name);
            active.insert(key.clone());
            let body_id = BodyId::new(format!("kinematic-link:{key}"));
            let pose = Pose {
                translation: link.translation,
                rotation_xyzw: quaternion_from_matrix(link.rotation_matrix),
            };
            if !self.link_bodies.contains_key(&key) {
                let collider_id = ColliderId::new(format!("kinematic-link:{key}:part:0"));
                self.world.create_body(
                    body_id.clone(),
                    BodyDesc {
                        mode: BodyMode::KinematicPosition,
                        pose,
                        ccd_enabled: true,
                        ..BodyDesc::default()
                    },
                )?;
                self.world.create_collider(
                    collider_id.clone(),
                    &body_id,
                    ColliderDesc {
                        shape: collider_shape(&link.geometry),
                        material: ColliderMaterial {
                            friction: 0.8,
                            restitution: 0.0,
                            density_kg_m3: 1.0,
                            contact_skin_m: 0.0,
                        },
                        ..ColliderDesc::new(ColliderShape::Box { size: [0.01; 3] })
                    },
                )?;
                self.link_bodies.insert(key.clone(), body_id.clone());
                self.link_colliders.insert(key.clone(), collider_id);
                self.link_metadata.insert(
                    key.clone(),
                    (
                        link.robot_id.clone(),
                        link.link_name.clone(),
                        link.geometry.clone(),
                    ),
                );
            }
            self.world.set_bounded_kinematic_target_with_mode(
                &body_id,
                BoundedKinematicTarget {
                    pose,
                    maximum_linear_speed_mps: 0.20,
                    maximum_angular_speed_rps: 2.0,
                    maximum_linear_acceleration_mps2: 24.0,
                    maximum_angular_acceleration_rps2: 240.0,
                },
                KinematicTargetMode::CoupledPose,
            )?;
        }
        let removed = self
            .link_bodies
            .keys()
            .filter(|key| !active.contains(*key))
            .cloned()
            .collect::<Vec<_>>();
        for key in removed {
            if let Some(body_id) = self.link_bodies.remove(&key) {
                self.world.remove_body(&body_id)?;
            }
            self.link_colliders.remove(&key);
            self.link_metadata.remove(&key);
        }
        Ok(())
    }

    pub(crate) fn robot_link_contacts(
        &self,
        object_id: &str,
    ) -> Vec<crate::scene_physics_contract::RobotLinkContact> {
        let Some(object_colliders) = self.object_colliders.get(object_id) else {
            return Vec::new();
        };
        let snapshot = self.snapshot();
        self.link_colliders
            .iter()
            .filter(|(_, link_collider)| {
                snapshot.contacts.iter().any(|contact| {
                    (contact.collider1 == **link_collider
                        && object_colliders.contains(&contact.collider2))
                        || (contact.collider2 == **link_collider
                            && object_colliders.contains(&contact.collider1))
                })
            })
            .map(|(key, _)| {
                let (robot_id, link_name, _) = &self.link_metadata[key];
                crate::scene_physics_contract::RobotLinkContact {
                    robot_id: robot_id.clone(),
                    link_name: link_name.clone(),
                    object_id: object_id.to_string(),
                }
            })
            .collect()
    }

    pub(crate) fn attach(
        &mut self,
        object_id: &str,
        attachment: SceneObjectAttachment,
        position: [f32; 3],
        rotation: [f32; 3],
    ) -> Result<(), Box<dyn Error>> {
        let state = self
            .object_states
            .get_mut(object_id)
            .ok_or_else(|| format!("scene object '{object_id}' has no physics body"))?;
        if !state.dynamic {
            return Err(format!("scene object '{object_id}' is not dynamic").into());
        }
        let body_id = self.object_bodies[object_id].clone();
        let pose = pose_from_rpy(position, rotation);
        self.world
            .set_body_mode(&body_id, BodyMode::KinematicPosition, true)?;
        self.world.set_body_pose(&body_id, pose, true)?;
        self.world.set_next_kinematic_pose(&body_id, pose)?;
        self.world
            .set_body_velocity(&body_id, [0.0; 3], [0.0; 3], true)?;
        state.position = position;
        state.rotation = rotation;
        state.velocity_mps = [0.0; 3];
        state.angular_velocity_rps = [0.0; 3];
        state.attachment = Some(attachment);
        self.publish_object_state_change(object_id);
        Ok(())
    }

    pub(crate) fn detach(&mut self, object_id: &str) -> Result<(), Box<dyn Error>> {
        let state = self
            .object_states
            .get_mut(object_id)
            .ok_or_else(|| format!("scene object '{object_id}' has no physics body"))?;
        if state.attachment.is_none() {
            return Err(format!("scene object '{object_id}' is not attached").into());
        }
        let body_id = self.object_bodies[object_id].clone();
        self.world
            .set_body_mode(&body_id, BodyMode::Dynamic, true)?;
        self.world
            .set_body_velocity(&body_id, [0.0; 3], [0.0; 3], true)?;
        self.world.wake_up(&body_id)?;
        state.attachment = None;
        self.publish_object_state_change(object_id);
        Ok(())
    }

    pub(crate) fn set_attached_pose(
        &mut self,
        object_id: &str,
        position: [f32; 3],
        rotation: [f32; 3],
    ) -> Result<(), Box<dyn Error>> {
        let state = self
            .object_states
            .get_mut(object_id)
            .ok_or_else(|| format!("scene object '{object_id}' has no physics body"))?;
        let body_id = self.object_bodies[object_id].clone();
        let pose = pose_from_rpy(position, rotation);
        self.world.set_body_pose(&body_id, pose, true)?;
        self.world.set_next_kinematic_pose(&body_id, pose)?;
        self.world
            .set_body_velocity(&body_id, [0.0; 3], [0.0; 3], true)?;
        state.position = position;
        state.rotation = rotation;
        state.velocity_mps = [0.0; 3];
        state.angular_velocity_rps = [0.0; 3];
        if state.attachment.is_some() {
            self.publish_object_state_change(object_id);
        }
        Ok(())
    }

    pub(crate) fn step(&mut self, dt: f32, clock_sec: f64) -> Result<(), Box<dyn Error>> {
        if dt <= 0.0 {
            return Ok(());
        }
        self.world.set_step_timing(dt, 1)?;
        let output = self.world.step();
        self.last_events = output.events.clone();
        self.frame_index = self.frame_index.saturating_add(1);
        self.sync_object_states()?;
        self.sync_vehicle_states()?;
        self.update_trigger_states(dt, clock_sec);
        let link_poses = output
            .snapshot
            .bodies
            .iter()
            .filter(|body| body.id.0.starts_with("kinematic-link:"))
            .map(|body| (body.id.0.clone(), body.pose))
            .collect();
        self.last_frame = Some(PgeSolvedFrame {
            frame_index: self.frame_index,
            simulation_time_sec: output.snapshot.simulation_time_sec,
            snapshot: output.snapshot,
            events: self.last_events.clone(),
            // Build the published frame from live PGE state. Public read APIs switch
            // to this frame after the assignment below, so calling their facades here
            // would accidentally retain data from the preceding solved frame.
            debug_entries: self.live_collider_debug_entries(),
            applied_calibrations: self.calibrations.clone(),
            object_states: self.object_states.values().cloned().collect(),
            trigger_states: self.trigger_states.values().cloned().collect(),
            vehicle_states: self.vehicle_states.values().cloned().collect(),
            link_poses,
        });
        Ok(())
    }

    #[allow(dead_code)] // consumed by the next facade-routing slice
    pub(crate) fn solved_frame(&self) -> Option<&PgeSolvedFrame> {
        self.last_frame.as_ref()
    }

    pub(crate) fn collider_debug_entries(&self) -> Vec<LivePhysicsColliderDebugEntry> {
        if let Some(frame) = &self.last_frame {
            return frame.debug_entries.clone();
        }
        self.live_collider_debug_entries()
    }

    fn live_collider_debug_entries(&self) -> Vec<LivePhysicsColliderDebugEntry> {
        let mut entries = self
            .object_states
            .iter()
            .map(|(id, state)| LivePhysicsColliderDebugEntry {
                id: format!("scene-object:{id}"),
                category: if state.dynamic {
                    "sceneDynamicCollider"
                } else {
                    "sceneStaticCollider"
                },
                color: if state.dynamic {
                    [1.0, 0.72, 0.16, 1.0]
                } else {
                    [0.18, 0.76, 0.95, 1.0]
                },
                transform: pge_transform(state.position, state.rotation),
                parts: self.object_configs[id]
                    .parts()
                    .into_iter()
                    .map(|part| LivePhysicsColliderDebugPart {
                        geometry: part.geometry,
                        local_transform: pge_transform(part.offset, part.rotation),
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();
        for (robot_id, state) in &self.vehicle_states {
            for (index, collider) in self.vehicle_colliders[robot_id].iter().enumerate() {
                entries.push(LivePhysicsColliderDebugEntry {
                    id: format!("vehicle:{robot_id}:{index}"),
                    category: "vehicleCollider",
                    color: [0.18, 0.95, 0.45, 1.0],
                    transform: pge_transform(state.position, state.rotation),
                    parts: vec![LivePhysicsColliderDebugPart {
                        geometry: collider.geometry.clone(),
                        local_transform: pge_transform(collider.offset, collider.rotation),
                    }],
                });
            }
        }
        let snapshot = self.snapshot();
        for (key, collider_id) in &self.link_colliders {
            let Some(collider) = snapshot
                .colliders
                .iter()
                .find(|collider| collider.id == *collider_id)
            else {
                continue;
            };
            let (_, _, geometry) = &self.link_metadata[key];
            entries.push(LivePhysicsColliderDebugEntry {
                id: format!("kinematic-link:{key}"),
                category: "kinematicRobotLinkCollider",
                color: [1.0, 0.28, 0.78, 1.0],
                transform: pge_transform(
                    collider.world_pose.translation,
                    rpy_from_quaternion(collider.world_pose.rotation_xyzw),
                ),
                parts: vec![LivePhysicsColliderDebugPart {
                    geometry: geometry.clone(),
                    local_transform: pge_transform([0.0; 3], [0.0; 3]),
                }],
            });
        }
        entries
    }

    pub(crate) fn collider_debug_transforms(&self) -> BTreeMap<String, pge_core::Transform> {
        self.collider_debug_entries()
            .into_iter()
            .map(|entry| (entry.id, entry.transform))
            .collect()
    }

    fn sync_object_states(&mut self) -> Result<(), Box<dyn Error>> {
        for (id, state) in &mut self.object_states {
            let body = self.world.body_snapshot(&self.object_bodies[id])?;
            state.position = body.pose.translation;
            state.rotation = rpy_from_quaternion(body.pose.rotation_xyzw);
            state.velocity_mps = body.linear_velocity_mps;
            state.angular_velocity_rps = body.angular_velocity_rps;
        }
        Ok(())
    }

    /// Attachment lifecycle commands are public semantic state changes, not
    /// deferred solver observations. Keep the completed frame's object slice
    /// aligned with the kinematic body so public snapshots immediately report
    /// an accepted grasp, tracked pose, or release.
    fn publish_object_state_change(&mut self, object_id: &str) {
        let Some(frame) = &mut self.last_frame else {
            return;
        };
        let Some(state) = self.object_states.get(object_id).cloned() else {
            return;
        };
        if let Some(published) = frame
            .object_states
            .iter_mut()
            .find(|published| published.id == object_id)
        {
            *published = state;
        }
    }

    fn sync_vehicle_states(&mut self) -> Result<(), Box<dyn Error>> {
        for (id, state) in &mut self.vehicle_states {
            let body = self.world.body_snapshot(&self.vehicle_bodies[id])?;
            state.position = body.pose.translation;
            state.rotation = rpy_from_quaternion(body.pose.rotation_xyzw);
            state.velocity_mps = body.linear_velocity_mps;
            state.angular_velocity_rps = body.angular_velocity_rps;
        }
        Ok(())
    }

    fn update_trigger_states(&mut self, dt: f32, clock_sec: f64) {
        for (id, config) in &self.trigger_configs {
            let Some(object) = self.object_states.get(&config.object_id) else {
                continue;
            };
            apply_scene_trigger_rule(
                self.trigger_states
                    .get_mut(id)
                    .expect("trigger state follows project config"),
                config,
                object,
                dt,
                clock_sec,
            );
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

fn geometry_radius(geometry: &ProjectSceneColliderGeometry) -> f32 {
    match geometry {
        ProjectSceneColliderGeometry::Box { size } => {
            0.5 * (size[0] * size[0] + size[1] * size[1] + size[2] * size[2]).sqrt()
        }
        ProjectSceneColliderGeometry::Sphere { radius } => *radius,
        ProjectSceneColliderGeometry::Cylinder { radius, height } => {
            (radius * radius + 0.25 * height * height).sqrt()
        }
    }
}

type Matrix3 = [[f32; 3]; 3];

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

fn matrix_transpose(matrix: Matrix3) -> Matrix3 {
    std::array::from_fn(|row| std::array::from_fn(|column| matrix[column][row]))
}

fn matrix_scale(matrix: Matrix3, scalar: f32) -> Matrix3 {
    std::array::from_fn(|row| std::array::from_fn(|column| matrix[row][column] * scalar))
}

fn outer_product(vector: [f32; 3]) -> Matrix3 {
    std::array::from_fn(|row| std::array::from_fn(|column| vector[row] * vector[column]))
}

fn rotation_matrix(rotation: [f32; 3]) -> Matrix3 {
    let [x, y, z, w] = pose_from_rpy([0.0; 3], rotation).rotation_xyzw;
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
            (
                part,
                mass_kg * scene_collider_volume(&part.geometry) / total_volume,
            )
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
            let rotated = matrix_multiply(
                matrix_multiply(rotation, scene_collider_inertia(&part.geometry, *mass)),
                matrix_transpose(rotation),
            );
            let offset = std::array::from_fn(|axis| part.offset[axis] - center_of_mass[axis]);
            let squared = offset.iter().map(|value| value * value).sum::<f32>();
            let parallel = matrix_scale(
                matrix_add(
                    [
                        [squared, 0.0, 0.0],
                        [0.0, squared, 0.0],
                        [0.0, 0.0, squared],
                    ],
                    matrix_scale(outer_product(offset), -1.0),
                ),
                *mass,
            );
            matrix_add(sum, matrix_add(rotated, parallel))
        });
    MassPropertiesDesc {
        mass_kg,
        center_of_mass_m: center_of_mass,
        principal_inertia_kg_m2: [inertia[0][0], inertia[1][1], inertia[2][2]],
        principal_inertia_frame_xyzw: [0.0, 0.0, 0.0, 1.0],
        inertia_tensor_kg_m2: Some(inertia),
    }
}

fn pose_from_rpy(translation: [f32; 3], rotation_rpy: [f32; 3]) -> Pose {
    let [roll, pitch, yaw] = rotation_rpy;
    let (sr, cr) = (roll * 0.5).sin_cos();
    let (sp, cp) = (pitch * 0.5).sin_cos();
    let (sy, cy) = (yaw * 0.5).sin_cos();
    Pose {
        translation,
        rotation_xyzw: [
            sr * cp * cy - cr * sp * sy,
            cr * sp * cy + sr * cp * sy,
            cr * cp * sy - sr * sp * cy,
            cr * cp * cy + sr * sp * sy,
        ],
    }
}

fn rpy_from_quaternion([x, y, z, w]: [f32; 4]) -> [f32; 3] {
    let roll = (2.0 * (w * x + y * z)).atan2(1.0 - 2.0 * (x * x + y * y));
    let pitch = (2.0 * (w * y - z * x)).clamp(-1.0, 1.0).asin();
    let yaw = (2.0 * (w * z + x * y)).atan2(1.0 - 2.0 * (y * y + z * z));
    [roll, pitch, yaw]
}

fn quaternion_from_matrix(matrix: [[f32; 3]; 3]) -> [f32; 4] {
    let trace = matrix[0][0] + matrix[1][1] + matrix[2][2];
    let (x, y, z, w) = if trace > 0.0 {
        let scale = (trace + 1.0).sqrt() * 2.0;
        (
            (matrix[2][1] - matrix[1][2]) / scale,
            (matrix[0][2] - matrix[2][0]) / scale,
            (matrix[1][0] - matrix[0][1]) / scale,
            0.25 * scale,
        )
    } else if matrix[0][0] > matrix[1][1] && matrix[0][0] > matrix[2][2] {
        let scale = (1.0 + matrix[0][0] - matrix[1][1] - matrix[2][2]).sqrt() * 2.0;
        (
            0.25 * scale,
            (matrix[0][1] + matrix[1][0]) / scale,
            (matrix[0][2] + matrix[2][0]) / scale,
            (matrix[2][1] - matrix[1][2]) / scale,
        )
    } else if matrix[1][1] > matrix[2][2] {
        let scale = (1.0 + matrix[1][1] - matrix[0][0] - matrix[2][2]).sqrt() * 2.0;
        (
            (matrix[0][1] + matrix[1][0]) / scale,
            0.25 * scale,
            (matrix[1][2] + matrix[2][1]) / scale,
            (matrix[0][2] - matrix[2][0]) / scale,
        )
    } else {
        let scale = (1.0 + matrix[2][2] - matrix[0][0] - matrix[1][1]).sqrt() * 2.0;
        (
            (matrix[0][2] + matrix[2][0]) / scale,
            (matrix[1][2] + matrix[2][1]) / scale,
            0.25 * scale,
            (matrix[1][0] - matrix[0][1]) / scale,
        )
    };
    [x, y, z, w]
}

fn vehicle_inertia_diagonal(mass_kg: f32, colliders: &[ProjectVehicleColliderConfig]) -> [f32; 3] {
    let mut half = [0.01_f32; 3];
    for collider in colliders {
        let extent = match collider.geometry {
            ProjectSceneColliderGeometry::Box { size } => {
                [size[0] * 0.5, size[1] * 0.5, size[2] * 0.5]
            }
            ProjectSceneColliderGeometry::Sphere { radius } => [radius; 3],
            ProjectSceneColliderGeometry::Cylinder { radius, height } => {
                [radius, radius, height * 0.5]
            }
        };
        for axis in 0..3 {
            half[axis] = half[axis].max(collider.offset[axis].abs() + extent[axis]);
        }
    }
    let size = half.map(|value| value * 2.0);
    [
        mass_kg * (size[1] * size[1] + size[2] * size[2]) / 12.0,
        mass_kg * (size[0] * size[0] + size[2] * size[2]) / 12.0,
        mass_kg * (size[0] * size[0] + size[1] * size[1]) / 12.0,
    ]
    .map(|value| value.max(1.0e-6))
}

fn vehicle_body_desc(
    vehicle: &ProjectVehiclePhysicsConfig,
    colliders: &[ProjectVehicleColliderConfig],
    base_translation: [f32; 3],
    base_rotation: [f32; 3],
) -> BodyDesc {
    let inertia = vehicle_inertia_diagonal(vehicle.mass_kg, colliders);
    BodyDesc {
        mode: BodyMode::Dynamic,
        pose: pose_from_rpy(base_translation, base_rotation),
        linear_damping: vehicle.linear_damping,
        angular_damping: vehicle.angular_damping,
        ccd_enabled: true,
        lock_translation: [false; 3],
        // Match the direct vehicle's 2.5D authority: gravity/contact solve
        // vertical translation and yaw, while roll/pitch remain locked.
        lock_rotation: [true, true, false],
        mass: Some(MassPropertiesDesc {
            mass_kg: vehicle.mass_kg,
            center_of_mass_m: vehicle.center_of_mass_m,
            principal_inertia_kg_m2: inertia,
            inertia_tensor_kg_m2: Some([
                [inertia[0], 0.0, 0.0],
                [0.0, inertia[1], 0.0],
                [0.0, 0.0, inertia[2]],
            ]),
            ..MassPropertiesDesc::default()
        }),
        ..BodyDesc::default()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    use pge_physics::ColliderShape;

    use super::{
        PgeScenePhysicsRuntime, collider_shape, pose_from_rpy, scene_object_mass_properties,
        vehicle_body_desc,
    };
    use crate::project::{
        HardwareConfig, ProjectConfig, ProjectRobotConfig, ProjectRobotModelConfig,
        ProjectRobotPhysicsConfig, ProjectSceneBodyKind, ProjectSceneColliderChildConfig,
        ProjectSceneColliderConfig, ProjectSceneColliderGeometry, ProjectSceneConfig,
        ProjectSceneObjectConfig, ProjectSceneObjectGeometry, ProjectSceneObjectPhysicsConfig,
        ProjectSceneTriggerConfig, ProjectVehicleColliderConfig, ProjectVehicleMotorConfig,
        ProjectVehiclePhysicsConfig, RobotLinkCollider,
    };
    use crate::scene_physics_contract::{SceneObjectAttachment, VehicleDriveCommand};

    #[test]
    fn puppybot_initial_support_penetration_and_vertical_response_are_characterized() {
        let project_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("../PuppyBot/robotdreams/project.json");
        let project = crate::project::project_config_from_manifest(&project_path).unwrap();
        let mut pge = PgeScenePhysicsRuntime::from_project(&project).unwrap();
        let mut direct = crate::scene_physics::ScenePhysicsRuntime::from_project(&project).unwrap();

        let (direct_vehicle, direct_colliders) =
            direct.authoritative_vehicle_descriptor("puppybot").unwrap();
        let pge_vehicle_desc =
            vehicle_body_desc(&direct_vehicle, &direct_colliders, [0.0; 3], [0.0; 3]);
        let direct_mass = direct
            .authoritative_vehicle_mass_properties("puppybot")
            .unwrap();
        assert!(pge_vehicle_desc.ccd_enabled);
        assert_eq!(pge_vehicle_desc.lock_translation, [false; 3]);
        assert_eq!(pge_vehicle_desc.lock_rotation, [true, true, false]);
        let pge_mass = pge_vehicle_desc.mass.unwrap();
        assert_eq!(pge_mass.mass_kg, direct_mass.mass_kg);
        assert_eq!(pge_mass.center_of_mass_m, direct_mass.center_of_mass_m);
        for (pge, direct) in pge_mass
            .inertia_tensor_kg_m2
            .unwrap()
            .into_iter()
            .flatten()
            .zip(direct_mass.inertia_tensor_kg_m2.into_iter().flatten())
        {
            assert!((pge - direct).abs() <= 1.0e-7);
        }
        let initial_snapshot = pge.snapshot();
        let pge_colliders = initial_snapshot
            .colliders
            .iter()
            .filter(|collider| collider.body_id.0 == "vehicle:puppybot")
            .collect::<Vec<_>>();
        assert_eq!(pge_colliders.len(), direct_colliders.len());
        for (pge_collider, direct_collider) in pge_colliders.iter().zip(&direct_colliders) {
            assert_eq!(pge_collider.desc.pose.translation, direct_collider.offset);
            assert_eq!(
                pge_collider.desc.pose.rotation_xyzw,
                pose_from_rpy(direct_collider.offset, direct_collider.rotation).rotation_xyzw
            );
            assert_eq!(
                pge_collider.desc.shape,
                collider_shape(&direct_collider.geometry)
            );
            assert_eq!(pge_collider.desc.material.friction, 0.5);
            assert_eq!(pge_collider.desc.material.restitution, 0.0);
            assert_eq!(pge_collider.desc.material.density_kg_m3, 0.0);
            assert_eq!(pge_collider.desc.material.contact_skin_m, 0.0);
        }

        let floor = initial_snapshot
            .colliders
            .iter()
            .find(|collider| collider.id.0 == "scene-object:floor_5m:part:0")
            .unwrap();
        let floor_top_z = floor.world_pose.translation[2]
            + match floor.desc.shape {
                ColliderShape::Box { size } => size[2] * 0.5,
                _ => unreachable!("PuppyBot floor is authored as a box"),
            };
        let vehicle_bottom_z = pge_colliders
            .iter()
            .map(|collider| {
                collider.world_pose.translation[2]
                    - match collider.desc.shape {
                        ColliderShape::Box { size } => size[2] * 0.5,
                        _ => unreachable!("PuppyBot vehicle profile is boxes"),
                    }
            })
            .fold(f32::INFINITY, f32::min);
        let initial_penetration_m = floor_top_z - vehicle_bottom_z;
        assert!(initial_penetration_m > 0.015 && initial_penetration_m < 0.016);

        let direct_initial = direct.vehicle_state("puppybot").unwrap().clone();
        for step in 1..=60 {
            pge.step(1.0 / 120.0, f64::from(step) / 120.0).unwrap();
            direct.step(1.0 / 120.0, f64::from(step) / 120.0);
        }
        let pge_after = pge.vehicle_state("puppybot").unwrap();
        let direct_after = direct.vehicle_state("puppybot").unwrap();
        let pge_support_contacts = pge
            .snapshot()
            .contacts
            .iter()
            .filter(|contact| {
                contact.collider1.0 == "scene-object:floor_5m:part:0"
                    && contact.collider2.0.starts_with("vehicle:puppybot:")
                    || contact.collider2.0 == "scene-object:floor_5m:part:0"
                        && contact.collider1.0.starts_with("vehicle:puppybot:")
            })
            .count();
        let direct_support_contacts = direct.authoritative_vehicle_contact_observations("puppybot");
        eprintln!(
            "PuppyBot support characterization after 60 x 1/120s: initial_penetration_m={initial_penetration_m:.6}, pge_dz={:.6}, pge_vz={:.6}, direct_dz={:.6}, direct_vz={:.6}, pge_support_contacts={pge_support_contacts}, direct_support_contacts={}",
            pge_after.position[2] - 0.0,
            pge_after.velocity_mps[2],
            direct_after.position[2] - direct_initial.position[2],
            direct_after.velocity_mps[2],
            direct_support_contacts.len(),
        );
        assert!(pge_support_contacts > 0);
        assert!(!direct_support_contacts.is_empty());
    }

    #[test]
    fn puppybot_startup_settle_matches_direct_support_without_public_timeline_or_events() {
        let project_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("../PuppyBot/robotdreams/project.json");
        let project = crate::project::project_config_from_manifest(&project_path).unwrap();
        let mut pge = PgeScenePhysicsRuntime::from_project(&project).unwrap();
        let mut direct = crate::scene_physics::ScenePhysicsRuntime::from_project(&project).unwrap();

        // The direct oracle has always been given this private startup settle
        // by the PuppyBot harness.  PGE must expose the same supported pose,
        // but begin its public contract at time/frame zero.
        direct.step(3.0, 3.0);
        pge.settle_initial_scene().unwrap();

        let pge_vehicle = pge.vehicle_state("puppybot").unwrap();
        let direct_vehicle = direct.vehicle_state("puppybot").unwrap();
        assert!(
            (pge_vehicle.position[2] - direct_vehicle.position[2]).abs() <= 1.0e-5,
            "PGE={pge_vehicle:?}, direct={direct_vehicle:?}"
        );
        assert_eq!(pge.frame_index(), 0);
        assert!(pge.last_events().is_empty());
        assert!(pge.solved_frame().is_none());
        let snapshot = pge.snapshot();
        assert_eq!(snapshot.step_index, 0);
        assert_eq!(snapshot.simulation_time_sec, 0.0);
    }

    fn calibrated_vehicle_project() -> ProjectConfig {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let fixture_dir = std::env::temp_dir().join(format!(
            "robotdreams-pge-calibration-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir_all(&fixture_dir).unwrap();
        std::fs::write(
            fixture_dir.join("calibration.json"),
            serde_json::json!({
                "format": "robotdreams.calibration.v1",
                "hardware_revision": "pge-fixture-rev",
                "provenance": {
                    "measured_at": "2026-07-22T10:00:00Z",
                    "operator": "fixture",
                    "method": "calibration adapter test",
                    "source": "pge fixture trace"
                },
                "vehicle": {
                    "mass_kg": 2.4,
                    "center_of_mass_m": [0.01, -0.02, 0.03],
                    "wheel_radius_m": 0.04,
                    "gear_ratio": 1.0,
                    "motor_stall_torque_nm": 0.45,
                    "motor_no_load_rpm": 120.0
                },
                "servos": [{"servo_id": 1, "max_speed_ticks_per_sec": 1000.0}],
                "drive_trace": [
                    {"time_sec": 0.0, "left_command": 0.0, "right_command": 0.0, "observed_linear_mps": 0.0, "observed_yaw_rps": 0.0},
                    {"time_sec": 1.0, "left_command": 0.5, "right_command": 0.5, "observed_linear_mps": 0.15, "observed_yaw_rps": 0.0}
                ],
                "servo_trace": [
                    {"time_sec": 0.0, "servo_id": 1, "target_ticks": 2200, "observed_present_ticks": 2048},
                    {"time_sec": 0.2, "servo_id": 1, "target_ticks": 2200, "observed_present_ticks": 2200}
                ]
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            fixture_dir.join("reviewed-colliders.json"),
            serde_json::json!({
                "colliders": [{
                    "shape": "box",
                    "size": [0.24, 0.16, 0.10],
                    "offset": [0.0, 0.0, 0.0],
                    "rotation": [0.0, 0.0, 0.0]
                }]
            })
            .to_string(),
        )
        .unwrap();

        ProjectConfig {
            format: "robotdreams.project.v1".to_string(),
            name: "PGE calibrated vehicle fixture".to_string(),
            manifest_path: fixture_dir.join("project.json"),
            base_dir: fixture_dir,
            model_profile_path: None,
            calibration_record_path: Some("calibration.json".to_string()),
            scene: ProjectSceneConfig::default(),
            robots: vec![ProjectRobotConfig {
                id: "calibrated_rover".to_string(),
                name: "Calibrated Rover".to_string(),
                model: ProjectRobotModelConfig {
                    type_name: "urdf".to_string(),
                    path: "fixture.urdf".to_string(),
                    model_transformation: None,
                },
                joint_names: HashMap::new(),
                base_translation: [0.0, 0.0, 0.1],
                base_rotation: [0.0; 3],
                physics: Some(ProjectRobotPhysicsConfig {
                    vehicle: Some(ProjectVehiclePhysicsConfig {
                        mass_kg: 8.0,
                        center_of_mass_m: [0.0; 3],
                        linear_damping: 0.1,
                        angular_damping: 0.1,
                        wheelbase_m: 0.22,
                        track_width_m: 0.18,
                        max_wheel_speed_mps: 0.4,
                        max_drive_force_n: 18.0,
                        lateral_grip_n_per_mps: 45.0,
                        steering_response_deg_per_sec: 240.0,
                        motor: ProjectVehicleMotorConfig {
                            wheel_radius_m: 0.03,
                            gear_ratio: 10.0,
                            stall_torque_nm: 0.03,
                            no_load_rpm: 60.0,
                            brake_torque_nm: 0.25,
                            rolling_resistance_n: 0.4,
                        },
                        colliders: Vec::new(),
                        collision_profile: Some("reviewed-colliders.json".to_string()),
                    }),
                    link_collision_profile: None,
                }),
            }],
            hardware: HardwareConfig::default(),
        }
    }

    #[test]
    fn calibrated_vehicle_profile_is_published_from_the_pge_solved_frame() {
        let project = calibrated_vehicle_project();
        let mut runtime = PgeScenePhysicsRuntime::from_project(&project).unwrap();

        // Calibration publication is part of a completed frame, rather than
        // construction-time mutable adapter state.
        assert!(runtime.applied_calibrations().is_empty());
        runtime.step(1.0 / 120.0, 1.0 / 120.0).unwrap();

        assert_eq!(runtime.applied_calibrations().len(), 1);
        let applied = &runtime.applied_calibrations()[0];
        assert_eq!(applied.robot_id, "calibrated_rover");
        assert_eq!(applied.hardware_revision, "pge-fixture-rev");
        assert_eq!(applied.provenance.source, "pge fixture trace");
        assert_eq!(
            runtime.solved_frame().unwrap().applied_calibrations,
            runtime.applied_calibrations()
        );

        let snapshot = runtime.snapshot();
        let vehicle = snapshot
            .bodies
            .iter()
            .find(|body| body.id.0 == "vehicle:calibrated_rover")
            .unwrap();
        assert_eq!(vehicle.authored_mass.as_ref().unwrap().mass_kg, 2.4);
        assert_eq!(
            vehicle.authored_mass.as_ref().unwrap().center_of_mass_m,
            [0.01, -0.02, 0.03]
        );
        assert!(
            snapshot
                .colliders
                .iter()
                .any(|collider| collider.id.0 == "vehicle:calibrated_rover:part:0")
        );
    }

    #[test]
    fn persistent_adapter_maps_scene_trigger_and_attachment_contracts() {
        let mut project = ProjectConfig {
            format: "robotdreams.project.v1".to_string(),
            name: "PGE adapter contract fixture".to_string(),
            manifest_path: PathBuf::from("project.json"),
            base_dir: PathBuf::new(),
            model_profile_path: None,
            calibration_record_path: None,
            scene: ProjectSceneConfig {
                objects: vec![ProjectSceneObjectConfig {
                    id: "bottle".to_string(),
                    name: "Bottle".to_string(),
                    type_name: "bottle".to_string(),
                    icon: "BOT".to_string(),
                    geometry: ProjectSceneObjectGeometry::Box {
                        size: [0.04, 0.04, 0.12],
                    },
                    color_rgb: [255, 255, 255],
                    position: [0.0, 0.0, 0.2],
                    rotation: [0.0; 3],
                    scale: None,
                    visual_transform: None,
                    include_in_fit: true,
                    physics: Some(ProjectSceneObjectPhysicsConfig {
                        body_kind: ProjectSceneBodyKind::Dynamic,
                        mass_kg: 0.1,
                        center_of_mass: None,
                        linear_damping: 0.1,
                        angular_damping: 0.1,
                        friction: 0.8,
                        restitution: 0.05,
                        collider: ProjectSceneColliderConfig {
                            geometry: ProjectSceneColliderGeometry::Box {
                                size: [0.04, 0.04, 0.12],
                            },
                            offset: [0.0; 3],
                            rotation: [0.0; 3],
                            children: Vec::new(),
                        },
                    }),
                }],
                triggers: vec![ProjectSceneTriggerConfig {
                    id: "bottle_in_bin".to_string(),
                    object_id: "bottle".to_string(),
                    position: [0.0, 0.0, 0.2],
                    size: [0.2; 3],
                    settle_speed_mps: 0.1,
                    settle_time_sec: 0.1,
                }],
                ..ProjectSceneConfig::default()
            },
            robots: vec![ProjectRobotConfig {
                id: "rover".to_string(),
                name: "Rover".to_string(),
                model: ProjectRobotModelConfig {
                    type_name: "urdf".to_string(),
                    path: "unused-in-adapter-test.urdf".to_string(),
                    model_transformation: None,
                },
                joint_names: HashMap::new(),
                base_translation: [0.0, -0.4, 0.1],
                base_rotation: [0.0; 3],
                physics: Some(ProjectRobotPhysicsConfig {
                    vehicle: Some(ProjectVehiclePhysicsConfig {
                        mass_kg: 2.0,
                        center_of_mass_m: [0.0; 3],
                        linear_damping: 0.1,
                        angular_damping: 0.1,
                        wheelbase_m: 0.2,
                        track_width_m: 0.16,
                        max_wheel_speed_mps: 1.0,
                        max_drive_force_n: 12.0,
                        lateral_grip_n_per_mps: 8.0,
                        steering_response_deg_per_sec: 180.0,
                        motor: ProjectVehicleMotorConfig {
                            wheel_radius_m: 0.03,
                            gear_ratio: 10.0,
                            stall_torque_nm: 0.03,
                            no_load_rpm: 120.0,
                            brake_torque_nm: 0.01,
                            rolling_resistance_n: 0.1,
                        },
                        colliders: vec![ProjectVehicleColliderConfig {
                            geometry: ProjectSceneColliderGeometry::Box {
                                size: [0.24, 0.16, 0.1],
                            },
                            offset: [0.0; 3],
                            rotation: [0.0; 3],
                        }],
                        collision_profile: None,
                    }),
                    link_collision_profile: None,
                }),
            }],
            hardware: HardwareConfig::default(),
        };
        project.scene.objects[0]
            .physics
            .as_mut()
            .unwrap()
            .collider
            .children = vec![
            ProjectSceneColliderChildConfig {
                geometry: ProjectSceneColliderGeometry::Cylinder {
                    radius: 0.018,
                    height: 0.07,
                },
                offset: [0.0, 0.0, 0.01],
                rotation: [0.0; 3],
            },
            ProjectSceneColliderChildConfig {
                geometry: ProjectSceneColliderGeometry::Cylinder {
                    radius: 0.014,
                    height: 0.03,
                },
                offset: [0.0, 0.0, 0.06],
                rotation: [0.0; 3],
            },
            ProjectSceneColliderChildConfig {
                geometry: ProjectSceneColliderGeometry::Box {
                    size: [0.012, 0.04, 0.012],
                },
                offset: [0.0, 0.0, -0.035],
                rotation: [0.0, 0.2, 0.0],
            },
            ProjectSceneColliderChildConfig {
                geometry: ProjectSceneColliderGeometry::Box {
                    size: [0.012, 0.018, 0.03],
                },
                offset: [0.012, 0.0, -0.025],
                rotation: [0.0, 0.0, 0.3],
            },
            ProjectSceneColliderChildConfig {
                geometry: ProjectSceneColliderGeometry::Box {
                    size: [0.012, 0.018, 0.03],
                },
                offset: [-0.012, 0.0, -0.025],
                rotation: [0.0, 0.0, -0.3],
            },
        ];
        let expected_mass =
            scene_object_mass_properties(project.scene.objects[0].physics.as_ref().unwrap());
        let mut runtime = PgeScenePhysicsRuntime::from_project(&project).unwrap();
        let bottle_body = runtime
            .snapshot()
            .bodies
            .into_iter()
            .find(|body| body.id.0 == "scene-object:bottle")
            .unwrap();
        assert_eq!(bottle_body.authored_mass, Some(expected_mass));
        assert_eq!(bottle_body.effective_mass.mass_kg, expected_mass.mass_kg);
        assert_eq!(
            bottle_body.effective_mass.center_of_mass_m,
            expected_mass.center_of_mass_m
        );
        for (actual, expected) in bottle_body
            .effective_mass
            .inertia_tensor_kg_m2
            .into_iter()
            .flatten()
            .zip(
                expected_mass
                    .inertia_tensor_kg_m2
                    .unwrap()
                    .into_iter()
                    .flatten(),
            )
        {
            assert!((actual - expected).abs() <= 1.0e-9);
        }
        let vehicle_colliders = runtime
            .snapshot()
            .colliders
            .into_iter()
            .filter(|collider| collider.body_id.0 == "vehicle:rover")
            .collect::<Vec<_>>();
        assert_eq!(vehicle_colliders.len(), 1);
        let vehicle_collider = &vehicle_colliders[0];
        assert_eq!(vehicle_collider.id.0, "vehicle:rover:part:0");
        assert_eq!(vehicle_collider.desc.pose.translation, [0.0; 3]);
        assert_eq!(
            vehicle_collider.desc.pose.rotation_xyzw,
            [0.0, 0.0, 0.0, 1.0]
        );
        assert!(
            matches!(vehicle_collider.desc.shape, pge_physics::ColliderShape::Box { size } if size == [0.24, 0.16, 0.1])
        );
        assert_eq!(vehicle_collider.desc.material.friction, 0.5);
        assert_eq!(vehicle_collider.desc.material.restitution, 0.0);
        assert_eq!(vehicle_collider.desc.material.density_kg_m3, 0.0);
        assert!(!vehicle_collider.desc.sensor);
        assert_eq!(vehicle_collider.desc.collision_memberships, u32::MAX);
        assert_eq!(vehicle_collider.desc.collision_filter, u32::MAX);
        assert!(runtime.object_state("bottle").unwrap().dynamic);
        assert!(runtime.trigger_state("bottle_in_bin").is_some());
        assert!(
            runtime
                .collider_debug_entries()
                .iter()
                .any(|entry| entry.id == "scene-object:bottle")
        );

        runtime
            .attach(
                "bottle",
                SceneObjectAttachment {
                    robot_id: "robot".to_string(),
                    frame_name: "tcp".to_string(),
                    offset_m: [0.0; 3],
                    rotation_offset_rpy: [0.0; 3],
                },
                [0.0, 0.0, 0.2],
                [0.0; 3],
            )
            .unwrap();
        runtime
            .set_attached_pose("bottle", [0.02, 0.0, 0.2], [0.0; 3])
            .unwrap();
        assert_eq!(
            runtime.object_state("bottle").unwrap().position,
            [0.02, 0.0, 0.2]
        );
        runtime.detach("bottle").unwrap();
        runtime
            .drive_vehicle(
                "rover",
                VehicleDriveCommand {
                    left_command: 0.5,
                    right_command: 0.5,
                    brake: false,
                    steering_target_rad: 0.1,
                },
                1.0 / 120.0,
            )
            .unwrap();
        let link = RobotLinkCollider {
            robot_id: "arm".to_string(),
            link_name: "gripper".to_string(),
            reviewed_profile_path: PathBuf::from("reviewed-link-profile.json"),
            candidate_artifact_path: None,
            geometry: ProjectSceneColliderGeometry::Box {
                size: [0.06, 0.06, 0.12],
            },
            translation: [0.02, 0.0, 0.2],
            rotation_matrix: [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]],
        };
        runtime.sync_robot_link_colliders(&[link]).unwrap();
        runtime.step(1.0 / 120.0, 1.0 / 120.0).unwrap();
        assert!(runtime.object_state("bottle").unwrap().attachment.is_none());
        assert_eq!(runtime.frame_index(), 1);
        assert!(
            runtime
                .vehicle_state("rover")
                .unwrap()
                .actuator
                .left_drive_force_n
                > 0.0
        );
        let snapshot = runtime.snapshot();
        assert!(
            snapshot
                .bodies
                .iter()
                .any(|body| body.id.0 == "vehicle:rover")
        );
        assert!(
            snapshot
                .bodies
                .iter()
                .any(|body| body.id.0 == "kinematic-link:arm:gripper:0")
        );
        let debug = runtime.collider_debug_entries();
        assert!(debug.iter().any(|entry| entry.id == "vehicle:rover:0"));
        assert!(
            debug
                .iter()
                .any(|entry| entry.id == "kinematic-link:arm:gripper:0")
        );
        assert!(!runtime.robot_link_contacts("bottle").is_empty());
        assert!(snapshot.contacts.iter().any(|contact| {
            contact.collider1.0 == "kinematic-link:arm:gripper:0:part:0"
                || contact.collider2.0 == "kinematic-link:arm:gripper:0:part:0"
        }));

        let published_object = runtime.object_state("bottle").unwrap().clone();
        let published_trigger = runtime.trigger_state("bottle_in_bin").unwrap().clone();
        runtime
            .set_attached_pose("bottle", [0.7, 0.0, 0.2], [0.0; 3])
            .unwrap();
        assert_eq!(runtime.object_state("bottle"), Some(&published_object));
        assert_eq!(
            runtime.trigger_state("bottle_in_bin"),
            Some(&published_trigger)
        );

        let published_vehicle = runtime.vehicle_state("rover").unwrap().clone();
        runtime
            .drive_vehicle(
                "rover",
                VehicleDriveCommand {
                    left_command: -0.5,
                    right_command: -0.5,
                    brake: true,
                    steering_target_rad: -0.1,
                },
                1.0 / 120.0,
            )
            .unwrap();
        assert_eq!(runtime.vehicle_state("rover"), Some(&published_vehicle));

        runtime.sync_robot_link_colliders(&[]).unwrap();
        // Query facades continue to expose the completed frame until the next PGE
        // step commits a replacement frame, even though the live world changed.
        assert!(
            runtime
                .collider_debug_entries()
                .iter()
                .any(|entry| entry.id == "kinematic-link:arm:gripper:0")
        );
        assert_eq!(
            runtime.last_events(),
            runtime.solved_frame().unwrap().events.as_slice()
        );
        assert_eq!(
            runtime.applied_calibrations(),
            runtime.solved_frame().unwrap().applied_calibrations.clone()
        );
        assert!(
            runtime
                .snapshot()
                .bodies
                .iter()
                .any(|body| body.id.0 == "kinematic-link:arm:gripper:0")
        );
        runtime.step(1.0 / 120.0, 2.0 / 120.0).unwrap();
        assert!(
            runtime
                .snapshot()
                .bodies
                .iter()
                .all(|body| body.id.0 != "kinematic-link:arm:gripper:0")
        );
        assert!(
            runtime
                .collider_debug_entries()
                .iter()
                .all(|entry| entry.id != "kinematic-link:arm:gripper:0")
        );
        assert!(runtime.last_events().iter().any(|event| {
            matches!(event.kind, pge_physics::PhysicsEventKind::ContactStopped)
                && (event.collider1.0 == "kinematic-link:arm:gripper:0:part:0"
                    || event.collider2.0 == "kinematic-link:arm:gripper:0:part:0")
        }));
    }
}
