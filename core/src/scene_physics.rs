use std::collections::BTreeMap;
use std::error::Error;

use pge_core as pge;
use pge_physics::rapier3d::na::Matrix3;
use pge_physics::rapier3d::prelude::*;

use crate::calibration::{
    MeasurementProvenance, apply_calibration_to_vehicle_profile, load_calibration,
};
use crate::project::{
    ProjectConfig, ProjectSceneBodyKind, ProjectSceneColliderConfig,
    ProjectSceneColliderGeometry, ProjectSceneObjectPhysicsConfig, ProjectSceneTriggerConfig,
    ProjectVehicleColliderConfig, ProjectVehiclePhysicsConfig, RobotLinkCollider,
};

#[derive(Clone, Debug, PartialEq)]
pub struct SceneObjectAttachment {
    pub robot_id: String,
    pub frame_name: String,
    pub offset_m: [f32; 3],
    /// Orientation of the object in the held frame at grasp time.  Together
    /// with `offset_m` this is the stable local grasp transform.
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

/// Solved state of a robot base using the dynamic vehicle path.
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

/// A physical contact between a dynamic scene object and a reviewed robot-link
/// profile.  This is evidence from Rapier's narrow phase, not a proximity
/// test and not an attachment request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RobotLinkContact {
    pub robot_id: String,
    pub link_name: String,
    pub object_id: String,
}

/// Safety limits for kinematically tracked reviewed-link colliders.
///
/// These bodies are a narrow bridge between an observed URDF pose and Rapier;
/// they are not a substitute for an articulated dynamic arm.  Position-based
/// kinematic bodies derive their contact velocity from successive poses, so an
/// uncapped pose jump would otherwise inject an unbounded impulse into a
/// dynamic scene object.
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

#[derive(Clone, Debug)]
struct PhysicsObject {
    handle: RigidBodyHandle,
    state: SceneObjectState,
    collider: ProjectSceneColliderConfig,
}

#[derive(Clone, Debug)]
struct TriggerRuntime {
    config: ProjectSceneTriggerConfig,
    state: SceneTriggerState,
}

#[derive(Clone, Debug)]
struct PhysicsVehicle {
    handle: RigidBodyHandle,
    config: ProjectVehiclePhysicsConfig,
    colliders: Vec<ProjectVehicleColliderConfig>,
    steering_angle_rad: f32,
    actuator: VehicleActuatorState,
    state: VehiclePhysicsState,
}

#[derive(Clone, Debug)]
struct PhysicsRobotLinkCollider {
    robot_id: String,
    link_name: String,
    body: RigidBodyHandle,
    collider: ColliderHandle,
    geometry: ProjectSceneColliderGeometry,
    target_pose: Isometry<Real>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct LivePhysicsColliderDebugPart {
    pub geometry: ProjectSceneColliderGeometry,
    pub local_transform: pge::Transform,
}

/// A live Rapier collider expressed in the narrow form required by the PGE
/// debug-overlay adapter.  This stays separate from rendering scene nodes so
/// a debug view represents the actual solver shape, rather than a mesh bound
/// or a visual-only asset transform.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct LivePhysicsColliderDebugEntry {
    pub id: String,
    pub category: &'static str,
    pub color: [f32; 4],
    pub transform: pge::Transform,
    pub parts: Vec<LivePhysicsColliderDebugPart>,
}

pub(crate) struct ScenePhysicsRuntime {
    world: LivePhysicsWorld,
    objects: BTreeMap<String, PhysicsObject>,
    triggers: BTreeMap<String, TriggerRuntime>,
    vehicles: BTreeMap<String, PhysicsVehicle>,
    robot_link_colliders: BTreeMap<String, PhysicsRobotLinkCollider>,
    kinematic_collider_motion: KinematicColliderMotionConfig,
    calibrations: Vec<AppliedCalibrationState>,
}

struct LivePhysicsWorld {
    gravity: Vector<Real>,
    integration_parameters: IntegrationParameters,
    pipeline: PhysicsPipeline,
    island_manager: IslandManager,
    broad_phase: DefaultBroadPhase,
    narrow_phase: NarrowPhase,
    bodies: RigidBodySet,
    colliders: ColliderSet,
    impulse_joints: ImpulseJointSet,
    multibody_joints: MultibodyJointSet,
    ccd_solver: CCDSolver,
    query_pipeline: QueryPipeline,
}

impl LivePhysicsWorld {
    fn new() -> Self {
        Self {
            gravity: Vector::new(0.0, 0.0, -9.81),
            integration_parameters: IntegrationParameters::default(),
            pipeline: PhysicsPipeline::new(),
            island_manager: IslandManager::new(),
            broad_phase: DefaultBroadPhase::new(),
            narrow_phase: NarrowPhase::new(),
            bodies: RigidBodySet::new(),
            colliders: ColliderSet::new(),
            impulse_joints: ImpulseJointSet::new(),
            multibody_joints: MultibodyJointSet::new(),
            ccd_solver: CCDSolver::new(),
            query_pipeline: QueryPipeline::new(),
        }
    }

    fn insert_compound(&mut self, body: RigidBody, colliders: Vec<Collider>) -> RigidBodyHandle {
        let handle = self.bodies.insert(body);
        for collider in colliders {
            self.colliders
                .insert_with_parent(collider, handle, &mut self.bodies);
        }
        self.bodies
            .get_mut(handle)
            .expect("inserted body exists")
            .recompute_mass_properties_from_colliders(&self.colliders);
        handle
    }

    fn insert_kinematic_collider(
        &mut self,
        body: RigidBody,
        collider: Collider,
    ) -> (RigidBodyHandle, ColliderHandle) {
        let body_handle = self.bodies.insert(body);
        let collider_handle =
            self.colliders
                .insert_with_parent(collider, body_handle, &mut self.bodies);
        (body_handle, collider_handle)
    }

    fn set_time_step(&mut self, dt: f32) {
        self.integration_parameters.dt = dt;
    }

    fn step(&mut self) {
        self.pipeline.step(
            &self.gravity,
            &self.integration_parameters,
            &mut self.island_manager,
            &mut self.broad_phase,
            &mut self.narrow_phase,
            &mut self.bodies,
            &mut self.colliders,
            &mut self.impulse_joints,
            &mut self.multibody_joints,
            &mut self.ccd_solver,
            Some(&mut self.query_pipeline),
            &(),
            &(),
        );
    }

    fn body(&self, handle: RigidBodyHandle) -> Option<&RigidBody> {
        self.bodies.get(handle)
    }

    fn body_mut(&mut self, handle: RigidBodyHandle) -> Option<&mut RigidBody> {
        self.bodies.get_mut(handle)
    }
}

fn vec3(value: [f32; 3]) -> Vector<f32> {
    Vector::new(value[0], value[1], value[2])
}

fn pge_transform(translation: [f32; 3], rotation: [f32; 3]) -> pge::Transform {
    pge::Transform {
        translation,
        rotation,
        rotation_matrix: None,
    }
}

fn rotation_from_matrix(matrix: [[f32; 3]; 3]) -> Rotation<Real> {
    Rotation::from_matrix(&Matrix::from_row_slice(&[
        matrix[0][0],
        matrix[0][1],
        matrix[0][2],
        matrix[1][0],
        matrix[1][1],
        matrix[1][2],
        matrix[2][0],
        matrix[2][1],
        matrix[2][2],
    ]))
}

fn robot_link_collider_pose(collider: &RobotLinkCollider) -> Isometry<Real> {
    Isometry::from_parts(
        Translation::from(vec3(collider.translation)),
        rotation_from_matrix(collider.rotation_matrix),
    )
}

fn robot_link_collider_key(collider: &RobotLinkCollider, index: usize) -> String {
    format!("{}:{}:{index}", collider.robot_id, collider.link_name)
}

fn bounded_kinematic_pose(
    current: Isometry<Real>,
    target: Isometry<Real>,
    config: KinematicColliderMotionConfig,
    dt: f32,
) -> Isometry<Real> {
    let distance = (target.translation.vector - current.translation.vector).norm();
    let rotation_delta = (current.rotation.inverse() * target.rotation).angle();
    let linear_fraction = if distance <= f32::EPSILON {
        1.0
    } else {
        (config.maximum_linear_speed_mps * dt / distance).min(1.0)
    };
    let angular_fraction = if rotation_delta <= f32::EPSILON {
        1.0
    } else {
        (config.maximum_angular_speed_rps * dt / rotation_delta).min(1.0)
    };
    let fraction = linear_fraction.min(angular_fraction);
    if fraction >= 1.0 {
        target
    } else {
        // A failed interpolation is safer as a stationary collider than a
        // discontinuous rotational jump with an unbounded contact velocity.
        current
            .try_lerp_slerp(&target, fraction, 1.0e-6)
            .unwrap_or(current)
    }
}

fn kinematic_substep_count(dt: f32, config: KinematicColliderMotionConfig) -> u32 {
    (dt / config.maximum_substep_seconds).ceil().max(1.0) as u32
}

fn validate_kinematic_collider_motion(
    config: KinematicColliderMotionConfig,
) -> Result<(), Box<dyn Error>> {
    if !config.maximum_linear_speed_mps.is_finite() || config.maximum_linear_speed_mps <= 0.0 {
        return Err("kinematic collider maximumLinearSpeedMps must be finite and positive".into());
    }
    if !config.maximum_angular_speed_rps.is_finite() || config.maximum_angular_speed_rps <= 0.0 {
        return Err("kinematic collider maximumAngularSpeedRps must be finite and positive".into());
    }
    if !config.maximum_substep_seconds.is_finite() || config.maximum_substep_seconds <= 0.0 {
        return Err("kinematic collider maximumSubstepSeconds must be finite and positive".into());
    }
    Ok(())
}

fn collider_builder(geometry: &ProjectSceneColliderGeometry) -> ColliderBuilder {
    match geometry {
        ProjectSceneColliderGeometry::Box { size } => {
            ColliderBuilder::cuboid(size[0] * 0.5, size[1] * 0.5, size[2] * 0.5)
        }
        ProjectSceneColliderGeometry::Sphere { radius } => ColliderBuilder::ball(*radius),
        ProjectSceneColliderGeometry::Cylinder { radius, height } => {
            ColliderBuilder::cylinder(height * 0.5, *radius)
        }
    }
}

fn scene_collider_builder(
    part: &crate::project::ProjectSceneColliderChildConfig,
) -> ColliderBuilder {
    collider_builder(&part.geometry)
        .position(Isometry::from_parts(
            Translation::from(vec3(part.offset)),
            Rotation::from_euler_angles(part.rotation[0], part.rotation[1], part.rotation[2]),
        ))
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

fn scene_collider_inertia(
    geometry: &ProjectSceneColliderGeometry,
    mass_kg: f32,
) -> Matrix3<Real> {
    let diagonal = match geometry {
        ProjectSceneColliderGeometry::Box { size } => Vector::new(
            mass_kg * (size[1].powi(2) + size[2].powi(2)) / 12.0,
            mass_kg * (size[0].powi(2) + size[2].powi(2)) / 12.0,
            mass_kg * (size[0].powi(2) + size[1].powi(2)) / 12.0,
        ),
        ProjectSceneColliderGeometry::Sphere { radius } => {
            Vector::repeat(2.0 * mass_kg * radius.powi(2) / 5.0)
        }
        // Rapier cylinders, like PGE's collider wireframes, have their
        // height on local Y. Preserve that convention in the inertia tensor.
        ProjectSceneColliderGeometry::Cylinder { radius, height } => {
            let transverse = mass_kg * (3.0 * radius.powi(2) + height.powi(2)) / 12.0;
            Vector::new(transverse, mass_kg * radius.powi(2) / 2.0, transverse)
        }
    };
    Matrix3::from_diagonal(&diagonal)
}

fn scene_object_mass_properties(config: &ProjectSceneObjectPhysicsConfig) -> MassProperties {
    let parts = config.collider.parts();
    let total_volume = parts
        .iter()
        .map(|part| scene_collider_volume(&part.geometry))
        .sum::<f32>()
        .max(0.000_001);
    let mass_kg = config.mass_kg.max(0.000_001);
    let parts_with_mass = parts
        .iter()
        .map(|part| {
            let mass = mass_kg * scene_collider_volume(&part.geometry) / total_volume;
            (part, mass)
        })
        .collect::<Vec<_>>();
    let derived_center_of_mass = parts_with_mass.iter().fold(Vector::zeros(), |sum, (part, mass)| {
        sum + vec3(part.offset) * *mass
    }) / mass_kg;
    let center_of_mass = config
        .center_of_mass
        .map(vec3)
        .unwrap_or(derived_center_of_mass);

    // Colliders have zero density because this is the sole source of the
    // dynamic body's mass. Assemble the primitive tensor directly so the
    // offset and rotation of every part remain in the object's coordinate
    // frame, then apply the parallel-axis theorem at the authored COM.
    let inertia = parts_with_mass.iter().fold(Matrix3::zeros(), |sum, (part, mass)| {
        let rotation = Rotation::from_euler_angles(
            part.rotation[0],
            part.rotation[1],
            part.rotation[2],
        )
        .to_rotation_matrix()
        .into_inner();
        let local_inertia = rotation
            * scene_collider_inertia(&part.geometry, *mass)
            * rotation.transpose();
        let offset = vec3(part.offset) - center_of_mass;
        sum + local_inertia
            + *mass
                * (Matrix3::identity() * offset.norm_squared() - offset * offset.transpose())
    });

    MassProperties::with_inertia_matrix(Point::from(center_of_mass), mass_kg, inertia)
}

fn scene_collider_bounding_radius(config: &ProjectSceneColliderConfig) -> f32 {
    config
        .parts()
        .into_iter()
        .map(|part| geometry_bounding_radius(&part.geometry) + vec3(part.offset).norm())
        .fold(0.0, f32::max)
}

fn vehicle_collider_builder(config: &ProjectVehicleColliderConfig) -> ColliderBuilder {
    collider_builder(&config.geometry)
        .position(Isometry::from_parts(
            Translation::from(vec3(config.offset)),
            Rotation::from_euler_angles(
                config.rotation[0],
                config.rotation[1],
                config.rotation[2],
            ),
        ))
        // Vehicle mass and COM are explicitly authored on the rigid body.
        .density(0.0)
}

fn vehicle_state(
    robot_id: String,
    body: &RigidBody,
    actuator: VehicleActuatorState,
) -> VehiclePhysicsState {
    let (roll, pitch, yaw) = body.rotation().euler_angles();
    VehiclePhysicsState {
        robot_id,
        position: [
            body.translation().x,
            body.translation().y,
            body.translation().z,
        ],
        rotation: [roll, pitch, yaw],
        velocity_mps: [body.linvel().x, body.linvel().y, body.linvel().z],
        angular_velocity_rps: [body.angvel().x, body.angvel().y, body.angvel().z],
        actuator,
    }
}

fn vehicle_inertia(mass_kg: f32, colliders: &[ProjectVehicleColliderConfig]) -> Vector<Real> {
    let mut half_extents = Vector::new(0.01, 0.01, 0.01);
    for collider in colliders {
        let extent = match collider.geometry {
            ProjectSceneColliderGeometry::Box { size } => {
                Vector::new(size[0] * 0.5, size[1] * 0.5, size[2] * 0.5)
            }
            ProjectSceneColliderGeometry::Sphere { radius } => Vector::repeat(radius),
            ProjectSceneColliderGeometry::Cylinder { radius, height } => {
                Vector::new(radius, radius, height * 0.5)
            }
        };
        let offset = vec3(collider.offset).abs();
        half_extents = half_extents.sup(&(offset + extent));
    }
    let size = half_extents * 2.0;
    Vector::new(
        mass_kg * (size.y * size.y + size.z * size.z) / 12.0,
        mass_kg * (size.x * size.x + size.z * size.z) / 12.0,
        mass_kg * (size.x * size.x + size.y * size.y) / 12.0,
    )
    .map(|value| value.max(1.0e-6))
}

fn speed(velocity: [f32; 3]) -> f32 {
    velocity
        .iter()
        .map(|component| component * component)
        .sum::<f32>()
        .sqrt()
}

fn geometry_bounding_radius(geometry: &ProjectSceneColliderGeometry) -> f32 {
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

/// Returns whether the physics-updated center of an object is in an AABB trigger.
///
/// Triggers intentionally use center containment rather than requiring a whole
/// collider to fit. This lets a ball resting on a physical bin bottom remain
/// detectable when its surface overlaps the authored trigger edge slightly.
fn center_inside_box(center: [f32; 3], box_center: [f32; 3], box_size: [f32; 3]) -> bool {
    (0..3).all(|axis| {
        let half_extent = box_size[axis] * 0.5;
        (center[axis] - box_center[axis]).abs() <= half_extent
    })
}

/// Reads the narrow generated-collider interchange used at the RobotDreams /
/// PGE boundary.  Keeping this adapter here means collision generation never
/// runs during simulation startup: the product checks in and reviews the
/// generated profile first.
fn generated_vehicle_colliders(
    project: &ProjectConfig,
    vehicle: &ProjectVehiclePhysicsConfig,
) -> Result<Vec<ProjectVehicleColliderConfig>, Box<dyn Error>> {
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
        // Direct PGE `CollisionCandidates` output: its conservative compound
        // boxes are already expressed in local metres and are the safest
        // automatic candidates to promote after product review.
        parts
            .iter()
            .map(|part| {
                serde_json::json!({
                    "shape": "box",
                    "size": part.get("size").cloned().unwrap_or(serde_json::Value::Null),
                    "offset": part.get("center").cloned().unwrap_or(serde_json::Value::Null),
                })
            })
            .collect()
    } else {
        return Err(format!(
            "vehicle collision profile '{}' must be RobotDreams reviewed {{colliders:[...]}} or PGE CollisionCandidates with compounds[0].parts",
            path.display()
        )
        .into());
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
                        .filter(|radius| radius.is_finite() && *radius > 0.0)
                        .ok_or_else(|| "sphere collider needs positive 'radius'".to_string())?
                        as f32,
                },
                "cylinder" => ProjectSceneColliderGeometry::Cylinder {
                    radius: collider
                        .get("radius")
                        .and_then(serde_json::Value::as_f64)
                        .filter(|radius| radius.is_finite() && *radius > 0.0)
                        .ok_or_else(|| "cylinder collider needs positive 'radius'".to_string())?
                        as f32,
                    height: collider
                        .get("height")
                        .and_then(serde_json::Value::as_f64)
                        .filter(|height| height.is_finite() && *height > 0.0)
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

impl ScenePhysicsRuntime {
    pub(crate) fn empty() -> Self {
        Self {
            world: LivePhysicsWorld::new(),
            objects: BTreeMap::new(),
            triggers: BTreeMap::new(),
            vehicles: BTreeMap::new(),
            robot_link_colliders: BTreeMap::new(),
            kinematic_collider_motion: KinematicColliderMotionConfig::default(),
            calibrations: Vec::new(),
        }
    }

    pub(crate) fn from_project(project: &ProjectConfig) -> Result<Self, Box<dyn Error>> {
        let mut world = LivePhysicsWorld::new();
        let mut objects = BTreeMap::new();

        for object in &project.scene.objects {
            let Some(config) = &object.physics else {
                continue;
            };
            let mut rigid_body = match config.body_kind {
                ProjectSceneBodyKind::Static => RigidBodyBuilder::fixed(),
                ProjectSceneBodyKind::Dynamic => RigidBodyBuilder::dynamic(),
            }
            .position(Isometry::from_parts(
                Translation::from(vec3(object.position)),
                Rotation::from_euler_angles(
                    object.rotation[0],
                    object.rotation[1],
                    object.rotation[2],
                ),
            ))
            .linear_damping(config.linear_damping)
            .angular_damping(config.angular_damping);
            let uses_explicit_mass_properties = config.body_kind == ProjectSceneBodyKind::Dynamic
                && (!config.collider.children.is_empty() || config.center_of_mass.is_some());
            if uses_explicit_mass_properties {
                rigid_body = rigid_body.additional_mass_properties(scene_object_mass_properties(config));
            }
            let rigid_body = rigid_body.build();
            let colliders = config
                .collider
                .parts()
                .into_iter()
                .map(|part| {
                    let builder = scene_collider_builder(&part)
                        .friction(config.friction.max(0.0))
                        .restitution(config.restitution.clamp(0.0, 1.0));
                    if uses_explicit_mass_properties {
                        builder.density(0.0).build()
                    } else {
                        builder.mass(config.mass_kg.max(0.000_001)).build()
                    }
                })
                .collect::<Vec<_>>();
            let handle = world.insert_compound(rigid_body, colliders);
            objects.insert(
                object.id.clone(),
                PhysicsObject {
                    handle,
                    state: SceneObjectState {
                        id: object.id.clone(),
                        position: object.position,
                        rotation: object.rotation,
                        velocity_mps: [0.0; 3],
                        angular_velocity_rps: [0.0; 3],
                        dynamic: config.body_kind == ProjectSceneBodyKind::Dynamic,
                        attachment: None,
                    },
                    collider: config.collider.clone(),
                },
            );
        }

        let mut triggers = BTreeMap::new();
        for config in &project.scene.triggers {
            if !objects.contains_key(&config.object_id) {
                return Err(format!(
                    "scene trigger '{}' refers to non-physical object '{}'",
                    config.id, config.object_id
                )
                .into());
            }
            triggers.insert(
                config.id.clone(),
                TriggerRuntime {
                    config: config.clone(),
                    state: SceneTriggerState {
                        id: config.id.clone(),
                        object_id: config.object_id.clone(),
                        inside: false,
                        entered: false,
                        entered_at_sec: None,
                        entry_count: 0,
                        settled: false,
                        triggered: false,
                        triggered_at_sec: None,
                        settled_time_sec: 0.0,
                    },
                },
            );
        }

        let calibration = match &project.calibration_record_path {
            Some(path) => Some(
                load_calibration(project.base_dir.join(path)).map_err(|error| {
                    format!("project calibrationRecord '{path}' could not be loaded: {error}")
                })?,
            ),
            None => None,
        };
        let mut vehicles = BTreeMap::new();
        let mut calibrations = Vec::new();
        for robot in &project.robots {
            let Some(vehicle) = robot
                .physics
                .as_ref()
                .and_then(|physics| physics.vehicle.as_ref())
            else {
                continue;
            };
            let calibrated = calibration
                .as_ref()
                .map(|record| apply_calibration_to_vehicle_profile(vehicle, record))
                .transpose()
                .map_err(|error| format!("project calibrationRecord is invalid: {error}"))?;
            let vehicle = calibrated
                .as_ref()
                .map(|calibrated| &calibrated.vehicle)
                .unwrap_or(vehicle);
            if let Some(calibrated) = &calibrated {
                calibrations.push(AppliedCalibrationState {
                    robot_id: robot.id.clone(),
                    hardware_revision: calibrated.hardware_revision.clone(),
                    provenance: calibrated.provenance.clone(),
                });
            }
            let mut colliders = vehicle.colliders.clone();
            colliders.extend(generated_vehicle_colliders(project, vehicle)?);
            if colliders.is_empty() {
                return Err(format!(
                    "dynamic vehicle '{}' needs at least one reviewed collider",
                    robot.id
                )
                .into());
            }
            let inertia = vehicle_inertia(vehicle.mass_kg, &colliders);
            let body = RigidBodyBuilder::dynamic()
                .position(Isometry::from_parts(
                    Translation::from(vec3(robot.base_translation)),
                    Rotation::from_euler_angles(
                        robot.base_rotation[0],
                        robot.base_rotation[1],
                        robot.base_rotation[2],
                    ),
                ))
                .additional_mass_properties(MassProperties::new(
                    Point::from(vec3(vehicle.center_of_mass_m)),
                    vehicle.mass_kg,
                    inertia,
                ))
                .linear_damping(vehicle.linear_damping)
                .angular_damping(vehicle.angular_damping)
                // This is a 2.5D vehicle model: vertical collision/gravity and
                // yaw are solved, while wheel suspension/tipping wait for an
                // articulated wheel model and measured inertias.
                .enabled_rotations(false, false, true)
                .ccd_enabled(true)
                .build();
            let collider_bodies = colliders
                .iter()
                .map(|collider| vehicle_collider_builder(collider).build())
                .collect();
            let handle = world.insert_compound(body, collider_bodies);
            let body = world
                .body(handle)
                .expect("newly inserted vehicle body is available");
            vehicles.insert(
                robot.id.clone(),
                PhysicsVehicle {
                    handle,
                    config: vehicle.clone(),
                    colliders,
                    steering_angle_rad: 0.0,
                    actuator: VehicleActuatorState {
                        left_command: 0.0,
                        right_command: 0.0,
                        left_drive_force_n: 0.0,
                        right_drive_force_n: 0.0,
                        braking: false,
                    },
                    state: vehicle_state(
                        robot.id.clone(),
                        body,
                        VehicleActuatorState {
                            left_command: 0.0,
                            right_command: 0.0,
                            left_drive_force_n: 0.0,
                            right_drive_force_n: 0.0,
                            braking: false,
                        },
                    ),
                },
            );
        }

        Ok(Self {
            world,
            objects,
            triggers,
            vehicles,
            robot_link_colliders: BTreeMap::new(),
            kinematic_collider_motion: KinematicColliderMotionConfig::default(),
            calibrations,
        })
    }

    pub(crate) fn object_state(&self, id: &str) -> Option<&SceneObjectState> {
        self.objects.get(id).map(|object| &object.state)
    }

    pub(crate) fn object_states(&self) -> Vec<SceneObjectState> {
        self.objects
            .values()
            .map(|object| object.state.clone())
            .collect()
    }

    pub(crate) fn robot_link_collider_spawn_is_clear(
        &self,
        object_id: &str,
        collider: &RobotLinkCollider,
    ) -> bool {
        const SPAWN_CLEARANCE_M: f32 = 0.002;
        let Some(object) = self.objects.get(object_id) else {
            return false;
        };
        let object_radius = scene_collider_bounding_radius(&object.collider);
        let link_radius = geometry_bounding_radius(&collider.geometry);
        let distance = (vec3(collider.translation) - vec3(object.state.position)).norm();
        distance > object_radius + link_radius + SPAWN_CLEARANCE_M
    }

    pub(crate) fn trigger_state(&self, id: &str) -> Option<&SceneTriggerState> {
        self.triggers.get(id).map(|trigger| &trigger.state)
    }

    pub(crate) fn trigger_states(&self) -> Vec<SceneTriggerState> {
        self.triggers
            .values()
            .map(|trigger| trigger.state.clone())
            .collect()
    }

    pub(crate) fn vehicle_state(&self, robot_id: &str) -> Option<&VehiclePhysicsState> {
        self.vehicles.get(robot_id).map(|vehicle| &vehicle.state)
    }

    pub(crate) fn vehicle_states(&self) -> Vec<VehiclePhysicsState> {
        self.vehicles
            .values()
            .map(|vehicle| vehicle.state.clone())
            .collect()
    }

    pub(crate) fn has_vehicle(&self, robot_id: &str) -> bool {
        self.vehicles.contains_key(robot_id)
    }

    pub(crate) fn applied_calibrations(&self) -> Vec<AppliedCalibrationState> {
        self.calibrations.clone()
    }

    /// Exports the solver's actual primitive colliders for PGE's generic
    /// debug overlay.  These entries deliberately do not create scene nodes:
    /// the overlay is observability metadata and must not alter PGE fitting or
    /// physics.  The returned order is stable because all live registries are
    /// keyed by `BTreeMap`.
    pub(crate) fn collider_debug_entries(&self) -> Vec<LivePhysicsColliderDebugEntry> {
        let mut entries = Vec::new();

        for (object_id, object) in &self.objects {
            entries.push(LivePhysicsColliderDebugEntry {
                id: format!("scene-object:{object_id}"),
                category: if object.state.dynamic {
                    "sceneDynamicCollider"
                } else {
                    "sceneStaticCollider"
                },
                color: if object.state.dynamic {
                    [1.0, 0.72, 0.16, 1.0]
                } else {
                    [0.18, 0.76, 0.95, 1.0]
                },
                transform: pge_transform(object.state.position, object.state.rotation),
                parts: object
                    .collider
                    .parts()
                    .into_iter()
                    .map(|part| LivePhysicsColliderDebugPart {
                        geometry: part.geometry,
                        local_transform: pge_transform(part.offset, part.rotation),
                    })
                    .collect(),
            });
        }

        for (robot_id, vehicle) in &self.vehicles {
            for (index, collider) in vehicle.colliders.iter().enumerate() {
                entries.push(LivePhysicsColliderDebugEntry {
                    id: format!("vehicle:{robot_id}:{index}"),
                    category: "vehicleCollider",
                    color: [0.18, 0.95, 0.45, 1.0],
                    transform: pge_transform(vehicle.state.position, vehicle.state.rotation),
                    parts: vec![LivePhysicsColliderDebugPart {
                        geometry: collider.geometry.clone(),
                        local_transform: pge_transform(collider.offset, collider.rotation),
                    }],
                });
            }
        }

        for (key, collider) in &self.robot_link_colliders {
            let Some(body) = self.world.body(collider.body) else {
                continue;
            };
            let (roll, pitch, yaw) = body.rotation().euler_angles();
            entries.push(LivePhysicsColliderDebugEntry {
                id: format!("kinematic-link:{key}"),
                category: "kinematicRobotLinkCollider",
                color: [1.0, 0.28, 0.78, 1.0],
                transform: pge_transform(
                    [
                        body.translation().x,
                        body.translation().y,
                        body.translation().z,
                    ],
                    [roll, pitch, yaw],
                ),
                parts: vec![LivePhysicsColliderDebugPart {
                    geometry: collider.geometry.clone(),
                    local_transform: pge::Transform::default(),
                }],
            });
        }

        entries
    }

    /// Current world transforms for live PGE collider diagnostics, keyed by
    /// the stable ids emitted from [`Self::collider_debug_entries`].
    ///
    /// This deliberately does not clone collider geometry. A renderer that
    /// has retained the debug wireframe shapes can refresh only these poses on
    /// each simulation frame.
    pub(crate) fn collider_debug_transforms(&self) -> BTreeMap<String, pge::Transform> {
        let mut transforms = BTreeMap::new();

        for (object_id, object) in &self.objects {
            transforms.insert(
                format!("scene-object:{object_id}"),
                pge_transform(object.state.position, object.state.rotation),
            );
        }

        for (robot_id, vehicle) in &self.vehicles {
            let transform = pge_transform(vehicle.state.position, vehicle.state.rotation);
            for index in 0..vehicle.colliders.len() {
                transforms.insert(format!("vehicle:{robot_id}:{index}"), transform);
            }
        }

        for (key, collider) in &self.robot_link_colliders {
            let Some(body) = self.world.body(collider.body) else {
                continue;
            };
            let (roll, pitch, yaw) = body.rotation().euler_angles();
            transforms.insert(
                format!("kinematic-link:{key}"),
                pge_transform(
                    [
                        body.translation().x,
                        body.translation().y,
                        body.translation().z,
                    ],
                    [roll, pitch, yaw],
                ),
            );
        }

        transforms
    }

    pub(crate) fn set_kinematic_collider_motion_config(
        &mut self,
        config: KinematicColliderMotionConfig,
    ) -> Result<(), Box<dyn Error>> {
        validate_kinematic_collider_motion(config)?;
        self.kinematic_collider_motion = config;
        Ok(())
    }

    /// Creates or updates kinematic Rapier bodies for the supplied reviewed
    /// link profiles. Observed URDF poses are retained as targets; `step`
    /// sweeps each body toward its target at the configured bounded speed.
    pub(crate) fn sync_robot_link_colliders(&mut self, colliders: Vec<RobotLinkCollider>) {
        let mut next_shape_index = BTreeMap::<(String, String), usize>::new();
        for collider in &colliders {
            let per_link_index = next_shape_index
                .entry((collider.robot_id.clone(), collider.link_name.clone()))
                .and_modify(|index| *index += 1)
                .or_insert(0);
            let key = robot_link_collider_key(collider, *per_link_index);
            let pose = robot_link_collider_pose(collider);
            if let Some(existing) = self.robot_link_colliders.get_mut(&key) {
                existing.target_pose = pose;
                continue;
            }

            let body = RigidBodyBuilder::kinematic_position_based()
                .position(pose)
                .ccd_enabled(true)
                .build();
            let shape = collider_builder(&collider.geometry)
                .friction(0.8)
                .restitution(0.0)
                .build();
            let (body, collider_handle) = self.world.insert_kinematic_collider(body, shape);
            self.robot_link_colliders.insert(
                key,
                PhysicsRobotLinkCollider {
                    robot_id: collider.robot_id.clone(),
                    link_name: collider.link_name.clone(),
                    body,
                    collider: collider_handle,
                    geometry: collider.geometry.clone(),
                    target_pose: pose,
                },
            );
        }
    }

    fn sweep_robot_link_colliders(&mut self, dt: f32) {
        let targets = self
            .robot_link_colliders
            .values()
            .map(|collider| (collider.body, collider.target_pose))
            .collect::<Vec<_>>();
        for (handle, target) in targets {
            let Some(body) = self.world.body_mut(handle) else {
                continue;
            };
            let next = bounded_kinematic_pose(
                *body.position(),
                target,
                self.kinematic_collider_motion,
                dt,
            );
            body.set_next_kinematic_position(next);
        }
    }

    pub(crate) fn robot_link_contacts(&self, object_id: &str) -> Vec<RobotLinkContact> {
        let Some(object) = self.objects.get(object_id) else {
            return Vec::new();
        };
        let object_colliders = self
            .world
            .colliders
            .iter()
            .filter_map(|(handle, collider)| {
                (collider.parent() == Some(object.handle)).then_some(handle)
            })
            .collect::<Vec<_>>();
        self.robot_link_colliders
            .values()
            .filter(|robot_link| {
                object_colliders.iter().any(|object_collider| {
                    self.world
                        .narrow_phase
                        .contact_pair(robot_link.collider, *object_collider)
                        .is_some_and(|pair| pair.has_any_active_contact)
                })
            })
            .map(|robot_link| RobotLinkContact {
                robot_id: robot_link.robot_id.clone(),
                link_name: robot_link.link_name.clone(),
                object_id: object_id.to_string(),
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
        let object = self
            .objects
            .get_mut(object_id)
            .ok_or_else(|| format!("scene object '{object_id}' has no physics body"))?;
        if !object.state.dynamic {
            return Err(format!("scene object '{object_id}' is not dynamic").into());
        }
        let body = self
            .world
            .body_mut(object.handle)
            .ok_or_else(|| format!("scene object '{object_id}' physics body is missing"))?;
        body.set_body_type(RigidBodyType::KinematicPositionBased, true);
        body.set_translation(vec3(position), true);
        body.set_rotation(
            Rotation::from_euler_angles(rotation[0], rotation[1], rotation[2]),
            true,
        );
        body.set_linvel(Vector::zeros(), true);
        body.set_angvel(Vector::zeros(), true);
        object.state.position = position;
        object.state.rotation = rotation;
        object.state.velocity_mps = [0.0; 3];
        object.state.angular_velocity_rps = [0.0; 3];
        object.state.attachment = Some(attachment);
        Ok(())
    }

    pub(crate) fn detach(&mut self, object_id: &str) -> Result<(), Box<dyn Error>> {
        let object = self
            .objects
            .get_mut(object_id)
            .ok_or_else(|| format!("scene object '{object_id}' has no physics body"))?;
        if object.state.attachment.is_none() {
            return Err(format!("scene object '{object_id}' is not attached").into());
        }
        let body = self
            .world
            .body_mut(object.handle)
            .ok_or_else(|| format!("scene object '{object_id}' physics body is missing"))?;
        body.set_body_type(RigidBodyType::Dynamic, true);
        body.set_linvel(Vector::zeros(), true);
        body.set_angvel(Vector::zeros(), true);
        // A body held as kinematic may have gone to sleep while its pose was
        // being driven by the attachment.  Releasing it must always resume
        // simulation on the next step, even when no contact wakes it first.
        body.wake_up(true);
        object.state.attachment = None;
        Ok(())
    }

    pub(crate) fn attachment_requests(&self) -> Vec<(String, SceneObjectAttachment)> {
        self.objects
            .iter()
            .filter_map(|(id, object)| {
                object
                    .state
                    .attachment
                    .clone()
                    .map(|attachment| (id.clone(), attachment))
            })
            .collect()
    }

    pub(crate) fn set_attached_pose(
        &mut self,
        object_id: &str,
        position: [f32; 3],
        rotation: [f32; 3],
    ) {
        let Some(object) = self.objects.get_mut(object_id) else {
            return;
        };
        let Some(body) = self.world.body_mut(object.handle) else {
            return;
        };
        body.set_translation(vec3(position), true);
        body.set_next_kinematic_translation(vec3(position));
        let orientation = Rotation::from_euler_angles(rotation[0], rotation[1], rotation[2]);
        body.set_rotation(orientation, true);
        body.set_next_kinematic_rotation(orientation);
        body.set_linvel(Vector::zeros(), true);
        body.set_angvel(Vector::zeros(), true);
        object.state.position = position;
        object.state.rotation = rotation;
        object.state.velocity_mps = [0.0; 3];
        object.state.angular_velocity_rps = [0.0; 3];
    }

    /// Applies a bounded motor and steering force model to the dynamic chassis.
    /// The body pose is never written directly: Rapier resolves acceleration,
    /// contact, and collision from these forces on the following `step`.
    pub(crate) fn drive_vehicle(&mut self, robot_id: &str, command: VehicleDriveCommand, dt: f32) {
        if dt <= 0.0 {
            return;
        }
        let Some(vehicle) = self.vehicles.get_mut(robot_id) else {
            return;
        };
        let response = vehicle.config.steering_response_deg_per_sec.to_radians() * dt;
        let steering_error = command.steering_target_rad - vehicle.steering_angle_rad;
        vehicle.steering_angle_rad += steering_error.clamp(-response, response);
        let Some(body) = self.world.body_mut(vehicle.handle) else {
            return;
        };
        let (_, _, yaw) = body.rotation().euler_angles();
        let forward = Vector::new(yaw.cos(), yaw.sin(), 0.0);
        let lateral = Vector::new(-yaw.sin(), yaw.cos(), 0.0);
        let current_forward_speed = body.linvel().dot(&forward);
        let current_lateral_speed = body.linvel().dot(&lateral);
        let left_command = command.left_command.clamp(-1.0, 1.0);
        let right_command = command.right_command.clamp(-1.0, 1.0);
        let motor = &vehicle.config.motor;
        let wheel_angular_speed = current_forward_speed / motor.wheel_radius_m;
        let motor_angular_speed = wheel_angular_speed * motor.gear_ratio;
        let no_load_rad_s = motor.no_load_rpm * std::f32::consts::TAU / 60.0;
        let wheel_force = |command: f32| {
            if command.abs() <= 1.0e-4 {
                return 0.0;
            }
            let torque = motor.stall_torque_nm
                * (command - motor_angular_speed / no_load_rad_s).clamp(-1.0, 1.0);
            (torque * motor.gear_ratio / motor.wheel_radius_m).clamp(
                -vehicle.config.max_drive_force_n,
                vehicle.config.max_drive_force_n,
            )
        };
        let left_force = wheel_force(left_command);
        let right_force = wheel_force(right_command);
        let origin = *body.translation();
        let rear_offset = -vehicle.config.wheelbase_m * 0.5;
        let half_track = vehicle.config.track_width_m * 0.5;
        let rear_left = Point::from(origin + forward * rear_offset + lateral * half_track);
        let rear_right = Point::from(origin + forward * rear_offset - lateral * half_track);
        body.add_force_at_point(forward * left_force, rear_left, true);
        body.add_force_at_point(forward * right_force, rear_right, true);

        // The front axle supplies the required centripetal force for the
        // measured steering angle; lateral velocity is dissipated as tire slip.
        let max_surface_speed = (no_load_rad_s / motor.gear_ratio * motor.wheel_radius_m)
            .min(vehicle.config.max_wheel_speed_mps);
        let mean_speed = 0.5 * (left_command + right_command) * max_surface_speed;
        let steering_force = vehicle.config.mass_kg
            * mean_speed.abs()
            * mean_speed
            * vehicle.steering_angle_rad.tan()
            / vehicle.config.wheelbase_m;
        let lateral_force =
            (steering_force - current_lateral_speed * vehicle.config.lateral_grip_n_per_mps).clamp(
                -vehicle.config.max_drive_force_n * 2.0,
                vehicle.config.max_drive_force_n * 2.0,
            );
        let front = Point::from(origin + forward * vehicle.config.wheelbase_m * 0.5);
        body.add_force_at_point(lateral * lateral_force, front, true);
        let braking = command.brake;
        let resistance = if braking {
            motor.brake_torque_nm * motor.gear_ratio / motor.wheel_radius_m
        } else {
            motor.rolling_resistance_n
        };
        if current_forward_speed.abs() > 1.0e-4 {
            body.add_force(-forward * resistance * current_forward_speed.signum(), true);
        }
        vehicle.actuator = VehicleActuatorState {
            left_command,
            right_command,
            left_drive_force_n: left_force,
            right_drive_force_n: right_force,
            braking,
        };
    }

    pub(crate) fn step(&mut self, dt: f32, clock_sec: f64) {
        if dt <= 0.0 {
            return;
        }
        let substeps = kinematic_substep_count(dt, self.kinematic_collider_motion);
        let substep_dt = dt / substeps as f32;
        let first_substep_clock = clock_sec - f64::from(dt);
        for substep in 1..=substeps {
            self.sweep_robot_link_colliders(substep_dt);
            self.world.set_time_step(substep_dt);
            self.world.step();
            self.sync_object_states();
            self.sync_vehicle_states();
            self.update_triggers(
                substep_dt,
                first_substep_clock + f64::from(substep_dt * substep as f32),
            );
        }
    }

    fn sync_object_states(&mut self) {
        for object in self.objects.values_mut() {
            let Some(body) = self.world.body(object.handle) else {
                continue;
            };
            object.state.position = [
                body.translation().x,
                body.translation().y,
                body.translation().z,
            ];
            let (roll, pitch, yaw) = body.rotation().euler_angles();
            object.state.rotation = [roll, pitch, yaw];
            object.state.velocity_mps = [body.linvel().x, body.linvel().y, body.linvel().z];
            object.state.angular_velocity_rps = [body.angvel().x, body.angvel().y, body.angvel().z];
        }
    }

    fn sync_vehicle_states(&mut self) {
        for vehicle in self.vehicles.values_mut() {
            let Some(body) = self.world.body(vehicle.handle) else {
                continue;
            };
            vehicle.state = vehicle_state(
                vehicle.state.robot_id.clone(),
                body,
                vehicle.actuator.clone(),
            );
        }
    }

    fn update_triggers(&mut self, dt: f32, clock_sec: f64) {
        for trigger in self.triggers.values_mut() {
            let Some(object) = self.objects.get(&trigger.config.object_id) else {
                continue;
            };
            let inside = object.state.dynamic
                && object.state.attachment.is_none()
                && center_inside_box(
                    object.state.position,
                    trigger.config.position,
                    trigger.config.size,
                );
            if inside && !trigger.state.inside {
                trigger.state.entered = true;
                trigger.state.entered_at_sec.get_or_insert(clock_sec);
                trigger.state.entry_count += 1;
            }
            trigger.state.inside = inside;
            if inside && speed(object.state.velocity_mps) <= trigger.config.settle_speed_mps {
                trigger.state.settled_time_sec += dt;
                if trigger.state.settled_time_sec >= trigger.config.settle_time_sec {
                    trigger.state.settled = true;
                    trigger.state.triggered = true;
                    trigger.state.triggered_at_sec.get_or_insert(clock_sec);
                }
            } else {
                trigger.state.settled_time_sec = 0.0;
                trigger.state.settled = false;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use pge_collision::{
        CollisionGenerationConfig, ReviewedProfileExportConfig, TriangleMesh,
        generate_collision_candidates,
    };
    use pge_physics::rapier3d::na::Matrix3;
    use pge_physics::rapier3d::prelude::{Real, Vector};

    use crate::project::{
        HardwareConfig, ProjectConfig, ProjectRobotConfig, ProjectRobotModelConfig,
        ProjectRobotPhysicsConfig, ProjectSceneBodyKind, ProjectSceneColliderConfig,
        ProjectSceneColliderChildConfig, ProjectSceneColliderGeometry, ProjectSceneConfig,
        ProjectSceneObjectConfig, ProjectSceneObjectGeometry, ProjectSceneObjectPhysicsConfig,
        ProjectSceneTriggerConfig, ProjectVehicleMotorConfig, ProjectVehiclePhysicsConfig,
        RobotLinkCollider,
    };

    use super::{
        KinematicColliderMotionConfig, SceneObjectAttachment, ScenePhysicsRuntime,
        VehicleDriveCommand,
    };

    fn physical_object(
        id: &str,
        position: [f32; 3],
        geometry: ProjectSceneObjectGeometry,
        body_kind: ProjectSceneBodyKind,
        collider: ProjectSceneColliderGeometry,
    ) -> ProjectSceneObjectConfig {
        ProjectSceneObjectConfig {
            id: id.to_string(),
            name: id.to_string(),
            type_name: "test".to_string(),
            icon: "TST".to_string(),
            geometry,
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
                    geometry: collider,
                    offset: [0.0; 3],
                    rotation: [0.0; 3],
                    children: Vec::new(),
                },
            }),
        }
    }

    fn ball_bin_project() -> ProjectConfig {
        let mut project = ProjectConfig {
            format: "robotdreams.project.v1".to_string(),
            name: "physics test".to_string(),
            manifest_path: PathBuf::from("project.json"),
            base_dir: PathBuf::new(),
            model_profile_path: None,
            calibration_record_path: None,
            scene: ProjectSceneConfig {
                objects: vec![
                    physical_object(
                        "bin_bottom",
                        [0.0, 0.0, 0.0],
                        ProjectSceneObjectGeometry::Box {
                            size: [0.2, 0.2, 0.02],
                        },
                        ProjectSceneBodyKind::Static,
                        ProjectSceneColliderGeometry::Box {
                            size: [0.2, 0.2, 0.02],
                        },
                    ),
                    physical_object(
                        "ball",
                        [0.0, 0.0, 0.18],
                        ProjectSceneObjectGeometry::Sphere { radius: 0.025 },
                        ProjectSceneBodyKind::Dynamic,
                        ProjectSceneColliderGeometry::Sphere { radius: 0.025 },
                    ),
                ],
                triggers: vec![ProjectSceneTriggerConfig {
                    id: "ball_in_bin".to_string(),
                    object_id: "ball".to_string(),
                    position: [0.0, 0.0, 0.125],
                    size: [0.15, 0.15, 0.22],
                    settle_speed_mps: 0.05,
                    settle_time_sec: 0.25,
                }],
                ..ProjectSceneConfig::default()
            },
            robots: Vec::new(),
            hardware: HardwareConfig::default(),
        };
        project.scene.objects[0]
            .physics
            .as_mut()
            .unwrap()
            .collider
            .offset = [0.0, 0.0, 0.01];
        project
    }

    fn compound_bottle_project(rotation: [f32; 3]) -> ProjectConfig {
        let mut project = ball_bin_project();
        project.scene.triggers.clear();
        project.scene.objects[0].position = [0.0, 0.0, 0.0];
        project.scene.objects[1] = physical_object(
            "bottle",
            [0.0, 0.0, 0.11],
            ProjectSceneObjectGeometry::Cylinder {
                radius_top: 0.042,
                radius_bottom: 0.042,
                height: 0.207,
            },
            ProjectSceneBodyKind::Dynamic,
            ProjectSceneColliderGeometry::Cylinder {
                radius: 0.042,
                height: 0.028,
            },
        );
        project.scene.objects[1].rotation = rotation;
        let bottle = project.scene.objects[1]
            .physics
            .as_mut()
            .expect("bottle physics");
        bottle.mass_kg = 0.08;
        bottle.center_of_mass = Some([0.0, -0.025, 0.0]);
        bottle.linear_damping = 0.08;
        bottle.angular_damping = 0.12;
        bottle.friction = 0.55;
        bottle.restitution = 0.03;
        bottle.collider.children = vec![
            ProjectSceneColliderChildConfig {
                geometry: ProjectSceneColliderGeometry::Cylinder {
                    radius: 0.042,
                    height: 0.028,
                },
                offset: [0.0, -0.086, 0.0],
                rotation: [0.0; 3],
            },
            ProjectSceneColliderChildConfig {
                geometry: ProjectSceneColliderGeometry::Cylinder {
                    radius: 0.040,
                    height: 0.102,
                },
                offset: [0.0, -0.021, 0.0],
                rotation: [0.0; 3],
            },
            ProjectSceneColliderChildConfig {
                geometry: ProjectSceneColliderGeometry::Cylinder {
                    radius: 0.034,
                    height: 0.028,
                },
                offset: [0.0, 0.044, 0.0],
                rotation: [0.0; 3],
            },
            ProjectSceneColliderChildConfig {
                geometry: ProjectSceneColliderGeometry::Cylinder {
                    radius: 0.023,
                    height: 0.030,
                },
                offset: [0.0, 0.073, 0.0],
                rotation: [0.0; 3],
            },
            ProjectSceneColliderChildConfig {
                geometry: ProjectSceneColliderGeometry::Cylinder {
                    radius: 0.025,
                    height: 0.020,
                },
                offset: [0.0, 0.097, 0.0],
                rotation: [0.0; 3],
            },
        ];
        project
    }

    #[test]
    fn compound_bottle_rests_upright_and_exports_all_physical_parts() {
        let mut runtime =
            ScenePhysicsRuntime::from_project(&compound_bottle_project([std::f32::consts::FRAC_PI_2, 0.0, 0.0]))
                .expect("compound bottle project");
        for step in 1..=600 {
            runtime.step(1.0 / 120.0, f64::from(step) / 120.0);
        }

        let bottle = runtime.object_state("bottle").expect("bottle state");
        assert!(bottle.position[2] > 0.115 && bottle.position[2] < 0.125, "{bottle:?}");
        assert!(
            bottle
                .velocity_mps
                .iter()
                .chain(bottle.angular_velocity_rps.iter())
                .all(|value| value.abs() < 0.05),
            "upright bottle should reach a physical rest: {bottle:?}"
        );

        let debug = runtime.collider_debug_entries();
        let bottle_debug = debug
            .iter()
            .find(|entry| entry.id == "scene-object:bottle")
            .expect("bottle debug entry");
        assert_eq!(bottle_debug.parts.len(), 5);
        assert_eq!(bottle_debug.parts[0].local_transform.translation, [0.0, -0.086, 0.0]);
        assert_eq!(bottle_debug.parts[4].local_transform.translation, [0.0, 0.097, 0.0]);
    }

    #[test]
    fn tipped_compound_bottle_rolls_then_settles() {
        let mut runtime = ScenePhysicsRuntime::from_project(&compound_bottle_project([
            std::f32::consts::FRAC_PI_2 + 0.35,
            0.0,
            0.0,
        ]))
        .expect("compound bottle project");
        let initial = runtime.object_state("bottle").expect("initial bottle").clone();
        for step in 1..=1_800 {
            runtime.step(1.0 / 120.0, f64::from(step) / 120.0);
        }

        let bottle = runtime.object_state("bottle").expect("bottle state");
        let lateral_displacement = (bottle.position[0] - initial.position[0]).abs()
            + (bottle.position[1] - initial.position[1]).abs();
        assert!(lateral_displacement > 0.01, "tipped bottle should roll: {bottle:?}");
        assert!(bottle.position[2] > 0.11 && bottle.position[2] < 0.13, "{bottle:?}");
        assert!(
            bottle
                .velocity_mps
                .iter()
                .chain(bottle.angular_velocity_rps.iter())
                .all(|value| value.abs() < 0.05),
            "compound bottle should settle after its roll: {bottle:?}"
        );
    }

    #[test]
    fn compound_bottle_maps_long_axis_inertia_through_object_rpy() {
        fn angular_response(impulse: Vector<Real>) -> ([f32; 3], Matrix3<Real>) {
            // Rapier cylinders are long on local Y. The scene's X quarter-turn
            // maps that bottle axis to world Z, exactly like the PuppyBot
            // fixture standing upright on its support.
            let mut runtime = ScenePhysicsRuntime::from_project(&compound_bottle_project([
                std::f32::consts::FRAC_PI_2,
                0.0,
                0.0,
            ]))
            .expect("compound bottle project");
            let handle = runtime.objects["bottle"].handle;
            let body = runtime.world.body_mut(handle).expect("bottle body");
            let inertia = body
                .mass_properties()
                .local_mprops
                .reconstruct_inertia_matrix();
            body.apply_torque_impulse(impulse, true);
            ([body.angvel().x, body.angvel().y, body.angvel().z], inertia)
        }

        let (long_axis_spin, inertia) = angular_response(Vector::new(0.0, 0.0, 0.001));
        let (transverse_roll, _) = angular_response(Vector::new(0.001, 0.0, 0.0));

        // The bottle's compound geometry is elongated along object-local Y,
        // so its Y inertia must be lower than a transverse axis. With the
        // authored RPY, that lower-inertia long axis is world Z.
        assert!(
            inertia[(1, 1)] < inertia[(0, 0)] * 0.75,
            "bottle local inertia must retain its Y long axis: {inertia:?}"
        );
        assert!(
            long_axis_spin[2].abs() > transverse_roll[0].abs() * 1.25,
            "object RPY must map bottle long-axis spin to world Z and transverse roll to world X: spin={long_axis_spin:?}, roll={transverse_roll:?}"
        );
    }

    #[test]
    fn released_ball_falls_settles_on_bin_bottom_and_triggers() {
        let mut runtime = ScenePhysicsRuntime::from_project(&ball_bin_project()).unwrap();
        runtime
            .attach(
                "ball",
                SceneObjectAttachment {
                    robot_id: "robot".to_string(),
                    frame_name: "tcp".to_string(),
                    offset_m: [0.0; 3],
                    rotation_offset_rpy: [0.0; 3],
                },
                [0.0, 0.0, 0.18],
                [0.0; 3],
            )
            .unwrap();
        runtime.detach("ball").unwrap();
        for step in 1..=360 {
            runtime.step(1.0 / 120.0, f64::from(step) / 120.0);
        }

        let ball = runtime.object_state("ball").unwrap();
        assert!(ball.position[2] > 0.04 && ball.position[2] < 0.051);
        assert!(ball.velocity_mps[2].abs() < 0.05);
        let trigger = runtime.trigger_state("ball_in_bin").unwrap();
        assert!(trigger.inside);
        assert!(trigger.entered);
        assert_eq!(trigger.entry_count, 1);
        assert!(trigger.settled);
        assert!(trigger.triggered);
        assert!(trigger.entered_at_sec.is_some());
        assert!(trigger.triggered_at_sec.is_some());
    }

    #[test]
    fn attachment_tracks_exactly_and_detach_restores_gravity() {
        let mut runtime = ScenePhysicsRuntime::from_project(&ball_bin_project()).unwrap();
        runtime
            .attach(
                "ball",
                SceneObjectAttachment {
                    robot_id: "robot".to_string(),
                    frame_name: "tcp".to_string(),
                    offset_m: [0.0; 3],
                    rotation_offset_rpy: [0.0; 3],
                },
                [0.1, 0.0, 0.4],
                [0.0; 3],
            )
            .unwrap();
        runtime.set_attached_pose("ball", [0.08, 0.02, 0.35], [0.0, 0.0, 0.5]);
        runtime.step(0.02, 0.02);
        let held = runtime.object_state("ball").unwrap();
        assert_eq!(held.position, [0.08, 0.02, 0.35]);
        assert_eq!(held.rotation, [0.0, 0.0, 0.5]);
        assert!(held.attachment.is_some());

        runtime.detach("ball").unwrap();
        runtime.step(0.05, 0.07);
        let released = runtime.object_state("ball").unwrap();
        assert!(released.attachment.is_none());
        assert!(released.position[2] < 0.35);
        assert!(released.velocity_mps[2] < 0.0);
    }

    #[test]
    fn compound_bottle_attachment_release_wakes_and_resumes_gravity() {
        let mut runtime = ScenePhysicsRuntime::from_project(&compound_bottle_project([
            std::f32::consts::FRAC_PI_2,
            0.0,
            0.0,
        ]))
        .expect("compound bottle project");
        runtime
            .attach(
                "bottle",
                SceneObjectAttachment {
                    robot_id: "robot".to_string(),
                    frame_name: "tcp".to_string(),
                    offset_m: [0.0; 3],
                    rotation_offset_rpy: [0.0; 3],
                },
                [0.0, 0.0, 0.4],
                [std::f32::consts::FRAC_PI_2, 0.0, 0.0],
            )
            .expect("attach compound bottle");
        runtime.detach("bottle").expect("release compound bottle");
        let release_z = runtime.object_state("bottle").expect("release state").position[2];
        runtime.step(0.02, 0.02);
        let released = runtime.object_state("bottle").expect("released state");
        assert!(released.position[2] < release_z, "{released:?}");
        assert!(released.velocity_mps[2] < 0.0, "{released:?}");
    }

    #[test]
    fn reviewed_kinematic_link_collider_pushes_a_dynamic_object_without_attachment() {
        let mut project = ball_bin_project();
        project.scene.objects[1].position = [0.0, 0.0, 0.045];
        let mut runtime = ScenePhysicsRuntime::from_project(&project).unwrap();
        let initial = RobotLinkCollider {
            robot_id: "robot".to_string(),
            link_name: "gripper_finger".to_string(),
            reviewed_profile_path: PathBuf::from("reviewed.json"),
            candidate_artifact_path: None,
            geometry: ProjectSceneColliderGeometry::Box {
                size: [0.04, 0.06, 0.06],
            },
            translation: [-0.10, 0.0, 0.045],
            rotation_matrix: [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]],
        };
        runtime.sync_robot_link_colliders(vec![initial.clone()]);
        let mut striking = initial;
        runtime.step(1.0 / 120.0, 1.0 / 120.0);
        let mut saw_contact = false;
        striking.translation[0] = 0.04;
        for step in 2..=100 {
            runtime.sync_robot_link_colliders(vec![striking.clone()]);
            runtime.step(1.0 / 120.0, f64::from(step) / 120.0);
            saw_contact |= runtime
                .robot_link_contacts("ball")
                .iter()
                .any(|contact| contact.link_name == "gripper_finger");
        }

        assert!(
            saw_contact,
            "the reported contact must come from Rapier's reviewed-link collider"
        );
        let ball = runtime.object_state("ball").unwrap();
        assert!(
            ball.attachment.is_none(),
            "contact must not attach the object"
        );
        assert!(
            ball.velocity_mps[0] > 0.0 || ball.position[0] > 0.0,
            "the moving kinematic link must impart a physical response: {ball:?}"
        );
    }

    #[test]
    fn reviewed_link_contact_pushes_a_bottle_off_support_then_gravity_lands_it_in_bin() {
        let mut project = ball_bin_project();
        project.scene.objects[0].position = [0.30, 0.0, 0.0];
        project.scene.objects[0].geometry = ProjectSceneObjectGeometry::Box {
            size: [1.00, 0.20, 0.02],
        };
        project.scene.objects[0]
            .physics
            .as_mut()
            .unwrap()
            .collider
            .geometry = ProjectSceneColliderGeometry::Box {
            size: [1.00, 0.20, 0.02],
        };
        project.scene.objects[1].position = [0.0, 0.0, 0.125];
        project.scene.objects[1]
            .physics
            .as_mut()
            .unwrap()
            .linear_damping = 8.0;
        project.scene.objects.push(physical_object(
            "support",
            [0.0, 0.0, 0.05],
            ProjectSceneObjectGeometry::Box {
                size: [0.09, 0.20, 0.10],
            },
            ProjectSceneBodyKind::Static,
            ProjectSceneColliderGeometry::Box {
                size: [0.09, 0.20, 0.10],
            },
        ));
        project.scene.triggers[0].position = [0.30, 0.0, 0.08];
        project.scene.triggers[0].size = [0.90, 0.18, 0.18];
        let mut runtime = ScenePhysicsRuntime::from_project(&project).unwrap();
        runtime
            .set_kinematic_collider_motion_config(KinematicColliderMotionConfig {
                maximum_linear_speed_mps: 0.20,
                maximum_angular_speed_rps: 2.0,
                maximum_substep_seconds: 1.0 / 120.0,
            })
            .unwrap();
        let mut link = RobotLinkCollider {
            robot_id: "robot".to_string(),
            link_name: "gripper_finger".to_string(),
            reviewed_profile_path: PathBuf::from("reviewed.json"),
            candidate_artifact_path: None,
            geometry: ProjectSceneColliderGeometry::Box {
                size: [0.04, 0.06, 0.06],
            },
            translation: [-0.10, 0.0, 0.125],
            rotation_matrix: [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]],
        };
        runtime.sync_robot_link_colliders(vec![link.clone()]);
        runtime.step(1.0 / 120.0, 1.0 / 120.0);

        let mut saw_contact = false;
        link.translation[0] = 0.02;
        for step in 2..=170 {
            runtime.sync_robot_link_colliders(vec![link.clone()]);
            runtime.step(1.0 / 120.0, f64::from(step) / 120.0);
            saw_contact |= !runtime.robot_link_contacts("ball").is_empty();
        }
        link.translation[0] = -0.10;
        for step in 171..=700 {
            runtime.sync_robot_link_colliders(vec![link.clone()]);
            runtime.step(1.0 / 120.0, f64::from(step) / 120.0);
        }

        let ball = runtime.object_state("ball").unwrap();
        assert!(saw_contact, "the bottle must have a reviewed-link contact");
        assert!(
            ball.attachment.is_none(),
            "physical contact must not attach it"
        );
        assert!(
            ball.position[0] > 0.06,
            "the contact must push it off support: {ball:?}"
        );
        assert!(
            ball.position[2] > 0.02 && ball.position[2] < 0.06,
            "after release from support it must settle on the bin floor: {ball:?}"
        );
        assert!(
            ball.position[0] < 0.45,
            "the bounded kinematic sweep must not launch the bottle across the scene: {ball:?}"
        );
        assert!(
            ball.velocity_mps
                .iter()
                .all(|velocity| velocity.abs() < 0.05),
            "the bottle must settle after falling from the support: {ball:?}"
        );
        let trigger = runtime.trigger_state("ball_in_bin").unwrap();
        assert!(
            trigger.triggered,
            "the physical drop must reach the bin trigger"
        );
    }

    #[test]
    fn generated_profile_compound_colliders_drive_a_dynamic_vehicle() {
        let profile_dir = std::env::temp_dir().join(format!(
            "robotdreams-vehicle-profile-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&profile_dir).unwrap();
        let profile_path = profile_dir.join("reviewed-colliders.json");
        let calibration_path = profile_dir.join("measured-calibration.json");
        let pge_candidates = generate_collision_candidates(
            &TriangleMesh {
                vertices: vec![
                    [-0.12, -0.09, 0.0],
                    [0.12, -0.09, 0.0],
                    [0.12, 0.09, 0.0],
                    [-0.12, 0.09, 0.0],
                    [-0.12, -0.09, 0.10],
                    [0.12, -0.09, 0.10],
                    [0.12, 0.09, 0.10],
                    [-0.12, 0.09, 0.10],
                ],
                triangles: vec![
                    [0, 2, 1],
                    [0, 3, 2],
                    [4, 5, 6],
                    [4, 6, 7],
                    [0, 1, 5],
                    [0, 5, 4],
                    [1, 2, 6],
                    [1, 6, 5],
                    [2, 3, 7],
                    [2, 7, 6],
                    [3, 0, 4],
                    [3, 4, 7],
                ],
            },
            CollisionGenerationConfig::default(),
        )
        .unwrap();
        let reviewed_profile = pge_candidates
            .export_reviewed_profile(ReviewedProfileExportConfig::default())
            .unwrap();
        std::fs::write(&profile_path, reviewed_profile.to_json_pretty().unwrap()).unwrap();
        std::fs::write(
            &calibration_path,
            serde_json::json!({
                "format": "robotdreams.calibration.v1",
                "hardware_revision": "test-rev",
                "provenance": {"measured_at":"2026-07-20T12:00:00Z","operator":"tester","method":"fixture","source":"test trace"},
                "vehicle": {"mass_kg":2.4,"center_of_mass_m":[0.01,-0.02,0.03],"wheel_radius_m":0.04,"gear_ratio":1.0,"motor_stall_torque_nm":0.45,"motor_no_load_rpm":120.0},
                "servos": [{"servo_id":1,"max_speed_ticks_per_sec":1000.0}],
                "drive_trace": [{"time_sec":0.0,"left_command":0.0,"right_command":0.0,"observed_linear_mps":0.0,"observed_yaw_rps":0.0},{"time_sec":1.0,"left_command":0.5,"right_command":0.5,"observed_linear_mps":0.15,"observed_yaw_rps":0.0}],
                "servo_trace": [{"time_sec":0.0,"servo_id":1,"target_ticks":2200,"observed_present_ticks":2048},{"time_sec":0.2,"servo_id":1,"target_ticks":2200,"observed_present_ticks":2200}]
            })
            .to_string(),
        )
        .unwrap();
        let mut project = ball_bin_project();
        project.scene.objects.truncate(1);
        project.scene.triggers.clear();
        project.scene.objects[0].geometry = ProjectSceneObjectGeometry::Box {
            size: [5.0, 5.0, 0.02],
        };
        project.scene.objects.push(physical_object(
            "bin_wall",
            [0.35, 0.0, 0.15],
            ProjectSceneObjectGeometry::Box {
                size: [0.02, 0.50, 0.30],
            },
            ProjectSceneBodyKind::Static,
            ProjectSceneColliderGeometry::Box {
                size: [0.02, 0.50, 0.30],
            },
        ));
        project.scene.objects[0]
            .physics
            .as_mut()
            .unwrap()
            .collider
            .geometry = ProjectSceneColliderGeometry::Box {
            size: [5.0, 5.0, 0.02],
        };
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
            base_translation: [0.0, 0.0, 0.06],
            base_rotation: [0.0; 3],
            physics: Some(ProjectRobotPhysicsConfig {
                vehicle: Some(ProjectVehiclePhysicsConfig {
                    mass_kg: 8.0,
                    center_of_mass_m: [0.01, -0.02, 0.03],
                    linear_damping: 0.2,
                    angular_damping: 1.0,
                    wheelbase_m: 0.22,
                    track_width_m: 0.18,
                    max_wheel_speed_mps: 0.4,
                    max_drive_force_n: 18.0,
                    lateral_grip_n_per_mps: 45.0,
                    steering_response_deg_per_sec: 240.0,
                    motor: ProjectVehicleMotorConfig {
                        wheel_radius_m: 0.04,
                        gear_ratio: 1.0,
                        stall_torque_nm: 0.45,
                        no_load_rpm: 120.0,
                        brake_torque_nm: 0.25,
                        rolling_resistance_n: 0.4,
                    },
                    colliders: Vec::new(),
                    collision_profile: Some("reviewed-colliders.json".to_string()),
                }),
                link_collision_profile: None,
            }),
        });
        let bad_calibration_path = profile_dir.join("invalid-calibration.json");
        std::fs::write(&bad_calibration_path, "{}").unwrap();
        let mut invalid_project = project.clone();
        invalid_project.calibration_record_path = Some("invalid-calibration.json".to_string());
        assert!(ScenePhysicsRuntime::from_project(&invalid_project).is_err());
        let mut runtime = ScenePhysicsRuntime::from_project(&project).unwrap();
        assert!(runtime.has_vehicle("puppybot"));
        let vehicle_debug = runtime
            .collider_debug_entries()
            .into_iter()
            .filter(|entry| entry.category == "vehicleCollider")
            .collect::<Vec<_>>();
        assert_eq!(vehicle_debug.len(), 1);
        assert_eq!(vehicle_debug[0].id, "vehicle:puppybot:0");
        assert!(matches!(
            vehicle_debug[0].parts[0].geometry,
            ProjectSceneColliderGeometry::Box { .. }
        ));
        assert_eq!(runtime.applied_calibrations().len(), 1);
        assert_eq!(
            runtime.applied_calibrations()[0].hardware_revision,
            "test-rev"
        );
        let vehicle = runtime.vehicles.get("puppybot").unwrap();
        assert_eq!(vehicle.config.mass_kg, 2.4);
        let body = runtime.world.body(vehicle.handle).unwrap();
        for (actual, expected) in body
            .local_center_of_mass()
            .coords
            .iter()
            .zip([0.01, -0.02, 0.03])
        {
            assert!((actual - expected).abs() < 1.0e-6);
        }
        for step in 1..=240 {
            runtime.drive_vehicle(
                "puppybot",
                VehicleDriveCommand {
                    left_command: 0.65,
                    right_command: 0.65,
                    brake: false,
                    steering_target_rad: 0.0,
                },
                1.0 / 120.0,
            );
            runtime.step(1.0 / 120.0, f64::from(step) / 120.0);
        }
        let state = runtime.vehicle_state("puppybot").unwrap();
        assert!(state.position[0] > 0.10, "vehicle did not move: {state:?}");
        assert!(
            state.position[0] < 0.24,
            "generated chassis collider drove through static wall: {state:?}"
        );
        assert!(
            state.position[2] > 0.005,
            "vehicle fell through floor: {state:?}"
        );
        std::fs::remove_dir_all(profile_dir).unwrap();
    }
}
