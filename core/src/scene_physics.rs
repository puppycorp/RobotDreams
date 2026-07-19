use std::collections::BTreeMap;
use std::error::Error;

use pge_physics::rapier3d::prelude::*;

use crate::project::{
    ProjectConfig, ProjectSceneBodyKind, ProjectSceneColliderGeometry, ProjectSceneTriggerConfig,
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

#[derive(Clone, Debug)]
struct PhysicsObject {
    handle: RigidBodyHandle,
    state: SceneObjectState,
}

#[derive(Clone, Debug)]
struct TriggerRuntime {
    config: ProjectSceneTriggerConfig,
    state: SceneTriggerState,
}

pub(crate) struct ScenePhysicsRuntime {
    world: LivePhysicsWorld,
    objects: BTreeMap<String, PhysicsObject>,
    triggers: BTreeMap<String, TriggerRuntime>,
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

    fn insert(&mut self, body: RigidBody, collider: Collider) -> RigidBodyHandle {
        let handle = self.bodies.insert(body);
        self.colliders
            .insert_with_parent(collider, handle, &mut self.bodies);
        handle
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

fn speed(velocity: [f32; 3]) -> f32 {
    velocity
        .iter()
        .map(|component| component * component)
        .sum::<f32>()
        .sqrt()
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

impl ScenePhysicsRuntime {
    pub(crate) fn empty() -> Self {
        Self {
            world: LivePhysicsWorld::new(),
            objects: BTreeMap::new(),
            triggers: BTreeMap::new(),
        }
    }

    pub(crate) fn from_project(project: &ProjectConfig) -> Result<Self, Box<dyn Error>> {
        let mut world = LivePhysicsWorld::new();
        let mut objects = BTreeMap::new();

        for object in &project.scene.objects {
            let Some(config) = &object.physics else {
                continue;
            };
            let rigid_body = match config.body_kind {
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
            .angular_damping(config.angular_damping)
            .build();
            let collider = collider_builder(&config.collider.geometry)
                .translation(vec3(config.collider.offset))
                .mass(config.mass_kg.max(0.000_001))
                .friction(config.friction.max(0.0))
                .restitution(config.restitution.clamp(0.0, 1.0))
                .build();
            let handle = world.insert(rigid_body, collider);
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

        Ok(Self {
            world,
            objects,
            triggers,
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

    pub(crate) fn trigger_state(&self, id: &str) -> Option<&SceneTriggerState> {
        self.triggers.get(id).map(|trigger| &trigger.state)
    }

    pub(crate) fn trigger_states(&self) -> Vec<SceneTriggerState> {
        self.triggers
            .values()
            .map(|trigger| trigger.state.clone())
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

    pub(crate) fn step(&mut self, dt: f32, clock_sec: f64) {
        if dt <= 0.0 {
            return;
        }
        self.world.set_time_step(dt);
        self.world.step();
        self.sync_object_states();
        self.update_triggers(dt, clock_sec);
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

    use crate::project::{
        HardwareConfig, ProjectConfig, ProjectSceneBodyKind, ProjectSceneColliderConfig,
        ProjectSceneColliderGeometry, ProjectSceneConfig, ProjectSceneObjectConfig,
        ProjectSceneObjectGeometry, ProjectSceneObjectPhysicsConfig, ProjectSceneTriggerConfig,
    };

    use super::{SceneObjectAttachment, ScenePhysicsRuntime};

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
            include_in_fit: true,
            physics: Some(ProjectSceneObjectPhysicsConfig {
                body_kind,
                mass_kg: 0.05,
                linear_damping: 0.2,
                angular_damping: 0.2,
                friction: 0.7,
                restitution: 0.0,
                collider: ProjectSceneColliderConfig {
                    geometry: collider,
                    offset: [0.0; 3],
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
}
