use std::collections::BTreeMap;
use std::error::Error;
use std::path::Path;
#[cfg(unix)]
use std::sync::Arc;
#[cfg(unix)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
#[cfg(unix)]
use std::thread;

use crate::physics::PhysicsWorld;
use crate::project::{BusConfig, DeviceConfig, ProjectConfig};
use crate::scene_graph::{
    CameraProjection, CameraSpec, EntityId, EntityMetadata, Geometry, GeometryBounds,
    LightSpec as SceneLightSpec, Material, ReflectionProbeSettings,
    RenderSettings as SceneRenderSettings, SceneGraph, SceneNode, Transform,
};
use crate::urdf::{UrdfModel, load_urdf};
use feetech_servo::servo::protocol::serial_bus::ProtocolError;
use feetech_servo::servo::sim::{FeetechBusEvent, FeetechBusSim, FeetechServoSnapshot};
use pge_core as pge;
use pge_renderer::{RenderRequest, RenderView, RgbaFrame};
use pge_wgpu_renderer::WgpuRenderer;

pub mod physics;
pub mod project;
pub mod scene_graph;
pub mod scene_harness;
pub mod urdf;

pub use project::{
    FrameState, JointState, LinkState, ModelEntityKind, ResolvedFrameState, RigidTransform,
    RobotDreamsEntity, RobotDreamsModel, RobotState, SceneLocation,
};
pub use scene_graph::{
    EnvironmentSettings, LightKind, LightSpec, ObservationMetadata, ObservationRequest,
    ObservationView, RenderSettings, SceneNodeKind, SegmentationPolicy, ShutterPolicy, ToneMapping,
};

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CoordinateDebugOverlayOptions {
    pub enabled: bool,
    pub grid_half_extent_m: f32,
    pub grid_step_m: f32,
    pub axis_length_m: f32,
    pub line_thickness_m: f32,
    pub marker_radius_m: f32,
}

impl Default for CoordinateDebugOverlayOptions {
    fn default() -> Self {
        Self {
            enabled: false,
            grid_half_extent_m: 0.5,
            grid_step_m: 0.05,
            axis_length_m: 0.55,
            line_thickness_m: 0.004,
            marker_radius_m: 0.028,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CoordinateDebugMarkerPositions {
    pub robot_id: String,
    pub floor_z: f32,
    pub current_tcp: Option<[f32; 3]>,
    pub target_tcp: Option<[f32; 3]>,
}

pub struct RobotDreams {
    physics: PhysicsWorld,
    model: Option<RobotDreamsModel>,
    models: Vec<UrdfModel>,
    hardware: HardwareRuntime,
    virtual_buses: Vec<VirtualBusRuntime>,
    rover_drives: Vec<RoverDriveRuntime>,
    clock_sec: f64,
    dt: f32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct RobotDreamsSnapshot {
    pub clock_sec: f64,
    pub hardware: HardwareRuntime,
    pub servo_snapshots: Vec<BusServoSnapshots>,
    pub robots: Vec<RobotState>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct BusServoSnapshots {
    pub bus_id: String,
    pub snapshots: Vec<FeetechServoSnapshot>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct HardwareRuntime {
    pub buses: Vec<HardwareBusRuntime>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HardwareBusRuntime {
    pub id: String,
    pub name: String,
    pub transport_type: String,
    pub device_path: Option<String>,
    pub baud: Option<u32>,
    pub protocol: String,
    pub devices: Vec<HardwareDeviceRuntime>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum HardwareDeviceRuntime {
    Servo(HardwareServoRuntime),
    DcMotor(HardwareDcMotorRuntime),
    Imu(HardwareImuRuntime),
    IoBoard(HardwareIoBoardRuntime),
}

#[derive(Clone, Debug, PartialEq)]
pub struct HardwareServoRuntime {
    pub id: u32,
    pub name: String,
    pub profile: String,
    pub drives_robot: String,
    pub drives_joint: String,
    pub steers_robot: String,
    pub steers_joints: Vec<String>,
    pub zero_offset: i16,
    pub direction: i8,
    pub target_position: i16,
    pub present_position: i16,
    pub torque_enabled: bool,
    pub temperature_c: i16,
    pub voltage_v: f32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HardwareDcMotorRuntime {
    pub id: u32,
    pub name: String,
    pub profile: String,
    pub drives_robot: String,
    pub drives_wheel: String,
    pub direction: i8,
    pub max_speed_mps: f32,
    pub command_speed: i16,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HardwareImuRuntime {
    pub id: u32,
    pub name: String,
    pub profile: String,
    pub mounted_robot: Option<String>,
    pub mounted_link: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HardwareIoBoardRuntime {
    pub id: u32,
    pub name: String,
    pub profile: String,
    pub digital_inputs: usize,
    pub digital_outputs: usize,
    pub analog_inputs: usize,
}

struct VirtualBusRuntime {
    id: String,
    sim: FeetechBusSim,
}

#[derive(Clone, Debug, PartialEq)]
struct RoverDriveRuntime {
    robot_id: String,
    steering_angle_deg: f64,
    steering_center_deg: f64,
    wheelbase_m: f64,
}

const SERVO_FULL_ROTATION_TICKS: f64 = 4096.0;
const DEFAULT_ROVER_WHEELBASE_M: f64 = 0.22;

static MESH_BOUNDS_CACHE: OnceLock<Mutex<BTreeMap<MeshBoundsCacheKey, Option<GeometryBounds>>>> =
    OnceLock::new();

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct MeshBoundsCacheKey {
    asset: String,
    scale_bits: [u32; 3],
}

fn servo_runtime_from_config(config: &crate::project::ServoDeviceConfig) -> HardwareServoRuntime {
    let drives = config.drives.as_ref();
    let steers = config.steers.as_ref();
    HardwareServoRuntime {
        id: config.id,
        name: config.name.clone(),
        profile: config.profile.clone(),
        drives_robot: drives
            .map(|mapping| mapping.robot.clone())
            .unwrap_or_default(),
        drives_joint: drives
            .map(|mapping| mapping.target.clone())
            .unwrap_or_default(),
        steers_robot: steers
            .map(|mapping| mapping.robot.clone())
            .unwrap_or_default(),
        steers_joints: steers
            .map(|mapping| mapping.joints.clone())
            .unwrap_or_default(),
        zero_offset: config.calibration.zero_offset,
        direction: config.calibration.direction,
        target_position: config.calibration.zero_offset,
        present_position: config.calibration.zero_offset,
        torque_enabled: true,
        temperature_c: 25,
        voltage_v: 7.4,
    }
}

fn dc_motor_runtime_from_config(
    config: &crate::project::DcMotorDeviceConfig,
) -> HardwareDcMotorRuntime {
    let drives = config.drives.as_ref();
    HardwareDcMotorRuntime {
        id: config.id,
        name: config.name.clone(),
        profile: config.profile.clone(),
        drives_robot: drives
            .map(|mapping| mapping.robot.clone())
            .unwrap_or_default(),
        drives_wheel: drives
            .map(|mapping| mapping.target.clone())
            .unwrap_or_default(),
        direction: config.calibration.direction,
        max_speed_mps: config.calibration.max_speed_mps,
        command_speed: 0,
    }
}

fn device_runtime_from_config(config: &DeviceConfig) -> HardwareDeviceRuntime {
    match config {
        DeviceConfig::Servo(config) => {
            HardwareDeviceRuntime::Servo(servo_runtime_from_config(config))
        }
        DeviceConfig::DcMotor(config) => {
            HardwareDeviceRuntime::DcMotor(dc_motor_runtime_from_config(config))
        }
        DeviceConfig::Imu(config) => HardwareDeviceRuntime::Imu(HardwareImuRuntime {
            id: config.id,
            name: config.name.clone(),
            profile: config.profile.clone(),
            mounted_robot: config
                .mounted_on
                .as_ref()
                .map(|mapping| mapping.robot.clone()),
            mounted_link: config
                .mounted_on
                .as_ref()
                .map(|mapping| mapping.target.clone()),
        }),
        DeviceConfig::IoBoard(config) => HardwareDeviceRuntime::IoBoard(HardwareIoBoardRuntime {
            id: config.id,
            name: config.name.clone(),
            profile: config.profile.clone(),
            digital_inputs: 0,
            digital_outputs: 0,
            analog_inputs: 0,
        }),
    }
}

fn bus_runtime_from_config(config: &BusConfig) -> HardwareBusRuntime {
    HardwareBusRuntime {
        id: config.id.clone(),
        name: config.name.clone(),
        transport_type: config.transport.type_name.clone(),
        device_path: config.transport.path.clone(),
        baud: config.transport.baud,
        protocol: config.protocol.clone(),
        devices: config
            .devices
            .iter()
            .map(device_runtime_from_config)
            .collect(),
    }
}

fn hardware_runtime_from_project(project: &ProjectConfig) -> HardwareRuntime {
    HardwareRuntime {
        buses: project
            .hardware
            .buses
            .iter()
            .map(bus_runtime_from_config)
            .collect(),
    }
}

fn hardware_runtime_from_model(model: &RobotDreamsModel) -> HardwareRuntime {
    model
        .project()
        .map(hardware_runtime_from_project)
        .unwrap_or_default()
}

fn bus_servo_count(bus: &HardwareBusRuntime) -> u8 {
    bus.devices
        .iter()
        .filter_map(|device| match device {
            HardwareDeviceRuntime::Servo(servo) => u8::try_from(servo.id).ok(),
            _ => None,
        })
        .max()
        .unwrap_or(1)
}

fn virtual_buses_from_hardware(hardware: &HardwareRuntime) -> Vec<VirtualBusRuntime> {
    hardware
        .buses
        .iter()
        .filter(|bus| bus.transport_type.eq_ignore_ascii_case("virtual"))
        .map(|bus| {
            let mut sim = FeetechBusSim::new();
            sim.set_servo_count(bus_servo_count(bus));
            for device in &bus.devices {
                let HardwareDeviceRuntime::Servo(servo) = device else {
                    continue;
                };
                let Ok(id) = u8::try_from(servo.id) else {
                    continue;
                };
                let _ = sim.set_target_position(id, servo.zero_offset);
            }
            sim.step(3.0);
            VirtualBusRuntime {
                id: bus.id.clone(),
                sim,
            }
        })
        .collect()
}

fn rover_drives_from_hardware(hardware: &HardwareRuntime) -> Vec<RoverDriveRuntime> {
    let mut robot_ids = Vec::<String>::new();
    for bus in &hardware.buses {
        for device in &bus.devices {
            let HardwareDeviceRuntime::DcMotor(motor) = device else {
                continue;
            };
            if motor.drives_robot.is_empty()
                || !motor.drives_wheel.to_ascii_lowercase().contains("left")
            {
                continue;
            }
            if !robot_ids
                .iter()
                .any(|robot_id| robot_id == &motor.drives_robot)
            {
                robot_ids.push(motor.drives_robot.clone());
            }
        }
    }

    robot_ids
        .into_iter()
        .map(|robot_id| RoverDriveRuntime {
            robot_id,
            steering_angle_deg: 90.0,
            steering_center_deg: 90.0,
            wheelbase_m: DEFAULT_ROVER_WHEELBASE_M,
        })
        .collect()
}

fn servo_snapshot<'a>(
    snapshots: &'a [FeetechServoSnapshot],
    servo: &HardwareServoRuntime,
) -> Option<&'a FeetechServoSnapshot> {
    let servo_id = u8::try_from(servo.id).ok()?;
    snapshots.iter().find(|snapshot| snapshot.id == servo_id)
}

fn servo_ticks_to_radians(ticks: i16, zero_offset: i16, direction: i8) -> f64 {
    let ticks = ticks.clamp(0, 4095) as f64;
    let zero_offset = zero_offset.clamp(0, 4095) as f64;
    let direction = if direction < 0 { -1.0 } else { 1.0 };
    direction * (ticks - zero_offset) * (std::f64::consts::TAU / SERVO_FULL_ROTATION_TICKS)
}

fn apply_servo_snapshots_to_hardware(
    hardware: &mut HardwareRuntime,
    bus_id: &str,
    snapshots: &[FeetechServoSnapshot],
) {
    let Some(bus) = hardware.buses.iter_mut().find(|bus| bus.id == bus_id) else {
        return;
    };
    for device in &mut bus.devices {
        let HardwareDeviceRuntime::Servo(servo) = device else {
            continue;
        };
        let Some(snapshot) = servo_snapshot(snapshots, servo) else {
            continue;
        };
        servo.target_position = snapshot.target_position;
        servo.present_position = snapshot.present_position;
        servo.torque_enabled = snapshot.torque_enabled;
        servo.temperature_c = i16::from(snapshot.temperature_c);
        servo.voltage_v = f32::from(snapshot.voltage_tenths) / 10.0;
    }
}

fn apply_servo_snapshots_to_model(
    model: &mut RobotDreamsModel,
    hardware: &HardwareRuntime,
    bus_id: &str,
    snapshots: &[FeetechServoSnapshot],
) {
    let Some(bus) = hardware.buses.iter().find(|bus| bus.id == bus_id) else {
        return;
    };
    for device in &bus.devices {
        let HardwareDeviceRuntime::Servo(servo) = device else {
            continue;
        };
        if servo.drives_joint.is_empty() {
            continue;
        }
        let Some(snapshot) = servo_snapshot(snapshots, servo) else {
            continue;
        };
        let radians = servo_ticks_to_radians(
            snapshot.present_position,
            servo.zero_offset,
            servo.direction,
        );
        let _ = model.set_joint_angle(&servo.drives_joint, radians);
    }
}

fn apply_steering_angle_to_model(
    model: &mut RobotDreamsModel,
    hardware: &HardwareRuntime,
    robot_id: &str,
    steering_angle_deg: f64,
    steering_center_deg: f64,
) {
    let radians = (steering_angle_deg - steering_center_deg).to_radians();
    for bus in &hardware.buses {
        for device in &bus.devices {
            let HardwareDeviceRuntime::Servo(servo) = device else {
                continue;
            };
            if servo.steers_robot != robot_id {
                continue;
            }
            for joint in &servo.steers_joints {
                let _ = model.set_joint_angle(joint, radians);
            }
        }
    }
}

fn dc_motor_speed_mps(hardware: &HardwareRuntime, robot_id: &str, wheel: &str) -> Option<f64> {
    for bus in &hardware.buses {
        for device in &bus.devices {
            let HardwareDeviceRuntime::DcMotor(motor) = device else {
                continue;
            };
            if motor.drives_robot != robot_id {
                continue;
            }
            if !motor.drives_wheel.to_ascii_lowercase().contains(wheel) {
                continue;
            }
            let direction = if motor.direction < 0 { -1.0 } else { 1.0 };
            let command = f64::from(motor.command_speed.clamp(-100, 100)) / 100.0;
            return Some(command * f64::from(motor.max_speed_mps) * direction);
        }
    }
    None
}

impl RobotDreams {
    pub fn new() -> Self {
        Self {
            physics: PhysicsWorld::new(),
            model: None,
            models: Vec::new(),
            hardware: HardwareRuntime::default(),
            virtual_buses: Vec::new(),
            rover_drives: Vec::new(),
            clock_sec: 0.0,
            dt: 1.0 / 200.0,
        }
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, Box<dyn Error>> {
        let model = RobotDreamsModel::open(path)?;
        let hardware = hardware_runtime_from_model(&model);
        let virtual_buses = virtual_buses_from_hardware(&hardware);
        let rover_drives = rover_drives_from_hardware(&hardware);
        let mut dreams = Self {
            model: Some(model),
            hardware,
            virtual_buses,
            rover_drives,
            ..Self::new()
        };
        dreams.apply_virtual_bus_snapshots();
        Ok(dreams)
    }

    pub fn set_time_step(&mut self, dt: f32) {
        if dt > 0.0 {
            self.dt = dt;
            self.physics.set_time_step(dt);
        }
    }

    pub fn add_urdf(&mut self, path: impl AsRef<Path>) -> Result<usize, Box<dyn Error>> {
        let model = load_urdf(path)?;
        self.models.push(model);
        Ok(self.models.len() - 1)
    }

    pub fn named(&self, name: &str) -> Option<RobotDreamsEntity> {
        self.model.as_ref()?.named(name)
    }

    pub fn location_of(&self, name: &str) -> Option<SceneLocation> {
        self.model.as_ref()?.location_of(name)
    }

    pub fn robot_state(&self, robot_id_or_name: &str) -> Option<RobotState> {
        self.model.as_ref()?.robot_state(robot_id_or_name)
    }

    pub fn frame_state(
        &self,
        robot_id_or_name: &str,
        frame_name: &str,
    ) -> Option<ResolvedFrameState> {
        self.model
            .as_ref()?
            .frame_state(robot_id_or_name, frame_name)
    }

    pub fn set_joint_angle(
        &mut self,
        name: impl AsRef<str>,
        radians: f64,
    ) -> Result<(), Box<dyn Error>> {
        self.model
            .as_mut()
            .ok_or_else(|| "No RobotDreams model is open".into())
            .and_then(|model| model.set_joint_angle(name, radians))
    }

    pub fn advance_seconds(&mut self, dt: f32) {
        if dt <= 0.0 {
            return;
        }
        self.clock_sec += f64::from(dt);
        for bus in &mut self.virtual_buses {
            bus.sim.step(dt);
        }
        self.apply_virtual_bus_snapshots();
        self.integrate_rover_drives(f64::from(dt));
        self.set_time_step(dt);
        self.physics.step();
    }

    pub fn advance_rover_drive_seconds(&mut self, dt: f32) {
        if dt <= 0.0 {
            return;
        }
        self.clock_sec += f64::from(dt);
        self.integrate_rover_drives(f64::from(dt));
    }

    pub fn snapshot(&self) -> RobotDreamsSnapshot {
        RobotDreamsSnapshot {
            clock_sec: self.clock_sec,
            hardware: self.hardware.clone(),
            servo_snapshots: self
                .virtual_buses
                .iter()
                .map(|bus| BusServoSnapshots {
                    bus_id: bus.id.clone(),
                    snapshots: bus.sim.servo_snapshots(),
                })
                .collect(),
            robots: self
                .model
                .as_ref()
                .map(|model| model.robot_states())
                .unwrap_or_default(),
        }
    }

    pub fn scene_graph(&self) -> SceneGraph {
        let mut graph = SceneGraph::empty();

        if let Some(model) = &self.model {
            for robot in model.robot_states() {
                let entity = format!("robot:{}", robot.id);
                graph.add_entity(EntityMetadata {
                    id: EntityId(entity.clone()),
                    name: robot.name.clone(),
                    kind: "robot".to_string(),
                    robot_id: Some(robot.id.clone()),
                    link_name: None,
                });
                graph
                    .root
                    .children
                    .push(SceneNode::group(entity, robot.name));
            }

            for (index, visual) in model.robot_visual_meshes().into_iter().enumerate() {
                let entity = format!(
                    "robot:{}:visual:{}:{index}",
                    visual.robot_id, visual.link_name
                );
                graph.add_entity(EntityMetadata {
                    id: EntityId(entity.clone()),
                    name: visual.name.clone(),
                    kind: "robotVisual".to_string(),
                    robot_id: Some(visual.robot_id.clone()),
                    link_name: Some(visual.link_name.clone()),
                });
                graph.root.children.push(SceneNode::mesh(
                    entity,
                    visual.name,
                    mesh_asset_geometry(&visual.asset, visual.scale),
                    Material {
                        color_rgb: [102, 151, 196],
                    },
                    Transform::matrix(visual.translation, visual.rotation_matrix),
                ));
            }

            if let Some(project) = model.project() {
                graph.render_settings = project
                    .scene
                    .render_settings
                    .as_ref()
                    .map(|settings| scene_render_settings(project, settings));
                graph.reflection_probes = project
                    .scene
                    .reflection_probes
                    .iter()
                    .map(|probe| scene_reflection_probe(project, probe))
                    .collect();

                for object in &project.scene.objects {
                    let entity = format!("object:{}", object.id);
                    graph.add_entity(EntityMetadata {
                        id: EntityId(entity.clone()),
                        name: object.name.clone(),
                        kind: "sceneObject".to_string(),
                        robot_id: None,
                        link_name: None,
                    });
                    let mut node = SceneNode::mesh(
                        entity,
                        object.name.clone(),
                        scene_object_geometry(project, object),
                        Material {
                            color_rgb: object.color_rgb,
                        },
                        Transform {
                            translation: object.position,
                            rotation: object.rotation,
                            rotation_matrix: None,
                        },
                    );
                    node.include_in_fit = object.include_in_fit;
                    graph.root.children.push(node);
                }

                for camera in &project.scene.cameras {
                    if let Some(spec) = self.camera_spec(&camera.id) {
                        let entity = format!("camera:{}", camera.id);
                        graph.add_entity(EntityMetadata {
                            id: EntityId(entity.clone()),
                            name: camera.name.clone(),
                            kind: "camera".to_string(),
                            robot_id: Some(camera.mounted_robot.clone()),
                            link_name: Some(camera.mounted_link.clone()),
                        });
                        graph.root.children.push(SceneNode::camera(
                            entity,
                            camera.name.clone(),
                            spec,
                        ));
                    }
                }

                for light in &project.scene.lights {
                    let entity = format!("light:{}", light.id);
                    graph.add_entity(EntityMetadata {
                        id: EntityId(entity.clone()),
                        name: light.name.clone(),
                        kind: "light".to_string(),
                        robot_id: None,
                        link_name: None,
                    });
                    graph.root.children.push(SceneNode::light(
                        entity,
                        light.name.clone(),
                        SceneLightSpec {
                            id: light.id.clone(),
                            name: light.name.clone(),
                            transform: Transform {
                                translation: light.position,
                                rotation: light.rotation,
                                rotation_matrix: None,
                            },
                            kind: light.kind,
                            color_rgb: light.color_rgb,
                            intensity: light.intensity,
                        },
                    ));
                }
            }
        }

        graph
    }

    pub fn scene_graph_with_coordinate_debug_overlay(
        &self,
        options: CoordinateDebugOverlayOptions,
    ) -> SceneGraph {
        let mut graph = self.scene_graph();
        if options.enabled {
            self.append_coordinate_debug_overlay(&mut graph, options);
        }
        graph
    }

    pub fn coordinate_debug_marker_positions(
        &self,
        options: CoordinateDebugOverlayOptions,
    ) -> Vec<CoordinateDebugMarkerPositions> {
        let Some(model) = &self.model else {
            return Vec::new();
        };
        let target_model = self.target_model_state();
        let overlay_z = options.line_thickness_m.max(0.001) * 2.0;
        model
            .robot_states()
            .into_iter()
            .filter_map(|robot| {
                let current_tcp = robot
                    .tcp
                    .as_ref()
                    .and_then(|tcp| tcp.location.as_ref())
                    .map(|location| f64_vec3_to_f32(location.position));
                let target_tcp = target_model
                    .as_ref()
                    .and_then(|target_model| target_model.robot_state(&robot.id))
                    .and_then(|target_robot| target_robot.tcp)
                    .and_then(|tcp| tcp.location)
                    .map(|location| f64_vec3_to_f32(location.position));
                if current_tcp.is_none() && target_tcp.is_none() {
                    return None;
                }
                Some(CoordinateDebugMarkerPositions {
                    robot_id: robot.id,
                    floor_z: f64_vec3_to_f32(robot.base.position)[2] + overlay_z,
                    current_tcp,
                    target_tcp,
                })
            })
            .collect()
    }

    pub fn world_state(&self) -> pge::WorldState {
        scene_graph_to_world_state(&self.scene_graph())
    }

    fn append_coordinate_debug_overlay(
        &self,
        graph: &mut SceneGraph,
        options: CoordinateDebugOverlayOptions,
    ) {
        let Some(model) = &self.model else {
            return;
        };
        let target_model = self.target_model_state();
        for robot in model.robot_states() {
            let Some(base_rotation) = robot.base.rotation else {
                continue;
            };
            let current_tcp = robot.tcp.as_ref().and_then(|tcp| tcp.location.as_ref());
            let target_tcp = target_model
                .as_ref()
                .and_then(|target_model| target_model.robot_state(&robot.id))
                .and_then(|target_robot| target_robot.tcp)
                .and_then(|tcp| tcp.location);
            append_robot_coordinate_debug_overlay(
                graph,
                &robot.id,
                f64_vec3_to_f32(robot.base.position),
                f64_vec3_to_f32(base_rotation),
                robot.frames.get("armBase"),
                current_tcp.map(|location| f64_vec3_to_f32(location.position)),
                target_tcp.map(|location| f64_vec3_to_f32(location.position)),
                options,
            );
        }
    }

    fn target_model_state(&self) -> Option<RobotDreamsModel> {
        let mut model = self.model.clone()?;
        for bus in &self.virtual_buses {
            let snapshots = bus
                .sim
                .servo_snapshots()
                .into_iter()
                .map(|mut snapshot| {
                    snapshot.present_position = snapshot.target_position;
                    snapshot
                })
                .collect::<Vec<_>>();
            apply_servo_snapshots_to_model(&mut model, &self.hardware, &bus.id, &snapshots);
        }
        Some(model)
    }

    pub fn camera_spec(&self, camera_id: &str) -> Option<CameraSpec> {
        let model = self.model.as_ref()?;
        let project = model.project()?;
        let camera = project
            .scene
            .cameras
            .iter()
            .find(|camera| camera.id == camera_id)?;
        let mounted_link = model
            .robot_state(&camera.mounted_robot)
            .and_then(|robot| robot.links.get(&camera.mounted_link).cloned())
            .and_then(|link| link.location);
        let mut translation = camera.position;
        let mut link_rotation = identity_mat3();
        if let Some(location) = mounted_link {
            if let Some(rotation) = location.rotation {
                link_rotation = rpy_matrix_f32(f64_vec3_to_f32(rotation));
            }
            translation = add_f32_vec3(
                f64_vec3_to_f32(location.position),
                mat3_vec_mul(link_rotation, camera.position),
            );
        }
        let rotation_matrix = native_camera_rotation_matrix(link_rotation, camera.rotation);

        Some(CameraSpec {
            id: camera.id.clone(),
            name: camera.name.clone(),
            transform: Transform::matrix(translation, rotation_matrix),
            fov_deg: camera.fov_deg,
            projection: camera.projection,
            resolution: camera.resolution.unwrap_or([320, 240]),
            intrinsics: camera.intrinsics,
            distortion: camera.distortion,
            depth_range_m: camera.depth_range_m,
            sensor_effects: camera.sensor_effects,
        })
    }

    pub fn hardware(&self) -> &HardwareRuntime {
        &self.hardware
    }

    pub fn servo_snapshots(&self, bus_id: &str) -> Option<Vec<FeetechServoSnapshot>> {
        self.virtual_buses
            .iter()
            .find(|bus| bus.id == bus_id)
            .map(|bus| bus.sim.servo_snapshots())
    }

    pub fn first_virtual_bus_id(&self) -> Option<&str> {
        self.virtual_buses.first().map(|bus| bus.id.as_str())
    }

    pub fn handle_virtual_bus_frame(
        &mut self,
        bus_id: &str,
        frame: &[u8],
    ) -> Result<Option<Vec<u8>>, ProtocolError> {
        self.handle_virtual_bus_frame_with_event(bus_id, frame).0
    }

    pub fn handle_virtual_bus_frame_with_event(
        &mut self,
        bus_id: &str,
        frame: &[u8],
    ) -> (Result<Option<Vec<u8>>, ProtocolError>, FeetechBusEvent) {
        let Some(bus) = self.virtual_buses.iter_mut().find(|bus| bus.id == bus_id) else {
            return (
                Ok(None),
                FeetechBusEvent {
                    instruction: "invalid".to_string(),
                    id: None,
                    broadcast: false,
                    address: None,
                    length: None,
                    ids: Vec::new(),
                    writes: Vec::new(),
                    data: Vec::new(),
                    target_position: None,
                    raw: frame.to_vec(),
                    error: Some(format!("virtual bus '{bus_id}' not found")),
                },
            );
        };
        let result = bus.sim.handle_frame_with_event(frame);
        self.apply_virtual_bus_snapshots();
        result
    }

    pub fn set_virtual_servo_target(
        &mut self,
        bus_id: &str,
        servo_id: u8,
        target_position: i16,
    ) -> bool {
        let Some(bus) = self.virtual_buses.iter_mut().find(|bus| bus.id == bus_id) else {
            return false;
        };
        let updated = bus.sim.set_target_position(servo_id, target_position);
        if updated {
            self.apply_virtual_bus_snapshots();
        }
        updated
    }

    pub fn set_virtual_drive_output(
        &mut self,
        bus_id: &str,
        robot_id: &str,
        left_motor_id: u32,
        right_motor_id: u32,
        left_speed: i16,
        right_speed: i16,
        steering_angle_deg: f64,
        steering_center_deg: f64,
    ) -> bool {
        let Some(bus) = self.hardware.buses.iter_mut().find(|bus| bus.id == bus_id) else {
            return false;
        };
        let mut updated_left = false;
        let mut updated_right = false;
        for device in &mut bus.devices {
            let HardwareDeviceRuntime::DcMotor(motor) = device else {
                continue;
            };
            if motor.id == left_motor_id {
                motor.command_speed = left_speed.clamp(-100, 100);
                updated_left = true;
            }
            if motor.id == right_motor_id {
                motor.command_speed = right_speed.clamp(-100, 100);
                updated_right = true;
            }
        }
        if !updated_left || !updated_right {
            return false;
        }

        if let Some(drive) = self
            .rover_drives
            .iter_mut()
            .find(|drive| drive.robot_id == robot_id)
        {
            drive.steering_angle_deg = steering_angle_deg;
            drive.steering_center_deg = steering_center_deg;
        } else {
            self.rover_drives.push(RoverDriveRuntime {
                robot_id: robot_id.to_string(),
                steering_angle_deg,
                steering_center_deg,
                wheelbase_m: DEFAULT_ROVER_WHEELBASE_M,
            });
        }
        if let Some(model) = &mut self.model {
            apply_steering_angle_to_model(
                model,
                &self.hardware,
                robot_id,
                steering_angle_deg,
                steering_center_deg,
            );
        }
        true
    }

    pub fn step(&mut self, steps: usize) {
        for _ in 0..steps {
            self.advance_seconds(self.dt);
        }
    }

    pub fn models(&self) -> &[UrdfModel] {
        &self.models
    }

    pub fn model(&self) -> Option<&RobotDreamsModel> {
        self.model.as_ref()
    }

    pub fn physics(&self) -> &PhysicsWorld {
        &self.physics
    }

    pub fn physics_mut(&mut self) -> &mut PhysicsWorld {
        &mut self.physics
    }

    fn apply_virtual_bus_snapshots(&mut self) {
        let snapshots_by_bus = self
            .virtual_buses
            .iter()
            .map(|bus| (bus.id.clone(), bus.sim.servo_snapshots()))
            .collect::<Vec<_>>();
        for (bus_id, snapshots) in snapshots_by_bus {
            apply_servo_snapshots_to_hardware(&mut self.hardware, &bus_id, &snapshots);
            if let Some(model) = &mut self.model {
                apply_servo_snapshots_to_model(model, &self.hardware, &bus_id, &snapshots);
            }
        }
    }

    fn integrate_rover_drives(&mut self, dt: f64) {
        if dt <= 0.0 {
            return;
        }

        let mut moves = Vec::new();
        for drive in &self.rover_drives {
            let Some(left_speed_mps) = dc_motor_speed_mps(&self.hardware, &drive.robot_id, "left")
            else {
                continue;
            };
            let Some(right_speed_mps) =
                dc_motor_speed_mps(&self.hardware, &drive.robot_id, "right")
            else {
                continue;
            };
            let linear_mps = 0.5 * (left_speed_mps + right_speed_mps);
            if linear_mps.abs() <= f64::EPSILON {
                continue;
            }
            let yaw = self
                .model
                .as_ref()
                .and_then(|model| model.robot_base_yaw(&drive.robot_id))
                .unwrap_or(0.0);
            let steering_rad = (drive.steering_angle_deg - drive.steering_center_deg).to_radians();
            let yaw_rate = if drive.wheelbase_m > 0.0 {
                linear_mps * steering_rad.tan() / drive.wheelbase_m
            } else {
                0.0
            };
            let distance_m = linear_mps * dt;
            moves.push((
                drive.robot_id.clone(),
                distance_m * yaw.cos(),
                distance_m * yaw.sin(),
                yaw_rate * dt,
            ));
        }

        let Some(model) = &mut self.model else {
            return;
        };
        for (robot_id, dx, dy, dyaw) in moves {
            model.move_robot_base_flat(&robot_id, dx, dy, dyaw);
        }
    }
}

pub fn world_state_from_scene_graph(graph: &SceneGraph) -> pge::WorldState {
    scene_graph_to_world_state(graph)
}

pub const ROBOTDREAMS_PGE_CAMERA_ID: &str = "robotdreams_pge_camera";

#[derive(Clone, Debug, PartialEq)]
pub struct RobotDreamsPgeTextLabel {
    pub id: String,
    pub text: String,
    pub position: [f32; 3],
    pub color: [f32; 4],
    pub background_color: [f32; 4],
    pub font_size_px: f32,
    pub billboard: bool,
}

impl RobotDreamsPgeTextLabel {
    pub fn overlay(id: impl Into<String>, text: impl Into<String>, row: usize) -> Self {
        Self {
            id: id.into(),
            text: text.into(),
            position: [12.0, 12.0 + row as f32 * 18.0, 0.0],
            color: [0.86, 0.94, 1.0, 1.0],
            background_color: [0.02, 0.05, 0.08, 0.78],
            font_size_px: 1.0,
            billboard: false,
        }
    }

    pub fn overlay_with_color(
        id: impl Into<String>,
        text: impl Into<String>,
        row: usize,
        color: [f32; 4],
    ) -> Self {
        let mut label = Self::overlay(id, text, row);
        label.color = color;
        label
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RobotDreamsPgeFrameOptions {
    pub resolution: [u32; 2],
    pub target: [f32; 3],
    pub camera_radius_m: f32,
    pub camera_elevation_deg: f32,
    pub debug_coordinate_overlay: bool,
    pub text_labels: Vec<RobotDreamsPgeTextLabel>,
}

impl Default for RobotDreamsPgeFrameOptions {
    fn default() -> Self {
        Self {
            resolution: [960, 540],
            target: [0.0, 0.0, 0.16],
            camera_radius_m: 1.2,
            camera_elevation_deg: 35.0,
            debug_coordinate_overlay: true,
            text_labels: Vec::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RobotDreamsPgeFrame {
    pub world: pge::WorldState,
    pub request: RenderRequest,
    pub camera_entity: pge::EntityId,
}

pub fn robotdreams_pge_frame(
    dreams: &RobotDreams,
    options: RobotDreamsPgeFrameOptions,
) -> RobotDreamsPgeFrame {
    let elevation_rad = options.camera_elevation_deg.to_radians();
    let eye = [
        options.target[0] + options.camera_radius_m * elevation_rad.cos(),
        options.target[1] - options.camera_radius_m * 0.45,
        options.target[2] + options.camera_radius_m * elevation_rad.sin(),
    ];
    let mut scene =
        dreams.scene_graph_with_coordinate_debug_overlay(CoordinateDebugOverlayOptions {
            enabled: options.debug_coordinate_overlay,
            ..CoordinateDebugOverlayOptions::default()
        });
    add_pge_camera(&mut scene, eye, options.target, options.resolution);
    let mut world = world_state_from_scene_graph(&scene);
    if options.debug_coordinate_overlay {
        for label in coordinate_debug_legend_labels(0) {
            world.text_labels.push(pge::TextLabel {
                entity: pge::EntityId(format!("label:{}", label.id)),
                text: label.text,
                position: label.position,
                color: label.color,
                background_color: label.background_color,
                font_size_px: label.font_size_px,
                billboard: label.billboard,
            });
        }
    }
    for label in options.text_labels {
        world.text_labels.push(pge::TextLabel {
            entity: pge::EntityId(format!("label:{}", label.id)),
            text: label.text,
            position: label.position,
            color: label.color,
            background_color: label.background_color,
            font_size_px: label.font_size_px,
            billboard: label.billboard,
        });
    }
    let camera_entity = pge::EntityId(format!("camera:{ROBOTDREAMS_PGE_CAMERA_ID}"));
    let request = RenderRequest {
        camera_id: Some(camera_entity.clone()),
        views: vec![RenderView::Rgb],
        resolution: options.resolution,
        settings: None,
    };
    RobotDreamsPgeFrame {
        world,
        request,
        camera_entity,
    }
}

pub fn render_robotdreams_pge_frame(
    frame: &RobotDreamsPgeFrame,
) -> Result<RgbaFrame, pge_renderer::RenderError> {
    let mut renderer = WgpuRenderer::new()?;
    renderer.render_rgba(&frame.world, &frame.request)
}

fn add_pge_camera(scene: &mut SceneGraph, eye: [f32; 3], target: [f32; 3], resolution: [u32; 2]) {
    let entity = EntityId(format!("camera:{ROBOTDREAMS_PGE_CAMERA_ID}"));
    scene.entities.insert(
        entity.clone(),
        EntityMetadata {
            id: entity.clone(),
            name: "RobotDreams PGE Camera".to_string(),
            kind: "camera".to_string(),
            robot_id: None,
            link_name: None,
        },
    );
    scene.root.children.push(SceneNode::camera(
        entity.0,
        "RobotDreams PGE Camera",
        CameraSpec {
            id: ROBOTDREAMS_PGE_CAMERA_ID.to_string(),
            name: "RobotDreams PGE Camera".to_string(),
            transform: Transform::matrix(eye, look_at_matrix_f32(eye, target, [0.0, 0.0, 1.0])),
            fov_deg: 55.0,
            projection: CameraProjection::Perspective,
            resolution,
            intrinsics: None,
            distortion: None,
            depth_range_m: None,
            sensor_effects: None,
        },
    ));
}

fn look_at_matrix_f32(eye: [f32; 3], target: [f32; 3], up: [f32; 3]) -> [[f32; 3]; 3] {
    let forward = normalize_f32_vec3(sub_f32_vec3(target, eye));
    let right = normalize_f32_vec3(cross_f32_vec3(up, forward));
    let up = cross_f32_vec3(forward, right);
    [
        [right[0], up[0], forward[0]],
        [right[1], up[1], forward[1]],
        [right[2], up[2], forward[2]],
    ]
}

fn append_robot_coordinate_debug_overlay(
    graph: &mut SceneGraph,
    robot_id: &str,
    base_translation: [f32; 3],
    base_rotation: [f32; 3],
    arm_base: Option<&ResolvedFrameState>,
    current_tcp: Option<[f32; 3]>,
    target_tcp: Option<[f32; 3]>,
    options: CoordinateDebugOverlayOptions,
) {
    let prefix = format!("debug:{robot_id}:frame:base");
    let mut group = SceneNode::group(prefix.clone(), format!("{robot_id} Base Frame Debug"));
    group.transform = Transform::matrix(base_translation, rpy_matrix_f32(base_rotation));
    group.include_in_fit = false;

    let half_extent = options.grid_half_extent_m.max(options.grid_step_m);
    let step = options.grid_step_m.max(0.001);
    let line_thickness = options.line_thickness_m.max(0.001);
    let overlay_z = line_thickness * 2.0;
    let mut grid_index = 0_u32;
    let steps = (half_extent / step).ceil() as i32;
    for index in -steps..=steps {
        let offset = index as f32 * step;
        let is_axis = index == 0;
        let color = if is_axis {
            [95, 150, 210]
        } else {
            [55, 82, 105]
        };
        group.children.push(debug_box_node(
            format!("{prefix}:grid:x:{grid_index}"),
            "Base grid X",
            [0.0, offset, overlay_z],
            [half_extent * 2.0, line_thickness, line_thickness],
            color,
            None,
        ));
        group.children.push(debug_box_node(
            format!("{prefix}:grid:y:{grid_index}"),
            "Base grid Y",
            [offset, 0.0, overlay_z],
            [line_thickness, half_extent * 2.0, line_thickness],
            color,
            None,
        ));
        grid_index += 1;
    }

    let axis_length = options.axis_length_m.max(0.01);
    group.children.push(debug_box_node(
        format!("{prefix}:axis:x"),
        "X forward",
        [axis_length * 0.5, 0.0, overlay_z],
        [axis_length, line_thickness * 2.0, line_thickness * 2.0],
        [255, 70, 70],
        None,
    ));
    group.children.push(debug_box_node(
        format!("{prefix}:axis:y"),
        "Y left",
        [0.0, axis_length * 0.5, overlay_z],
        [line_thickness * 2.0, axis_length, line_thickness * 2.0],
        [40, 220, 110],
        None,
    ));
    group.children.push(debug_box_node(
        format!("{prefix}:axis:z"),
        "Z up",
        [0.0, 0.0, overlay_z + axis_length * 0.5],
        [line_thickness * 2.0, line_thickness * 2.0, axis_length],
        [80, 160, 255],
        None,
    ));
    append_axis_letter_nodes(
        &mut group.children,
        &prefix,
        'X',
        [axis_length + 0.045, 0.0, overlay_z],
        0.055,
        line_thickness * 1.5,
        [255, 70, 70],
    );
    append_axis_letter_nodes(
        &mut group.children,
        &prefix,
        'Y',
        [0.0, axis_length + 0.045, overlay_z],
        0.055,
        line_thickness * 1.5,
        [40, 220, 110],
    );
    append_axis_letter_nodes(
        &mut group.children,
        &prefix,
        'Z',
        [0.0, 0.0, overlay_z + axis_length + 0.045],
        0.055,
        line_thickness * 1.5,
        [80, 160, 255],
    );

    register_debug_node(graph, &group, robot_id);
    graph.root.children.push(group);

    if let Some(arm_base) = arm_base {
        append_compact_frame_debug_overlay(graph, robot_id, arm_base, options);
    }

    if let Some(position) = current_tcp {
        push_debug_node(
            graph,
            robot_id,
            debug_sphere_node(
                format!("debug:{robot_id}:tcp:current"),
                "Current TCP",
                position,
                options.marker_radius_m,
                [0, 220, 255],
            ),
        );
        push_debug_node(
            graph,
            robot_id,
            debug_box_node(
                format!("debug:{robot_id}:tcp:current:floor"),
                "Current TCP floor projection",
                [position[0], position[1], base_translation[2] + overlay_z],
                [
                    options.marker_radius_m * 0.9,
                    options.marker_radius_m * 0.9,
                    line_thickness,
                ],
                [0, 140, 180],
                None,
            ),
        );
    }
    if let Some(position) = target_tcp {
        push_debug_node(
            graph,
            robot_id,
            debug_box_node(
                format!("debug:{robot_id}:tcp:target"),
                "Target TCP",
                position,
                [
                    options.marker_radius_m * 1.5,
                    options.marker_radius_m * 1.5,
                    options.marker_radius_m * 1.5,
                ],
                [255, 190, 0],
                None,
            ),
        );
        push_debug_node(
            graph,
            robot_id,
            debug_box_node(
                format!("debug:{robot_id}:tcp:target:floor"),
                "Target TCP floor projection",
                [position[0], position[1], base_translation[2] + overlay_z],
                [
                    options.marker_radius_m * 0.9,
                    options.marker_radius_m * 0.9,
                    line_thickness,
                ],
                [210, 130, 0],
                None,
            ),
        );
    }
    if let (Some(current), Some(target)) = (current_tcp, target_tcp)
        && distance_f32(current, target) > 0.001
    {
        push_debug_node(
            graph,
            robot_id,
            debug_line_node(
                format!("debug:{robot_id}:tcp:delta"),
                "Current to target TCP",
                current,
                target,
                options.line_thickness_m * 1.5,
                [255, 255, 255],
            ),
        );
    }
}

fn append_compact_frame_debug_overlay(
    graph: &mut SceneGraph,
    robot_id: &str,
    frame: &ResolvedFrameState,
    options: CoordinateDebugOverlayOptions,
) {
    let prefix = format!("debug:{robot_id}:frame:{}", frame.id);
    let mut group = SceneNode::group(
        prefix.clone(),
        format!("{} {} Frame Debug", robot_id, frame.name),
    );
    group.transform = Transform::matrix(
        f64_vec3_to_f32(frame.world_transform.translation_m),
        f64_matrix_to_f32(frame.world_transform.rotation),
    );
    group.include_in_fit = false;

    let axis_length = (options.axis_length_m * 0.3).max(0.04);
    let line_thickness = options.line_thickness_m.max(0.001) * 1.5;
    group.children.push(debug_sphere_node(
        format!("{prefix}:origin"),
        &format!("{} origin", frame.name),
        [0.0, 0.0, 0.0],
        options.marker_radius_m.max(line_thickness * 2.0),
        [255, 235, 120],
    ));
    group.children.push(debug_box_node(
        format!("{prefix}:axis:x"),
        &format!("{} X axis", frame.name),
        [axis_length * 0.5, 0.0, 0.0],
        [axis_length, line_thickness, line_thickness],
        [255, 70, 70],
        None,
    ));
    group.children.push(debug_box_node(
        format!("{prefix}:axis:y"),
        &format!("{} Y axis", frame.name),
        [0.0, axis_length * 0.5, 0.0],
        [line_thickness, axis_length, line_thickness],
        [40, 220, 110],
        None,
    ));
    group.children.push(debug_box_node(
        format!("{prefix}:axis:z"),
        &format!("{} Z axis", frame.name),
        [0.0, 0.0, axis_length * 0.5],
        [line_thickness, line_thickness, axis_length],
        [80, 160, 255],
        None,
    ));
    group.children.push(debug_box_node(
        format!("{prefix}:label"),
        &format!("{} frame label", frame.name),
        [axis_length * 0.6, 0.0, line_thickness],
        [
            axis_length * 0.45,
            line_thickness * 2.0,
            line_thickness * 2.0,
        ],
        [255, 235, 120],
        None,
    ));
    register_debug_node(graph, &group, robot_id);
    graph.root.children.push(group);
}

pub fn coordinate_debug_legend_labels(row_start: usize) -> Vec<RobotDreamsPgeTextLabel> {
    vec![
        RobotDreamsPgeTextLabel::overlay_with_color(
            "coordinate_debug_legend_current",
            "CYAN SPHERE = CURRENT TCP",
            row_start,
            [0.0, 0.86, 1.0, 1.0],
        ),
        RobotDreamsPgeTextLabel::overlay_with_color(
            "coordinate_debug_legend_target",
            "YELLOW CUBE = TARGET TCP",
            row_start + 1,
            [1.0, 0.75, 0.0, 1.0],
        ),
        RobotDreamsPgeTextLabel::overlay_with_color(
            "coordinate_debug_legend_floor",
            "SMALL FLAT MARKS = FLOOR XY",
            row_start + 2,
            [0.86, 0.94, 1.0, 1.0],
        ),
    ]
}

fn append_axis_letter_nodes(
    nodes: &mut Vec<SceneNode>,
    prefix: &str,
    letter: char,
    center: [f32; 3],
    size: f32,
    thickness: f32,
    color_rgb: [u8; 3],
) {
    let half = size * 0.5;
    let strokes: &[([f32; 3], [f32; 3])] = match letter {
        'X' => &[
            ([-half, -half, 0.0], [half, half, 0.0]),
            ([-half, half, 0.0], [half, -half, 0.0]),
        ],
        'Y' => &[
            ([-half, half, 0.0], [0.0, 0.0, 0.0]),
            ([half, half, 0.0], [0.0, 0.0, 0.0]),
            ([0.0, 0.0, 0.0], [0.0, -half, 0.0]),
        ],
        'Z' => &[
            ([-half, half, 0.0], [half, half, 0.0]),
            ([half, half, 0.0], [-half, -half, 0.0]),
            ([-half, -half, 0.0], [half, -half, 0.0]),
        ],
        _ => &[],
    };

    for (index, (start, end)) in strokes.iter().enumerate() {
        nodes.push(debug_line_node(
            format!("{prefix}:axis:label:{letter}:{index}"),
            &format!("{letter} axis label"),
            add_f32_vec3(center, *start),
            add_f32_vec3(center, *end),
            thickness,
            color_rgb,
        ));
    }
}

fn push_debug_node(graph: &mut SceneGraph, robot_id: &str, node: SceneNode) {
    register_debug_node(graph, &node, robot_id);
    graph.root.children.push(node);
}

fn register_debug_node(graph: &mut SceneGraph, node: &SceneNode, robot_id: &str) {
    graph.add_entity(EntityMetadata {
        id: node.entity.clone(),
        name: node.name.clone(),
        kind: "debugOverlay".to_string(),
        robot_id: Some(robot_id.to_string()),
        link_name: None,
    });
    for child in &node.children {
        register_debug_node(graph, child, robot_id);
    }
}

fn debug_box_node(
    entity: String,
    name: &str,
    translation: [f32; 3],
    size: [f32; 3],
    color_rgb: [u8; 3],
    rotation_matrix: Option<[[f32; 3]; 3]>,
) -> SceneNode {
    let mut node = SceneNode::mesh(
        entity,
        name.to_string(),
        Geometry::Box { size },
        Material { color_rgb },
        Transform {
            translation,
            rotation: [0.0, 0.0, 0.0],
            rotation_matrix,
        },
    );
    node.include_in_fit = false;
    node
}

fn debug_sphere_node(
    entity: String,
    name: &str,
    translation: [f32; 3],
    radius: f32,
    color_rgb: [u8; 3],
) -> SceneNode {
    let mut node = SceneNode::mesh(
        entity,
        name.to_string(),
        Geometry::Sphere {
            radius: radius.max(0.001),
        },
        Material { color_rgb },
        Transform::translated(translation),
    );
    node.include_in_fit = false;
    node
}

fn debug_line_node(
    entity: String,
    name: &str,
    start: [f32; 3],
    end: [f32; 3],
    thickness: f32,
    color_rgb: [u8; 3],
) -> SceneNode {
    let delta = sub_f32_vec3(end, start);
    let length = length_f32(delta).max(0.001);
    debug_box_node(
        entity,
        name,
        scale_add_f32_vec3(start, delta, 0.5),
        [length, thickness.max(0.001), thickness.max(0.001)],
        color_rgb,
        Some(line_rotation_matrix(delta)),
    )
}

fn line_rotation_matrix(delta: [f32; 3]) -> [[f32; 3]; 3] {
    let x_axis = normalize_f32_vec3(delta);
    let reference = if x_axis[2].abs() > 0.95 {
        [0.0, 1.0, 0.0]
    } else {
        [0.0, 0.0, 1.0]
    };
    let y_axis = normalize_f32_vec3(cross_f32_vec3(reference, x_axis));
    let z_axis = normalize_f32_vec3(cross_f32_vec3(x_axis, y_axis));
    [x_axis, y_axis, z_axis]
}

fn scene_graph_to_world_state(graph: &SceneGraph) -> pge::WorldState {
    let mut world = pge::WorldState::new();
    let scene = world.scenes.insert(pge::Scene {
        name: Some(graph.root.name.clone()),
        gravity_mps2: [0.0, 0.0, -9.81],
        physics_enabled: true,
    });

    for metadata in graph.entities.values() {
        world.push_entity(pge_entity_metadata(metadata));
    }

    insert_scene_node(&mut world, &graph.root, pge::NodeParent::Scene(scene));
    world
}

fn insert_scene_node(
    world: &mut pge::WorldState,
    node: &SceneNode,
    parent: pge::NodeParent,
) -> pge::ArenaId<pge::Node> {
    if world
        .entity(&pge::EntityId(node.entity.0.clone()))
        .is_none()
    {
        world.push_entity(pge::EntityMetadata {
            id: pge::EntityId(node.entity.0.clone()),
            name: node.name.clone(),
            kind: scene_node_kind_name(&node.kind).to_string(),
            robot_id: None,
            link_name: None,
        });
    }

    let mut pge_node = pge::Node {
        entity: pge::EntityId(node.entity.0.clone()),
        name: Some(node.name.clone()),
        parent,
        transform: pge_transform(node.transform),
        mesh: None,
        camera: None,
        light: None,
        body: None,
        collider: None,
    };

    match &node.kind {
        SceneNodeKind::Group => {}
        SceneNodeKind::Mesh { geometry, material } => {
            let material = world.materials.insert(pge_material(material));
            let mesh = world.meshes.insert(pge::Mesh {
                name: Some(node.name.clone()),
                source: pge_mesh_source(geometry),
                material: Some(material),
            });
            pge_node.mesh = Some(mesh);
            pge_node.collider = pge_collider(geometry);
        }
        SceneNodeKind::Camera(camera) => {
            let camera = world.cameras.insert(pge_camera(camera));
            pge_node.camera = Some(camera);
        }
        SceneNodeKind::Light(light) => {
            let light = world.lights.insert(pge_light(light));
            pge_node.light = Some(light);
        }
    }

    let node_id = world.nodes.insert(pge_node);
    for child in &node.children {
        insert_scene_node(world, child, pge::NodeParent::Node(node_id));
    }
    node_id
}

fn scene_node_kind_name(kind: &SceneNodeKind) -> &'static str {
    match kind {
        SceneNodeKind::Group => "group",
        SceneNodeKind::Mesh { .. } => "mesh",
        SceneNodeKind::Camera(_) => "camera",
        SceneNodeKind::Light(_) => "light",
    }
}

fn pge_entity_metadata(metadata: &EntityMetadata) -> pge::EntityMetadata {
    pge::EntityMetadata {
        id: pge::EntityId(metadata.id.0.clone()),
        name: metadata.name.clone(),
        kind: metadata.kind.clone(),
        robot_id: metadata.robot_id.clone(),
        link_name: metadata.link_name.clone(),
    }
}

fn pge_transform(transform: Transform) -> pge::Transform {
    pge::Transform {
        translation: transform.translation,
        rotation: transform.rotation,
        rotation_matrix: transform.rotation_matrix,
    }
}

fn pge_geometry_bounds(bounds: GeometryBounds) -> pge::GeometryBounds {
    pge::GeometryBounds {
        min: bounds.min,
        max: bounds.max,
    }
}

fn centered_bounds(size: [f32; 3]) -> pge::GeometryBounds {
    pge::GeometryBounds {
        min: [-size[0] * 0.5, -size[1] * 0.5, -size[2] * 0.5],
        max: [size[0] * 0.5, size[1] * 0.5, size[2] * 0.5],
    }
}

fn pge_mesh_source(geometry: &Geometry) -> pge::MeshSource {
    match geometry {
        Geometry::Box { size } => pge::MeshSource::Procedural(pge::Geometry::Box { size: *size }),
        Geometry::Sphere { radius } => {
            pge::MeshSource::Procedural(pge::Geometry::Sphere { radius: *radius })
        }
        Geometry::Cylinder { radius, height } => {
            pge::MeshSource::Procedural(pge::Geometry::Cylinder {
                radius: *radius,
                height: *height,
            })
        }
        Geometry::MeshBounds { size, asset } => pge::MeshSource::Asset {
            path: asset.clone(),
            scale: [1.0, 1.0, 1.0],
            bounds: Some(centered_bounds(*size)),
        },
        Geometry::MeshAsset {
            asset,
            scale,
            bounds,
        } => pge::MeshSource::Asset {
            path: asset.clone(),
            scale: *scale,
            bounds: bounds.map(pge_geometry_bounds),
        },
    }
}

fn pge_collider(geometry: &Geometry) -> Option<pge::Collider> {
    match geometry {
        Geometry::Box { size } => Some(pge::Collider::Box { size: *size }),
        Geometry::Sphere { radius } => Some(pge::Collider::Sphere { radius: *radius }),
        Geometry::Cylinder { radius, height } => Some(pge::Collider::Cylinder {
            radius: *radius,
            height: *height,
        }),
        Geometry::MeshBounds { size, .. } => Some(pge::Collider::MeshBounds { size: *size }),
        Geometry::MeshAsset { bounds, .. } => bounds.map(|bounds| pge::Collider::MeshBounds {
            size: [
                bounds.max[0] - bounds.min[0],
                bounds.max[1] - bounds.min[1],
                bounds.max[2] - bounds.min[2],
            ],
        }),
    }
}

fn pge_material(material: &Material) -> pge::Material {
    pge::Material {
        name: None,
        base_color_factor: [
            f32::from(material.color_rgb[0]) / 255.0,
            f32::from(material.color_rgb[1]) / 255.0,
            f32::from(material.color_rgb[2]) / 255.0,
            1.0,
        ],
        ..pge::Material::default()
    }
}

fn pge_camera(camera: &CameraSpec) -> pge::Camera {
    pge::Camera {
        name: Some(camera.name.clone()),
        fov_deg: camera.fov_deg,
        projection: match camera.projection {
            scene_graph::CameraProjection::Perspective => pge::CameraProjection::Perspective,
            scene_graph::CameraProjection::Orthographic { size_m } => {
                pge::CameraProjection::Orthographic { size_m }
            }
        },
        resolution: camera.resolution,
        intrinsics: camera.intrinsics.map(|intrinsics| pge::CameraIntrinsics {
            fx: intrinsics.fx,
            fy: intrinsics.fy,
            cx: intrinsics.cx,
            cy: intrinsics.cy,
            skew: intrinsics.skew,
        }),
        distortion: camera.distortion.map(|distortion| pge::CameraDistortion {
            k1: distortion.k1,
            k2: distortion.k2,
            p1: distortion.p1,
            p2: distortion.p2,
            k3: distortion.k3,
        }),
        depth_range_m: camera.depth_range_m,
        sensor_effects: camera
            .sensor_effects
            .map(|effects| pge::CameraSensorEffects {
                exposure: effects.exposure,
                gamma: effects.gamma,
                rgb_noise_stddev: effects.rgb_noise_stddev,
                depth_noise_stddev_m: effects.depth_noise_stddev_m,
                depth_quantization_m: effects.depth_quantization_m,
                noise_seed: effects.noise_seed,
            }),
    }
}

fn pge_light(light: &SceneLightSpec) -> pge::Light {
    pge::Light {
        name: Some(light.name.clone()),
        kind: match light.kind {
            scene_graph::LightKind::Directional {
                direction,
                angular_radius_deg,
            } => pge::LightKind::Directional {
                direction,
                angular_radius_deg,
            },
            scene_graph::LightKind::Point { range_m } => pge::LightKind::Point { range_m },
            scene_graph::LightKind::Spot {
                direction,
                inner_cone_deg,
                outer_cone_deg,
                range_m,
            } => pge::LightKind::Spot {
                direction,
                inner_cone_deg,
                outer_cone_deg,
                range_m,
            },
        },
        color_rgb: light.color_rgb,
        intensity: light.intensity,
    }
}

fn scene_object_geometry(
    project: &ProjectConfig,
    object: &project::ProjectSceneObjectConfig,
) -> Geometry {
    match &object.geometry {
        project::ProjectSceneObjectGeometry::Mesh { asset } => mesh_asset_geometry(
            Path::new(&scene_object_asset_path(project, asset)),
            object.scale.unwrap_or([1.0, 1.0, 1.0]),
        ),
        project::ProjectSceneObjectGeometry::Box { size } => Geometry::Box { size: *size },
        project::ProjectSceneObjectGeometry::Sphere { radius } => {
            Geometry::Sphere { radius: *radius }
        }
        project::ProjectSceneObjectGeometry::Cylinder {
            radius_top,
            radius_bottom,
            height,
        } => Geometry::Cylinder {
            radius: radius_top.max(*radius_bottom),
            height: *height,
        },
    }
}

fn mesh_asset_geometry(path: &Path, scale: [f32; 3]) -> Geometry {
    Geometry::MeshAsset {
        asset: path.display().to_string(),
        scale,
        bounds: mesh_asset_bounds(path, scale),
    }
}

fn mesh_asset_bounds(path: &Path, scale: [f32; 3]) -> Option<GeometryBounds> {
    let key = MeshBoundsCacheKey {
        asset: path.display().to_string(),
        scale_bits: scale.map(f32::to_bits),
    };
    if let Ok(cache) = MESH_BOUNDS_CACHE
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        && let Some(bounds) = cache.get(&key)
    {
        return *bounds;
    }

    let bounds = load_mesh_asset_bounds(path, scale);
    if let Ok(mut cache) = MESH_BOUNDS_CACHE
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
    {
        cache.insert(key, bounds);
    }
    bounds
}

fn load_mesh_asset_bounds(path: &Path, scale: [f32; 3]) -> Option<GeometryBounds> {
    let (document, buffers, _) = gltf::import(path).ok()?;
    let scene = document
        .default_scene()
        .or_else(|| document.scenes().next())?;
    let root_transform = mat4_scale(scale);
    let mut builder = GeometryBoundsBuilder::default();
    for node in scene.nodes() {
        collect_gltf_node_bounds(&node, root_transform, &buffers, &mut builder);
    }
    builder.finish()
}

fn collect_gltf_node_bounds(
    node: &gltf::Node<'_>,
    parent_transform: [[f32; 4]; 4],
    buffers: &[gltf::buffer::Data],
    builder: &mut GeometryBoundsBuilder,
) {
    let node_transform = mat4_mul(parent_transform, node.transform().matrix());
    if let Some(mesh) = node.mesh() {
        for primitive in mesh.primitives() {
            let reader = primitive.reader(|buffer| buffers.get(buffer.index()).map(|data| &**data));
            let Some(positions) = reader.read_positions() else {
                continue;
            };
            for position in positions {
                builder.add_point(transform_point4(node_transform, position));
            }
        }
    }
    for child in node.children() {
        collect_gltf_node_bounds(&child, node_transform, buffers, builder);
    }
}

#[derive(Default)]
struct GeometryBoundsBuilder {
    min: Option<[f32; 3]>,
    max: Option<[f32; 3]>,
}

impl GeometryBoundsBuilder {
    fn add_point(&mut self, point: [f32; 3]) {
        match (&mut self.min, &mut self.max) {
            (Some(min), Some(max)) => {
                for axis in 0..3 {
                    min[axis] = min[axis].min(point[axis]);
                    max[axis] = max[axis].max(point[axis]);
                }
            }
            _ => {
                self.min = Some(point);
                self.max = Some(point);
            }
        }
    }

    fn finish(self) -> Option<GeometryBounds> {
        Some(GeometryBounds {
            min: self.min?,
            max: self.max?,
        })
    }
}

fn mat4_scale(scale: [f32; 3]) -> [[f32; 4]; 4] {
    [
        [scale[0], 0.0, 0.0, 0.0],
        [0.0, scale[1], 0.0, 0.0],
        [0.0, 0.0, scale[2], 0.0],
        [0.0, 0.0, 0.0, 1.0],
    ]
}

fn mat4_mul(left: [[f32; 4]; 4], right: [[f32; 4]; 4]) -> [[f32; 4]; 4] {
    let mut result = [[0.0; 4]; 4];
    for col in 0..4 {
        for row in 0..4 {
            result[col][row] = left[0][row] * right[col][0]
                + left[1][row] * right[col][1]
                + left[2][row] * right[col][2]
                + left[3][row] * right[col][3];
        }
    }
    result
}

fn transform_point4(matrix: [[f32; 4]; 4], point: [f32; 3]) -> [f32; 3] {
    [
        matrix[0][0] * point[0] + matrix[1][0] * point[1] + matrix[2][0] * point[2] + matrix[3][0],
        matrix[0][1] * point[0] + matrix[1][1] * point[1] + matrix[2][1] * point[2] + matrix[3][1],
        matrix[0][2] * point[0] + matrix[1][2] * point[1] + matrix[2][2] * point[2] + matrix[3][2],
    ]
}

fn scene_object_asset_path(project: &ProjectConfig, asset: &str) -> String {
    let path = Path::new(asset);
    if path.is_absolute() {
        path.display().to_string()
    } else {
        project.base_dir.join(path).display().to_string()
    }
}

fn scene_reflection_probe(
    project: &ProjectConfig,
    probe: &ReflectionProbeSettings,
) -> ReflectionProbeSettings {
    let mut probe = probe.clone();
    probe.map = scene_object_asset_path(project, &probe.map);
    probe
}

fn scene_render_settings(
    project: &ProjectConfig,
    settings: &SceneRenderSettings,
) -> SceneRenderSettings {
    let mut settings = settings.clone();
    if let Some(environment) = &mut settings.environment
        && let Some(map) = &environment.map
    {
        environment.map = Some(scene_object_asset_path(project, map));
    }
    if let Some(probe) = &mut settings.reflection_probe {
        *probe = scene_reflection_probe(project, probe);
    }
    settings.reflection_probes = settings
        .reflection_probes
        .iter()
        .map(|probe| scene_reflection_probe(project, probe))
        .collect();
    settings
}

fn f64_vec3_to_f32(value: [f64; 3]) -> [f32; 3] {
    [value[0] as f32, value[1] as f32, value[2] as f32]
}

fn f64_matrix_to_f32(value: [[f64; 3]; 3]) -> [[f32; 3]; 3] {
    [
        [value[0][0] as f32, value[0][1] as f32, value[0][2] as f32],
        [value[1][0] as f32, value[1][1] as f32, value[1][2] as f32],
        [value[2][0] as f32, value[2][1] as f32, value[2][2] as f32],
    ]
}

fn add_f32_vec3(left: [f32; 3], right: [f32; 3]) -> [f32; 3] {
    [left[0] + right[0], left[1] + right[1], left[2] + right[2]]
}

fn sub_f32_vec3(left: [f32; 3], right: [f32; 3]) -> [f32; 3] {
    [left[0] - right[0], left[1] - right[1], left[2] - right[2]]
}

fn scale_add_f32_vec3(origin: [f32; 3], delta: [f32; 3], scale: f32) -> [f32; 3] {
    [
        origin[0] + delta[0] * scale,
        origin[1] + delta[1] * scale,
        origin[2] + delta[2] * scale,
    ]
}

fn length_f32(value: [f32; 3]) -> f32 {
    (value[0] * value[0] + value[1] * value[1] + value[2] * value[2]).sqrt()
}

fn distance_f32(left: [f32; 3], right: [f32; 3]) -> f32 {
    length_f32(sub_f32_vec3(left, right))
}

fn normalize_f32_vec3(value: [f32; 3]) -> [f32; 3] {
    let length = length_f32(value);
    if length <= f32::EPSILON {
        [1.0, 0.0, 0.0]
    } else {
        [value[0] / length, value[1] / length, value[2] / length]
    }
}

fn cross_f32_vec3(left: [f32; 3], right: [f32; 3]) -> [f32; 3] {
    [
        left[1] * right[2] - left[2] * right[1],
        left[2] * right[0] - left[0] * right[2],
        left[0] * right[1] - left[1] * right[0],
    ]
}

fn native_camera_rotation_matrix(
    link_rotation: [[f32; 3]; 3],
    camera_rotation: [f32; 3],
) -> [[f32; 3]; 3] {
    // Project cameras follow the Three.js/RobotDreams authoring convention:
    // local -Z is optical forward, +Y is image up, +X is image right.
    // Native renderer rays use local +X forward, +Z up, and +Y left.
    let project_to_native_camera = [[0.0, -1.0, 0.0], [0.0, 0.0, 1.0], [-1.0, 0.0, 0.0]];
    mat3_mul(
        mat3_mul(link_rotation, rpy_matrix_f32(camera_rotation)),
        project_to_native_camera,
    )
}

fn identity_mat3() -> [[f32; 3]; 3] {
    [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]]
}

fn rpy_matrix_f32(rpy: [f32; 3]) -> [[f32; 3]; 3] {
    let basis_x = rotate_f32(rpy, [1.0, 0.0, 0.0]);
    let basis_y = rotate_f32(rpy, [0.0, 1.0, 0.0]);
    let basis_z = rotate_f32(rpy, [0.0, 0.0, 1.0]);
    [
        [basis_x[0], basis_y[0], basis_z[0]],
        [basis_x[1], basis_y[1], basis_z[1]],
        [basis_x[2], basis_y[2], basis_z[2]],
    ]
}

fn rotate_f32(rpy: [f32; 3], vector: [f32; 3]) -> [f32; 3] {
    let [roll, pitch, yaw] = rpy;
    let (sr, cr) = roll.sin_cos();
    let (sp, cp) = pitch.sin_cos();
    let (sy, cy) = yaw.sin_cos();
    let x1 = vector[0];
    let y1 = cr * vector[1] - sr * vector[2];
    let z1 = sr * vector[1] + cr * vector[2];
    let x2 = cp * x1 + sp * z1;
    let y2 = y1;
    let z2 = -sp * x1 + cp * z1;
    [cy * x2 - sy * y2, sy * x2 + cy * y2, z2]
}

fn mat3_mul(left: [[f32; 3]; 3], right: [[f32; 3]; 3]) -> [[f32; 3]; 3] {
    let mut result = [[0.0; 3]; 3];
    for row in 0..3 {
        for col in 0..3 {
            result[row][col] = left[row][0] * right[0][col]
                + left[row][1] * right[1][col]
                + left[row][2] * right[2][col];
        }
    }
    result
}

fn mat3_vec_mul(matrix: [[f32; 3]; 3], vector: [f32; 3]) -> [f32; 3] {
    [
        matrix[0][0] * vector[0] + matrix[0][1] * vector[1] + matrix[0][2] * vector[2],
        matrix[1][0] * vector[0] + matrix[1][1] * vector[1] + matrix[1][2] * vector[2],
        matrix[2][0] * vector[0] + matrix[2][1] * vector[1] + matrix[2][2] * vector[2],
    ]
}

impl Default for RobotDreams {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod robotdreams_tests {
    use std::path::{Path, PathBuf};

    use crate::scene_graph::{
        Geometry, LightKind, SceneNode, SceneNodeKind, ToneMapping, scene_bounds,
    };

    use super::{
        CoordinateDebugOverlayOptions, ModelEntityKind, RobotDreams, RobotDreamsPgeFrameOptions,
        f64_vec3_to_f32, robotdreams_pge_frame,
    };

    fn project_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("core crate has workspace parent")
            .to_path_buf()
    }

    fn puppyarm_project_path() -> PathBuf {
        project_root().join("examples/puppyarm/project.json")
    }

    fn puppybot_project_path() -> PathBuf {
        project_root().join("../PuppyBot/robotdreams/project.json")
    }

    fn puppyarm_urdf_path() -> PathBuf {
        project_root().join("examples/puppyarm/model/final/urdf/final.urdf")
    }

    fn write_temp_project(name: &str, value: serde_json::Value) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("robotdreams-core-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create temp project dir");

        let path = dir.join("project.json");
        std::fs::write(
            &path,
            serde_json::to_string_pretty(&value).expect("serialize temp project"),
        )
        .expect("write temp project");
        path
    }

    fn distance(left: [f64; 3], right: [f64; 3]) -> f64 {
        let dx = left[0] - right[0];
        let dy = left[1] - right[1];
        let dz = left[2] - right[2];
        (dx * dx + dy * dy + dz * dz).sqrt()
    }

    fn find_node_by_entity<'a>(node: &'a SceneNode, entity_id: &str) -> Option<&'a SceneNode> {
        if node.entity.0 == entity_id {
            return Some(node);
        }
        node.children
            .iter()
            .find_map(|child| find_node_by_entity(child, entity_id))
    }

    #[test]
    fn opens_project_and_reads_scene_state() {
        let dreams = RobotDreams::open(puppyarm_project_path()).expect("open PuppyArm project");

        let trashbin = dreams.named("trashbin").expect("trashbin entity");
        assert_eq!(trashbin.kind, ModelEntityKind::SceneObject);
        let trashbin_location = dreams.location_of("trashbin").expect("trashbin location");
        assert!((trashbin_location.position[0] - 0.38).abs() < 1.0e-6);
        assert!((trashbin_location.position[1] - 0.0).abs() < 1.0e-6);
        assert!((trashbin_location.position[2] + 0.28).abs() < 1.0e-6);
    }

    #[test]
    fn reads_robot_state_and_updates_joint_dependent_tcp() {
        let mut dreams = RobotDreams::open(puppyarm_project_path()).expect("open PuppyArm project");

        let first_state = dreams.robot_state("puppyarm").expect("puppyarm state");
        assert_eq!(first_state.id, "puppyarm");
        assert!(first_state.joints.contains_key("wrist"));
        assert!(first_state.joints.contains_key("yaw"));
        assert!(first_state.links.contains_key("part_1_1"));
        assert_eq!(
            first_state.tcp.as_ref().map(|tcp| tcp.link.as_str()),
            Some("part_1_1")
        );
        let first_tcp = first_state
            .tcp
            .and_then(|tcp| tcp.location)
            .expect("tcp location")
            .position;

        dreams
            .set_joint_angle("yaw", 0.5)
            .expect("set semantic yaw joint");
        let moved_state = dreams
            .robot_state("PuppyArm")
            .expect("puppyarm state after joint update");
        assert!((moved_state.joints["yaw"].position_rad - 0.5).abs() < 1.0e-9);
        let moved_tcp = moved_state
            .tcp
            .and_then(|tcp| tcp.location)
            .expect("moved tcp location")
            .position;

        assert!(
            distance(first_tcp, moved_tcp) > 1.0e-6,
            "expected TCP location to change after yaw joint update"
        );
    }

    #[test]
    fn project_camera_zero_rotation_uses_robotdreams_optical_axis() {
        let dreams = RobotDreams::open(puppybot_project_path()).expect("open PuppyBot project");
        let camera = dreams
            .camera_spec("overhead_camera")
            .expect("overhead camera");
        let rotation = camera
            .transform
            .rotation_matrix
            .expect("project camera should use native optical rotation matrix");
        let forward = [rotation[0][0], rotation[1][0], rotation[2][0]];

        assert!(
            forward[2] < -0.99,
            "zero-rotation RobotDreams project camera should look down local -Z in native renderer, got {forward:?}"
        );
        assert!(
            (camera.transform.translation[2] - 0.65).abs() < 1.0e-6,
            "overhead camera should keep authored mount height"
        );
    }

    #[test]
    fn builds_scene_graph_from_project_state() {
        let dreams = RobotDreams::open(puppyarm_project_path()).expect("open PuppyArm project");
        let scene = dreams.scene_graph();

        assert!(
            scene
                .entities
                .contains_key(&super::EntityId("object:trashbin".to_string()))
        );
        assert!(
            scene
                .root
                .children
                .iter()
                .any(|node| node.name == "Overhead Camera")
        );
        assert!(
            scene
                .root
                .children
                .iter()
                .any(|node| node.name == "Trash Bin")
        );
        let floor = scene
            .root
            .children
            .iter()
            .find(|node| node.name == "Floor 5m")
            .expect("floor scene node");
        assert!(
            !floor.include_in_fit,
            "scene graph should preserve includeInFit:false for camera fitting"
        );
        let trashbin = scene
            .root
            .children
            .iter()
            .find(|node| node.name == "Trash Bin")
            .expect("trash bin scene node");
        let SceneNodeKind::Mesh { geometry, .. } = &trashbin.kind else {
            panic!("trash bin should be a renderable mesh node");
        };
        let Geometry::MeshAsset {
            asset,
            scale,
            bounds,
        } = geometry
        else {
            panic!("trash bin should render its mesh asset, not a proxy bound");
        };
        assert!(
            asset.ends_with("trashbin.gltf"),
            "trash bin asset should resolve to trashbin.gltf, got {asset}"
        );
        assert_eq!(*scale, [1.0, 1.0, 1.0]);
        assert!(
            bounds.is_some(),
            "trash bin mesh should carry real asset bounds for native camera fitting"
        );
        assert!(
            scene
                .entities
                .values()
                .filter(|entity| entity.kind == "robotVisual")
                .count()
                > 10,
            "scene graph should include URDF visual mesh nodes, not only a robot proxy"
        );
        let (min, max) = scene_bounds(&scene).expect("fit bounds");
        assert!(
            max[0] - min[0] < 2.0 && max[2] - min[2] < 2.0,
            "fit bounds should exclude the 5 m floor, got min {min:?} max {max:?}"
        );
    }

    #[test]
    fn coordinate_debug_overlay_adds_base_grid_and_tcp_markers() {
        let dreams = RobotDreams::open(puppybot_project_path()).expect("open PuppyBot project");
        let scene =
            dreams.scene_graph_with_coordinate_debug_overlay(CoordinateDebugOverlayOptions {
                enabled: true,
                ..CoordinateDebugOverlayOptions::default()
            });

        let group = find_node_by_entity(&scene.root, "debug:puppybot:frame:base")
            .expect("base-frame debug overlay group");
        assert!(
            !group.include_in_fit,
            "debug overlay should not affect camera fitting"
        );
        assert!(
            group.children.iter().any(|node| node.name == "X forward"),
            "overlay should include ROS X-forward axis"
        );
        assert!(
            group.children.iter().any(|node| node.name == "Y left"),
            "overlay should include ROS Y-left axis"
        );
        assert!(
            group.children.iter().any(|node| node.name == "Z up"),
            "overlay should include ROS Z-up axis"
        );
        assert!(
            find_node_by_entity(&scene.root, "debug:puppybot:frame:base:axis:label:X:0").is_some(),
            "overlay should include an X axis label at the positive X end"
        );
        assert!(
            find_node_by_entity(&scene.root, "debug:puppybot:frame:base:axis:label:Y:0").is_some(),
            "overlay should include a Y axis label at the positive Y end"
        );
        assert!(
            find_node_by_entity(&scene.root, "debug:puppybot:frame:base:axis:label:Z:0").is_some(),
            "overlay should include a Z axis label at the positive Z end"
        );
        assert!(
            find_node_by_entity(&scene.root, "debug:puppybot:tcp:current").is_some(),
            "overlay should include current TCP marker"
        );
        assert!(
            find_node_by_entity(&scene.root, "debug:puppybot:tcp:target").is_some(),
            "overlay should include target TCP marker"
        );
    }

    #[test]
    fn coordinate_debug_overlay_uses_stable_base_and_arm_base_frame_roots() {
        let dreams = RobotDreams::open(puppyarm_project_path()).expect("open PuppyArm project");
        let scene =
            dreams.scene_graph_with_coordinate_debug_overlay(CoordinateDebugOverlayOptions {
                enabled: true,
                ..CoordinateDebugOverlayOptions::default()
            });
        let base = find_node_by_entity(&scene.root, "debug:puppyarm:frame:base")
            .expect("base-frame debug root");
        let arm_base = find_node_by_entity(&scene.root, "debug:puppyarm:frame:armBase")
            .expect("arm-base debug root");
        let frame = dreams
            .frame_state("puppyarm", "armBase")
            .expect("resolved arm base state");

        assert!(base.children.iter().any(|node| node.name == "Base grid X"));
        assert!(
            arm_base
                .children
                .iter()
                .any(|node| node.name == "Arm Base origin")
        );
        assert!(
            arm_base
                .children
                .iter()
                .any(|node| node.name == "Arm Base frame label")
        );
        assert_eq!(
            arm_base.transform.translation,
            f64_vec3_to_f32(frame.world_transform.translation_m)
        );
    }

    #[test]
    fn coordinate_debug_overlay_uses_distinct_marker_shapes_and_legend() {
        let dreams = RobotDreams::open(puppybot_project_path()).expect("open PuppyBot project");
        let scene =
            dreams.scene_graph_with_coordinate_debug_overlay(CoordinateDebugOverlayOptions {
                enabled: true,
                ..CoordinateDebugOverlayOptions::default()
            });

        assert!(
            matches!(
                find_node_by_entity(&scene.root, "debug:puppybot:tcp:current")
                    .expect("current TCP marker")
                    .kind,
                crate::scene_graph::SceneNodeKind::Mesh {
                    geometry: crate::scene_graph::Geometry::Sphere { .. },
                    ..
                }
            ),
            "current TCP marker should be a sphere"
        );
        assert!(
            matches!(
                find_node_by_entity(&scene.root, "debug:puppybot:tcp:target")
                    .expect("target TCP marker")
                    .kind,
                crate::scene_graph::SceneNodeKind::Mesh {
                    geometry: crate::scene_graph::Geometry::Box { .. },
                    ..
                }
            ),
            "target TCP marker should be a cube"
        );

        let frame = robotdreams_pge_frame(&dreams, RobotDreamsPgeFrameOptions::default());
        let legend_text: Vec<_> = frame
            .world
            .text_labels
            .iter()
            .map(|label| label.text.as_str())
            .collect();
        assert!(
            legend_text.contains(&"CYAN SPHERE = CURRENT TCP"),
            "coordinate overlay should explain current TCP marker"
        );
        assert!(
            legend_text.contains(&"YELLOW CUBE = TARGET TCP"),
            "coordinate overlay should explain target TCP marker"
        );
    }

    #[test]
    fn coordinate_debug_overlay_target_marker_uses_servo_target_ticks() {
        let mut dreams = RobotDreams::open(puppybot_project_path()).expect("open PuppyBot project");
        assert!(dreams.set_virtual_servo_target("main_bus", 1, 3100));

        let scene =
            dreams.scene_graph_with_coordinate_debug_overlay(CoordinateDebugOverlayOptions {
                enabled: true,
                ..CoordinateDebugOverlayOptions::default()
            });
        let current = find_node_by_entity(&scene.root, "debug:puppybot:tcp:current")
            .expect("current TCP marker")
            .transform
            .translation;
        let target = find_node_by_entity(&scene.root, "debug:puppybot:tcp:target")
            .expect("target TCP marker")
            .transform
            .translation;

        assert!(
            super::distance_f32(current, target) > 0.001,
            "target marker should move when servo target differs from present tick; current {current:?} target {target:?}"
        );
        assert!(
            find_node_by_entity(&scene.root, "debug:puppybot:tcp:delta").is_some(),
            "overlay should draw current-to-target delta when they differ"
        );
    }

    #[test]
    fn exports_scene_graph_as_pge_world_state() {
        let dreams = RobotDreams::open(puppyarm_project_path()).expect("open PuppyArm project");
        let world = dreams.world_state();

        assert_eq!(world.scenes.len(), 1);
        assert!(
            world
                .entity(&pge_core::EntityId("object:trashbin".to_string()))
                .is_some(),
            "PGE world should preserve RobotDreams entity metadata"
        );
        assert!(
            world
                .nodes
                .iter()
                .any(|(_, node)| node.name.as_deref() == Some("Trash Bin")
                    && node.mesh.is_some()
                    && node.collider.is_some()),
            "PGE world should include the trash bin mesh node with bounds"
        );
        assert!(
            world
                .cameras
                .iter()
                .any(|(_, camera)| camera.name.as_deref() == Some("Overhead Camera")),
            "PGE world should include project cameras"
        );
        assert!(
            world
                .entities
                .iter()
                .filter(|entity| entity.kind == "robotVisual")
                .count()
                > 10,
            "PGE world should include URDF visual entities"
        );
    }

    #[test]
    fn scene_graph_includes_project_render_defaults_lights_and_reflection_probes() {
        let project_path = write_temp_project(
            "scene-probes",
            serde_json::json!({
                "format": "robotdreams.project.v1",
                "name": "Scene Probe Project",
                "robots": [
                    {
                        "id": "puppyarm",
                        "name": "PuppyArm",
                        "model": {
                            "type": "urdf",
                            "path": puppyarm_urdf_path()
                        }
                    }
                ],
                "scene": {
                    "renderSettings": {
                        "backgroundRgb": "#010203",
                        "environment": {
                            "map": "assets/studio.hdr",
                            "intensity": 1.25
                        },
                        "toneMapping": "Reinhard"
                    },
                    "lights": [
                        {
                            "id": "sun",
                            "name": "Sun",
                            "kind": "directional",
                            "direction": [0.0, -1.0, -1.0],
                            "intensity": 2.0
                        }
                    ],
                    "reflectionProbes": [
                        {
                            "map": "assets/studio.hdr",
                            "rotationDeg": 45.0,
                            "position": [0.1, 0.2, 0.3],
                            "influenceRadiusM": 2.0
                        }
                    ]
                }
            }),
        );
        let dreams = RobotDreams::open(&project_path).expect("open temp project");
        let scene = dreams.scene_graph();
        let expected_map = project_path
            .parent()
            .expect("project dir")
            .join("assets/studio.hdr")
            .display()
            .to_string();

        let settings = scene.render_settings.as_ref().expect("render settings");
        assert_eq!(settings.background_rgb, [1, 2, 3]);
        assert_eq!(
            settings
                .environment
                .as_ref()
                .and_then(|environment| environment.map.as_deref()),
            Some(expected_map.as_str())
        );
        assert_eq!(settings.tone_mapping, ToneMapping::Reinhard);

        assert_eq!(scene.reflection_probes.len(), 1);
        assert_eq!(scene.reflection_probes[0].map, expected_map);
        assert_eq!(scene.reflection_probes[0].rotation_deg, 45.0);
        assert_eq!(scene.reflection_probes[0].position, Some([0.1, 0.2, 0.3]));
        assert_eq!(scene.reflection_probes[0].influence_radius_m, Some(2.0));
        assert!(
            scene
                .entities
                .contains_key(&super::EntityId("light:sun".to_string()))
        );
        let sun = scene
            .root
            .children
            .iter()
            .find(|node| node.name == "Sun")
            .expect("sun light node");
        let SceneNodeKind::Light(light) = &sun.kind else {
            panic!("sun should be a light node");
        };
        assert_eq!(light.intensity, 2.0);
        assert!(matches!(light.kind, LightKind::Directional { .. }));

        let _ = std::fs::remove_dir_all(project_path.parent().expect("temp project parent"));
    }

    #[test]
    fn snapshot_includes_project_hardware_and_virtual_servo_state() {
        let dreams = RobotDreams::open(puppyarm_project_path()).expect("open PuppyArm project");
        let snapshot = dreams.snapshot();

        assert_eq!(snapshot.hardware.buses.len(), 1);
        assert_eq!(snapshot.hardware.buses[0].id, "main_bus");
        assert_eq!(snapshot.servo_snapshots.len(), 1);
        assert_eq!(snapshot.servo_snapshots[0].bus_id, "main_bus");
        assert_eq!(snapshot.servo_snapshots[0].snapshots.len(), 4);
        assert_eq!(snapshot.robots.len(), 1);
        assert_eq!(snapshot.robots[0].id, "puppyarm");
    }

    #[test]
    fn virtual_servo_target_updates_robot_state_through_shared_simulation() {
        let mut dreams = RobotDreams::open(puppyarm_project_path()).expect("open PuppyArm project");
        let start_tcp = dreams
            .robot_state("puppyarm")
            .and_then(|robot| robot.tcp)
            .and_then(|tcp| tcp.location)
            .expect("start tcp")
            .position;

        assert!(dreams.set_virtual_servo_target("main_bus", 1, 2593));
        dreams.advance_seconds(3.0);
        let snapshot = dreams.snapshot();
        let yaw = snapshot.robots[0].joints["yaw"].position_rad;
        let moved_tcp = snapshot.robots[0]
            .tcp
            .as_ref()
            .and_then(|tcp| tcp.location.clone())
            .expect("moved tcp")
            .position;

        assert!(
            yaw > 0.5,
            "yaw should move from virtual servo target: {yaw}"
        );
        assert!(
            distance(start_tcp, moved_tcp) > 1.0e-3,
            "TCP should move when virtual servo updates the model"
        );
    }
}

#[derive(Clone, Copy, Debug)]
pub struct VirtualServoSimConfig {
    pub first_servo_id: u8,
    pub last_servo_id: u8,
    pub step_seconds: f32,
    pub idle_sleep_ms: u64,
}

impl Default for VirtualServoSimConfig {
    fn default() -> Self {
        Self {
            first_servo_id: 1,
            last_servo_id: 6,
            step_seconds: 0.005,
            idle_sleep_ms: 2,
        }
    }
}

#[cfg(unix)]
pub struct VirtualServoSimHandle {
    stop: Arc<AtomicBool>,
    join: Option<thread::JoinHandle<()>>,
    slave_path: String,
}

#[cfg(unix)]
impl VirtualServoSimHandle {
    pub fn slave_path(&self) -> &str {
        &self.slave_path
    }

    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

#[cfg(unix)]
impl Drop for VirtualServoSimHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(unix)]
fn validate_virtual_servo_sim_config(config: VirtualServoSimConfig) -> std::io::Result<()> {
    if config.first_servo_id > config.last_servo_id {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "first-servo-id must be less than or equal to last-servo-id",
        ));
    }

    if config.step_seconds <= 0.0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "step-seconds must be greater than 0",
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn create_virtual_servo_bus_sim(
    config: VirtualServoSimConfig,
) -> feetech_servo::servo::sim::FeetechBusSim {
    let mut sim = feetech_servo::servo::sim::FeetechBusSim::new();
    for id in config.first_servo_id..=config.last_servo_id {
        sim.add_servo(id);
    }
    sim
}

#[cfg(unix)]
fn run_virtual_servo_sim_loop(
    mut port: feetech_servo::servo::protocol::virtual_uart::VirtualUartPort,
    config: VirtualServoSimConfig,
    stop: Option<Arc<AtomicBool>>,
) {
    use std::time::Duration;

    use feetech_servo::servo::protocol::port_handler::PortHandler;

    let mut sim = create_virtual_servo_bus_sim(config);
    let mut buffer: Vec<u8> = Vec::new();
    let mut last_step = std::time::Instant::now();

    loop {
        if let Some(stop_flag) = &stop {
            if stop_flag.load(Ordering::Relaxed) {
                break;
            }
        }

        let mut data = port.read_port(512);
        let has_io = !data.is_empty();
        if has_io {
            buffer.append(&mut data);
        }

        let frames = extract_frames(&mut buffer);
        for frame in frames {
            match sim.handle_frame(&frame) {
                Ok(Some(response)) => {
                    let _ = port.write_port(&response);
                }
                Ok(None) => {}
                Err(_) => {}
            }
        }

        let now = std::time::Instant::now();
        let dt = (now - last_step).as_secs_f32();
        if dt >= config.step_seconds {
            sim.step(dt);
            last_step = now;
        }

        if !has_io {
            thread::sleep(Duration::from_millis(config.idle_sleep_ms));
        }
    }
}

#[cfg(unix)]
pub fn spawn_virtual_servo_sim(
    config: VirtualServoSimConfig,
) -> std::io::Result<VirtualServoSimHandle> {
    use feetech_servo::servo::protocol::virtual_uart::VirtualUartPort;

    validate_virtual_servo_sim_config(config)?;

    let port = VirtualUartPort::new()?;
    let slave_path = port.slave_path().to_string();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_in_thread = Arc::clone(&stop);

    let join = thread::spawn(move || {
        run_virtual_servo_sim_loop(port, config, Some(stop_in_thread));
    });

    Ok(VirtualServoSimHandle {
        stop,
        join: Some(join),
        slave_path,
    })
}

#[cfg(unix)]
pub fn run_virtual_servo_sim(config: VirtualServoSimConfig) -> std::io::Result<()> {
    use feetech_servo::servo::protocol::virtual_uart::VirtualUartPort;

    validate_virtual_servo_sim_config(config)?;

    let port = VirtualUartPort::new()?;
    let slave_path = port.slave_path().to_string();

    println!("Virtual servo bus ready.");
    println!("Slave device: {}", slave_path);
    println!(
        "Servo IDs: {}..={}",
        config.first_servo_id, config.last_servo_id
    );
    println!("Press Ctrl-C to stop.");

    run_virtual_servo_sim_loop(port, config, None);
    Ok(())
}

#[cfg(not(unix))]
pub fn run_virtual_servo_sim(_config: VirtualServoSimConfig) -> std::io::Result<()> {
    eprintln!("robot_dreams is only supported on Unix-like systems.");
    Ok(())
}

#[cfg(unix)]
fn extract_frames(buffer: &mut Vec<u8>) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();

    loop {
        if buffer.len() < 2 {
            break;
        }

        let mut start = None;
        for idx in 0..(buffer.len() - 1) {
            if buffer[idx] == 0xFF && buffer[idx + 1] == 0xFF {
                start = Some(idx);
                break;
            }
        }

        let Some(start) = start else {
            buffer.clear();
            break;
        };

        if start > 0 {
            buffer.drain(0..start);
        }

        if buffer.len() < 4 {
            break;
        }

        let length = buffer[3] as usize;
        if length < 2 {
            buffer.drain(0..1);
            continue;
        }

        let total_len = length + 4;
        if total_len < 6 {
            buffer.drain(0..1);
            continue;
        }

        if buffer.len() < total_len {
            break;
        }

        let frame: Vec<u8> = buffer.drain(0..total_len).collect();
        frames.push(frame);
    }

    frames
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::ffi::CString;
    use std::io;
    use std::os::unix::io::RawFd;
    use std::thread;
    use std::time::{Duration, Instant};

    use feetech_servo::servo::protocol::port_handler::PortHandler;
    use feetech_servo::servo::protocol::stservo_def::COMM_SUCCESS;
    use feetech_servo::servo::scscl::{SCSCL_GOAL_POSITION_L, Scscl};

    struct UnixRawTestPort {
        fd: RawFd,
        baudrate: u32,
        tx_time_per_byte_ms: f64,
        packet_start_time: Instant,
        packet_timeout_ms: f64,
        start_time: Instant,
    }

    impl UnixRawTestPort {
        fn open(path: &str, baudrate: u32) -> io::Result<Self> {
            let c_path = CString::new(path.as_bytes())
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid device path"))?;

            unsafe {
                let fd = libc::open(c_path.as_ptr(), libc::O_RDWR | libc::O_NOCTTY);
                if fd < 0 {
                    return Err(io::Error::last_os_error());
                }

                let mut term: libc::termios = std::mem::zeroed();
                if libc::tcgetattr(fd, &mut term) == 0 {
                    libc::cfmakeraw(&mut term);
                    term.c_cc[libc::VMIN] = 0;
                    term.c_cc[libc::VTIME] = 0;
                    let _ = libc::tcsetattr(fd, libc::TCSANOW, &term);
                }

                let mut port = Self {
                    fd,
                    baudrate,
                    tx_time_per_byte_ms: 0.0,
                    packet_start_time: Instant::now(),
                    packet_timeout_ms: 0.0,
                    start_time: Instant::now(),
                };
                port.update_tx_time_per_byte();
                Ok(port)
            }
        }

        fn update_tx_time_per_byte(&mut self) {
            self.tx_time_per_byte_ms = (1000.0 / self.baudrate as f64) * 10.0;
        }

        fn elapsed_ms(&self, since: Instant) -> f64 {
            since.elapsed().as_secs_f64() * 1000.0
        }
    }

    impl Drop for UnixRawTestPort {
        fn drop(&mut self) {
            unsafe {
                libc::close(self.fd);
            }
        }
    }

    impl PortHandler for UnixRawTestPort {
        fn clear_port(&mut self) {
            let available = self.get_bytes_available();
            if available > 0 {
                let _ = self.read_port(available);
            }
        }

        fn read_port(&mut self, length: usize) -> Vec<u8> {
            let mut out = Vec::with_capacity(length);
            if length == 0 {
                return out;
            }

            unsafe {
                out.set_len(length);
                let read_len = libc::read(self.fd, out.as_mut_ptr() as *mut libc::c_void, length);
                if read_len <= 0 {
                    out.clear();
                } else {
                    out.truncate(read_len as usize);
                }
            }
            out
        }

        fn write_port(&mut self, packet: &[u8]) -> usize {
            if packet.is_empty() {
                return 0;
            }

            let start = Instant::now();
            let mut total = 0usize;
            while total < packet.len() {
                let slice = &packet[total..];
                let written = unsafe {
                    libc::write(self.fd, slice.as_ptr() as *const libc::c_void, slice.len())
                };
                if written > 0 {
                    total += written as usize;
                    continue;
                }
                if written == 0 {
                    break;
                }

                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::WouldBlock
                    || err.kind() == io::ErrorKind::Interrupted
                {
                    if start.elapsed() > Duration::from_millis(200) {
                        break;
                    }
                    thread::sleep(Duration::from_micros(200));
                    continue;
                }
                break;
            }
            total
        }

        fn set_packet_timeout(&mut self, packet_length: usize) {
            self.packet_start_time = Instant::now();
            self.packet_timeout_ms = (self.tx_time_per_byte_ms * packet_length as f64)
                + (self.tx_time_per_byte_ms * 3.0)
                + 50.0;
        }

        fn set_packet_timeout_millis(&mut self, msec: u64) {
            self.packet_start_time = Instant::now();
            self.packet_timeout_ms = msec as f64;
        }

        fn is_packet_timeout(&mut self) -> bool {
            if self.packet_timeout_ms <= 0.0 {
                return false;
            }
            if self.get_time_since_start() > self.packet_timeout_ms {
                self.packet_timeout_ms = 0.0;
                return true;
            }
            false
        }

        fn get_current_time(&self) -> f64 {
            self.elapsed_ms(self.start_time)
        }

        fn get_time_since_start(&self) -> f64 {
            self.elapsed_ms(self.packet_start_time)
        }

        fn set_baud_rate(&mut self, baudrate: u32) -> bool {
            self.baudrate = baudrate;
            self.update_tx_time_per_byte();
            true
        }

        fn get_baud_rate(&self) -> u32 {
            self.baudrate
        }

        fn get_bytes_available(&self) -> usize {
            unsafe {
                let mut bytes: libc::c_int = 0;
                if libc::ioctl(self.fd, libc::FIONREAD, &mut bytes) == 0 {
                    bytes as usize
                } else {
                    0
                }
            }
        }
    }

    fn angle_deg_to_ticks(angle_deg: f64) -> u16 {
        (((angle_deg + 180.0) / 360.0) * 4095.0)
            .round()
            .clamp(0.0, 4095.0) as u16
    }

    #[test]
    fn virtual_servo_bus_e2e_receives_angle_targets() {
        let mut sim = spawn_virtual_servo_sim(VirtualServoSimConfig {
            first_servo_id: 1,
            last_servo_id: 4,
            step_seconds: 0.002,
            idle_sleep_ms: 1,
        })
        .expect("spawn virtual servo sim");

        let mut client = {
            let deadline = Instant::now() + Duration::from_secs(1);
            loop {
                match UnixRawTestPort::open(sim.slave_path(), 1_000_000) {
                    Ok(port) => break Scscl::new(port),
                    Err(_) if Instant::now() < deadline => thread::sleep(Duration::from_millis(20)),
                    Err(err) => panic!("open client on {}: {err}", sim.slave_path()),
                }
            }
        };

        for id in 1..=4 {
            let (_model, result, error) = client.handler.ping(id);
            assert_eq!(
                result, COMM_SUCCESS,
                "ping failed for id {id} with error byte {error}"
            );
        }
        let (_model, missing_result, _missing_error) = client.handler.ping(5);
        assert_ne!(
            missing_result, COMM_SUCCESS,
            "servo 5 unexpectedly responded outside configured range"
        );

        let commands = [(1_u8, 80.0), (2_u8, -45.0), (3_u8, 20.0), (4_u8, 5.0)];
        for (id, angle_deg) in commands {
            let target_ticks = angle_deg_to_ticks(angle_deg);
            let (write_result, write_error) = client.write_pos(id, target_ticks, 0, 1500);
            assert_eq!(
                write_result, COMM_SUCCESS,
                "write failed for id {id} target {target_ticks} (error byte {write_error})"
            );

            let (goal_ticks, read_result, read_error) =
                client.handler.read_2byte_tx_rx(id, SCSCL_GOAL_POSITION_L);
            assert_eq!(
                read_result, COMM_SUCCESS,
                "goal read failed for id {id} (error byte {read_error})"
            );
            assert_eq!(
                goal_ticks, target_ticks,
                "goal register mismatch for id {id}"
            );
            let (_present, _speed, state_result, state_error) = client.read_pos_speed(id);
            assert_eq!(
                state_result, COMM_SUCCESS,
                "state read failed for id {id} (error byte {state_error})"
            );
        }

        sim.stop();
    }
}
