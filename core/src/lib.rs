use std::error::Error;
use std::path::Path;
#[cfg(unix)]
use std::sync::Arc;
#[cfg(unix)]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(unix)]
use std::thread;

use crate::physics::PhysicsWorld;
use crate::project::{BusConfig, DeviceConfig, ProjectConfig};
use crate::urdf::{UrdfModel, load_urdf};
use feetech_servo::servo::protocol::serial_bus::ProtocolError;
use feetech_servo::servo::sim::{FeetechBusEvent, FeetechBusSim, FeetechServoSnapshot};

pub mod physics;
pub mod project;
pub mod scene_harness;
pub mod urdf;

pub use project::{
    FrameState, JointState, LinkState, ModelEntityKind, RobotDreamsEntity, RobotDreamsModel,
    RobotState, SceneLocation,
};

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

fn servo_runtime_from_config(config: &crate::project::ServoDeviceConfig) -> HardwareServoRuntime {
    let drives = config.drives.as_ref();
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
                .and_then(|model| model.robot_state(&drive.robot_id))
                .and_then(|state| state.base.rotation)
                .map(|rotation| rotation[2])
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

impl Default for RobotDreams {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod robotdreams_tests {
    use std::path::{Path, PathBuf};

    use super::{ModelEntityKind, RobotDreams};

    fn project_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("core crate has workspace parent")
            .to_path_buf()
    }

    fn puppyarm_project_path() -> PathBuf {
        project_root().join("examples/puppyarm/project.json")
    }

    fn distance(left: [f64; 3], right: [f64; 3]) -> f64 {
        let dx = left[0] - right[0];
        let dy = left[1] - right[1];
        let dz = left[2] - right[2];
        (dx * dx + dy * dy + dz * dz).sqrt()
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
