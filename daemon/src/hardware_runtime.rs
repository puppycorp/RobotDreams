use feetech_servo::servo::sim::FeetechServoSnapshot;

use crate::{
    BusConfig, DcMotorDeviceConfig, DeviceConfig, HardwareConfig, ProjectConfig, ServoDeviceConfig,
    UrdfViewerState, joint_display_label, project_joint_name, servo_ticks_to_joint_slider_value,
};

#[derive(Debug, Clone)]
pub(crate) struct HardwareRuntime {
    pub(crate) buses: Vec<HardwareBusRuntime>,
}

#[derive(Debug, Clone)]
pub(crate) struct HardwareBusRuntime {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) transport_type: String,
    pub(crate) device_path: Option<String>,
    pub(crate) baud: Option<u32>,
    pub(crate) protocol: String,
    pub(crate) devices: Vec<HardwareDeviceRuntime>,
}

#[derive(Debug, Clone)]
pub(crate) enum HardwareDeviceRuntime {
    Servo(HardwareServoRuntime),
    DcMotor(HardwareDcMotorRuntime),
    Imu(HardwareImuRuntime),
    IoBoard(HardwareIoBoardRuntime),
}

#[derive(Debug, Clone)]
pub(crate) struct HardwareServoRuntime {
    pub(crate) id: u32,
    pub(crate) name: String,
    pub(crate) profile: String,
    pub(crate) drives_robot: String,
    pub(crate) drives_joint: String,
    pub(crate) zero_offset: i16,
    pub(crate) direction: i8,
    pub(crate) target_position: i16,
    pub(crate) present_position: i16,
    pub(crate) torque_enabled: bool,
    pub(crate) temperature_c: i16,
    pub(crate) voltage_v: f32,
}

#[derive(Debug, Clone)]
pub(crate) struct HardwareDcMotorRuntime {
    pub(crate) id: u32,
    pub(crate) name: String,
    pub(crate) profile: String,
    pub(crate) drives_robot: String,
    pub(crate) drives_wheel: String,
    pub(crate) direction: i8,
    pub(crate) max_speed_mps: f32,
    pub(crate) command_speed: i16,
}

#[derive(Debug, Clone)]
pub(crate) struct HardwareImuRuntime {
    pub(crate) id: u32,
    pub(crate) name: String,
    pub(crate) profile: String,
    pub(crate) mounted_robot: Option<String>,
    pub(crate) mounted_link: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct HardwareIoBoardRuntime {
    pub(crate) id: u32,
    pub(crate) name: String,
    pub(crate) profile: String,
    pub(crate) digital_inputs: usize,
    pub(crate) digital_outputs: usize,
    pub(crate) analog_inputs: usize,
}

fn inferred_servo_runtime(slot: usize, joint_name: &str) -> HardwareDeviceRuntime {
    HardwareDeviceRuntime::Servo(HardwareServoRuntime {
        id: (slot + 1) as u32,
        name: format!("Servo {}", slot + 1),
        profile: "ST3215".to_string(),
        drives_robot: "puppyarm".to_string(),
        drives_joint: joint_name.to_string(),
        zero_offset: 2048,
        direction: 1,
        target_position: 2048,
        present_position: 2048,
        torque_enabled: true,
        temperature_c: 25,
        voltage_v: 7.4,
    })
}

fn servo_runtime_from_config(config: &ServoDeviceConfig) -> HardwareServoRuntime {
    let drives = config.drives.as_ref();
    HardwareServoRuntime {
        id: config.id,
        name: config.name.clone(),
        profile: config.profile.clone(),
        drives_robot: drives
            .map(|mapping| mapping.robot.clone())
            .unwrap_or_else(|| "puppyarm".to_string()),
        drives_joint: drives
            .map(|mapping| mapping.target.clone())
            .unwrap_or_else(|| "-".to_string()),
        zero_offset: config.calibration.zero_offset,
        direction: config.calibration.direction,
        target_position: config.calibration.zero_offset,
        present_position: config.calibration.zero_offset,
        torque_enabled: true,
        temperature_c: 25,
        voltage_v: 7.4,
    }
}

fn dc_motor_runtime_from_config(config: &DcMotorDeviceConfig) -> HardwareDcMotorRuntime {
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

fn inferred_hardware_runtime(state: &UrdfViewerState) -> HardwareRuntime {
    let devices = state
        .robot
        .as_ref()
        .map(|robot| {
            robot
                .movable_joint_indices
                .iter()
                .enumerate()
                .filter_map(|(slot, joint_index)| {
                    robot
                        .joints
                        .get(*joint_index)
                        .map(|joint| inferred_servo_runtime(slot, &joint.name))
                })
                .collect()
        })
        .unwrap_or_default();

    HardwareRuntime {
        buses: vec![HardwareBusRuntime {
            id: "main_bus".to_string(),
            name: "Main Serial Bus".to_string(),
            transport_type: "virtual".to_string(),
            device_path: None,
            baud: Some(1_000_000),
            protocol: "feetech".to_string(),
            devices,
        }],
    }
}

fn hardware_runtime_from_config(hardware: &HardwareConfig) -> HardwareRuntime {
    HardwareRuntime {
        buses: hardware.buses.iter().map(bus_runtime_from_config).collect(),
    }
}

pub(crate) fn hardware_runtime_from_project(
    project_config: Option<&ProjectConfig>,
    state: &UrdfViewerState,
) -> HardwareRuntime {
    if let Some(project_config) = project_config
        && !project_config.hardware.buses.is_empty()
    {
        return hardware_runtime_from_config(&project_config.hardware);
    }

    inferred_hardware_runtime(state)
}

pub(crate) fn hardware_device_id(device: &HardwareDeviceRuntime) -> u32 {
    match device {
        HardwareDeviceRuntime::Servo(device) => device.id,
        HardwareDeviceRuntime::DcMotor(device) => device.id,
        HardwareDeviceRuntime::Imu(device) => device.id,
        HardwareDeviceRuntime::IoBoard(device) => device.id,
    }
}

pub(crate) fn hardware_device_kind(device: &HardwareDeviceRuntime) -> &'static str {
    match device {
        HardwareDeviceRuntime::Servo(_) => "servo",
        HardwareDeviceRuntime::DcMotor(_) => "dc_motor",
        HardwareDeviceRuntime::Imu(_) => "imu",
        HardwareDeviceRuntime::IoBoard(_) => "io_board",
    }
}

fn title_case_ascii(value: &str) -> String {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return String::new();
    };

    format!("{}{}", first.to_ascii_uppercase(), chars.as_str())
}

pub(crate) fn servo_display_name(
    servo: &HardwareServoRuntime,
    project_config: Option<&ProjectConfig>,
) -> String {
    project_joint_name(project_config, &servo.drives_joint)
        .map(|joint_name| format!("{} Servo", title_case_ascii(joint_name)))
        .unwrap_or_else(|| servo.name.clone())
}

fn hardware_device_json(
    device: &HardwareDeviceRuntime,
    project_config: Option<&ProjectConfig>,
) -> serde_json::Value {
    match device {
        HardwareDeviceRuntime::Servo(device) => serde_json::json!({
            "type": "servo",
            "id": device.id,
            "name": servo_display_name(device, project_config),
            "configuredName": device.name,
            "profile": device.profile,
            "drives": {
                "robot": device.drives_robot,
                "joint": device.drives_joint,
                "displayName": joint_display_label(project_config, &device.drives_joint),
                "semanticName": project_joint_name(project_config, &device.drives_joint),
            },
            "calibration": {
                "zeroOffset": device.zero_offset,
                "direction": device.direction,
            },
            "state": {
                "targetPosition": device.target_position,
                "presentPosition": device.present_position,
                "torqueEnabled": device.torque_enabled,
                "temperatureC": device.temperature_c,
                "voltageV": device.voltage_v,
            },
        }),
        HardwareDeviceRuntime::DcMotor(device) => serde_json::json!({
            "type": "dc_motor",
            "id": device.id,
            "name": device.name,
            "profile": device.profile,
            "drives": {
                "robot": device.drives_robot,
                "wheel": device.drives_wheel,
            },
            "calibration": {
                "direction": device.direction,
                "maxSpeedMps": device.max_speed_mps,
            },
            "state": {
                "commandSpeed": device.command_speed,
            },
        }),
        HardwareDeviceRuntime::Imu(device) => serde_json::json!({
            "type": "imu",
            "id": device.id,
            "name": device.name,
            "profile": device.profile,
            "mountedOn": {
                "robot": device.mounted_robot,
                "link": device.mounted_link,
            },
            "state": {
                "orientation": [0.0, 0.0, 0.0],
                "angularVelocity": [0.0, 0.0, 0.0],
                "linearAcceleration": [0.0, 0.0, 0.0],
            },
        }),
        HardwareDeviceRuntime::IoBoard(device) => serde_json::json!({
            "type": "io_board",
            "id": device.id,
            "name": device.name,
            "profile": device.profile,
            "state": {
                "digitalInputs": device.digital_inputs,
                "digitalOutputs": device.digital_outputs,
                "analogInputs": device.analog_inputs,
            },
        }),
    }
}

pub(crate) fn hardware_runtime_json(
    hardware_runtime: &HardwareRuntime,
    project_config: Option<&ProjectConfig>,
) -> serde_json::Value {
    serde_json::json!({
        "buses": hardware_runtime.buses.iter().map(|bus| {
            serde_json::json!({
                "id": bus.id,
                "name": bus.name,
                "transport": {
                    "type": bus.transport_type,
                    "path": bus.device_path,
                    "baud": bus.baud,
                },
                "protocol": bus.protocol,
                "devices": bus.devices.iter().map(|device| {
                    hardware_device_json(device, project_config)
                }).collect::<Vec<_>>(),
            })
        }).collect::<Vec<_>>(),
    })
}

pub(crate) fn bus_servo_count(bus: &HardwareBusRuntime) -> u8 {
    bus.devices
        .iter()
        .filter_map(|device| match device {
            HardwareDeviceRuntime::Servo(servo) => u8::try_from(servo.id).ok(),
            _ => None,
        })
        .max()
        .unwrap_or(1)
}

pub(crate) fn max_servo_count(hardware_runtime: &HardwareRuntime) -> u8 {
    hardware_runtime
        .buses
        .iter()
        .map(bus_servo_count)
        .max()
        .unwrap_or(1)
}

fn servo_snapshot<'a>(
    snapshots: &'a [FeetechServoSnapshot],
    servo: &HardwareServoRuntime,
) -> Option<&'a FeetechServoSnapshot> {
    let servo_id = u8::try_from(servo.id).ok()?;
    snapshots.iter().find(|snapshot| snapshot.id == servo_id)
}

pub(crate) fn apply_servo_snapshots_to_hardware(
    hardware_runtime: &mut HardwareRuntime,
    snapshots: &[FeetechServoSnapshot],
) {
    for bus in &mut hardware_runtime.buses {
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
}

pub(crate) fn apply_servo_snapshots_to_urdf(
    state: &mut UrdfViewerState,
    hardware_runtime: &HardwareRuntime,
    snapshots: &[FeetechServoSnapshot],
) {
    let Some(robot) = state.robot.as_ref() else {
        return;
    };
    let mut updates = Vec::new();
    for bus in &hardware_runtime.buses {
        for device in &bus.devices {
            let HardwareDeviceRuntime::Servo(servo) = device else {
                continue;
            };
            let Some(snapshot) = servo_snapshot(snapshots, servo) else {
                continue;
            };
            let Some(joint_index) = robot
                .joints
                .iter()
                .position(|joint| joint.name == servo.drives_joint)
            else {
                continue;
            };
            let joint = &robot.joints[joint_index];
            updates.push((
                joint_index,
                servo_ticks_to_joint_slider_value(
                    snapshot.present_position,
                    joint,
                    servo.zero_offset,
                    servo.direction,
                ),
            ));
        }
    }

    for (joint_index, value) in updates {
        if let Some(joint_value) = state.joint_values.get_mut(joint_index) {
            *joint_value = value;
        }
    }
}
