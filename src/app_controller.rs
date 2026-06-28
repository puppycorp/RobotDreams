use std::cell::Cell;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[cfg(unix)]
use feetech_servo::servo::protocol::port_handler::PortHandler;
#[cfg(unix)]
use feetech_servo::servo::protocol::virtual_uart::VirtualUartPort;
use feetech_servo::servo::sim::{FeetechBusSim, FeetechServoSnapshot};
use wgui::wui::runtime::WuiValue;
use wgui::{WguiModel, wgui_controller};

use crate::{
    BusConfig, DeviceConfig, HardwareConfig, ProjectConfig, ROBOT_SCENE_CONTROLLER_ENTRY,
    ServoDeviceConfig, URDF_BASE_SLIDER_BASE_ID, URDF_JOINT_SLIDER_BASE_ID, UrdfViewerState,
    WORKBENCH_REFRESH_CONTROLLER_ENTRY, apply_urdf_slider_change, reload_urdf_state,
    robot_scene_props_with_static_scene, servo_ticks_to_slider_value, slider_value_to_servo_ticks,
    urdf_joint_slider_range, urdf_joint_type_name, urdf_slider_value_to_units,
    urdf_value_to_joint_units,
};

const SCENE_SECTION_ENVIRONMENT: u32 = 1;
const SCENE_SECTION_ROBOTS: u32 = 2;
const SCENE_SECTION_LINKS: u32 = 1_003;
const SCENE_SECTION_JOINTS: u32 = 1_004;
const SCENE_SECTION_HARDWARE: u32 = 5;
const SCENE_SECTION_OBJECTS: u32 = 6;
const SCENE_SECTION_SENSORS: u32 = 7;
const SCENE_ROW_WAREHOUSE: u32 = 1;
const SCENE_ROW_FLOOR: u32 = 2;
const SCENE_ROW_LIGHTS: u32 = 3;
const SCENE_ROW_ROBOT: u32 = 100;
const SCENE_ROW_OBJECT_WORKTABLE: u32 = 200;
const SCENE_ROW_OBJECT_BIN: u32 = 201;
const SCENE_ROW_OBJECT_FIXTURE: u32 = 202;
const SCENE_ROW_OBJECT_TAG: u32 = 203;
const SCENE_ROW_SENSOR_JOINT_STATES: u32 = 300;
const SCENE_ROW_SENSOR_LIDAR: u32 = 302;
const SCENE_ROW_SENSOR_CAMERA: u32 = 303;
const SCENE_ROW_LINK_BASE: u32 = 10_000;
const SCENE_ROW_JOINT_BASE: u32 = 20_000;
const SCENE_ROW_BUS_BASE: u32 = 30_000;
const SCENE_ROW_DEVICE_BASE: u32 = 40_000;
const SCENE_ROW_DEVICE_BUS_STRIDE: u32 = 1_000;

#[derive(Debug, Clone, WguiModel)]
pub(crate) struct WorkbenchSectionModel {
    id: u32,
    title: String,
    toggle_label: String,
    expanded: bool,
    rows: Vec<WorkbenchRowModel>,
}

#[derive(Debug, Clone, WguiModel)]
pub(crate) struct WorkbenchRowModel {
    id: u32,
    action_arg: u32,
    indent_width: u32,
    icon: String,
    label: String,
    detail: String,
    status: String,
    selected: bool,
    select_static_action: bool,
    select_robot_action: bool,
    select_link_action: bool,
    select_joint_action: bool,
    select_bus_action: bool,
    select_device_action: bool,
    toggle_section_action: bool,
}

#[derive(Debug, Clone, WguiModel)]
pub(crate) struct WorkbenchSliderModel {
    index: u32,
    label: String,
    min: i32,
    max: i32,
    value: i32,
    display_value: String,
    velocity_display: String,
    torque_display: String,
}

#[derive(Debug, Clone, WguiModel)]
pub(crate) struct WorkbenchPropertyModel {
    name: String,
    value: String,
}

#[derive(Debug, Clone, WguiModel)]
pub(crate) struct WorkbenchSensorModel {
    name: String,
    target: String,
    rate: String,
}

#[derive(Debug, Clone, WguiModel)]
pub(crate) struct WorkbenchModel {
    project_name: String,
    simulation_label: String,
    simulation_time: String,
    gpu_label: String,
    memory_label: String,
    robot_scene_name: String,
    robot_scene_entry: String,
    robot_scene_props: WuiValue,
    refresh_timer_name: String,
    refresh_timer_entry: String,
    refresh_timer_props: WuiValue,
    lidar_preview_name: String,
    lidar_preview_props: WuiValue,
    camera_preview_name: String,
    camera_preview_props: WuiValue,
    viewport_title: String,
    file_label: String,
    status: String,
    selected_name: String,
    selected_type: String,
    selected_status: String,
    selected_badge: String,
    selected_accent: String,
    loaded_summary: String,
    has_robot: bool,
    no_movable_joints: bool,
    show_bus_controls: bool,
    bus_control_label: String,
    show_servo_controls: bool,
    show_robot_controls: bool,
    servo_target_controls: Vec<WorkbenchSliderModel>,
    scene_sections: Vec<WorkbenchSectionModel>,
    transform_properties: Vec<WorkbenchPropertyModel>,
    physics_properties: Vec<WorkbenchPropertyModel>,
    base_controls: Vec<WorkbenchSliderModel>,
    joint_controls: Vec<WorkbenchSliderModel>,
    sensors: Vec<WorkbenchSensorModel>,
}

pub(crate) struct AppController {
    urdf_state: UrdfViewerState,
    project_config: Option<ProjectConfig>,
    hardware_runtime: HardwareRuntime,
    virtual_bus: WorkbenchVirtualBus,
    robot_static_scene_dirty: Cell<bool>,
    simulation_running: bool,
    selected_scene_row_id: u32,
    collapsed_scene_section_ids: Vec<u32>,
}

fn serde_json_to_wui_value(value: &serde_json::Value) -> WuiValue {
    match value {
        serde_json::Value::Null => WuiValue::Null,
        serde_json::Value::Bool(value) => WuiValue::Bool(*value),
        serde_json::Value::Number(value) => WuiValue::Number(value.as_f64().unwrap_or(0.0)),
        serde_json::Value::String(value) => WuiValue::String(value.clone()),
        serde_json::Value::Array(values) => {
            WuiValue::List(values.iter().map(serde_json_to_wui_value).collect())
        }
        serde_json::Value::Object(values) => WuiValue::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), serde_json_to_wui_value(value)))
                .collect(),
        ),
    }
}

fn workbench_row(
    id: u32,
    icon: &str,
    label: &str,
    detail: &str,
    status: &str,
    selected: bool,
) -> WorkbenchRowModel {
    WorkbenchRowModel {
        id,
        action_arg: id,
        indent_width: 0,
        icon: icon.to_string(),
        label: label.to_string(),
        detail: detail.to_string(),
        status: status.to_string(),
        selected,
        select_static_action: false,
        select_robot_action: false,
        select_link_action: false,
        select_joint_action: false,
        select_bus_action: false,
        select_device_action: false,
        toggle_section_action: false,
    }
}

fn select_static_row(mut row: WorkbenchRowModel, row_id: u32) -> WorkbenchRowModel {
    row.action_arg = row_id;
    row.select_static_action = true;
    row
}

fn select_robot_row(mut row: WorkbenchRowModel) -> WorkbenchRowModel {
    row.select_robot_action = true;
    row
}

fn select_link_row(mut row: WorkbenchRowModel, index: usize) -> WorkbenchRowModel {
    row.action_arg = index as u32;
    row.select_link_action = true;
    row
}

fn select_joint_row(mut row: WorkbenchRowModel, index: usize) -> WorkbenchRowModel {
    row.action_arg = index as u32;
    row.select_joint_action = true;
    row
}

fn select_bus_row(mut row: WorkbenchRowModel, index: usize) -> WorkbenchRowModel {
    row.action_arg = index as u32;
    row.select_bus_action = true;
    row
}

fn select_device_row(
    mut row: WorkbenchRowModel,
    bus_index: usize,
    device_index: usize,
) -> WorkbenchRowModel {
    row.action_arg = bus_index as u32 * SCENE_ROW_DEVICE_BUS_STRIDE + device_index as u32;
    row.select_device_action = true;
    row
}

fn toggle_section_row(mut row: WorkbenchRowModel, section_id: u32) -> WorkbenchRowModel {
    row.action_arg = section_id;
    row.toggle_section_action = true;
    row
}

fn indented_workbench_row(
    depth: usize,
    id: u32,
    icon: &str,
    label: &str,
    detail: &str,
    status: &str,
    selected: bool,
) -> WorkbenchRowModel {
    let mut row = workbench_row(id, icon, label, detail, status, selected);
    row.indent_width = (depth as u32) * 18;
    row
}

fn scene_section_expanded(collapsed_scene_section_ids: &[u32], section_id: u32) -> bool {
    !collapsed_scene_section_ids.contains(&section_id)
}

fn workbench_section(
    id: u32,
    title: &str,
    rows: Vec<WorkbenchRowModel>,
    collapsed_scene_section_ids: &[u32],
) -> WorkbenchSectionModel {
    let expanded = scene_section_expanded(collapsed_scene_section_ids, id);
    WorkbenchSectionModel {
        id,
        title: title.to_string(),
        toggle_label: if expanded { "v" } else { ">" }.to_string(),
        expanded,
        rows,
    }
}

fn scene_group_row(
    id: u32,
    title: &str,
    detail: &str,
    collapsed_scene_section_ids: &[u32],
) -> WorkbenchRowModel {
    let toggle_label = if scene_section_expanded(collapsed_scene_section_ids, id) {
        "v"
    } else {
        ">"
    };
    toggle_section_row(
        indented_workbench_row(1, id, toggle_label, title, detail, "", false),
        id,
    )
}

fn workbench_property(name: &str, value: impl Into<String>) -> WorkbenchPropertyModel {
    WorkbenchPropertyModel {
        name: name.to_string(),
        value: value.into(),
    }
}

fn workbench_sensor(name: &str, target: &str, rate: &str) -> WorkbenchSensorModel {
    WorkbenchSensorModel {
        name: name.to_string(),
        target: target.to_string(),
        rate: rate.to_string(),
    }
}

fn extract_servo_frames(buffer: &mut Vec<u8>) -> Vec<Vec<u8>> {
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

        frames.push(buffer.drain(0..total_len).collect());
    }

    frames
}

#[cfg(unix)]
struct WorkbenchVirtualBus {
    sim: Arc<Mutex<FeetechBusSim>>,
    stop: Option<Arc<AtomicBool>>,
    join: Option<thread::JoinHandle<()>>,
    path: Option<String>,
    status: String,
}

#[cfg(not(unix))]
struct WorkbenchVirtualBus {
    sim: Arc<Mutex<FeetechBusSim>>,
    status: String,
}

#[cfg(unix)]
impl WorkbenchVirtualBus {
    fn new() -> Self {
        Self {
            sim: Arc::new(Mutex::new(FeetechBusSim::new())),
            stop: None,
            join: None,
            path: None,
            status: "Stopped".to_string(),
        }
    }

    fn is_running(&self) -> bool {
        self.stop.is_some()
    }

    fn path(&self) -> Option<&str> {
        self.path.as_deref()
    }

    fn status(&self) -> &str {
        &self.status
    }

    fn set_servo_count(&self, servo_count: u8) {
        if let Ok(mut sim) = self.sim.lock() {
            sim.set_servo_count(servo_count.max(1));
        }
    }

    fn configure_from_hardware(&self, hardware_runtime: &HardwareRuntime) {
        if let Ok(mut sim) = self.sim.lock() {
            sim.set_servo_count(max_servo_count(hardware_runtime));
            for bus in &hardware_runtime.buses {
                for device in &bus.devices {
                    let HardwareDeviceRuntime::Servo(servo) = device else {
                        continue;
                    };
                    if let Ok(servo_id) = u8::try_from(servo.id) {
                        let _ = sim.set_target_position(servo_id, servo.zero_offset);
                    }
                }
            }
            sim.step(3.0);
        }
    }

    fn set_target_position(&self, id: u8, target: i16) {
        if let Ok(mut sim) = self.sim.lock() {
            let _ = sim.set_target_position(id, target);
            sim.step(0.05);
        }
    }

    fn snapshots(&self) -> Vec<FeetechServoSnapshot> {
        self.sim
            .lock()
            .map(|mut sim| {
                sim.step(0.02);
                sim.servo_snapshots()
            })
            .unwrap_or_default()
    }

    fn start(&mut self, servo_count: u8) -> Result<String, String> {
        self.stop();
        self.set_servo_count(servo_count);

        let mut port = VirtualUartPort::new()
            .map_err(|err| format!("Failed to create virtual bus device: {err}"))?;
        let path = port.slave_path().to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_in_thread = Arc::clone(&stop);
        let sim = Arc::clone(&self.sim);

        let join = thread::spawn(move || {
            let mut buffer = Vec::new();
            let mut last_step = Instant::now();

            while !stop_in_thread.load(Ordering::Relaxed) {
                let now = Instant::now();
                let dt = (now - last_step).as_secs_f32();
                last_step = now;
                if let Ok(mut sim) = sim.lock() {
                    sim.step(dt.max(0.001));
                }

                let mut incoming = port.read_port(512);
                if incoming.is_empty() {
                    thread::sleep(Duration::from_millis(2));
                    continue;
                }

                buffer.append(&mut incoming);
                for frame in extract_servo_frames(&mut buffer) {
                    let response = sim
                        .lock()
                        .ok()
                        .and_then(|mut sim| sim.handle_frame(&frame).ok().flatten());
                    if let Some(response) = response {
                        let _ = port.write_port(&response);
                    }
                }
            }
        });

        self.stop = Some(stop);
        self.join = Some(join);
        self.path = Some(path.clone());
        self.status = "Running".to_string();
        Ok(path)
    }

    fn stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            stop.store(true, Ordering::Relaxed);
        }
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
        self.path = None;
        self.status = "Stopped".to_string();
    }
}

#[cfg(unix)]
impl Drop for WorkbenchVirtualBus {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(not(unix))]
impl WorkbenchVirtualBus {
    fn new() -> Self {
        Self {
            sim: Arc::new(Mutex::new(FeetechBusSim::new())),
            status: "Virtual bus devices are only available on Unix".to_string(),
        }
    }

    fn is_running(&self) -> bool {
        false
    }

    fn path(&self) -> Option<&str> {
        None
    }

    fn status(&self) -> &str {
        &self.status
    }

    fn set_servo_count(&self, servo_count: u8) {
        if let Ok(mut sim) = self.sim.lock() {
            sim.set_servo_count(servo_count.max(1));
        }
    }

    fn configure_from_hardware(&self, hardware_runtime: &HardwareRuntime) {
        if let Ok(mut sim) = self.sim.lock() {
            sim.set_servo_count(max_servo_count(hardware_runtime));
            for bus in &hardware_runtime.buses {
                for device in &bus.devices {
                    let HardwareDeviceRuntime::Servo(servo) = device else {
                        continue;
                    };
                    if let Ok(servo_id) = u8::try_from(servo.id) {
                        let _ = sim.set_target_position(servo_id, servo.zero_offset);
                    }
                }
            }
            sim.step(3.0);
        }
    }

    fn set_target_position(&self, id: u8, target: i16) {
        if let Ok(mut sim) = self.sim.lock() {
            let _ = sim.set_target_position(id, target);
            sim.step(0.05);
        }
    }

    fn snapshots(&self) -> Vec<FeetechServoSnapshot> {
        self.sim
            .lock()
            .map(|mut sim| {
                sim.step(0.02);
                sim.servo_snapshots()
            })
            .unwrap_or_default()
    }

    fn start(&mut self, _servo_count: u8) -> Result<String, String> {
        Err(self.status.clone())
    }

    fn stop(&mut self) {}
}

#[derive(Debug, Clone)]
struct HardwareRuntime {
    buses: Vec<HardwareBusRuntime>,
}

#[derive(Debug, Clone)]
struct HardwareBusRuntime {
    id: String,
    name: String,
    transport_type: String,
    device_path: Option<String>,
    baud: Option<u32>,
    protocol: String,
    devices: Vec<HardwareDeviceRuntime>,
}

#[derive(Debug, Clone)]
enum HardwareDeviceRuntime {
    Servo(HardwareServoRuntime),
    Imu(HardwareImuRuntime),
    IoBoard(HardwareIoBoardRuntime),
}

#[derive(Debug, Clone)]
struct HardwareServoRuntime {
    id: u32,
    name: String,
    profile: String,
    drives_robot: String,
    drives_joint: String,
    zero_offset: i16,
    direction: i8,
    target_position: i16,
    present_position: i16,
    torque_enabled: bool,
    temperature_c: i16,
    voltage_v: f32,
}

#[derive(Debug, Clone)]
struct HardwareImuRuntime {
    id: u32,
    name: String,
    profile: String,
    mounted_robot: Option<String>,
    mounted_link: Option<String>,
}

#[derive(Debug, Clone)]
struct HardwareIoBoardRuntime {
    id: u32,
    name: String,
    profile: String,
    digital_inputs: usize,
    digital_outputs: usize,
    analog_inputs: usize,
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

fn device_runtime_from_config(config: &DeviceConfig) -> HardwareDeviceRuntime {
    match config {
        DeviceConfig::Servo(config) => {
            HardwareDeviceRuntime::Servo(servo_runtime_from_config(config))
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

fn hardware_runtime_from_project(
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

fn file_label(state: &UrdfViewerState) -> String {
    state
        .urdf_path
        .as_ref()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| {
            "(auto-discovery failed: place a .urdf in ./models, ./examples, or ./model)".to_string()
        })
}

fn base_controls(state: &UrdfViewerState) -> Vec<WorkbenchSliderModel> {
    let base_labels = [
        "Base X position",
        "Base Y position",
        "Base Z position",
        "Base roll",
        "Base pitch",
        "Base yaw",
    ];

    base_labels
        .iter()
        .enumerate()
        .map(|(index, label)| {
            let value = if index < 3 {
                state.base_translation_values[index]
            } else {
                state.base_rotation_values[index - 3]
            };
            let (min, max) = if index < 3 {
                (
                    crate::URDF_BASE_POSITION_RANGE,
                    crate::URDF_BASE_POSITION_RANGE,
                )
            } else {
                (crate::URDF_BASE_ROTATION_MIN, crate::URDF_BASE_ROTATION_MAX)
            };

            WorkbenchSliderModel {
                index: index as u32,
                label: (*label).to_string(),
                min: if index < 3 { -min } else { min },
                max,
                value,
                display_value: format!("{:.3}", urdf_slider_value_to_units(value)),
                velocity_display: "0.0".to_string(),
                torque_display: "-".to_string(),
            }
        })
        .collect()
}

fn joint_controls(state: &UrdfViewerState) -> Vec<WorkbenchSliderModel> {
    let Some(robot) = state.robot.as_ref() else {
        return Vec::new();
    };

    robot
        .movable_joint_indices
        .iter()
        .enumerate()
        .filter_map(|(slider_slot, joint_index)| {
            let joint = robot.joints.get(*joint_index)?;
            let value = state.joint_values.get(*joint_index).copied().unwrap_or(0);
            let (min, max) = urdf_joint_slider_range(joint);
            Some(WorkbenchSliderModel {
                index: slider_slot as u32,
                label: joint.name.clone(),
                min,
                max,
                value,
                display_value: format!("{:.3}", urdf_value_to_joint_units(joint, value)),
                velocity_display: format!("{:.1}", (slider_slot as f64 + 1.0) * 1.7),
                torque_display: format!("{:.1}", (slider_slot as f64 + 1.0) * 0.8 + 2.4),
            })
        })
        .collect()
}

fn link_row_id(index: usize) -> u32 {
    SCENE_ROW_LINK_BASE + index as u32
}

fn joint_row_id(index: usize) -> u32 {
    SCENE_ROW_JOINT_BASE + index as u32
}

fn bus_row_id(index: usize) -> u32 {
    SCENE_ROW_BUS_BASE + index as u32
}

fn device_row_id(bus_index: usize, device_index: usize) -> u32 {
    SCENE_ROW_DEVICE_BASE + bus_index as u32 * SCENE_ROW_DEVICE_BUS_STRIDE + device_index as u32
}

fn selected_row(selected_scene_row_id: u32, row_id: u32) -> bool {
    selected_scene_row_id == row_id
}

fn sorted_link_names(state: &UrdfViewerState) -> Vec<String> {
    let Some(robot) = state.robot.as_ref() else {
        return Vec::new();
    };

    let mut names: Vec<String> = robot.links.keys().cloned().collect();
    names.sort();
    names
}

fn link_detail(state: &UrdfViewerState, link_name: &str) -> String {
    let Some(robot) = state.robot.as_ref() else {
        return "link".to_string();
    };

    if robot.roots.iter().any(|root| root == link_name) {
        return "root link".to_string();
    }

    let child_count = robot
        .children_by_parent
        .get(link_name)
        .map(|children| children.len())
        .unwrap_or(0);
    if child_count == 1 {
        "1 child".to_string()
    } else {
        format!("{child_count} children")
    }
}

fn link_rows(state: &UrdfViewerState, selected_scene_row_id: u32) -> Vec<WorkbenchRowModel> {
    sorted_link_names(state)
        .iter()
        .enumerate()
        .map(|(index, link_name)| {
            let row_id = link_row_id(index);
            select_link_row(
                indented_workbench_row(
                    2,
                    row_id,
                    "LN",
                    link_name,
                    &link_detail(state, link_name),
                    "ok",
                    selected_row(selected_scene_row_id, row_id),
                ),
                index,
            )
        })
        .collect()
}

fn joint_rows(state: &UrdfViewerState, selected_scene_row_id: u32) -> Vec<WorkbenchRowModel> {
    let Some(robot) = state.robot.as_ref() else {
        return Vec::new();
    };

    robot
        .joints
        .iter()
        .enumerate()
        .map(|(index, joint)| {
            let row_id = joint_row_id(index);
            select_joint_row(
                indented_workbench_row(
                    2,
                    row_id,
                    "JT",
                    &joint.name,
                    urdf_joint_type_name(joint.joint_type),
                    "ok",
                    selected_row(selected_scene_row_id, row_id),
                ),
                index,
            )
        })
        .collect()
}

fn robot_rows(
    state: &UrdfViewerState,
    selected_scene_row_id: u32,
    collapsed_scene_section_ids: &[u32],
) -> Vec<WorkbenchRowModel> {
    let mut rows = vec![select_robot_row(workbench_row(
        SCENE_ROW_ROBOT,
        "ARM",
        "PuppyArm",
        "URDF",
        "ok",
        selected_row(selected_scene_row_id, SCENE_ROW_ROBOT),
    ))];

    let links = link_rows(state, selected_scene_row_id);
    if !links.is_empty() {
        rows.push(scene_group_row(
            SCENE_SECTION_LINKS,
            "Links",
            &links.len().to_string(),
            collapsed_scene_section_ids,
        ));
        if scene_section_expanded(collapsed_scene_section_ids, SCENE_SECTION_LINKS) {
            rows.extend(links);
        }
    }

    let joints = joint_rows(state, selected_scene_row_id);
    if !joints.is_empty() {
        rows.push(scene_group_row(
            SCENE_SECTION_JOINTS,
            "Joints",
            &joints.len().to_string(),
            collapsed_scene_section_ids,
        ));
        if scene_section_expanded(collapsed_scene_section_ids, SCENE_SECTION_JOINTS) {
            rows.extend(joints);
        }
    }

    rows
}

fn device_row_detail(device: &HardwareDeviceRuntime) -> String {
    match device {
        HardwareDeviceRuntime::Servo(device) => {
            format!("{} -> {}", device.profile, device.drives_joint)
        }
        HardwareDeviceRuntime::Imu(device) => device
            .mounted_link
            .as_ref()
            .map(|link| format!("{} on {}", device.profile, link))
            .unwrap_or_else(|| device.profile.clone()),
        HardwareDeviceRuntime::IoBoard(device) => device.profile.clone(),
    }
}

fn device_row_label(device: &HardwareDeviceRuntime) -> &str {
    match device {
        HardwareDeviceRuntime::Servo(device) => &device.name,
        HardwareDeviceRuntime::Imu(device) => &device.name,
        HardwareDeviceRuntime::IoBoard(device) => &device.name,
    }
}

fn device_row_icon(device: &HardwareDeviceRuntime) -> &'static str {
    match device {
        HardwareDeviceRuntime::Servo(_) => "SRV",
        HardwareDeviceRuntime::Imu(_) => "IMU",
        HardwareDeviceRuntime::IoBoard(_) => "IO",
    }
}

fn hardware_rows(
    hardware_runtime: &HardwareRuntime,
    selected_scene_row_id: u32,
) -> Vec<WorkbenchRowModel> {
    let mut rows = Vec::new();
    for (bus_index, bus) in hardware_runtime.buses.iter().enumerate() {
        let row_id = bus_row_id(bus_index);
        rows.push(select_bus_row(
            workbench_row(
                row_id,
                "BUS",
                &bus.name,
                &format!("{} / {}", bus.transport_type, bus.protocol),
                "ok",
                selected_row(selected_scene_row_id, row_id),
            ),
            bus_index,
        ));

        for (device_index, device) in bus.devices.iter().enumerate() {
            let row_id = device_row_id(bus_index, device_index);
            rows.push(select_device_row(
                indented_workbench_row(
                    1,
                    row_id,
                    device_row_icon(device),
                    device_row_label(device),
                    &device_row_detail(device),
                    "ok",
                    selected_row(selected_scene_row_id, row_id),
                ),
                bus_index,
                device_index,
            ));
        }
    }

    rows
}

fn scene_sections(
    state: &UrdfViewerState,
    hardware_runtime: &HardwareRuntime,
    selected_scene_row_id: u32,
    collapsed_scene_section_ids: &[u32],
) -> Vec<WorkbenchSectionModel> {
    let mut sections = vec![
        workbench_section(
            SCENE_SECTION_ENVIRONMENT,
            "Environment",
            vec![
                select_static_row(
                    workbench_row(
                        SCENE_ROW_WAREHOUSE,
                        "ENV",
                        "Warehouse",
                        "world",
                        "ok",
                        selected_row(selected_scene_row_id, SCENE_ROW_WAREHOUSE),
                    ),
                    SCENE_ROW_WAREHOUSE,
                ),
                select_static_row(
                    workbench_row(
                        SCENE_ROW_FLOOR,
                        "PLN",
                        "Floor",
                        "plane",
                        "ok",
                        selected_row(selected_scene_row_id, SCENE_ROW_FLOOR),
                    ),
                    SCENE_ROW_FLOOR,
                ),
                select_static_row(
                    workbench_row(
                        SCENE_ROW_LIGHTS,
                        "LGT",
                        "Lights",
                        "3 sources",
                        "ok",
                        selected_row(selected_scene_row_id, SCENE_ROW_LIGHTS),
                    ),
                    SCENE_ROW_LIGHTS,
                ),
            ],
            collapsed_scene_section_ids,
        ),
        workbench_section(
            SCENE_SECTION_ROBOTS,
            "Robots",
            robot_rows(state, selected_scene_row_id, collapsed_scene_section_ids),
            collapsed_scene_section_ids,
        ),
    ];

    sections.push(workbench_section(
        SCENE_SECTION_HARDWARE,
        "Hardware",
        hardware_rows(hardware_runtime, selected_scene_row_id),
        collapsed_scene_section_ids,
    ));

    sections.push(workbench_section(
        SCENE_SECTION_OBJECTS,
        "Objects",
        vec![
            select_static_row(
                workbench_row(
                    SCENE_ROW_OBJECT_WORKTABLE,
                    "TBL",
                    "Worktable",
                    "fixture",
                    "ok",
                    selected_row(selected_scene_row_id, SCENE_ROW_OBJECT_WORKTABLE),
                ),
                SCENE_ROW_OBJECT_WORKTABLE,
            ),
            select_static_row(
                workbench_row(
                    SCENE_ROW_OBJECT_BIN,
                    "BIN",
                    "Bin Blue",
                    "container",
                    "ok",
                    selected_row(selected_scene_row_id, SCENE_ROW_OBJECT_BIN),
                ),
                SCENE_ROW_OBJECT_BIN,
            ),
            select_static_row(
                workbench_row(
                    SCENE_ROW_OBJECT_FIXTURE,
                    "FIX",
                    "Fixture Plate",
                    "tooling",
                    "ok",
                    selected_row(selected_scene_row_id, SCENE_ROW_OBJECT_FIXTURE),
                ),
                SCENE_ROW_OBJECT_FIXTURE,
            ),
            select_static_row(
                workbench_row(
                    SCENE_ROW_OBJECT_TAG,
                    "TAG",
                    "Calibration Tag",
                    "marker",
                    "--",
                    selected_row(selected_scene_row_id, SCENE_ROW_OBJECT_TAG),
                ),
                SCENE_ROW_OBJECT_TAG,
            ),
        ],
        collapsed_scene_section_ids,
    ));

    sections.push(workbench_section(
        SCENE_SECTION_SENSORS,
        "Sensors",
        vec![
            select_static_row(
                workbench_row(
                    SCENE_ROW_SENSOR_JOINT_STATES,
                    "JS",
                    "Joint States",
                    "100 Hz",
                    "ok",
                    selected_row(selected_scene_row_id, SCENE_ROW_SENSOR_JOINT_STATES),
                ),
                SCENE_ROW_SENSOR_JOINT_STATES,
            ),
            select_static_row(
                workbench_row(
                    SCENE_ROW_SENSOR_LIDAR,
                    "LDR",
                    "Lidar_1",
                    "10 Hz",
                    "ok",
                    selected_row(selected_scene_row_id, SCENE_ROW_SENSOR_LIDAR),
                ),
                SCENE_ROW_SENSOR_LIDAR,
            ),
            select_static_row(
                workbench_row(
                    SCENE_ROW_SENSOR_CAMERA,
                    "CAM",
                    "Camera_1",
                    "30 Hz",
                    "--",
                    selected_row(selected_scene_row_id, SCENE_ROW_SENSOR_CAMERA),
                ),
                SCENE_ROW_SENSOR_CAMERA,
            ),
        ],
        collapsed_scene_section_ids,
    ));

    sections
}

fn transform_properties() -> Vec<WorkbenchPropertyModel> {
    [
        ("Frame", "World"),
        ("Position X", "0.000 m"),
        ("Position Y", "0.000 m"),
        ("Position Z", "0.000 m"),
        ("Roll", "0.0 deg"),
        ("Pitch", "0.0 deg"),
        ("Yaw", "0.0 deg"),
    ]
    .into_iter()
    .map(|(name, value)| workbench_property(name, value))
    .collect()
}

fn physics_properties() -> Vec<WorkbenchPropertyModel> {
    [
        ("Gravity", "enabled"),
        ("Collisions", "enabled"),
        ("Linear damping", "0.01"),
        ("Angular damping", "0.01"),
        ("Solver iterations", "20"),
    ]
    .into_iter()
    .map(|(name, value)| workbench_property(name, value))
    .collect()
}

struct SelectedSceneInfo {
    name: String,
    selected_type: String,
    status: String,
    badge: String,
    accent: String,
    transform_properties: Vec<WorkbenchPropertyModel>,
    physics_properties: Vec<WorkbenchPropertyModel>,
}

fn format_vec3(value: [f32; 3]) -> String {
    format!("{:.3}, {:.3}, {:.3}", value[0], value[1], value[2])
}

fn static_scene_info(
    name: &str,
    selected_type: &str,
    badge: &str,
    accent: &str,
    transform_properties: Vec<WorkbenchPropertyModel>,
) -> SelectedSceneInfo {
    SelectedSceneInfo {
        name: name.to_string(),
        selected_type: selected_type.to_string(),
        status: "OK".to_string(),
        badge: badge.to_string(),
        accent: accent.to_string(),
        transform_properties,
        physics_properties: physics_properties(),
    }
}

fn robot_scene_info(state: &UrdfViewerState) -> SelectedSceneInfo {
    let status = if state.robot.is_some() {
        "OK".to_string()
    } else {
        "Missing model".to_string()
    };

    SelectedSceneInfo {
        name: "PuppyArm".to_string(),
        selected_type: "URDF robot".to_string(),
        status,
        badge: "ARM".to_string(),
        accent: "#1c6ea4".to_string(),
        transform_properties: transform_properties(),
        physics_properties: physics_properties(),
    }
}

fn mapped_servo_for_joint<'a>(
    hardware_runtime: &'a HardwareRuntime,
    joint_name: &str,
) -> Option<&'a HardwareServoRuntime> {
    hardware_runtime
        .buses
        .iter()
        .flat_map(|bus| bus.devices.iter())
        .find_map(|device| match device {
            HardwareDeviceRuntime::Servo(servo) if servo.drives_joint == joint_name => Some(servo),
            _ => None,
        })
}

fn selected_link_info(
    state: &UrdfViewerState,
    selected_scene_row_id: u32,
) -> Option<SelectedSceneInfo> {
    if !(SCENE_ROW_LINK_BASE..SCENE_ROW_JOINT_BASE).contains(&selected_scene_row_id) {
        return None;
    }

    let robot = state.robot.as_ref()?;
    let link_index = (selected_scene_row_id - SCENE_ROW_LINK_BASE) as usize;
    let link_name = sorted_link_names(state).get(link_index)?.clone();
    let link = robot.links.get(&link_name)?;
    let child_count = robot
        .children_by_parent
        .get(&link_name)
        .map(|children| children.len())
        .unwrap_or(0);

    Some(SelectedSceneInfo {
        name: link_name.clone(),
        selected_type: "URDF link".to_string(),
        status: "OK".to_string(),
        badge: "LN".to_string(),
        accent: "#3e7f65".to_string(),
        transform_properties: vec![
            workbench_property("Frame", "Robot"),
            workbench_property("Visuals", link.visuals.len().to_string()),
            workbench_property("Child joints", child_count.to_string()),
            workbench_property("Detail", link_detail(state, &link_name)),
        ],
        physics_properties: vec![
            workbench_property("Geometry", "URDF visual mesh"),
            workbench_property("Collisions", "not imported yet"),
            workbench_property("Mass", "not imported yet"),
        ],
    })
}

fn selected_joint_info(
    state: &UrdfViewerState,
    hardware_runtime: &HardwareRuntime,
    selected_scene_row_id: u32,
) -> Option<SelectedSceneInfo> {
    if selected_scene_row_id < SCENE_ROW_JOINT_BASE {
        return None;
    }

    let robot = state.robot.as_ref()?;
    let joint_index = (selected_scene_row_id - SCENE_ROW_JOINT_BASE) as usize;
    let joint = robot.joints.get(joint_index)?;
    let current_value = state
        .joint_values
        .get(joint_index)
        .map(|value| format!("{:.4}", urdf_value_to_joint_units(joint, *value)))
        .unwrap_or_else(|| "-".to_string());
    let limit = match (joint.lower, joint.upper) {
        (Some(lower), Some(upper)) => format!("{lower:.3} .. {upper:.3}"),
        _ => "unbounded".to_string(),
    };
    let mapped_device = mapped_servo_for_joint(hardware_runtime, &joint.name)
        .map(|servo| format!("Servo {} ({})", servo.id, servo.name))
        .unwrap_or_else(|| "-".to_string());

    Some(SelectedSceneInfo {
        name: joint.name.clone(),
        selected_type: format!("URDF {} joint", urdf_joint_type_name(joint.joint_type)),
        status: "OK".to_string(),
        badge: "JT".to_string(),
        accent: "#6052a8".to_string(),
        transform_properties: vec![
            workbench_property("Parent", joint.parent.clone()),
            workbench_property("Child", joint.child.clone()),
            workbench_property("Axis", format_vec3(joint.axis)),
            workbench_property("Origin xyz", format_vec3(joint.origin_xyz)),
            workbench_property("Origin rpy", format_vec3(joint.origin_rpy)),
        ],
        physics_properties: vec![
            workbench_property("Current", current_value),
            workbench_property("Limits", limit),
            workbench_property("Mapped device", mapped_device),
            workbench_property("Control", "slider / hardware mapping"),
        ],
    })
}

fn selected_bus_info(
    hardware_runtime: &HardwareRuntime,
    selected_scene_row_id: u32,
) -> Option<SelectedSceneInfo> {
    if !(SCENE_ROW_BUS_BASE..SCENE_ROW_DEVICE_BASE).contains(&selected_scene_row_id) {
        return None;
    }

    let bus_index = (selected_scene_row_id - SCENE_ROW_BUS_BASE) as usize;
    let bus = hardware_runtime.buses.get(bus_index)?;
    Some(SelectedSceneInfo {
        name: bus.name.clone(),
        selected_type: "hardware bus".to_string(),
        status: "OK".to_string(),
        badge: "BUS".to_string(),
        accent: "#245c95".to_string(),
        transform_properties: vec![
            workbench_property("Id", bus.id.clone()),
            workbench_property("Transport", bus.transport_type.clone()),
            workbench_property(
                "Device path",
                bus.device_path.clone().unwrap_or_else(|| "-".to_string()),
            ),
            workbench_property("Protocol", bus.protocol.clone()),
            workbench_property(
                "Baud",
                bus.baud
                    .map(|baud| baud.to_string())
                    .unwrap_or_else(|| "-".to_string()),
            ),
        ],
        physics_properties: vec![
            workbench_property("Devices", bus.devices.len().to_string()),
            workbench_property(
                "Connection",
                if bus.device_path.is_some() {
                    "running"
                } else {
                    "stopped"
                },
            ),
        ],
    })
}

fn selected_servo_info(servo: &HardwareServoRuntime) -> SelectedSceneInfo {
    SelectedSceneInfo {
        name: servo.name.clone(),
        selected_type: "servo device".to_string(),
        status: "OK".to_string(),
        badge: "SRV".to_string(),
        accent: "#245c95".to_string(),
        transform_properties: vec![
            workbench_property("Id", servo.id.to_string()),
            workbench_property("Profile", servo.profile.clone()),
            workbench_property("Mapped robot", servo.drives_robot.clone()),
            workbench_property("Mapped joint", servo.drives_joint.clone()),
            workbench_property("Zero offset", servo.zero_offset.to_string()),
            workbench_property("Direction", servo.direction.to_string()),
        ],
        physics_properties: vec![
            workbench_property("Target position", servo.target_position.to_string()),
            workbench_property("Present position", servo.present_position.to_string()),
            workbench_property("Torque", if servo.torque_enabled { "on" } else { "off" }),
            workbench_property("Temperature", format!("{} C", servo.temperature_c)),
            workbench_property("Voltage", format!("{:.1} V", servo.voltage_v)),
        ],
    }
}

fn selected_imu_info(imu: &HardwareImuRuntime) -> SelectedSceneInfo {
    SelectedSceneInfo {
        name: imu.name.clone(),
        selected_type: "imu device".to_string(),
        status: "OK".to_string(),
        badge: "IMU".to_string(),
        accent: "#245c95".to_string(),
        transform_properties: vec![
            workbench_property("Id", imu.id.to_string()),
            workbench_property("Profile", imu.profile.clone()),
            workbench_property(
                "Mounted robot",
                imu.mounted_robot.clone().unwrap_or_else(|| "-".to_string()),
            ),
            workbench_property(
                "Mounted link",
                imu.mounted_link.clone().unwrap_or_else(|| "-".to_string()),
            ),
        ],
        physics_properties: vec![
            workbench_property("Orientation", "0.000, 0.000, 0.000"),
            workbench_property("Angular velocity", "0.000, 0.000, 0.000"),
            workbench_property("Linear acceleration", "0.000, 0.000, 0.000"),
        ],
    }
}

fn selected_io_board_info(io_board: &HardwareIoBoardRuntime) -> SelectedSceneInfo {
    SelectedSceneInfo {
        name: io_board.name.clone(),
        selected_type: "io board device".to_string(),
        status: "OK".to_string(),
        badge: "IO".to_string(),
        accent: "#245c95".to_string(),
        transform_properties: vec![
            workbench_property("Id", io_board.id.to_string()),
            workbench_property("Profile", io_board.profile.clone()),
        ],
        physics_properties: vec![
            workbench_property("Digital inputs", io_board.digital_inputs.to_string()),
            workbench_property("Digital outputs", io_board.digital_outputs.to_string()),
            workbench_property("Analog inputs", io_board.analog_inputs.to_string()),
        ],
    }
}

fn selected_sensor_info(
    name: &str,
    sensor_type: &str,
    badge: &str,
    properties: Vec<WorkbenchPropertyModel>,
    telemetry: Vec<WorkbenchPropertyModel>,
) -> SelectedSceneInfo {
    SelectedSceneInfo {
        name: name.to_string(),
        selected_type: sensor_type.to_string(),
        status: "OK".to_string(),
        badge: badge.to_string(),
        accent: "#245c95".to_string(),
        transform_properties: properties,
        physics_properties: telemetry,
    }
}

fn selected_device_info(
    hardware_runtime: &HardwareRuntime,
    selected_scene_row_id: u32,
) -> Option<SelectedSceneInfo> {
    if selected_scene_row_id < SCENE_ROW_DEVICE_BASE {
        return None;
    }

    let raw = selected_scene_row_id - SCENE_ROW_DEVICE_BASE;
    let bus_index = (raw / SCENE_ROW_DEVICE_BUS_STRIDE) as usize;
    let device_index = (raw % SCENE_ROW_DEVICE_BUS_STRIDE) as usize;
    let device = hardware_runtime
        .buses
        .get(bus_index)?
        .devices
        .get(device_index)?;
    Some(match device {
        HardwareDeviceRuntime::Servo(servo) => selected_servo_info(servo),
        HardwareDeviceRuntime::Imu(imu) => selected_imu_info(imu),
        HardwareDeviceRuntime::IoBoard(io_board) => selected_io_board_info(io_board),
    })
}

fn selected_static_scene_info(
    state: &UrdfViewerState,
    selected_scene_row_id: u32,
) -> SelectedSceneInfo {
    match selected_scene_row_id {
        SCENE_ROW_WAREHOUSE => static_scene_info(
            "Warehouse",
            "environment",
            "ENV",
            "#4b5f6f",
            vec![
                workbench_property("Frame", "World"),
                workbench_property("Size", "demo layout"),
                workbench_property("Origin", "0, 0, 0"),
            ],
        ),
        SCENE_ROW_FLOOR => static_scene_info(
            "Floor",
            "collision plane",
            "PLN",
            "#4b5f6f",
            vec![
                workbench_property("Frame", "World"),
                workbench_property("Normal", "Z+"),
                workbench_property("Height", "0.000 m"),
            ],
        ),
        SCENE_ROW_LIGHTS => static_scene_info(
            "Lights",
            "scene lighting",
            "LGT",
            "#4b5f6f",
            vec![
                workbench_property("Sources", "3"),
                workbench_property("Mode", "viewport"),
            ],
        ),
        SCENE_ROW_OBJECT_WORKTABLE => static_scene_info(
            "Worktable",
            "fixture",
            "TBL",
            "#5e6c7c",
            vec![workbench_property("Frame", "World")],
        ),
        SCENE_ROW_OBJECT_BIN => static_scene_info(
            "Bin Blue",
            "container",
            "BIN",
            "#5e6c7c",
            vec![workbench_property("Frame", "World")],
        ),
        SCENE_ROW_OBJECT_FIXTURE => static_scene_info(
            "Fixture Plate",
            "tooling",
            "FIX",
            "#5e6c7c",
            vec![workbench_property("Frame", "Worktable")],
        ),
        SCENE_ROW_OBJECT_TAG => static_scene_info(
            "Calibration Tag",
            "marker",
            "TAG",
            "#5e6c7c",
            vec![workbench_property("Frame", "Fixture")],
        ),
        SCENE_ROW_SENSOR_JOINT_STATES => selected_sensor_info(
            "Joint States",
            "joint telemetry",
            "JS",
            vec![
                workbench_property("Source", "backend joint state"),
                workbench_property("Rate", "100 Hz"),
            ],
            vec![
                workbench_property("Position", "authoritative"),
                workbench_property("Velocity", "estimated"),
                workbench_property("Effort", "not connected"),
            ],
        ),
        SCENE_ROW_SENSOR_LIDAR => selected_sensor_info(
            "Lidar_1",
            "lidar sensor",
            "LDR",
            vec![
                workbench_property("Frame", "World"),
                workbench_property("Rate", "10 Hz"),
            ],
            vec![
                workbench_property("Points", "124567"),
                workbench_property("Status", "simulated"),
            ],
        ),
        SCENE_ROW_SENSOR_CAMERA => selected_sensor_info(
            "Camera_1",
            "camera sensor",
            "CAM",
            vec![
                workbench_property("Frame", "World"),
                workbench_property("Rate", "30 Hz"),
            ],
            vec![
                workbench_property("Stream", "preview"),
                workbench_property("Status", "simulated"),
            ],
        ),
        _ => robot_scene_info(state),
    }
}

fn selected_scene_info(
    state: &UrdfViewerState,
    hardware_runtime: &HardwareRuntime,
    selected_scene_row_id: u32,
) -> SelectedSceneInfo {
    if let Some(info) = selected_link_info(state, selected_scene_row_id) {
        return info;
    }

    if let Some(info) = selected_joint_info(state, hardware_runtime, selected_scene_row_id) {
        return info;
    }

    if let Some(info) = selected_bus_info(hardware_runtime, selected_scene_row_id) {
        return info;
    }

    if let Some(info) = selected_device_info(hardware_runtime, selected_scene_row_id) {
        return info;
    }

    selected_static_scene_info(state, selected_scene_row_id)
}

fn show_robot_controls(selected_scene_row_id: u32) -> bool {
    selected_scene_row_id == SCENE_ROW_ROBOT
}

fn selected_bus_index(selected_scene_row_id: u32) -> Option<usize> {
    if !(SCENE_ROW_BUS_BASE..SCENE_ROW_DEVICE_BASE).contains(&selected_scene_row_id) {
        return None;
    }

    Some((selected_scene_row_id - SCENE_ROW_BUS_BASE) as usize)
}

fn first_virtual_bus_index(hardware_runtime: &HardwareRuntime) -> Option<usize> {
    hardware_runtime
        .buses
        .iter()
        .position(|bus| bus.transport_type == "virtual")
}

fn selected_virtual_bus_index(
    hardware_runtime: &HardwareRuntime,
    selected_scene_row_id: u32,
) -> Option<usize> {
    let bus_index = selected_bus_index(selected_scene_row_id)?;
    let bus = hardware_runtime.buses.get(bus_index)?;
    (bus.transport_type == "virtual").then_some(bus_index)
}

fn selected_servo(
    hardware_runtime: &HardwareRuntime,
    selected_scene_row_id: u32,
) -> Option<&HardwareServoRuntime> {
    if selected_scene_row_id < SCENE_ROW_DEVICE_BASE {
        return None;
    }

    let raw = selected_scene_row_id - SCENE_ROW_DEVICE_BASE;
    let bus_index = (raw / SCENE_ROW_DEVICE_BUS_STRIDE) as usize;
    let device_index = (raw % SCENE_ROW_DEVICE_BUS_STRIDE) as usize;
    match hardware_runtime
        .buses
        .get(bus_index)?
        .devices
        .get(device_index)?
    {
        HardwareDeviceRuntime::Servo(servo) => Some(servo),
        _ => None,
    }
}

fn selected_servo_target_controls(
    hardware_runtime: &HardwareRuntime,
    selected_scene_row_id: u32,
) -> Vec<WorkbenchSliderModel> {
    let Some(servo) = selected_servo(hardware_runtime, selected_scene_row_id) else {
        return Vec::new();
    };

    vec![WorkbenchSliderModel {
        index: servo.id,
        label: "Target position".to_string(),
        min: 0,
        max: 4095,
        value: i32::from(servo.target_position.clamp(0, 4095)),
        display_value: servo.target_position.to_string(),
        velocity_display: String::new(),
        torque_display: String::new(),
    }]
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

fn max_servo_count(hardware_runtime: &HardwareRuntime) -> u8 {
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

fn apply_servo_snapshots_to_hardware(
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

fn find_joint_index_by_name(state: &UrdfViewerState, joint_name: &str) -> Option<usize> {
    state
        .robot
        .as_ref()?
        .joints
        .iter()
        .position(|joint| joint.name == joint_name)
}

fn apply_servo_snapshots_to_urdf(
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
            let (min, max) = urdf_joint_slider_range(joint);
            updates.push((
                joint_index,
                servo_ticks_to_slider_value(snapshot.present_position, min, max),
            ));
        }
    }

    for (joint_index, value) in updates {
        if let Some(joint_value) = state.joint_values.get_mut(joint_index) {
            *joint_value = value;
        }
    }
}

fn sensors() -> Vec<WorkbenchSensorModel> {
    vec![
        workbench_sensor("Joint States", "JS_1", "100 Hz"),
        workbench_sensor("Camera", "Camera_1", "30 Hz"),
    ]
}

fn dashboard_preview_props(mode: &str) -> WuiValue {
    let value = match mode {
        "lidar" => serde_json::json!({
            "mode": "lidar",
            "title": "LIDAR_1",
            "points": 124567,
            "rate": "10 Hz"
        }),
        "camera" => serde_json::json!({
            "mode": "camera",
            "title": "CAMERA_1",
            "subtitle": "Camera_1",
            "rate": "30 Hz"
        }),
        _ => serde_json::json!({ "mode": mode }),
    };
    serde_json_to_wui_value(&value)
}

fn project_name(project_config: Option<&ProjectConfig>) -> String {
    project_config
        .map(|config| config.name.clone())
        .unwrap_or_else(|| "PuppyArm".to_string())
}

fn joint_slider_servo_target(
    hardware_runtime: &HardwareRuntime,
    state: &UrdfViewerState,
    slider_slot: usize,
    value: i32,
) -> Option<(u8, i16, String)> {
    let robot = state.robot.as_ref()?;
    let joint_index = *robot.movable_joint_indices.get(slider_slot)?;
    let joint = robot.joints.get(joint_index)?;
    let (min, max) = urdf_joint_slider_range(joint);
    let target = slider_value_to_servo_ticks(value, min, max);
    let servo = mapped_servo_for_joint(hardware_runtime, &joint.name)?;
    Some((u8::try_from(servo.id).ok()?, target, joint.name.clone()))
}

#[wgui_controller(template = "app_controller")]
impl AppController {
    pub(crate) fn new(urdf_path: Option<PathBuf>, project_config: Option<ProjectConfig>) -> Self {
        let mut urdf_state = UrdfViewerState::new(urdf_path);
        reload_urdf_state(&mut urdf_state);
        let hardware_runtime = hardware_runtime_from_project(project_config.as_ref(), &urdf_state);
        let virtual_bus = WorkbenchVirtualBus::new();
        virtual_bus.configure_from_hardware(&hardware_runtime);
        Self {
            urdf_state,
            project_config,
            hardware_runtime,
            virtual_bus,
            robot_static_scene_dirty: Cell::new(true),
            simulation_running: true,
            selected_scene_row_id: SCENE_ROW_ROBOT,
            collapsed_scene_section_ids: vec![SCENE_SECTION_LINKS, SCENE_SECTION_JOINTS],
        }
    }

    pub(crate) fn state(&self) -> WorkbenchModel {
        let snapshots = self.virtual_bus.snapshots();
        let mut live_hardware_runtime = self.hardware_runtime.clone();
        apply_servo_snapshots_to_hardware(&mut live_hardware_runtime, &snapshots);

        let mut live_urdf_state = self.urdf_state.clone();
        apply_servo_snapshots_to_urdf(&mut live_urdf_state, &live_hardware_runtime, &snapshots);

        let base_translation = [
            urdf_slider_value_to_units(live_urdf_state.base_translation_values[0]),
            urdf_slider_value_to_units(live_urdf_state.base_translation_values[1]),
            urdf_slider_value_to_units(live_urdf_state.base_translation_values[2]),
        ];
        let base_rotation = [
            urdf_slider_value_to_units(live_urdf_state.base_rotation_values[0]),
            urdf_slider_value_to_units(live_urdf_state.base_rotation_values[1]),
            urdf_slider_value_to_units(live_urdf_state.base_rotation_values[2]),
        ];

        let loaded_summary = live_urdf_state
            .robot
            .as_ref()
            .map(|robot| {
                format!(
                    "{} links / {} joints",
                    robot.links.len(),
                    robot.joints.len()
                )
            })
            .unwrap_or_else(|| "No robot loaded".to_string());
        let no_movable_joints = live_urdf_state
            .robot
            .as_ref()
            .map(|robot| robot.movable_joint_indices.is_empty())
            .unwrap_or(false);
        let selected_info = selected_scene_info(
            &live_urdf_state,
            &live_hardware_runtime,
            self.selected_scene_row_id,
        );

        WorkbenchModel {
            project_name: project_name(self.project_config.as_ref()),
            simulation_label: if self.simulation_running {
                "Simulation".to_string()
            } else {
                "Paused".to_string()
            },
            simulation_time: "00:12:48".to_string(),
            gpu_label: "GPU 72%".to_string(),
            memory_label: "Memory 4.2 GB".to_string(),
            robot_scene_name: "robot-scene".to_string(),
            robot_scene_entry: ROBOT_SCENE_CONTROLLER_ENTRY.to_string(),
            robot_scene_props: serde_json_to_wui_value(&robot_scene_props_with_static_scene(
                &live_urdf_state,
                base_translation,
                base_rotation,
                self.robot_static_scene_dirty.replace(false),
            )),
            refresh_timer_name: "workbench-refresh".to_string(),
            refresh_timer_entry: WORKBENCH_REFRESH_CONTROLLER_ENTRY.to_string(),
            refresh_timer_props: serde_json_to_wui_value(&serde_json::json!({
                "enabled": self.virtual_bus.is_running(),
                "intervalMs": 250,
            })),
            lidar_preview_name: "lidar-preview".to_string(),
            lidar_preview_props: dashboard_preview_props("lidar"),
            camera_preview_name: "camera-preview".to_string(),
            camera_preview_props: dashboard_preview_props("camera"),
            viewport_title: "Viewport 1".to_string(),
            file_label: file_label(&live_urdf_state),
            status: live_urdf_state.status.clone(),
            selected_name: selected_info.name,
            selected_type: selected_info.selected_type,
            selected_status: selected_info.status,
            selected_badge: selected_info.badge,
            selected_accent: selected_info.accent,
            loaded_summary,
            has_robot: live_urdf_state.robot.is_some(),
            no_movable_joints,
            show_bus_controls: selected_virtual_bus_index(
                &live_hardware_runtime,
                self.selected_scene_row_id,
            )
            .is_some(),
            bus_control_label: if self.virtual_bus.is_running() {
                "Stop virtual bus".to_string()
            } else {
                "Start virtual bus".to_string()
            },
            show_servo_controls: selected_servo(&live_hardware_runtime, self.selected_scene_row_id)
                .is_some(),
            show_robot_controls: show_robot_controls(self.selected_scene_row_id),
            servo_target_controls: selected_servo_target_controls(
                &live_hardware_runtime,
                self.selected_scene_row_id,
            ),
            scene_sections: scene_sections(
                &live_urdf_state,
                &live_hardware_runtime,
                self.selected_scene_row_id,
                &self.collapsed_scene_section_ids,
            ),
            transform_properties: selected_info.transform_properties,
            physics_properties: selected_info.physics_properties,
            base_controls: base_controls(&live_urdf_state),
            joint_controls: joint_controls(&live_urdf_state),
            sensors: sensors(),
        }
    }

    pub(crate) fn title(&self) -> String {
        "RobotDreams Workbench".to_string()
    }

    pub(crate) fn play_simulation(&mut self) {
        self.simulation_running = true;
    }

    pub(crate) fn pause_simulation(&mut self) {
        self.simulation_running = false;
    }

    pub(crate) fn stop_simulation(&mut self) {
        self.simulation_running = false;
    }

    pub(crate) fn reset_simulation(&mut self) {
        self.simulation_running = false;
        self.virtual_bus.stop();
        reload_urdf_state(&mut self.urdf_state);
        self.hardware_runtime =
            hardware_runtime_from_project(self.project_config.as_ref(), &self.urdf_state);
        self.virtual_bus
            .configure_from_hardware(&self.hardware_runtime);
        self.robot_static_scene_dirty.set(true);
    }

    pub(crate) fn reload_urdf(&mut self) {
        self.virtual_bus.stop();
        reload_urdf_state(&mut self.urdf_state);
        self.hardware_runtime =
            hardware_runtime_from_project(self.project_config.as_ref(), &self.urdf_state);
        self.virtual_bus
            .configure_from_hardware(&self.hardware_runtime);
        self.robot_static_scene_dirty.set(true);
    }

    fn refresh_from_virtual_bus(&mut self) {
        let snapshots = self.virtual_bus.snapshots();
        apply_servo_snapshots_to_hardware(&mut self.hardware_runtime, &snapshots);
        apply_servo_snapshots_to_urdf(&mut self.urdf_state, &self.hardware_runtime, &snapshots);
    }

    pub(crate) fn select_static_scene_row(&mut self, arg: u32) {
        self.selected_scene_row_id = arg;
    }

    pub(crate) fn select_robot(&mut self) {
        self.selected_scene_row_id = SCENE_ROW_ROBOT;
    }

    pub(crate) fn select_link(&mut self, arg: u32) {
        self.selected_scene_row_id = link_row_id(arg as usize);
    }

    pub(crate) fn select_joint(&mut self, arg: u32) {
        self.selected_scene_row_id = joint_row_id(arg as usize);
    }

    pub(crate) fn select_bus(&mut self, arg: u32) {
        self.selected_scene_row_id = bus_row_id(arg as usize);
    }

    pub(crate) fn select_device(&mut self, arg: u32) {
        let bus_index = (arg / SCENE_ROW_DEVICE_BUS_STRIDE) as usize;
        let device_index = (arg % SCENE_ROW_DEVICE_BUS_STRIDE) as usize;
        self.selected_scene_row_id = device_row_id(bus_index, device_index);
    }

    pub(crate) fn toggle_scene_section(&mut self, arg: u32) {
        if let Some(index) = self
            .collapsed_scene_section_ids
            .iter()
            .position(|section_id| *section_id == arg)
        {
            self.collapsed_scene_section_ids.remove(index);
        } else {
            self.collapsed_scene_section_ids.push(arg);
        }
    }

    pub(crate) fn toggle_selected_virtual_bus(&mut self) {
        let Some(bus_index) =
            selected_virtual_bus_index(&self.hardware_runtime, self.selected_scene_row_id)
                .or_else(|| first_virtual_bus_index(&self.hardware_runtime))
        else {
            return;
        };

        if self.virtual_bus.is_running() {
            self.virtual_bus.stop();
            if let Some(bus) = self.hardware_runtime.buses.get_mut(bus_index) {
                bus.device_path = None;
            }
            return;
        }

        let Some(bus) = self.hardware_runtime.buses.get(bus_index) else {
            return;
        };
        let servo_count = bus_servo_count(bus);
        if let Ok(path) = self.virtual_bus.start(servo_count)
            && let Some(bus) = self.hardware_runtime.buses.get_mut(bus_index)
        {
            log::info!("virtual servo bus listening at {path}");
            bus.device_path = Some(path);
        }
    }

    pub(crate) fn set_base_control(&mut self, arg: u32, value: i32) {
        let _ =
            apply_urdf_slider_change(&mut self.urdf_state, URDF_BASE_SLIDER_BASE_ID + arg, value);
    }

    pub(crate) fn set_joint_control(&mut self, arg: u32, value: i32) {
        if let Some((servo_id, target, joint_name)) = joint_slider_servo_target(
            &self.hardware_runtime,
            &self.urdf_state,
            arg as usize,
            value,
        ) {
            log::info!(
                "joint command: slot={} joint={} slider={} -> servo_id={} target_ticks={}",
                arg,
                joint_name,
                value,
                servo_id,
                target
            );
            self.virtual_bus.set_target_position(servo_id, target);
            self.refresh_from_virtual_bus();
        }

        let _ =
            apply_urdf_slider_change(&mut self.urdf_state, URDF_JOINT_SLIDER_BASE_ID + arg, value);
    }

    pub(crate) fn set_selected_servo_target(&mut self, arg: u32, value: i32) {
        let servo_id = u8::try_from(arg).ok();
        if let Some(servo_id) = servo_id {
            log::info!(
                "servo command: servo_id={} target_ticks={}",
                servo_id,
                value.clamp(0, 4095)
            );
            self.virtual_bus
                .set_target_position(servo_id, value.clamp(0, 4095) as i16);
            self.refresh_from_virtual_bus();
        }
    }

    pub(crate) fn handle_event(&mut self, event: &wgui::ClientEvent) -> bool {
        let wgui::ClientEvent::OnCustom(custom) = event else {
            return false;
        };
        if custom.name != "refresh" {
            return false;
        }
        self.refresh_from_virtual_bus();
        true
    }
}
