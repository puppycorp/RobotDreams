use std::cell::{Cell, RefCell};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use wgui::wui::runtime::WuiValue;
use wgui::{WguiModel, wgui_controller};

use crate::hardware_runtime::{
    HardwareDeviceRuntime, HardwareImuRuntime, HardwareIoBoardRuntime, HardwareRuntime,
    HardwareServoRuntime, apply_servo_snapshots_to_hardware, apply_servo_snapshots_to_urdf,
    bus_servo_count, hardware_device_id, hardware_device_kind, hardware_runtime_from_project,
    hardware_runtime_json, servo_display_name,
};
use crate::robot_scene_component::servo_snapshots_json;
use crate::scene_transform::{
    add_vec3, project_robot_base_rotation, project_robot_base_translation,
};
use crate::system_metrics::{
    CpuSample, GpuMetricCache, read_cpu_sample, system_cpu_label, system_gpu_label,
    system_memory_label,
};
use crate::virtual_bus::{TimedFeetechBusEvent, WorkbenchVirtualBusHandle};
use crate::{
    ProjectCameraConfig, ProjectConfig, ProjectSceneObjectConfig, ProjectSceneObjectGeometry,
    URDF_BASE_SLIDER_BASE_ID, URDF_JOINT_SLIDER_BASE_ID, UrdfViewerState, apply_urdf_slider_change,
    joint_display_label, project_config_from_manifest, project_joint_name, reload_urdf_state,
    robot_scene_props_with_static_scene, slider_value_to_servo_ticks, urdf_joint_slider_range,
    urdf_joint_type_name, urdf_slider_value_to_units, urdf_value_to_joint_units,
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
const SCENE_ROW_PROJECT_OBJECT_BASE: u32 = 60_000;
const SCENE_ROW_PROJECT_CAMERA_BASE: u32 = 70_000;
const SENSOR_PREVIEW_COMPONENT_BASE: u32 = 80_000;
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
    action_arg: u32,
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
pub(crate) struct WorkbenchSensorPreviewModel {
    id: String,
    component_id: u32,
    name: String,
    detail: String,
    props: WuiValue,
}

#[derive(Debug, Clone, WguiModel)]
pub(crate) struct WorkbenchModel {
    project_name: String,
    gpu_label: String,
    cpu_label: String,
    memory_label: String,
    project_save_label: String,
    project_dirty: bool,
    robot_scene_props: WuiValue,
    lidar_preview_name: String,
    lidar_preview_props: WuiValue,
    camera_preview_name: String,
    camera_preview_props: WuiValue,
    has_sensor_previews: bool,
    sensor_previews: Vec<WorkbenchSensorPreviewModel>,
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
    show_camera_controls: bool,
    show_robot_controls: bool,
    servo_target_controls: Vec<WorkbenchSliderModel>,
    camera_transform_controls: Vec<WorkbenchSliderModel>,
    scene_sections: Vec<WorkbenchSectionModel>,
    transform_properties: Vec<WorkbenchPropertyModel>,
    physics_properties: Vec<WorkbenchPropertyModel>,
    base_controls: Vec<WorkbenchSliderModel>,
    joint_controls: Vec<WorkbenchSliderModel>,
    sensors: Vec<WorkbenchSensorModel>,
}

#[derive(Debug, Clone, WguiModel)]
pub(crate) struct ViewportModel {
    robot_scene_props: WuiValue,
}

pub(crate) struct AppController {
    urdf_state: UrdfViewerState,
    project_config: RefCell<Option<ProjectConfig>>,
    project_config_modified: Cell<Option<SystemTime>>,
    project_config_dirty: Cell<bool>,
    hardware_runtime: HardwareRuntime,
    virtual_bus: WorkbenchVirtualBusHandle,
    simulation_runtime: SimulationRuntimeHandle,
    robot_static_scene_dirty: Cell<bool>,
    selected_scene_row_id: u32,
    collapsed_scene_section_ids: Vec<u32>,
    cpu_sample: Cell<Option<CpuSample>>,
    gpu_metric: RefCell<Option<GpuMetricCache>>,
}

pub(crate) struct ViewportController {
    urdf_state: UrdfViewerState,
    project_config: RefCell<Option<ProjectConfig>>,
    project_config_modified: Cell<Option<SystemTime>>,
    hardware_runtime: HardwareRuntime,
    virtual_bus: WorkbenchVirtualBusHandle,
    robot_static_scene_dirty: Cell<bool>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HeadlessSummary {
    pub(crate) project_name: String,
    pub(crate) status: String,
    pub(crate) loaded_summary: String,
    pub(crate) virtual_bus_running: bool,
    pub(crate) virtual_bus_path: Option<String>,
    pub(crate) servo_count: usize,
    pub(crate) snapshots: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum SimulationStatus {
    Stopped,
    Running,
    Paused,
}

impl SimulationStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            SimulationStatus::Stopped => "stopped",
            SimulationStatus::Running => "running",
            SimulationStatus::Paused => "paused",
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub(crate) enum SimulationEvent {
    Lifecycle {
        status: SimulationStatus,
        clock_sec: f64,
    },
    ServoCommand {
        servo_id: u32,
        target_position: i32,
        clock_sec: f64,
    },
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SimulationSample {
    clock_sec: f64,
    servo_snapshots: serde_json::Value,
}

pub(crate) trait RecordingSink: Send {
    fn event(&mut self, event: SimulationEvent);
    fn sample(&mut self, sample: SimulationSample);
}

#[derive(Default)]
struct NoopRecordingSink;

impl RecordingSink for NoopRecordingSink {
    fn event(&mut self, _event: SimulationEvent) {}

    fn sample(&mut self, _sample: SimulationSample) {}
}

struct SimulationRuntime {
    status: SimulationStatus,
    clock_sec: f64,
    recording_sink: Box<dyn RecordingSink>,
}

#[derive(Clone)]
pub(crate) struct SimulationRuntimeHandle {
    inner: Arc<Mutex<SimulationRuntime>>,
}

impl SimulationRuntimeHandle {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(SimulationRuntime {
                status: SimulationStatus::Stopped,
                clock_sec: 0.0,
                recording_sink: Box::<NoopRecordingSink>::default(),
            })),
        }
    }

    pub(crate) fn status(&self) -> SimulationStatus {
        self.inner
            .lock()
            .map(|runtime| runtime.status)
            .unwrap_or(SimulationStatus::Stopped)
    }

    pub(crate) fn clock_sec(&self) -> f64 {
        self.inner
            .lock()
            .map(|runtime| runtime.clock_sec)
            .unwrap_or(0.0)
    }

    pub(crate) fn set_status(&self, status: SimulationStatus) {
        if let Ok(mut runtime) = self.inner.lock() {
            runtime.status = status;
            let clock_sec = runtime.clock_sec;
            runtime
                .recording_sink
                .event(SimulationEvent::Lifecycle { status, clock_sec });
        }
    }

    pub(crate) fn reset_clock(&self) {
        if let Ok(mut runtime) = self.inner.lock() {
            runtime.clock_sec = 0.0;
        }
    }

    fn advance(&self, dt_sec: f64, servo_snapshots: serde_json::Value) {
        if let Ok(mut runtime) = self.inner.lock()
            && runtime.status == SimulationStatus::Running
        {
            runtime.clock_sec += dt_sec.max(0.0);
            let clock_sec = runtime.clock_sec;
            runtime.recording_sink.sample(SimulationSample {
                clock_sec,
                servo_snapshots,
            });
        }
    }

    fn record_servo_command(&self, servo_id: u32, target_position: i32) {
        if let Ok(mut runtime) = self.inner.lock() {
            let clock_sec = runtime.clock_sec;
            runtime.recording_sink.event(SimulationEvent::ServoCommand {
                servo_id,
                target_position,
                clock_sec,
            });
        }
    }
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
    row.action_arg = link_row_id(index);
    row.select_link_action = true;
    row
}

fn select_joint_row(mut row: WorkbenchRowModel, index: usize) -> WorkbenchRowModel {
    row.action_arg = joint_row_id(index);
    row.select_joint_action = true;
    row
}

fn select_bus_row(mut row: WorkbenchRowModel, index: usize) -> WorkbenchRowModel {
    row.action_arg = bus_row_id(index);
    row.select_bus_action = true;
    row
}

fn select_device_row(
    mut row: WorkbenchRowModel,
    bus_index: usize,
    device_index: usize,
) -> WorkbenchRowModel {
    row.action_arg = device_row_id(bus_index, device_index);
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

fn bus_write_event_json(
    write: &feetech_servo::servo::sim::FeetechBusWriteEvent,
) -> serde_json::Value {
    serde_json::json!({
        "id": write.id,
        "data": write.data,
        "targetPosition": write.target_position,
    })
}

fn bus_events_json(events: &[TimedFeetechBusEvent]) -> serde_json::Value {
    serde_json::Value::Array(
        events
            .iter()
            .map(|timed| {
                let event = &timed.event;
                serde_json::json!({
                    "sequence": timed.sequence,
                    "unixMs": timed.unix_ms,
                    "instruction": event.instruction,
                    "id": event.id,
                    "broadcast": event.broadcast,
                    "address": event.address,
                    "length": event.length,
                    "ids": event.ids,
                    "writes": event.writes.iter().map(bus_write_event_json).collect::<Vec<_>>(),
                    "data": event.data,
                    "targetPosition": event.target_position,
                    "raw": event.raw,
                    "error": event.error,
                })
            })
            .collect(),
    )
}

fn slider_json(slider: WorkbenchSliderModel) -> serde_json::Value {
    serde_json::json!({
        "index": slider.index,
        "actionArg": slider.action_arg,
        "label": slider.label,
        "min": slider.min,
        "max": slider.max,
        "value": slider.value,
        "displayValue": slider.display_value,
        "velocityDisplay": slider.velocity_display,
        "torqueDisplay": slider.torque_display,
    })
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
                action_arg: index as u32,
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

fn selected_project_camera_index(selected_scene_row_id: u32) -> Option<usize> {
    if selected_scene_row_id < SCENE_ROW_PROJECT_CAMERA_BASE {
        return None;
    }
    Some((selected_scene_row_id - SCENE_ROW_PROJECT_CAMERA_BASE) as usize)
}

fn selected_camera_transform_controls(
    project_config: Option<&ProjectConfig>,
    selected_scene_row_id: u32,
) -> Vec<WorkbenchSliderModel> {
    let Some(index) = selected_project_camera_index(selected_scene_row_id) else {
        return Vec::new();
    };
    let Some(camera) = project_config.and_then(|project| project.scene.cameras.get(index)) else {
        return Vec::new();
    };

    let labels = ["X", "Y", "Z", "Roll", "Pitch", "Yaw"];
    labels
        .iter()
        .enumerate()
        .map(|(axis, label)| {
            let value = if axis < 3 {
                (camera.position[axis] * crate::URDF_VALUE_SCALE).round() as i32
            } else {
                (camera.rotation[axis - 3] * crate::URDF_VALUE_SCALE).round() as i32
            };
            let (min, max) = if axis < 3 {
                (
                    -crate::URDF_BASE_POSITION_RANGE,
                    crate::URDF_BASE_POSITION_RANGE,
                )
            } else {
                (crate::URDF_BASE_ROTATION_MIN, crate::URDF_BASE_ROTATION_MAX)
            };
            WorkbenchSliderModel {
                index: axis as u32,
                action_arg: (index as u32) * 6 + axis as u32,
                label: (*label).to_string(),
                min,
                max,
                value,
                display_value: if axis < 3 {
                    format!("{:.3}", value as f32 / crate::URDF_VALUE_SCALE)
                } else {
                    format!(
                        "{:.1}",
                        (value as f32 / crate::URDF_VALUE_SCALE).to_degrees()
                    )
                },
                velocity_display: String::new(),
                torque_display: String::new(),
            }
        })
        .collect()
}

fn joint_controls(
    state: &UrdfViewerState,
    project_config: Option<&ProjectConfig>,
) -> Vec<WorkbenchSliderModel> {
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
                action_arg: *joint_index as u32,
                label: joint_display_label(project_config, &joint.name),
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

fn project_object_row_id(index: usize) -> u32 {
    SCENE_ROW_PROJECT_OBJECT_BASE + index as u32
}

fn project_camera_row_id(index: usize) -> u32 {
    SCENE_ROW_PROJECT_CAMERA_BASE + index as u32
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

fn joint_rows(
    state: &UrdfViewerState,
    project_config: Option<&ProjectConfig>,
    selected_scene_row_id: u32,
) -> Vec<WorkbenchRowModel> {
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
                    &joint_display_label(project_config, &joint.name),
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
    project_config: Option<&ProjectConfig>,
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

    let joints = joint_rows(state, project_config, selected_scene_row_id);
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

fn device_row_detail(
    device: &HardwareDeviceRuntime,
    project_config: Option<&ProjectConfig>,
) -> String {
    match device {
        HardwareDeviceRuntime::Servo(device) => {
            format!(
                "{} -> {}",
                device.profile,
                joint_display_label(project_config, &device.drives_joint)
            )
        }
        HardwareDeviceRuntime::Imu(device) => device
            .mounted_link
            .as_ref()
            .map(|link| format!("{} on {}", device.profile, link))
            .unwrap_or_else(|| device.profile.clone()),
        HardwareDeviceRuntime::IoBoard(device) => device.profile.clone(),
    }
}

fn device_row_label(
    device: &HardwareDeviceRuntime,
    project_config: Option<&ProjectConfig>,
) -> String {
    match device {
        HardwareDeviceRuntime::Servo(device) => servo_display_name(device, project_config),
        HardwareDeviceRuntime::Imu(device) => device.name.clone(),
        HardwareDeviceRuntime::IoBoard(device) => device.name.clone(),
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
    project_config: Option<&ProjectConfig>,
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
                    &device_row_label(device, project_config),
                    &device_row_detail(device, project_config),
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

fn project_scene_object_rows(
    project_config: Option<&ProjectConfig>,
    selected_scene_row_id: u32,
) -> Vec<WorkbenchRowModel> {
    project_config
        .map(|project_config| {
            project_config
                .scene
                .objects
                .iter()
                .enumerate()
                .map(|(index, object)| {
                    let row_id = project_object_row_id(index);
                    select_static_row(
                        workbench_row(
                            row_id,
                            &object.icon,
                            &object.name,
                            &object.type_name,
                            "ok",
                            selected_row(selected_scene_row_id, row_id),
                        ),
                        row_id,
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

fn project_camera_rows(
    project_config: Option<&ProjectConfig>,
    selected_scene_row_id: u32,
) -> Vec<WorkbenchRowModel> {
    project_config
        .map(|project_config| {
            project_config
                .scene
                .cameras
                .iter()
                .enumerate()
                .map(|(index, camera)| {
                    let row_id = project_camera_row_id(index);
                    select_static_row(
                        workbench_row(
                            row_id,
                            &camera.icon,
                            &camera.name,
                            &format!("{} on {}", camera.rate, camera.mounted_link),
                            "ok",
                            selected_row(selected_scene_row_id, row_id),
                        ),
                        row_id,
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

fn scene_sections(
    state: &UrdfViewerState,
    hardware_runtime: &HardwareRuntime,
    project_config: Option<&ProjectConfig>,
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
            robot_rows(
                state,
                project_config,
                selected_scene_row_id,
                collapsed_scene_section_ids,
            ),
            collapsed_scene_section_ids,
        ),
    ];

    sections.push(workbench_section(
        SCENE_SECTION_HARDWARE,
        "Hardware",
        hardware_rows(hardware_runtime, project_config, selected_scene_row_id),
        collapsed_scene_section_ids,
    ));

    let mut object_rows = vec![
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
    ];
    object_rows.extend(project_scene_object_rows(
        project_config,
        selected_scene_row_id,
    ));

    sections.push(workbench_section(
        SCENE_SECTION_OBJECTS,
        "Objects",
        object_rows,
        collapsed_scene_section_ids,
    ));

    let mut sensor_rows = vec![
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
    ];
    sensor_rows.extend(project_camera_rows(project_config, selected_scene_row_id));

    sections.push(workbench_section(
        SCENE_SECTION_SENSORS,
        "Sensors",
        sensor_rows,
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

fn robot_transform_properties(
    project_config: Option<&ProjectConfig>,
) -> Vec<WorkbenchPropertyModel> {
    let translation = project_robot_base_translation(project_config);
    let rotation = project_robot_base_rotation(project_config);
    [
        ("Frame".to_string(), "World".to_string()),
        ("Position X".to_string(), format!("{:.3} m", translation[0])),
        ("Position Y".to_string(), format!("{:.3} m", translation[1])),
        ("Position Z".to_string(), format!("{:.3} m", translation[2])),
        (
            "Roll".to_string(),
            format!("{:.1} deg", rotation[0].to_degrees()),
        ),
        (
            "Pitch".to_string(),
            format!("{:.1} deg", rotation[1].to_degrees()),
        ),
        (
            "Yaw".to_string(),
            format!("{:.1} deg", rotation[2].to_degrees()),
        ),
    ]
    .into_iter()
    .map(|(name, value)| workbench_property(&name, value))
    .collect()
}

fn robot_scene_info(
    state: &UrdfViewerState,
    project_config: Option<&ProjectConfig>,
) -> SelectedSceneInfo {
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
        transform_properties: robot_transform_properties(project_config),
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

fn joint_exists(state: &UrdfViewerState, joint_name: &str) -> bool {
    state
        .robot
        .as_ref()
        .map(|robot| robot.joints.iter().any(|joint| joint.name == joint_name))
        .unwrap_or(false)
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
    project_config: Option<&ProjectConfig>,
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
    let mapped_servo = mapped_servo_for_joint(hardware_runtime, &joint.name);
    let mapped_device = mapped_servo
        .map(|servo| format!("Servo {} ({})", servo.id, servo.name))
        .unwrap_or_else(|| "-".to_string());
    let mapping_status = if mapped_servo.is_some() {
        "OK"
    } else {
        "No mapped device"
    };
    let semantic_name = project_joint_name(project_config, &joint.name);

    Some(SelectedSceneInfo {
        name: joint_display_label(project_config, &joint.name),
        selected_type: format!("URDF {} joint", urdf_joint_type_name(joint.joint_type)),
        status: mapping_status.to_string(),
        badge: "JT".to_string(),
        accent: "#6052a8".to_string(),
        transform_properties: vec![
            workbench_property("Semantic name", semantic_name.unwrap_or("-").to_string()),
            workbench_property("URDF joint", joint.name.clone()),
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
            workbench_property("Mapping status", mapping_status.to_string()),
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

fn selected_servo_info(
    state: &UrdfViewerState,
    servo: &HardwareServoRuntime,
    project_config: Option<&ProjectConfig>,
) -> SelectedSceneInfo {
    let mapping_status = if joint_exists(state, &servo.drives_joint) {
        "OK"
    } else {
        "Missing URDF joint"
    };

    SelectedSceneInfo {
        name: servo_display_name(servo, project_config),
        selected_type: "servo device".to_string(),
        status: mapping_status.to_string(),
        badge: "SRV".to_string(),
        accent: "#245c95".to_string(),
        transform_properties: vec![
            workbench_property("Id", servo.id.to_string()),
            workbench_property("Configured name", servo.name.clone()),
            workbench_property("Profile", servo.profile.clone()),
            workbench_property("Mapped robot", servo.drives_robot.clone()),
            workbench_property(
                "Mapped joint",
                joint_display_label(project_config, &servo.drives_joint),
            ),
            workbench_property("URDF joint", servo.drives_joint.clone()),
            workbench_property("Mapping status", mapping_status.to_string()),
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
    state: &UrdfViewerState,
    hardware_runtime: &HardwareRuntime,
    project_config: Option<&ProjectConfig>,
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
        HardwareDeviceRuntime::Servo(servo) => selected_servo_info(state, servo, project_config),
        HardwareDeviceRuntime::Imu(imu) => selected_imu_info(imu),
        HardwareDeviceRuntime::IoBoard(io_board) => selected_io_board_info(io_board),
    })
}

fn selected_project_scene_object_info(
    project_config: Option<&ProjectConfig>,
    selected_scene_row_id: u32,
) -> Option<SelectedSceneInfo> {
    if !(SCENE_ROW_PROJECT_OBJECT_BASE..SCENE_ROW_PROJECT_CAMERA_BASE)
        .contains(&selected_scene_row_id)
    {
        return None;
    }

    let index = (selected_scene_row_id - SCENE_ROW_PROJECT_OBJECT_BASE) as usize;
    let object = project_config?.scene.objects.get(index)?;
    Some(project_scene_object_info(object))
}

fn selected_project_camera_info(
    project_config: Option<&ProjectConfig>,
    selected_scene_row_id: u32,
) -> Option<SelectedSceneInfo> {
    if selected_scene_row_id < SCENE_ROW_PROJECT_CAMERA_BASE {
        return None;
    }

    let index = (selected_scene_row_id - SCENE_ROW_PROJECT_CAMERA_BASE) as usize;
    let camera = project_config?.scene.cameras.get(index)?;
    Some(project_camera_info(camera))
}

fn project_scene_object_info(object: &ProjectSceneObjectConfig) -> SelectedSceneInfo {
    let mut transform_properties = vec![
        workbench_property("Id", object.id.clone()),
        workbench_property("Frame", "World"),
        workbench_property(
            "Geometry",
            project_scene_object_geometry_label(&object.geometry),
        ),
        workbench_property("Position", format_vec3(object.position)),
        workbench_property("Rotation", format_vec3(object.rotation)),
        workbench_property("Include in fit", object.include_in_fit.to_string()),
        workbench_property(
            "Color",
            format!(
                "#{:02x}{:02x}{:02x}",
                object.color_rgb[0], object.color_rgb[1], object.color_rgb[2]
            ),
        ),
    ];

    if let Some(scale) = object.scale {
        transform_properties.push(workbench_property("Scale", format_vec3(scale)));
    }

    static_scene_info(
        &object.name,
        &object.type_name,
        &object.icon,
        "#5e6c7c",
        transform_properties,
    )
}

fn project_camera_info(camera: &ProjectCameraConfig) -> SelectedSceneInfo {
    selected_sensor_info(
        &camera.name,
        &camera.type_name,
        &camera.icon,
        vec![
            workbench_property("Id", camera.id.clone()),
            workbench_property("Mounted robot", camera.mounted_robot.clone()),
            workbench_property("Mounted link", camera.mounted_link.clone()),
            workbench_property("Position", format_vec3(camera.position)),
            workbench_property("Rotation", format_vec3(camera.rotation)),
        ],
        vec![
            workbench_property("FOV", format!("{:.1} deg", camera.fov_deg)),
            workbench_property("Rate", camera.rate.clone()),
            workbench_property("Resolution", project_camera_resolution_label(camera)),
            workbench_property("Preview", "enabled"),
        ],
    )
}

fn project_camera_resolution_label(camera: &ProjectCameraConfig) -> String {
    camera
        .resolution
        .map(|resolution| format!("{} x {}", resolution[0], resolution[1]))
        .unwrap_or_else(|| "-".to_string())
}

fn project_scene_object_geometry_label(geometry: &ProjectSceneObjectGeometry) -> String {
    match geometry {
        ProjectSceneObjectGeometry::Mesh { asset } => format!("mesh {asset}"),
        ProjectSceneObjectGeometry::Box { size } => {
            format!("box {:.3} x {:.3} x {:.3}", size[0], size[1], size[2])
        }
        ProjectSceneObjectGeometry::Sphere { radius } => format!("sphere r={radius:.3}"),
        ProjectSceneObjectGeometry::Cylinder {
            radius_top,
            radius_bottom,
            height,
        } => format!("cylinder rt={radius_top:.3} rb={radius_bottom:.3} h={height:.3}"),
    }
}

fn selected_static_scene_info(
    state: &UrdfViewerState,
    project_config: Option<&ProjectConfig>,
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
        _ => robot_scene_info(state, project_config),
    }
}

fn selected_scene_info(
    state: &UrdfViewerState,
    hardware_runtime: &HardwareRuntime,
    project_config: Option<&ProjectConfig>,
    selected_scene_row_id: u32,
) -> SelectedSceneInfo {
    if let Some(info) = selected_link_info(state, selected_scene_row_id) {
        return info;
    }

    if let Some(info) = selected_joint_info(
        state,
        hardware_runtime,
        project_config,
        selected_scene_row_id,
    ) {
        return info;
    }

    if let Some(info) = selected_bus_info(hardware_runtime, selected_scene_row_id) {
        return info;
    }

    if let Some(info) = selected_device_info(
        state,
        hardware_runtime,
        project_config,
        selected_scene_row_id,
    ) {
        return info;
    }

    if let Some(info) = selected_project_scene_object_info(project_config, selected_scene_row_id) {
        return info;
    }

    if let Some(info) = selected_project_camera_info(project_config, selected_scene_row_id) {
        return info;
    }

    selected_static_scene_info(state, project_config, selected_scene_row_id)
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
        action_arg: servo.id,
        label: "Target position".to_string(),
        min: 0,
        max: 4095,
        value: i32::from(servo.target_position.clamp(0, 4095)),
        display_value: servo.target_position.to_string(),
        velocity_display: String::new(),
        torque_display: String::new(),
    }]
}

fn servo_target_controls(
    hardware_runtime: &HardwareRuntime,
    project_config: Option<&ProjectConfig>,
) -> Vec<WorkbenchSliderModel> {
    hardware_runtime
        .buses
        .iter()
        .flat_map(|bus| bus.devices.iter())
        .filter_map(|device| match device {
            HardwareDeviceRuntime::Servo(servo) => Some(WorkbenchSliderModel {
                index: servo.id,
                action_arg: servo.id,
                label: servo_display_name(servo, project_config),
                min: 0,
                max: 4095,
                value: i32::from(servo.target_position.clamp(0, 4095)),
                display_value: servo.target_position.to_string(),
                velocity_display: format!("present {}", servo.present_position),
                torque_display: if servo.torque_enabled { "on" } else { "off" }.to_string(),
            }),
            _ => None,
        })
        .collect()
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

fn camera_preview_props(
    base_scene_props: &serde_json::Value,
    camera: &ProjectCameraConfig,
) -> WuiValue {
    let mut value = base_scene_props.clone();
    if let Some(object) = value.as_object_mut() {
        object.insert("preview".to_string(), serde_json::Value::Bool(true));
        object.insert(
            "previewCameraId".to_string(),
            serde_json::Value::String(camera.id.clone()),
        );
        object.insert(
            "previewCameraName".to_string(),
            serde_json::Value::String(camera.name.clone()),
        );
        object.insert(
            "previewFov".to_string(),
            serde_json::Value::from(camera.fov_deg),
        );
    }
    serde_json_to_wui_value(&value)
}

fn sensor_preview_models(
    project_config: Option<&ProjectConfig>,
    base_scene_props: &serde_json::Value,
) -> Vec<WorkbenchSensorPreviewModel> {
    project_config
        .map(|project_config| {
            project_config
                .scene
                .cameras
                .iter()
                .enumerate()
                .map(|(index, camera)| WorkbenchSensorPreviewModel {
                    id: camera.id.clone(),
                    component_id: SENSOR_PREVIEW_COMPONENT_BASE + index as u32,
                    name: camera.name.clone(),
                    detail: format!("{} on {}", camera.rate, camera.mounted_link),
                    props: camera_preview_props(base_scene_props, camera),
                })
                .collect()
        })
        .unwrap_or_default()
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

fn project_manifest_modified(project_config: Option<&ProjectConfig>) -> Option<SystemTime> {
    project_config
        .and_then(|project| project.manifest_path.metadata().ok())
        .and_then(|metadata| metadata.modified().ok())
}

fn update_project_manifest_json(
    manifest_path: &std::path::Path,
    update: impl FnOnce(&mut serde_json::Value) -> Result<(), String>,
) -> Result<(), String> {
    let raw = std::fs::read_to_string(manifest_path)
        .map_err(|err| format!("read {}: {err}", manifest_path.display()))?;
    let mut json: serde_json::Value = serde_json::from_str(&raw)
        .map_err(|err| format!("parse {}: {err}", manifest_path.display()))?;
    update(&mut json)?;
    let formatted = serde_json::to_string_pretty(&json)
        .map_err(|err| format!("format {}: {err}", manifest_path.display()))?;
    std::fs::write(manifest_path, format!("{formatted}\n"))
        .map_err(|err| format!("write {}: {err}", manifest_path.display()))
}

fn set_project_camera_transform_json(
    json: &mut serde_json::Value,
    camera_index: usize,
    camera: &ProjectCameraConfig,
) -> Result<(), String> {
    let Some(camera_json) = json
        .get_mut("scene")
        .and_then(|scene| scene.get_mut("cameras"))
        .and_then(|cameras| cameras.as_array_mut())
        .and_then(|cameras| cameras.get_mut(camera_index))
    else {
        return Err(format!("camera index {camera_index} is not writable"));
    };
    camera_json["position"] = serde_json::json!(camera.position);
    camera_json["rotation"] = serde_json::json!(camera.rotation);
    Ok(())
}

#[wgui_controller(template = "app_controller")]
impl AppController {
    pub(crate) fn new(
        urdf_path: Option<PathBuf>,
        project_config: Option<ProjectConfig>,
        virtual_bus: WorkbenchVirtualBusHandle,
        simulation_runtime: SimulationRuntimeHandle,
    ) -> Self {
        let mut urdf_state = UrdfViewerState::new(urdf_path);
        reload_urdf_state(&mut urdf_state);
        let hardware_runtime = hardware_runtime_from_project(project_config.as_ref(), &urdf_state);
        let project_config_modified = project_manifest_modified(project_config.as_ref());
        virtual_bus.configure_from_hardware(&hardware_runtime);
        Self {
            urdf_state,
            project_config: RefCell::new(project_config),
            project_config_modified: Cell::new(project_config_modified),
            project_config_dirty: Cell::new(false),
            hardware_runtime,
            virtual_bus,
            simulation_runtime,
            robot_static_scene_dirty: Cell::new(true),
            selected_scene_row_id: SCENE_ROW_ROBOT,
            collapsed_scene_section_ids: vec![SCENE_SECTION_LINKS, SCENE_SECTION_JOINTS],
            cpu_sample: Cell::new(read_cpu_sample()),
            gpu_metric: RefCell::new(None),
        }
    }

    fn reload_project_config_if_changed(&self) {
        if self.project_config_dirty.get() {
            return;
        }
        let manifest_path = self
            .project_config
            .borrow()
            .as_ref()
            .map(|project| project.manifest_path.clone());
        let Some(manifest_path) = manifest_path else {
            return;
        };
        let modified = manifest_path
            .metadata()
            .ok()
            .and_then(|metadata| metadata.modified().ok());
        if modified == self.project_config_modified.get() {
            return;
        }
        let Some(project_config) = project_config_from_manifest(&manifest_path) else {
            return;
        };
        *self.project_config.borrow_mut() = Some(project_config);
        self.project_config_modified.set(modified);
        self.robot_static_scene_dirty.set(true);
    }

    fn save_project_manifest(&self) -> Result<(), String> {
        let Some((manifest_path, cameras)) = self
            .project_config
            .borrow()
            .as_ref()
            .map(|project| (project.manifest_path.clone(), project.scene.cameras.clone()))
        else {
            self.project_config_dirty.set(false);
            return Ok(());
        };

        update_project_manifest_json(&manifest_path, |json| {
            for (index, camera) in cameras.iter().enumerate() {
                set_project_camera_transform_json(json, index, camera)?;
            }
            Ok(())
        })?;

        self.project_config_modified.set(
            manifest_path
                .metadata()
                .ok()
                .and_then(|metadata| metadata.modified().ok()),
        );
        self.project_config_dirty.set(false);
        Ok(())
    }

    pub(crate) fn state(&self) -> WorkbenchModel {
        self.reload_project_config_if_changed();
        let project_config_borrow = self.project_config.borrow();
        let project_config = project_config_borrow.as_ref();
        let snapshots = self.virtual_bus.snapshots();
        let mut live_hardware_runtime = self.hardware_runtime.clone();
        apply_servo_snapshots_to_hardware(&mut live_hardware_runtime, &snapshots);

        let mut live_urdf_state = self.urdf_state.clone();
        apply_servo_snapshots_to_urdf(&mut live_urdf_state, &live_hardware_runtime, &snapshots);

        let live_base_translation = [
            urdf_slider_value_to_units(live_urdf_state.base_translation_values[0]),
            urdf_slider_value_to_units(live_urdf_state.base_translation_values[1]),
            urdf_slider_value_to_units(live_urdf_state.base_translation_values[2]),
        ];
        let live_base_rotation = [
            urdf_slider_value_to_units(live_urdf_state.base_rotation_values[0]),
            urdf_slider_value_to_units(live_urdf_state.base_rotation_values[1]),
            urdf_slider_value_to_units(live_urdf_state.base_rotation_values[2]),
        ];
        let base_translation = add_vec3(
            project_robot_base_translation(project_config),
            live_base_translation,
        );
        let base_rotation = add_vec3(
            project_robot_base_rotation(project_config),
            live_base_rotation,
        );

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
            project_config,
            self.selected_scene_row_id,
        );
        let include_static_scene = self.robot_static_scene_dirty.replace(false);
        let robot_scene_props = robot_scene_props_with_static_scene(
            &live_urdf_state,
            project_config,
            base_translation,
            base_rotation,
            include_static_scene,
        );
        let sensor_previews = sensor_preview_models(project_config, &robot_scene_props);
        let camera_transform_controls =
            selected_camera_transform_controls(project_config, self.selected_scene_row_id);

        WorkbenchModel {
            project_name: project_name(project_config),
            gpu_label: system_gpu_label(&self.gpu_metric),
            cpu_label: system_cpu_label(&self.cpu_sample),
            memory_label: system_memory_label(),
            project_save_label: if self.project_config_dirty.get() {
                "Save*".to_string()
            } else {
                "Save".to_string()
            },
            project_dirty: self.project_config_dirty.get(),
            robot_scene_props: serde_json_to_wui_value(&robot_scene_props),
            lidar_preview_name: "lidar-preview".to_string(),
            lidar_preview_props: dashboard_preview_props("lidar"),
            camera_preview_name: "camera-preview".to_string(),
            camera_preview_props: dashboard_preview_props("camera"),
            has_sensor_previews: !sensor_previews.is_empty(),
            sensor_previews,
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
                .is_some()
                && self.simulation_runtime.status() != SimulationStatus::Stopped,
            show_camera_controls: !camera_transform_controls.is_empty(),
            show_robot_controls: show_robot_controls(self.selected_scene_row_id),
            servo_target_controls: selected_servo_target_controls(
                &live_hardware_runtime,
                self.selected_scene_row_id,
            ),
            camera_transform_controls,
            scene_sections: scene_sections(
                &live_urdf_state,
                &live_hardware_runtime,
                project_config,
                self.selected_scene_row_id,
                &self.collapsed_scene_section_ids,
            ),
            transform_properties: selected_info.transform_properties,
            physics_properties: selected_info.physics_properties,
            base_controls: base_controls(&live_urdf_state),
            joint_controls: joint_controls(&live_urdf_state, project_config),
            sensors: sensors(),
        }
    }

    pub(crate) fn title(&self) -> String {
        "RobotDreams Workbench".to_string()
    }

    pub(crate) async fn process(ctx: wgui::wui::runtime::ControllerProcessCtx) {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            ctx.refresh();
        }
    }

    pub(crate) fn play_simulation(&mut self) {
        if self.virtual_bus.is_running() {
            self.simulation_runtime
                .set_status(SimulationStatus::Running);
            return;
        }

        if self.start_virtual_bus().is_ok() {
            self.simulation_runtime
                .set_status(SimulationStatus::Running);
        }
    }

    pub(crate) fn pause_simulation(&mut self) {
        if self.simulation_runtime.status() == SimulationStatus::Running {
            self.simulation_runtime.set_status(SimulationStatus::Paused);
        }
    }

    pub(crate) fn stop_simulation(&mut self) {
        self.stop_virtual_bus();
        self.simulation_runtime
            .set_status(SimulationStatus::Stopped);
    }

    pub(crate) fn reset_simulation(&mut self) {
        self.virtual_bus.stop();
        self.simulation_runtime
            .set_status(SimulationStatus::Stopped);
        self.simulation_runtime.reset_clock();
        reload_urdf_state(&mut self.urdf_state);
        let project_config = self.project_config.borrow();
        self.hardware_runtime =
            hardware_runtime_from_project(project_config.as_ref(), &self.urdf_state);
        self.virtual_bus.reset_from_hardware(&self.hardware_runtime);
        self.robot_static_scene_dirty.set(true);
    }

    pub(crate) fn reload_urdf(&mut self) {
        self.virtual_bus.stop();
        reload_urdf_state(&mut self.urdf_state);
        let project_config = self.project_config.borrow();
        self.hardware_runtime =
            hardware_runtime_from_project(project_config.as_ref(), &self.urdf_state);
        self.virtual_bus.reset_from_hardware(&self.hardware_runtime);
        self.robot_static_scene_dirty.set(true);
    }

    fn refresh_from_virtual_bus(&mut self) {
        let snapshots = self.virtual_bus.snapshots();
        self.simulation_runtime
            .advance(0.02, servo_snapshots_json(&snapshots));
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
        self.selected_scene_row_id = if (SCENE_ROW_LINK_BASE..SCENE_ROW_JOINT_BASE).contains(&arg) {
            arg
        } else {
            link_row_id(arg as usize)
        };
    }

    pub(crate) fn select_joint(&mut self, arg: u32) {
        self.selected_scene_row_id = if (SCENE_ROW_JOINT_BASE..SCENE_ROW_BUS_BASE).contains(&arg) {
            arg
        } else {
            joint_row_id(arg as usize)
        };
    }

    pub(crate) fn select_bus(&mut self, arg: u32) {
        self.selected_scene_row_id = if (SCENE_ROW_BUS_BASE..SCENE_ROW_DEVICE_BASE).contains(&arg) {
            arg
        } else {
            bus_row_id(arg as usize)
        };
    }

    pub(crate) fn select_device(&mut self, arg: u32) {
        if arg >= SCENE_ROW_DEVICE_BASE {
            self.selected_scene_row_id = arg;
            return;
        }

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
            self.simulation_runtime
                .set_status(SimulationStatus::Running);
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
            self.simulation_runtime
                .record_servo_command(u32::from(servo_id), i32::from(target));
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
            self.simulation_runtime
                .record_servo_command(u32::from(servo_id), value.clamp(0, 4095));
            self.refresh_from_virtual_bus();
        }
    }

    pub(crate) fn set_camera_transform_control(&mut self, arg: u32, value: i32) {
        let camera_index = (arg / 6) as usize;
        let axis = (arg % 6) as usize;
        let scalar = value as f32 / crate::URDF_VALUE_SCALE;
        self.set_camera_transform_axis(camera_index, axis, scalar);
    }

    fn set_camera_transform_axis(&mut self, camera_index: usize, axis: usize, scalar: f32) {
        {
            let mut project_config = self.project_config.borrow_mut();
            let Some(project_config) = project_config.as_mut() else {
                return;
            };
            let Some(camera) = project_config.scene.cameras.get_mut(camera_index) else {
                return;
            };
            if axis < 3 {
                camera.position[axis] = scalar;
            } else {
                camera.rotation[axis - 3] = scalar;
            }
        }
        self.project_config_dirty.set(true);
    }

    fn set_selected_camera_transform_text(&mut self, axis: usize, value: String) {
        let Some(camera_index) = selected_project_camera_index(self.selected_scene_row_id) else {
            return;
        };
        let Ok(parsed) = value.trim().parse::<f32>() else {
            return;
        };
        let scalar = if axis < 3 {
            parsed
        } else {
            parsed.to_radians()
        };
        self.set_camera_transform_axis(camera_index, axis, scalar);
    }

    pub(crate) fn set_camera_x(&mut self, value: String) {
        self.set_selected_camera_transform_text(0, value);
    }

    pub(crate) fn set_camera_y(&mut self, value: String) {
        self.set_selected_camera_transform_text(1, value);
    }

    pub(crate) fn set_camera_z(&mut self, value: String) {
        self.set_selected_camera_transform_text(2, value);
    }

    pub(crate) fn set_camera_roll(&mut self, value: String) {
        self.set_selected_camera_transform_text(3, value);
    }

    pub(crate) fn set_camera_pitch(&mut self, value: String) {
        self.set_selected_camera_transform_text(4, value);
    }

    pub(crate) fn set_camera_yaw(&mut self, value: String) {
        self.set_selected_camera_transform_text(5, value);
    }

    pub(crate) fn save_project(&mut self) {
        if let Err(err) = self.save_project_manifest() {
            log::warn!("failed to save project manifest: {err}");
        }
    }
}

#[wgui_controller(template = "viewport_controller")]
impl ViewportController {
    pub(crate) fn new(
        urdf_path: Option<PathBuf>,
        project_config: Option<ProjectConfig>,
        virtual_bus: WorkbenchVirtualBusHandle,
    ) -> Self {
        let mut urdf_state = UrdfViewerState::new(urdf_path);
        reload_urdf_state(&mut urdf_state);
        let hardware_runtime = hardware_runtime_from_project(project_config.as_ref(), &urdf_state);
        let project_config_modified = project_manifest_modified(project_config.as_ref());
        virtual_bus.configure_from_hardware(&hardware_runtime);
        Self {
            urdf_state,
            project_config: RefCell::new(project_config),
            project_config_modified: Cell::new(project_config_modified),
            hardware_runtime,
            virtual_bus,
            robot_static_scene_dirty: Cell::new(true),
        }
    }

    fn reload_project_config_if_changed(&self) {
        let manifest_path = self
            .project_config
            .borrow()
            .as_ref()
            .map(|project| project.manifest_path.clone());
        let Some(manifest_path) = manifest_path else {
            return;
        };
        let modified = manifest_path
            .metadata()
            .ok()
            .and_then(|metadata| metadata.modified().ok());
        if modified == self.project_config_modified.get() {
            return;
        }
        let Some(project_config) = project_config_from_manifest(&manifest_path) else {
            return;
        };
        *self.project_config.borrow_mut() = Some(project_config);
        self.project_config_modified.set(modified);
        self.robot_static_scene_dirty.set(true);
    }

    pub(crate) fn state(&self) -> ViewportModel {
        self.reload_project_config_if_changed();
        let project_config_borrow = self.project_config.borrow();
        let project_config = project_config_borrow.as_ref();
        let snapshots = self.virtual_bus.snapshots();
        let mut live_hardware_runtime = self.hardware_runtime.clone();
        apply_servo_snapshots_to_hardware(&mut live_hardware_runtime, &snapshots);

        let mut live_urdf_state = self.urdf_state.clone();
        apply_servo_snapshots_to_urdf(&mut live_urdf_state, &live_hardware_runtime, &snapshots);

        let live_base_translation = [
            urdf_slider_value_to_units(live_urdf_state.base_translation_values[0]),
            urdf_slider_value_to_units(live_urdf_state.base_translation_values[1]),
            urdf_slider_value_to_units(live_urdf_state.base_translation_values[2]),
        ];
        let live_base_rotation = [
            urdf_slider_value_to_units(live_urdf_state.base_rotation_values[0]),
            urdf_slider_value_to_units(live_urdf_state.base_rotation_values[1]),
            urdf_slider_value_to_units(live_urdf_state.base_rotation_values[2]),
        ];
        let base_translation = add_vec3(
            project_robot_base_translation(project_config),
            live_base_translation,
        );
        let base_rotation = add_vec3(
            project_robot_base_rotation(project_config),
            live_base_rotation,
        );
        let include_static_scene = self.robot_static_scene_dirty.replace(false);
        let robot_scene_props = robot_scene_props_with_static_scene(
            &live_urdf_state,
            project_config,
            base_translation,
            base_rotation,
            include_static_scene,
        );

        ViewportModel {
            robot_scene_props: serde_json_to_wui_value(&robot_scene_props),
        }
    }

    pub(crate) fn title(&self) -> String {
        "RobotDreams Viewport".to_string()
    }
}

impl AppController {
    pub(crate) fn start_virtual_bus(&mut self) -> Result<Option<String>, String> {
        let Some(bus_index) = first_virtual_bus_index(&self.hardware_runtime) else {
            return Ok(None);
        };
        self.start_virtual_bus_at_index(bus_index)
    }

    fn start_virtual_bus_at_index(&mut self, bus_index: usize) -> Result<Option<String>, String> {
        let Some(bus) = self.hardware_runtime.buses.get(bus_index) else {
            return Ok(None);
        };
        if bus.transport_type != "virtual" {
            return Err(format!("bus {} is not virtual", bus.id));
        }
        let path = self.virtual_bus.start(bus_servo_count(bus))?;
        if let Some(bus) = self.hardware_runtime.buses.get_mut(bus_index) {
            bus.device_path = Some(path.clone());
        }
        self.simulation_runtime
            .set_status(SimulationStatus::Running);
        Ok(Some(path))
    }

    pub(crate) fn start_virtual_bus_by_id(
        &mut self,
        bus_id: &str,
    ) -> Result<Option<String>, String> {
        let Some(bus_index) = self
            .hardware_runtime
            .buses
            .iter()
            .position(|bus| bus.id == bus_id)
        else {
            return Err(format!("unknown bus {bus_id}"));
        };
        self.start_virtual_bus_at_index(bus_index)
    }

    pub(crate) fn stop_virtual_bus(&mut self) {
        self.virtual_bus.stop();
        for bus in &mut self.hardware_runtime.buses {
            if bus.transport_type == "virtual" {
                bus.device_path = None;
            }
        }
        self.simulation_runtime
            .set_status(SimulationStatus::Stopped);
    }

    pub(crate) fn tick_headless(&mut self) {
        self.refresh_from_virtual_bus();
    }

    pub(crate) fn headless_summary(&self) -> HeadlessSummary {
        self.reload_project_config_if_changed();
        let project_config = self.project_config.borrow();
        let snapshots = self.virtual_bus.snapshots();
        let mut live_hardware_runtime = self.hardware_runtime.clone();
        apply_servo_snapshots_to_hardware(&mut live_hardware_runtime, &snapshots);

        let loaded_summary = self
            .urdf_state
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

        HeadlessSummary {
            project_name: project_name(project_config.as_ref()),
            status: self.urdf_state.status.clone(),
            loaded_summary,
            virtual_bus_running: self.virtual_bus.is_running(),
            virtual_bus_path: self.virtual_bus.path(),
            servo_count: snapshots.len(),
            snapshots: servo_snapshots_json(&snapshots),
        }
    }

    pub(crate) fn project_state_json(&mut self) -> serde_json::Value {
        self.reload_project_config_if_changed();
        self.refresh_from_virtual_bus();
        let project_config = self.project_config.borrow();
        let snapshots = self.virtual_bus.snapshots();
        let bus_events = self.virtual_bus.recent_events();
        let bus_events_json = bus_events_json(&bus_events);
        let mut live_hardware_runtime = self.hardware_runtime.clone();
        apply_servo_snapshots_to_hardware(&mut live_hardware_runtime, &snapshots);
        let live_base_translation = [
            urdf_slider_value_to_units(self.urdf_state.base_translation_values[0]),
            urdf_slider_value_to_units(self.urdf_state.base_translation_values[1]),
            urdf_slider_value_to_units(self.urdf_state.base_translation_values[2]),
        ];
        let live_base_rotation = [
            urdf_slider_value_to_units(self.urdf_state.base_rotation_values[0]),
            urdf_slider_value_to_units(self.urdf_state.base_rotation_values[1]),
            urdf_slider_value_to_units(self.urdf_state.base_rotation_values[2]),
        ];
        let base_translation = add_vec3(
            project_robot_base_translation(project_config.as_ref()),
            live_base_translation,
        );
        let base_rotation = add_vec3(
            project_robot_base_rotation(project_config.as_ref()),
            live_base_rotation,
        );
        let robot_scene_props = robot_scene_props_with_static_scene(
            &self.urdf_state,
            project_config.as_ref(),
            base_translation,
            base_rotation,
            false,
        );

        let robots = project_config
            .as_ref()
            .map(|project| {
                project
                    .robots
                    .iter()
                    .map(|robot| {
                        serde_json::json!({
                            "id": robot.id,
                            "name": robot.name,
                            "model": {
                                "type": robot.model.type_name,
                                "path": robot.model.path,
                            },
                            "jointNames": &robot.joint_names,
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| {
                self.urdf_state
                    .robot
                    .as_ref()
                    .map(|_| {
                        vec![serde_json::json!({
                            "id": "robot",
                            "name": project_name(project_config.as_ref()),
                            "model": {
                                "type": "urdf",
                                "path": self.urdf_state.urdf_path.as_ref().map(|path| path.display().to_string()),
                            },
                        })]
                    })
                    .unwrap_or_default()
            });

        let (links, joints, movable_joints) = self
            .urdf_state
            .robot
            .as_ref()
            .map(|robot| {
                let mut links: Vec<_> = robot.links.keys().cloned().collect();
                links.sort();
                let joints = robot
                    .joints
                    .iter()
                    .map(|joint| {
                        serde_json::json!({
                            "name": joint.name,
                            "displayName": joint_display_label(
                                project_config.as_ref(),
                                &joint.name,
                            ),
                            "semanticName": project_joint_name(
                                project_config.as_ref(),
                                &joint.name,
                            ),
                            "type": urdf_joint_type_name(joint.joint_type),
                            "parent": joint.parent,
                            "child": joint.child,
                            "axis": joint.axis,
                            "originXyz": joint.origin_xyz,
                            "originRpy": joint.origin_rpy,
                        })
                    })
                    .collect::<Vec<_>>();
                let movable_joints = robot
                    .movable_joint_indices
                    .iter()
                    .filter_map(|joint_index| robot.joints.get(*joint_index))
                    .map(|joint| {
                        serde_json::json!({
                            "name": joint.name,
                            "displayName": joint_display_label(
                                project_config.as_ref(),
                                &joint.name,
                            ),
                            "semanticName": project_joint_name(
                                project_config.as_ref(),
                                &joint.name,
                            ),
                        })
                    })
                    .collect::<Vec<_>>();
                (links, joints, movable_joints)
            })
            .unwrap_or_default();

        serde_json::json!({
            "project": {
                "name": project_name(project_config.as_ref()),
                "format": project_config.as_ref().map(|project| project.format.clone()),
                "status": self.urdf_state.status,
            },
            "urdf": {
                "path": self.urdf_state.urdf_path.as_ref().map(|path| path.display().to_string()),
                "links": links,
                "joints": joints,
                "movableJoints": movable_joints,
            },
            "robots": robots,
            "hardware": hardware_runtime_json(&live_hardware_runtime, project_config.as_ref()),
            "controls": {
                "base": base_controls(&self.urdf_state).into_iter().map(slider_json).collect::<Vec<_>>(),
                "joints": joint_controls(&self.urdf_state, project_config.as_ref()).into_iter().map(slider_json).collect::<Vec<_>>(),
                "servos": servo_target_controls(&live_hardware_runtime, project_config.as_ref()).into_iter().map(slider_json).collect::<Vec<_>>(),
            },
            "virtualBus": {
                "running": self.virtual_bus.is_running(),
                "path": self.virtual_bus.path(),
                "snapshots": servo_snapshots_json(&snapshots),
                "events": bus_events_json.clone(),
            },
            "simulation": {
                "status": self.simulation_runtime.status().as_str(),
                "clockSec": self.simulation_runtime.clock_sec(),
                "virtualBus": {
                    "running": self.virtual_bus.is_running(),
                    "path": self.virtual_bus.path(),
                },
                "servoSnapshots": servo_snapshots_json(&snapshots),
                "busEvents": bus_events_json,
                "robotScene": robot_scene_props,
            },
        })
    }

    pub(crate) fn project_items_json(&mut self) -> serde_json::Value {
        self.reload_project_config_if_changed();
        self.refresh_from_virtual_bus();
        let project_state = self.project_state_json();
        let project_config = self.project_config.borrow();
        let mut items = vec![
            serde_json::json!({"id": "project", "type": "project", "name": project_name(project_config.as_ref())}),
            serde_json::json!({"id": "urdf", "type": "urdf", "name": self.urdf_state.urdf_path.as_ref().map(|path| path.display().to_string()).unwrap_or_else(|| "URDF".to_string())}),
            serde_json::json!({"id": "hardware", "type": "hardware", "name": "Hardware"}),
            serde_json::json!({"id": "virtual-bus", "type": "virtualBus", "name": "Virtual Bus"}),
            serde_json::json!({"id": "snapshots", "type": "snapshots", "name": "Servo Snapshots"}),
        ];

        for robot in project_state["robots"].as_array().into_iter().flatten() {
            if let Some(id) = robot.get("id").and_then(|id| id.as_str()) {
                items.push(serde_json::json!({
                    "id": format!("robot:{id}"),
                    "type": "robot",
                    "name": robot.get("name").and_then(|name| name.as_str()).unwrap_or(id),
                }));
            }
        }

        for bus in &self.hardware_runtime.buses {
            items.push(serde_json::json!({
                "id": format!("bus:{}", bus.id),
                "type": "bus",
                "name": bus.name,
            }));
            for device in &bus.devices {
                let device_id = hardware_device_id(device);
                items.push(serde_json::json!({
                    "id": format!("device:{}:{device_id}", bus.id),
                    "type": hardware_device_kind(device),
                    "name": device_row_label(device, project_config.as_ref()),
                }));
            }
        }

        serde_json::Value::Array(items)
    }
}
