use std::path::PathBuf;

use wgui::wui::runtime::WuiValue;
use wgui::{WguiModel, wgui_controller};

use crate::{
    ROBOT_SCENE_CONTROLLER_ENTRY, URDF_BASE_SLIDER_BASE_ID, URDF_JOINT_SLIDER_BASE_ID,
    UrdfViewerState, apply_urdf_slider_change, reload_urdf_state, robot_scene_props,
    urdf_joint_slider_range, urdf_joint_type_name, urdf_slider_value_to_units,
    urdf_value_to_joint_units,
};

const DASHBOARD_PREVIEW_CONTROLLER_ENTRY: &str =
    "/fs/wgui-controllers/dashboard-preview/controller.js?v=workbench-assets-window-resize";
const SCENE_SECTION_ENVIRONMENT: u32 = 1;
const SCENE_SECTION_ROBOTS: u32 = 2;
const SCENE_SECTION_LINKS: u32 = 3;
const SCENE_SECTION_JOINTS: u32 = 4;
const SCENE_SECTION_OBJECTS: u32 = 5;
const SCENE_SECTION_SENSORS: u32 = 6;
const SCENE_ROW_WAREHOUSE: u32 = 1;
const SCENE_ROW_FLOOR: u32 = 2;
const SCENE_ROW_LIGHTS: u32 = 3;
const SCENE_ROW_ROBOT: u32 = 100;
const SCENE_ROW_SERVO_BUS: u32 = 101;
const SCENE_ROW_OBJECT_WORKTABLE: u32 = 200;
const SCENE_ROW_OBJECT_BIN: u32 = 201;
const SCENE_ROW_OBJECT_FIXTURE: u32 = 202;
const SCENE_ROW_OBJECT_TAG: u32 = 203;
const SCENE_ROW_SENSOR_JOINT_STATES: u32 = 300;
const SCENE_ROW_SENSOR_SERVO_BUS: u32 = 301;
const SCENE_ROW_SENSOR_LIDAR: u32 = 302;
const SCENE_ROW_SENSOR_CAMERA: u32 = 303;
const SCENE_ROW_LINK_BASE: u32 = 10_000;
const SCENE_ROW_JOINT_BASE: u32 = 20_000;

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
    indent_width: u32,
    icon: String,
    label: String,
    detail: String,
    status: String,
    selected: bool,
}

#[derive(Debug, Clone, WguiModel)]
pub(crate) struct WorkbenchAssetModel {
    name: String,
    kind: String,
    badge: String,
    accent: String,
    preview_name: String,
    preview_props: WuiValue,
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
    project_version: String,
    simulation_label: String,
    simulation_time: String,
    gpu_label: String,
    memory_label: String,
    robot_scene_name: String,
    robot_scene_entry: String,
    robot_scene_props: WuiValue,
    dashboard_preview_entry: String,
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
    scene_sections: Vec<WorkbenchSectionModel>,
    assets: Vec<WorkbenchAssetModel>,
    transform_properties: Vec<WorkbenchPropertyModel>,
    physics_properties: Vec<WorkbenchPropertyModel>,
    base_controls: Vec<WorkbenchSliderModel>,
    joint_controls: Vec<WorkbenchSliderModel>,
    sensors: Vec<WorkbenchSensorModel>,
}

pub(crate) struct AppController {
    urdf_state: UrdfViewerState,
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
        indent_width: 0,
        icon: icon.to_string(),
        label: label.to_string(),
        detail: detail.to_string(),
        status: status.to_string(),
        selected,
    }
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
    indented_workbench_row(1, id, toggle_label, title, detail, "", false)
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
            indented_workbench_row(
                2,
                row_id,
                "LN",
                link_name,
                &link_detail(state, link_name),
                "ok",
                selected_row(selected_scene_row_id, row_id),
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
            indented_workbench_row(
                2,
                row_id,
                "JT",
                &joint.name,
                urdf_joint_type_name(joint.joint_type),
                "ok",
                selected_row(selected_scene_row_id, row_id),
            )
        })
        .collect()
}

fn robot_rows(
    state: &UrdfViewerState,
    selected_scene_row_id: u32,
    collapsed_scene_section_ids: &[u32],
) -> Vec<WorkbenchRowModel> {
    let mut rows = vec![workbench_row(
        SCENE_ROW_ROBOT,
        "ARM",
        "PuppyArm",
        "URDF",
        "ok",
        selected_row(selected_scene_row_id, SCENE_ROW_ROBOT),
    )];

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

    rows.push(workbench_row(
        SCENE_ROW_SERVO_BUS,
        "BUS",
        "Virtual ServoBus",
        "ST3215",
        "ok",
        selected_row(selected_scene_row_id, SCENE_ROW_SERVO_BUS),
    ));

    rows
}

fn scene_sections(
    state: &UrdfViewerState,
    selected_scene_row_id: u32,
    collapsed_scene_section_ids: &[u32],
) -> Vec<WorkbenchSectionModel> {
    let mut sections = vec![
        workbench_section(
            SCENE_SECTION_ENVIRONMENT,
            "Environment",
            vec![
                workbench_row(
                    SCENE_ROW_WAREHOUSE,
                    "ENV",
                    "Warehouse",
                    "world",
                    "ok",
                    selected_row(selected_scene_row_id, SCENE_ROW_WAREHOUSE),
                ),
                workbench_row(
                    SCENE_ROW_FLOOR,
                    "PLN",
                    "Floor",
                    "plane",
                    "ok",
                    selected_row(selected_scene_row_id, SCENE_ROW_FLOOR),
                ),
                workbench_row(
                    SCENE_ROW_LIGHTS,
                    "LGT",
                    "Lights",
                    "3 sources",
                    "ok",
                    selected_row(selected_scene_row_id, SCENE_ROW_LIGHTS),
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
        SCENE_SECTION_OBJECTS,
        "Objects",
        vec![
            workbench_row(
                SCENE_ROW_OBJECT_WORKTABLE,
                "TBL",
                "Worktable",
                "fixture",
                "ok",
                selected_row(selected_scene_row_id, SCENE_ROW_OBJECT_WORKTABLE),
            ),
            workbench_row(
                SCENE_ROW_OBJECT_BIN,
                "BIN",
                "Bin Blue",
                "container",
                "ok",
                selected_row(selected_scene_row_id, SCENE_ROW_OBJECT_BIN),
            ),
            workbench_row(
                SCENE_ROW_OBJECT_FIXTURE,
                "FIX",
                "Fixture Plate",
                "tooling",
                "ok",
                selected_row(selected_scene_row_id, SCENE_ROW_OBJECT_FIXTURE),
            ),
            workbench_row(
                SCENE_ROW_OBJECT_TAG,
                "TAG",
                "Calibration Tag",
                "marker",
                "--",
                selected_row(selected_scene_row_id, SCENE_ROW_OBJECT_TAG),
            ),
        ],
        collapsed_scene_section_ids,
    ));

    sections.push(workbench_section(
        SCENE_SECTION_SENSORS,
        "Sensors",
        vec![
            workbench_row(
                SCENE_ROW_SENSOR_JOINT_STATES,
                "JS",
                "Joint States",
                "100 Hz",
                "ok",
                selected_row(selected_scene_row_id, SCENE_ROW_SENSOR_JOINT_STATES),
            ),
            workbench_row(
                SCENE_ROW_SENSOR_SERVO_BUS,
                "BUS",
                "Servo Bus",
                "virtual",
                "ok",
                selected_row(selected_scene_row_id, SCENE_ROW_SENSOR_SERVO_BUS),
            ),
            workbench_row(
                SCENE_ROW_SENSOR_LIDAR,
                "LDR",
                "Lidar_1",
                "10 Hz",
                "ok",
                selected_row(selected_scene_row_id, SCENE_ROW_SENSOR_LIDAR),
            ),
            workbench_row(
                SCENE_ROW_SENSOR_CAMERA,
                "CAM",
                "Camera_1",
                "30 Hz",
                "--",
                selected_row(selected_scene_row_id, SCENE_ROW_SENSOR_CAMERA),
            ),
        ],
        collapsed_scene_section_ids,
    ));

    sections
}

fn asset_preview_props(name: &str, kind: &str, badge: &str, accent: &str) -> WuiValue {
    let value = serde_json::json!({
        "mode": "asset",
        "name": name,
        "kind": kind,
        "badge": badge,
        "accent": accent
    });
    serde_json_to_wui_value(&value)
}

fn asset_models() -> Vec<WorkbenchAssetModel> {
    [
        ("PuppyArm", "Demo robot", "ARM", "#1c6ea4"),
        ("ST3215", "Servo profile", "ST", "#5b7188"),
        ("Virtual Bus", "Component", "BUS", "#245c95"),
        ("Warehouse", "Environment", "ENV", "#4b5f6f"),
        ("Joint Plot", "Telemetry", "PLOT", "#6052a8"),
        ("Firmware Bridge", "Connector", "FW", "#3e7f65"),
        ("Rover X1", "Mobile base", "X1", "#3b5a70"),
        ("Fixture Kit", "Tooling", "FIX", "#5e6c7c"),
    ]
    .into_iter()
    .map(|(name, kind, badge, accent)| WorkbenchAssetModel {
        name: name.to_string(),
        kind: kind.to_string(),
        badge: badge.to_string(),
        accent: accent.to_string(),
        preview_name: format!(
            "asset-preview-{}",
            name.to_ascii_lowercase().replace(' ', "-")
        ),
        preview_props: asset_preview_props(name, kind, badge, accent),
    })
    .collect()
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
            workbench_property("Control", "slider / servo bus"),
        ],
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
        SCENE_ROW_SERVO_BUS => static_scene_info(
            "Virtual ServoBus",
            "servo bus",
            "BUS",
            "#245c95",
            vec![
                workbench_property("Profile", "ST3215"),
                workbench_property("Transport", "virtual"),
                workbench_property("Rate", "1 MHz"),
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
        SCENE_ROW_SENSOR_JOINT_STATES => static_scene_info(
            "Joint States",
            "telemetry",
            "JS",
            "#245c95",
            vec![workbench_property("Rate", "100 Hz")],
        ),
        SCENE_ROW_SENSOR_SERVO_BUS => static_scene_info(
            "Servo Bus",
            "telemetry",
            "BUS",
            "#245c95",
            vec![workbench_property("Source", "virtual bus")],
        ),
        SCENE_ROW_SENSOR_LIDAR => static_scene_info(
            "Lidar_1",
            "sensor",
            "LDR",
            "#245c95",
            vec![workbench_property("Rate", "10 Hz")],
        ),
        SCENE_ROW_SENSOR_CAMERA => static_scene_info(
            "Camera_1",
            "sensor",
            "CAM",
            "#245c95",
            vec![workbench_property("Rate", "30 Hz")],
        ),
        _ => robot_scene_info(state),
    }
}

fn selected_scene_info(state: &UrdfViewerState, selected_scene_row_id: u32) -> SelectedSceneInfo {
    if let Some(info) = selected_link_info(state, selected_scene_row_id) {
        return info;
    }

    if let Some(info) = selected_joint_info(state, selected_scene_row_id) {
        return info;
    }

    selected_static_scene_info(state, selected_scene_row_id)
}

fn sensors() -> Vec<WorkbenchSensorModel> {
    vec![
        workbench_sensor("Joint States", "JS_1", "100 Hz"),
        workbench_sensor("Servo Bus", "ST3215", "1 MHz"),
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

#[wgui_controller(template = "app_controller")]
impl AppController {
    pub(crate) fn new(urdf_path: Option<PathBuf>) -> Self {
        let mut urdf_state = UrdfViewerState::new(urdf_path);
        reload_urdf_state(&mut urdf_state);
        Self {
            urdf_state,
            simulation_running: true,
            selected_scene_row_id: SCENE_ROW_ROBOT,
            collapsed_scene_section_ids: Vec::new(),
        }
    }

    pub(crate) fn state(&self) -> WorkbenchModel {
        let base_translation = [
            urdf_slider_value_to_units(self.urdf_state.base_translation_values[0]),
            urdf_slider_value_to_units(self.urdf_state.base_translation_values[1]),
            urdf_slider_value_to_units(self.urdf_state.base_translation_values[2]),
        ];
        let base_rotation = [
            urdf_slider_value_to_units(self.urdf_state.base_rotation_values[0]),
            urdf_slider_value_to_units(self.urdf_state.base_rotation_values[1]),
            urdf_slider_value_to_units(self.urdf_state.base_rotation_values[2]),
        ];

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
        let no_movable_joints = self
            .urdf_state
            .robot
            .as_ref()
            .map(|robot| robot.movable_joint_indices.is_empty())
            .unwrap_or(false);
        let selected_info = selected_scene_info(&self.urdf_state, self.selected_scene_row_id);

        WorkbenchModel {
            project_name: "SmartFactory".to_string(),
            project_version: "v2.4.1 v".to_string(),
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
            robot_scene_props: serde_json_to_wui_value(&robot_scene_props(
                &self.urdf_state,
                base_translation,
                base_rotation,
            )),
            dashboard_preview_entry: DASHBOARD_PREVIEW_CONTROLLER_ENTRY.to_string(),
            lidar_preview_name: "lidar-preview".to_string(),
            lidar_preview_props: dashboard_preview_props("lidar"),
            camera_preview_name: "camera-preview".to_string(),
            camera_preview_props: dashboard_preview_props("camera"),
            viewport_title: "Viewport 1".to_string(),
            file_label: file_label(&self.urdf_state),
            status: self.urdf_state.status.clone(),
            selected_name: selected_info.name,
            selected_type: selected_info.selected_type,
            selected_status: selected_info.status,
            selected_badge: selected_info.badge,
            selected_accent: selected_info.accent,
            loaded_summary,
            has_robot: self.urdf_state.robot.is_some(),
            no_movable_joints,
            scene_sections: scene_sections(
                &self.urdf_state,
                self.selected_scene_row_id,
                &self.collapsed_scene_section_ids,
            ),
            assets: asset_models(),
            transform_properties: selected_info.transform_properties,
            physics_properties: selected_info.physics_properties,
            base_controls: base_controls(&self.urdf_state),
            joint_controls: joint_controls(&self.urdf_state),
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
        reload_urdf_state(&mut self.urdf_state);
    }

    pub(crate) fn reload_urdf(&mut self) {
        reload_urdf_state(&mut self.urdf_state);
    }

    pub(crate) fn select_scene_row(&mut self, arg: u32) {
        if matches!(arg, SCENE_SECTION_LINKS | SCENE_SECTION_JOINTS) {
            self.toggle_scene_section(arg);
            return;
        }

        self.selected_scene_row_id = arg;
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

    pub(crate) fn set_base_control(&mut self, arg: u32, value: i32) {
        let _ =
            apply_urdf_slider_change(&mut self.urdf_state, URDF_BASE_SLIDER_BASE_ID + arg, value);
    }

    pub(crate) fn set_joint_control(&mut self, arg: u32, value: i32) {
        let _ =
            apply_urdf_slider_change(&mut self.urdf_state, URDF_JOINT_SLIDER_BASE_ID + arg, value);
    }
}
