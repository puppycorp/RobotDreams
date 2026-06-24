use std::path::PathBuf;

use wgui::wui::runtime::WuiValue;
use wgui::{WguiModel, wgui_controller};

use crate::{
    ROBOT_SCENE_CONTROLLER_ENTRY, URDF_BASE_SLIDER_BASE_ID, URDF_JOINT_SLIDER_BASE_ID,
    UrdfViewerState, apply_urdf_slider_change, reload_urdf_state, robot_scene_props,
    urdf_joint_slider_range, urdf_slider_value_to_units, urdf_value_to_joint_units,
};

const DASHBOARD_PREVIEW_CONTROLLER_ENTRY: &str =
    "/fs/wgui-controllers/dashboard-preview/controller.js?v=workbench-assets";

#[derive(Debug, Clone, WguiModel)]
pub(crate) struct WorkbenchSectionModel {
    title: String,
    rows: Vec<WorkbenchRowModel>,
}

#[derive(Debug, Clone, WguiModel)]
pub(crate) struct WorkbenchRowModel {
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
pub(crate) struct WorkbenchLogModel {
    time: String,
    level: String,
    message: String,
    color: String,
}

#[derive(Debug, Clone, WguiModel)]
pub(crate) struct WorkbenchScriptLineModel {
    number: u32,
    text: String,
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
    telemetry_plot_name: String,
    telemetry_plot_props: WuiValue,
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
    logs: Vec<WorkbenchLogModel>,
    script_lines: Vec<WorkbenchScriptLineModel>,
    plot_series: Vec<WorkbenchPropertyModel>,
    transform_properties: Vec<WorkbenchPropertyModel>,
    physics_properties: Vec<WorkbenchPropertyModel>,
    base_controls: Vec<WorkbenchSliderModel>,
    joint_controls: Vec<WorkbenchSliderModel>,
    sensors: Vec<WorkbenchSensorModel>,
}

pub(crate) struct AppController {
    urdf_state: UrdfViewerState,
    simulation_running: bool,
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
    icon: &str,
    label: &str,
    detail: &str,
    status: &str,
    selected: bool,
) -> WorkbenchRowModel {
    WorkbenchRowModel {
        icon: icon.to_string(),
        label: label.to_string(),
        detail: detail.to_string(),
        status: status.to_string(),
        selected,
    }
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

    let display_labels = [
        "Shoulder Pan",
        "Shoulder Lift",
        "Elbow",
        "Wrist 1",
        "Wrist 2",
        "Wrist 3",
    ];

    robot
        .movable_joint_indices
        .iter()
        .enumerate()
        .take(display_labels.len())
        .filter_map(|(slider_slot, joint_index)| {
            let joint = robot.joints.get(*joint_index)?;
            let value = state.joint_values.get(*joint_index).copied().unwrap_or(0);
            let (min, max) = urdf_joint_slider_range(joint);
            Some(WorkbenchSliderModel {
                index: slider_slot as u32,
                label: display_labels
                    .get(slider_slot)
                    .copied()
                    .unwrap_or(joint.name.as_str())
                    .to_string(),
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

fn scene_sections(state: &UrdfViewerState) -> Vec<WorkbenchSectionModel> {
    let mut sections = vec![
        WorkbenchSectionModel {
            title: "Environment".to_string(),
            rows: vec![
                workbench_row("▣", "Warehouse", "world", "◉", false),
                workbench_row("▤", "Floor", "plane", "◉", false),
                workbench_row("✦", "Lights", "3 sources", "◉", false),
            ],
        },
        WorkbenchSectionModel {
            title: "Robots".to_string(),
            rows: vec![
                workbench_row("⚙", "PuppyArm", "URDF", "◉", true),
                workbench_row("⇄", "Virtual ServoBus", "ST3215", "◉", false),
            ],
        },
    ];

    if let Some(robot) = state.robot.as_ref() {
        sections.push(WorkbenchSectionModel {
            title: "Frames".to_string(),
            rows: robot
                .roots
                .iter()
                .map(|root| workbench_row("◇", root, "root link", "◌", false))
                .collect(),
        });
    }

    sections.push(WorkbenchSectionModel {
        title: "Objects".to_string(),
        rows: vec![
            workbench_row("▤", "Worktable", "fixture", "◉", false),
            workbench_row("▣", "Bin Blue", "container", "◉", false),
            workbench_row("▧", "Fixture Plate", "tooling", "◉", false),
            workbench_row("◇", "Calibration Tag", "marker", "◌", false),
        ],
    });

    sections.push(WorkbenchSectionModel {
        title: "Sensors".to_string(),
        rows: vec![
            workbench_row("⌁", "Joint States", "100 Hz", "◉", false),
            workbench_row("⇄", "Servo Bus", "virtual", "◉", false),
            workbench_row("◎", "Lidar_1", "10 Hz", "◉", false),
            workbench_row("◉", "Camera_1", "30 Hz", "◌", false),
        ],
    });

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

fn logs(state: &UrdfViewerState) -> Vec<WorkbenchLogModel> {
    [
        ("12:45:58", "INFO", "Workbench initialized", "#43d17a"),
        (
            "12:45:59",
            "INFO",
            "Loading PuppyArm model profile",
            "#43d17a",
        ),
        ("12:46:00", "INFO", state.status.as_str(), "#43d17a"),
        ("12:46:02", "INFO", "Virtual ServoBus attached", "#43d17a"),
        (
            "12:46:05",
            "WARN",
            "Calibration offsets are placeholders",
            "#f3b84f",
        ),
    ]
    .into_iter()
    .map(|(time, level, message, color)| WorkbenchLogModel {
        time: time.to_string(),
        level: level.to_string(),
        message: message.to_string(),
        color: color.to_string(),
    })
    .collect()
}

fn script_lines() -> Vec<WorkbenchScriptLineModel> {
    [
        "import rds",
        "from rds import Robot, Pose",
        "",
        "robot = Robot(\"PuppyArm\")",
        "bus = robot.virtual_bus(\"ST3215\")",
        "robot.move_joints(home_pose)",
        "bus.connect_firmware()",
        "robot.watch_telemetry()",
    ]
    .iter()
    .enumerate()
    .map(|(index, text)| WorkbenchScriptLineModel {
        number: index as u32 + 1,
        text: (*text).to_string(),
    })
    .collect()
}

fn plot_series() -> Vec<WorkbenchPropertyModel> {
    [
        ("Shoulder pan", "124 deg"),
        ("Shoulder lift", "82 deg"),
        ("Elbow", "-31 deg"),
        ("Wrist 1", "18 deg"),
        ("Wrist 2", "-44 deg"),
    ]
    .into_iter()
    .map(|(name, value)| workbench_property(name, value))
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

fn sensors() -> Vec<WorkbenchSensorModel> {
    vec![
        workbench_sensor("Joint States", "JS_1", "100 Hz"),
        workbench_sensor("Servo Bus", "ST3215", "1 MHz"),
        workbench_sensor("Camera", "Wrist_Cam", "30 Hz"),
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
            "subtitle": "Wrist_Cam",
            "rate": "30 Hz"
        }),
        "plot" => serde_json::json!({
            "mode": "plot",
            "title": "Joint Positions",
            "series": [
                { "name": "Shoulder Pan", "color": "#2aa7ff" },
                { "name": "Shoulder Lift", "color": "#43d17a" },
                { "name": "Elbow", "color": "#f3b84f" },
                { "name": "Wrist 1", "color": "#ff4f70" },
                { "name": "Wrist 2", "color": "#d45cff" }
            ]
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

        WorkbenchModel {
            project_name: "SmartFactory".to_string(),
            project_version: "v2.4.1 ▾".to_string(),
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
            telemetry_plot_name: "telemetry-plot".to_string(),
            telemetry_plot_props: dashboard_preview_props("plot"),
            viewport_title: "Viewport 1".to_string(),
            file_label: file_label(&self.urdf_state),
            status: self.urdf_state.status.clone(),
            selected_name: "PuppyArm".to_string(),
            selected_type: "URDF robot".to_string(),
            selected_status: if self.urdf_state.robot.is_some() {
                "OK".to_string()
            } else {
                "Missing model".to_string()
            },
            selected_badge: "ARM".to_string(),
            selected_accent: "#1c6ea4".to_string(),
            loaded_summary,
            has_robot: self.urdf_state.robot.is_some(),
            no_movable_joints,
            scene_sections: scene_sections(&self.urdf_state),
            assets: asset_models(),
            logs: logs(&self.urdf_state),
            script_lines: script_lines(),
            plot_series: plot_series(),
            transform_properties: transform_properties(),
            physics_properties: physics_properties(),
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

    pub(crate) fn set_base_control(&mut self, arg: u32, value: i32) {
        let _ =
            apply_urdf_slider_change(&mut self.urdf_state, URDF_BASE_SLIDER_BASE_ID + arg, value);
    }

    pub(crate) fn set_joint_control(&mut self, arg: u32, value: i32) {
        let _ =
            apply_urdf_slider_change(&mut self.urdf_state, URDF_JOINT_SLIDER_BASE_ID + arg, value);
    }
}
