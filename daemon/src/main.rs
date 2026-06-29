#![allow(dead_code)]
#![allow(non_snake_case)]

mod app_controller;
mod physics;
mod projects_controller;
mod urdf;

use clap::{Args as ClapArgs, Parser, Subcommand};
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use crate::app_controller::{AppController, RobotSceneComponent, WorkbenchVirtualBusHandle};
use crate::projects_controller::ProjectsController;
use feetech_servo::servo::sim::FeetechServoSnapshot;
use log::LevelFilter;
use robotdreams_core::{VirtualServoSimConfig, run_virtual_servo_sim};
use roxmltree::Document;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::Mutex as AsyncMutex;
use wgui::*;

const ROBOT_DREAMS_CSS: &str = include_str!("../../wui/robotdreams.css");
const ROBOT_DREAMS_PROJECT_FORMAT: &str = "robotdreams.project.v1";
const URDF_JOINT_SLIDER_BASE_ID: u32 = 30_000;
const URDF_BASE_SLIDER_BASE_ID: u32 = 31_000;

const URDF_VALUE_SCALE: f32 = 1000.0;
const URDF_BASE_POSITION_RANGE: i32 = 2000;
const URDF_BASE_ROTATION_MIN: i32 = (-std::f32::consts::PI * URDF_VALUE_SCALE) as i32;
const URDF_BASE_ROTATION_MAX: i32 = (std::f32::consts::PI * URDF_VALUE_SCALE) as i32;
const URDF_ROTATION_ORDER: &str = "ZYX";
const URDF_TO_VIEW_ROT_X: f32 = -std::f32::consts::FRAC_PI_2;
const STATIC_SCENE_ROOT_ID: u32 = 89;
const ROBOT_BASE_GROUP_ID: u32 = 90;
const PROJECT_SCENE_OBJECT_ID_BASE: u32 = 50_000;
const PROJECT_SCENE_OBJECT_ID_STRIDE: u32 = 10;

#[derive(Debug, Parser)]
#[command(
    name = "robot_dreams",
    about = "Robot Dreams workbench app and virtual servo bus tools"
)]
struct Args {
    #[arg(
        value_name = "PROJECT_OR_URDF_PATH",
        help = "RobotDreams project/model JSON, URDF file, or folder to open in the workbench"
    )]
    path: Option<PathBuf>,

    #[arg(long, default_value = "0.0.0.0:8345")]
    bind: String,

    #[arg(long, default_value = "/tmp/robotdreams-daemon.sock")]
    socket: PathBuf,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run a headless virtual servo bus and print its PTY path.
    Vbus(VbusArgs),
    /// Load a project/model and run the workbench runtime without WGUI.
    Headless(HeadlessArgs),
}

#[derive(Debug, ClapArgs)]
struct VbusArgs {
    #[arg(long, default_value_t = 1)]
    first_servo_id: u8,

    #[arg(long, default_value_t = 6)]
    last_servo_id: u8,

    #[arg(long, default_value_t = 0.005)]
    step_seconds: f32,

    #[arg(long, default_value_t = 2)]
    idle_sleep_ms: u64,
}

#[derive(Debug, ClapArgs)]
struct HeadlessArgs {
    #[arg(
        value_name = "PROJECT_OR_URDF_PATH",
        help = "RobotDreams project/model JSON, URDF file, or folder to load"
    )]
    path: Option<PathBuf>,

    #[arg(long, default_value_t = 1.0)]
    seconds: f32,

    #[arg(long, default_value_t = 30.0)]
    hz: f32,

    #[arg(long)]
    start_virtual_bus: bool,

    #[arg(long)]
    json: bool,
}

#[derive(Clone)]
struct UrdfViewerState {
    urdf_path: Option<PathBuf>,
    workspace_root: PathBuf,
    status: String,
    robot: Option<RobotModel>,
    joint_values: Vec<i32>,
    base_translation_values: [i32; 3],
    base_rotation_values: [i32; 3],
}

impl UrdfViewerState {
    fn new(urdf_path: Option<PathBuf>) -> Self {
        Self {
            urdf_path,
            workspace_root: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            status: "Waiting to load URDF".to_string(),
            robot: None,
            joint_values: Vec::new(),
            base_translation_values: [0, 0, 0],
            base_rotation_values: [0, 0, 0],
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ProjectConfig {
    pub(crate) format: String,
    pub(crate) name: String,
    pub(crate) base_dir: PathBuf,
    pub(crate) scene: ProjectSceneConfig,
    pub(crate) robots: Vec<ProjectRobotConfig>,
    pub(crate) hardware: HardwareConfig,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ProjectSceneConfig {
    pub(crate) objects: Vec<ProjectSceneObjectConfig>,
    pub(crate) cameras: Vec<ProjectCameraConfig>,
}

#[derive(Clone, Debug)]
pub(crate) struct ProjectSceneObjectConfig {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) type_name: String,
    pub(crate) icon: String,
    pub(crate) geometry: ProjectSceneObjectGeometry,
    pub(crate) color_rgb: [u8; 3],
    pub(crate) position: [f32; 3],
    pub(crate) rotation: [f32; 3],
    pub(crate) scale: Option<[f32; 3]>,
    pub(crate) include_in_fit: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct ProjectCameraConfig {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) type_name: String,
    pub(crate) icon: String,
    pub(crate) mounted_robot: String,
    pub(crate) mounted_link: String,
    pub(crate) position: [f32; 3],
    pub(crate) rotation: [f32; 3],
    pub(crate) fov_deg: f32,
    pub(crate) rate: String,
    pub(crate) resolution: Option<[u32; 2]>,
}

#[derive(Clone, Debug)]
pub(crate) enum ProjectSceneObjectGeometry {
    Mesh {
        asset: String,
    },
    Box {
        size: [f32; 3],
    },
    Sphere {
        radius: f32,
    },
    Cylinder {
        radius_top: f32,
        radius_bottom: f32,
        height: f32,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct ProjectRobotConfig {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) model: ProjectRobotModelConfig,
    pub(crate) joint_names: HashMap<String, String>,
    pub(crate) base_translation: [f32; 3],
    pub(crate) base_rotation: [f32; 3],
}

#[derive(Clone, Debug)]
pub(crate) struct ProjectRobotModelConfig {
    pub(crate) type_name: String,
    pub(crate) path: String,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct HardwareConfig {
    pub(crate) buses: Vec<BusConfig>,
}

#[derive(Clone, Debug)]
pub(crate) struct BusConfig {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) transport: BusTransportConfig,
    pub(crate) protocol: String,
    pub(crate) devices: Vec<DeviceConfig>,
}

#[derive(Clone, Debug)]
pub(crate) struct BusTransportConfig {
    pub(crate) type_name: String,
    pub(crate) path: Option<String>,
    pub(crate) baud: Option<u32>,
}

#[derive(Clone, Debug)]
pub(crate) enum DeviceConfig {
    Servo(ServoDeviceConfig),
    Imu(ImuDeviceConfig),
    IoBoard(IoBoardDeviceConfig),
}

#[derive(Clone, Debug)]
pub(crate) struct DeviceMapping {
    pub(crate) robot: String,
    pub(crate) target: String,
}

#[derive(Clone, Debug)]
pub(crate) struct ServoCalibrationConfig {
    pub(crate) zero_offset: i16,
    pub(crate) direction: i8,
}

#[derive(Clone, Debug)]
pub(crate) struct ServoDeviceConfig {
    pub(crate) id: u32,
    pub(crate) name: String,
    pub(crate) profile: String,
    pub(crate) drives: Option<DeviceMapping>,
    pub(crate) calibration: ServoCalibrationConfig,
}

#[derive(Clone, Debug)]
pub(crate) struct ImuDeviceConfig {
    pub(crate) id: u32,
    pub(crate) name: String,
    pub(crate) profile: String,
    pub(crate) mounted_on: Option<DeviceMapping>,
}

#[derive(Clone, Debug)]
pub(crate) struct IoBoardDeviceConfig {
    pub(crate) id: u32,
    pub(crate) name: String,
    pub(crate) profile: String,
}

fn first_urdf_in_dir(dir: &Path) -> Option<PathBuf> {
    if !dir.is_dir() {
        return None;
    }

    let mut candidates = Vec::new();
    let entries = std::fs::read_dir(dir).ok()?;
    for entry in entries.flatten() {
        let path = entry.path();
        if path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.eq_ignore_ascii_case("urdf"))
            .unwrap_or(false)
        {
            candidates.push(path);
        }
    }

    candidates.sort();
    candidates.into_iter().next()
}

fn collect_urdfs_recursive(dir: &Path, candidates: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_urdfs_recursive(&path, candidates);
        } else if path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.eq_ignore_ascii_case("urdf"))
            .unwrap_or(false)
        {
            candidates.push(path);
        }
    }
}

fn first_urdf_recursive(dir: &Path) -> Option<PathBuf> {
    if !dir.is_dir() {
        return None;
    }

    let mut candidates = Vec::new();
    collect_urdfs_recursive(dir, &mut candidates);
    candidates.sort();
    candidates.into_iter().next()
}

fn read_json_file(path: &Path) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let text = std::fs::read_to_string(path)?;
    Ok(serde_json::from_str(&text)?)
}

fn json_string_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a str> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_str()
}

fn json_u32_path(value: &serde_json::Value, path: &[&str]) -> Option<u32> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_u64().and_then(|value| u32::try_from(value).ok())
}

fn json_i16_path(value: &serde_json::Value, path: &[&str]) -> Option<i16> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_i64().and_then(|value| i16::try_from(value).ok())
}

fn json_i8_path(value: &serde_json::Value, path: &[&str]) -> Option<i8> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_i64().and_then(|value| i8::try_from(value).ok())
}

fn json_f32_path(value: &serde_json::Value, path: &[&str]) -> Option<f32> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_f64().map(|value| value as f32)
}

fn json_bool_path(value: &serde_json::Value, path: &[&str]) -> Option<bool> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_bool()
}

fn json_vec2_u32_path(value: &serde_json::Value, path: &[&str]) -> Option<[u32; 2]> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    let values = current.as_array()?;
    if values.len() != 2 {
        return None;
    }
    Some([
        u32::try_from(values[0].as_u64()?).ok()?,
        u32::try_from(values[1].as_u64()?).ok()?,
    ])
}

fn json_vec3_path(value: &serde_json::Value, path: &[&str]) -> Option<[f32; 3]> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    let values = current.as_array()?;
    if values.len() != 3 {
        return None;
    }
    Some([
        values[0].as_f64()? as f32,
        values[1].as_f64()? as f32,
        values[2].as_f64()? as f32,
    ])
}

fn parse_hex_color(value: &str) -> Option<[u8; 3]> {
    let value = value.trim().trim_start_matches('#');
    if value.len() != 6 {
        return None;
    }
    Some([
        u8::from_str_radix(&value[0..2], 16).ok()?,
        u8::from_str_radix(&value[2..4], 16).ok()?,
        u8::from_str_radix(&value[4..6], 16).ok()?,
    ])
}

fn json_color_path(value: &serde_json::Value, path: &[&str]) -> Option<[u8; 3]> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }

    if let Some(color) = current.as_str() {
        return parse_hex_color(color);
    }

    let values = current.as_array()?;
    if values.len() != 3 {
        return None;
    }
    Some([
        u8::try_from(values[0].as_u64()?).ok()?,
        u8::try_from(values[1].as_u64()?).ok()?,
        u8::try_from(values[2].as_u64()?).ok()?,
    ])
}

fn parse_device_mapping(
    value: &serde_json::Value,
    field: &str,
    target: &str,
) -> Option<DeviceMapping> {
    let mapping = value.get(field)?;
    Some(DeviceMapping {
        robot: json_string_path(mapping, &["robot"])
            .unwrap_or("puppyarm")
            .to_string(),
        target: json_string_path(mapping, &[target])?.to_string(),
    })
}

fn parse_servo_calibration(value: &serde_json::Value) -> ServoCalibrationConfig {
    ServoCalibrationConfig {
        zero_offset: json_i16_path(value, &["calibration", "zeroOffset"]).unwrap_or(2048),
        direction: json_i8_path(value, &["calibration", "direction"]).unwrap_or(1),
    }
}

fn parse_device_config(value: &serde_json::Value) -> Option<DeviceConfig> {
    let device_type = json_string_path(value, &["type"])?;
    let id = json_u32_path(value, &["id"])?;
    let name = json_string_path(value, &["name"])
        .map(str::to_string)
        .unwrap_or_else(|| format!("{device_type} {id}"));
    let profile = json_string_path(value, &["profile"])
        .map(str::to_string)
        .unwrap_or_else(|| "custom".to_string());

    match device_type {
        "servo" => Some(DeviceConfig::Servo(ServoDeviceConfig {
            id,
            name,
            profile,
            drives: parse_device_mapping(value, "drives", "joint"),
            calibration: parse_servo_calibration(value),
        })),
        "imu" => Some(DeviceConfig::Imu(ImuDeviceConfig {
            id,
            name,
            profile,
            mounted_on: parse_device_mapping(value, "mountedOn", "link"),
        })),
        "io_board" => Some(DeviceConfig::IoBoard(IoBoardDeviceConfig {
            id,
            name,
            profile,
        })),
        _ => None,
    }
}

fn parse_bus_transport_config(value: &serde_json::Value) -> BusTransportConfig {
    BusTransportConfig {
        type_name: json_string_path(value, &["transport", "type"])
            .unwrap_or("virtual")
            .to_string(),
        path: json_string_path(value, &["transport", "path"]).map(str::to_string),
        baud: json_u32_path(value, &["transport", "baud"]),
    }
}

fn parse_bus_config(value: &serde_json::Value) -> Option<BusConfig> {
    let id = json_string_path(value, &["id"])?.to_string();
    let name = json_string_path(value, &["name"])
        .map(str::to_string)
        .unwrap_or_else(|| id.clone());
    let protocol = json_string_path(value, &["protocol"])
        .unwrap_or("feetech")
        .to_string();
    let devices = value
        .get("devices")
        .and_then(|devices| devices.as_array())
        .map(|devices| devices.iter().filter_map(parse_device_config).collect())
        .unwrap_or_default();

    Some(BusConfig {
        id,
        name,
        transport: parse_bus_transport_config(value),
        protocol,
        devices,
    })
}

fn parse_hardware_config(value: &serde_json::Value) -> HardwareConfig {
    let buses = value
        .get("hardware")
        .and_then(|hardware| hardware.get("buses"))
        .and_then(|buses| buses.as_array())
        .map(|buses| buses.iter().filter_map(parse_bus_config).collect())
        .unwrap_or_default();
    HardwareConfig { buses }
}

fn parse_project_scene_object_config(
    value: &serde_json::Value,
) -> Option<ProjectSceneObjectConfig> {
    let id = json_string_path(value, &["id"])?.to_string();
    let name = json_string_path(value, &["name"])
        .map(str::to_string)
        .unwrap_or_else(|| id.clone());
    let type_name = json_string_path(value, &["type"])
        .unwrap_or("object")
        .to_string();
    let icon = json_string_path(value, &["icon"])
        .unwrap_or("OBJ")
        .to_string();
    let geometry = parse_project_scene_object_geometry(value)?;

    Some(ProjectSceneObjectConfig {
        id,
        name,
        type_name,
        icon,
        geometry,
        color_rgb: json_color_path(value, &["color"])
            .or_else(|| json_color_path(value, &["material", "color"]))
            .unwrap_or([52, 118, 168]),
        position: json_vec3_path(value, &["position"])
            .or_else(|| json_vec3_path(value, &["transform", "position"]))
            .unwrap_or([0.0, 0.0, 0.0]),
        rotation: json_vec3_path(value, &["rotation"])
            .or_else(|| json_vec3_path(value, &["transform", "rotation"]))
            .unwrap_or([0.0, 0.0, 0.0]),
        scale: json_vec3_path(value, &["scale"])
            .or_else(|| json_vec3_path(value, &["transform", "scale"])),
        include_in_fit: json_bool_path(value, &["includeInFit"])
            .or_else(|| json_bool_path(value, &["fit"]))
            .unwrap_or(true),
    })
}

fn parse_project_scene_object_geometry(
    value: &serde_json::Value,
) -> Option<ProjectSceneObjectGeometry> {
    if let Some(asset) = json_string_path(value, &["asset"])
        .or_else(|| json_string_path(value, &["model"]))
        .or_else(|| json_string_path(value, &["path"]))
    {
        return Some(ProjectSceneObjectGeometry::Mesh {
            asset: asset.to_string(),
        });
    }

    let shape = json_string_path(value, &["shape"])
        .or_else(|| json_string_path(value, &["geometry", "shape"]))
        .or_else(|| json_string_path(value, &["geometry", "type"]))
        .or_else(|| json_string_path(value, &["type"]))?;

    match shape {
        "box" | "cube" | "cuboid" | "rectangle" | "rect" => {
            let size = json_vec3_path(value, &["size"])
                .or_else(|| json_vec3_path(value, &["dimensions"]))
                .or_else(|| json_vec3_path(value, &["geometry", "size"]))
                .or_else(|| json_vec3_path(value, &["geometry", "dimensions"]))
                .unwrap_or_else(|| {
                    let width = json_f32_path(value, &["width"])
                        .or_else(|| json_f32_path(value, &["geometry", "width"]))
                        .unwrap_or(1.0);
                    let height = json_f32_path(value, &["height"])
                        .or_else(|| json_f32_path(value, &["geometry", "height"]))
                        .unwrap_or(width);
                    let default_depth = if matches!(shape, "rectangle" | "rect") {
                        0.02
                    } else {
                        width
                    };
                    let depth = json_f32_path(value, &["depth"])
                        .or_else(|| json_f32_path(value, &["geometry", "depth"]))
                        .unwrap_or(default_depth);
                    [width, height, depth]
                });
            Some(ProjectSceneObjectGeometry::Box { size })
        }
        "sphere" => Some(ProjectSceneObjectGeometry::Sphere {
            radius: json_f32_path(value, &["radius"])
                .or_else(|| json_f32_path(value, &["geometry", "radius"]))
                .unwrap_or(0.5),
        }),
        "cylinder" => {
            let radius = json_f32_path(value, &["radius"])
                .or_else(|| json_f32_path(value, &["geometry", "radius"]))
                .unwrap_or(0.5);
            Some(ProjectSceneObjectGeometry::Cylinder {
                radius_top: json_f32_path(value, &["radiusTop"])
                    .or_else(|| json_f32_path(value, &["geometry", "radiusTop"]))
                    .unwrap_or(radius),
                radius_bottom: json_f32_path(value, &["radiusBottom"])
                    .or_else(|| json_f32_path(value, &["geometry", "radiusBottom"]))
                    .unwrap_or(radius),
                height: json_f32_path(value, &["height"])
                    .or_else(|| json_f32_path(value, &["geometry", "height"]))
                    .unwrap_or(1.0),
            })
        }
        _ => None,
    }
}

fn parse_project_camera_config(value: &serde_json::Value) -> Option<ProjectCameraConfig> {
    let id = json_string_path(value, &["id"])?.to_string();
    let name = json_string_path(value, &["name"])
        .map(str::to_string)
        .unwrap_or_else(|| id.clone());
    let type_name = json_string_path(value, &["type"])
        .unwrap_or("camera")
        .to_string();
    let icon = json_string_path(value, &["icon"])
        .unwrap_or("CAM")
        .to_string();
    let mounted_link = json_string_path(value, &["mountedOn", "link"])
        .or_else(|| json_string_path(value, &["link"]))
        .or_else(|| json_string_path(value, &["frame"]))?
        .to_string();

    Some(ProjectCameraConfig {
        id,
        name,
        type_name,
        icon,
        mounted_robot: json_string_path(value, &["mountedOn", "robot"])
            .or_else(|| json_string_path(value, &["robot"]))
            .unwrap_or("puppyarm")
            .to_string(),
        mounted_link,
        position: json_vec3_path(value, &["position"])
            .or_else(|| json_vec3_path(value, &["transform", "position"]))
            .unwrap_or([0.0, 0.0, 0.0]),
        rotation: json_vec3_path(value, &["rotation"])
            .or_else(|| json_vec3_path(value, &["transform", "rotation"]))
            .unwrap_or([0.0, 0.0, 0.0]),
        fov_deg: json_f32_path(value, &["fov"])
            .or_else(|| json_f32_path(value, &["fovDeg"]))
            .or_else(|| json_f32_path(value, &["camera", "fov"]))
            .unwrap_or(58.0),
        rate: json_string_path(value, &["rate"])
            .unwrap_or("30 Hz")
            .to_string(),
        resolution: json_vec2_u32_path(value, &["resolution"])
            .or_else(|| json_vec2_u32_path(value, &["camera", "resolution"])),
    })
}

fn parse_project_scene_config(value: &serde_json::Value) -> ProjectSceneConfig {
    let objects = value
        .get("scene")
        .and_then(|scene| scene.get("objects"))
        .and_then(|objects| objects.as_array())
        .map(|objects| {
            objects
                .iter()
                .filter_map(parse_project_scene_object_config)
                .collect()
        })
        .unwrap_or_default();
    let cameras = value
        .get("scene")
        .and_then(|scene| scene.get("cameras"))
        .and_then(|cameras| cameras.as_array())
        .map(|cameras| {
            cameras
                .iter()
                .filter_map(parse_project_camera_config)
                .collect()
        })
        .unwrap_or_default();
    ProjectSceneConfig { objects, cameras }
}

fn parse_project_robot_model_config(value: &serde_json::Value) -> Option<ProjectRobotModelConfig> {
    let model = value.get("model")?;
    Some(ProjectRobotModelConfig {
        type_name: json_string_path(model, &["type"])
            .unwrap_or("urdf")
            .to_string(),
        path: json_string_path(model, &["path"])?.to_string(),
    })
}

fn parse_project_joint_names(value: &serde_json::Value) -> HashMap<String, String> {
    let mut joint_names = HashMap::new();

    if let Some(names) = value.get("jointNames").and_then(|names| names.as_object()) {
        for (urdf_name, semantic_name) in names {
            if let Some(semantic_name) = semantic_name.as_str() {
                joint_names.insert(urdf_name.clone(), semantic_name.to_string());
            }
        }
    }

    if let Some(joints) = value.get("joints").and_then(|joints| joints.as_array()) {
        for joint in joints {
            let Some(urdf_name) =
                json_string_path(joint, &["urdf"]).or_else(|| json_string_path(joint, &["joint"]))
            else {
                continue;
            };
            let Some(semantic_name) =
                json_string_path(joint, &["name"]).or_else(|| json_string_path(joint, &["label"]))
            else {
                continue;
            };
            joint_names.insert(urdf_name.to_string(), semantic_name.to_string());
        }
    }

    joint_names
}

fn parse_project_robot_config(value: &serde_json::Value) -> Option<ProjectRobotConfig> {
    let id = json_string_path(value, &["id"])?.to_string();
    let name = json_string_path(value, &["name"])
        .map(str::to_string)
        .unwrap_or_else(|| id.clone());
    Some(ProjectRobotConfig {
        id,
        name,
        model: parse_project_robot_model_config(value)?,
        joint_names: parse_project_joint_names(value),
        base_translation: json_vec3_path(value, &["base", "translation"])
            .or_else(|| json_vec3_path(value, &["base", "position"]))
            .unwrap_or([0.0, 0.0, 0.0]),
        base_rotation: json_vec3_path(value, &["base", "rotation"]).unwrap_or([0.0, 0.0, 0.0]),
    })
}

fn parse_project_config(value: &serde_json::Value, base_dir: PathBuf) -> Option<ProjectConfig> {
    let format = json_string_path(value, &["format"])?;
    if format != ROBOT_DREAMS_PROJECT_FORMAT {
        return None;
    }

    let name = json_string_path(value, &["name"])
        .unwrap_or("RobotDreams Project")
        .to_string();
    let robots = value
        .get("robots")
        .and_then(|robots| robots.as_array())
        .map(|robots| {
            robots
                .iter()
                .filter_map(parse_project_robot_config)
                .collect()
        })
        .unwrap_or_default();

    Some(ProjectConfig {
        format: format.to_string(),
        name,
        base_dir,
        scene: parse_project_scene_config(value),
        robots,
        hardware: parse_hardware_config(value),
    })
}

fn project_config_from_manifest(path: &Path) -> Option<ProjectConfig> {
    let json = read_json_file(path).ok()?;
    let base_dir = path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    parse_project_config(&json, base_dir)
}

fn project_manifest_for_input_path(path: &Path) -> Option<PathBuf> {
    if path.is_file()
        && path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.eq_ignore_ascii_case("json"))
            .unwrap_or(false)
    {
        let json = read_json_file(path).ok()?;
        if json_string_path(&json, &["format"]) == Some(ROBOT_DREAMS_PROJECT_FORMAT) {
            return Some(path.to_path_buf());
        }
    }

    if path.is_dir() {
        for candidate in [
            path.join("project.json"),
            path.join("robotdreams.json"),
            path.join("robotdreams.example.json"),
        ] {
            if project_config_from_manifest(&candidate).is_some() {
                return Some(candidate);
            }
        }
    }

    None
}

fn resolve_json_urdf_path(path: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let json = read_json_file(path)?;
    let base = path.parent().unwrap_or_else(|| Path::new("."));

    if json_string_path(&json, &["format"]) == Some(ROBOT_DREAMS_PROJECT_FORMAT) {
        if let Some(model_profile) = json_string_path(&json, &["modelProfile"]) {
            return resolve_robot_input_path(base.join(model_profile));
        }

        if let Some(urdf_path) = json
            .get("robots")
            .and_then(|robots| robots.as_array())
            .and_then(|robots| robots.first())
            .and_then(|robot| robot.get("model"))
            .and_then(|model| {
                let model_type = model
                    .get("type")
                    .and_then(|value| value.as_str())
                    .unwrap_or("urdf");
                model_type
                    .eq_ignore_ascii_case("urdf")
                    .then(|| model.get("path").and_then(|value| value.as_str()))
                    .flatten()
            })
        {
            return Ok(base.join(urdf_path));
        }

        return Err(format!(
            "RobotDreams project '{}' is missing modelProfile or robots[].model.path",
            path.display()
        )
        .into());
    }

    if let Some(model_profile) = json_string_path(&json, &["modelProfile"]) {
        return resolve_robot_input_path(base.join(model_profile));
    }

    if let Some(model_profile) = json
        .get("scene")
        .and_then(|scene| scene.get("robots"))
        .and_then(|robots| robots.as_array())
        .and_then(|robots| robots.first())
        .and_then(|robot| robot.get("model"))
        .and_then(|model| model.as_str())
    {
        return resolve_robot_input_path(base.join(model_profile));
    }

    let model_type = json_string_path(&json, &["robot", "model", "type"]);
    if model_type.is_none_or(|value| value.eq_ignore_ascii_case("urdf"))
        && let Some(urdf_path) = json_string_path(&json, &["robot", "model", "path"])
    {
        return Ok(base.join(urdf_path));
    }

    Err(format!(
        "JSON file '{}' is not a recognized RobotDreams project or model profile",
        path.display()
    )
    .into())
}

fn resolve_robot_input_path(path: PathBuf) -> Result<PathBuf, Box<dyn std::error::Error>> {
    if path.is_dir() {
        for candidate in [
            path.join("project.json"),
            path.join("robotdreams.json"),
            path.join("robotdreams.example.json"),
            path.join("model/robotdreams.json"),
        ] {
            if candidate.exists() {
                return resolve_json_urdf_path(&candidate);
            }
        }

        if let Some(first) = first_urdf_recursive(&path) {
            return Ok(first);
        }
    }

    if path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("json"))
        .unwrap_or(false)
    {
        return resolve_json_urdf_path(&path);
    }

    Ok(path)
}

fn resolve_urdf_path(path: Option<PathBuf>) -> Result<PathBuf, Box<dyn std::error::Error>> {
    if let Some(path) = path {
        return resolve_robot_input_path(path);
    }

    if let Some(first) = first_urdf_recursive(Path::new("models")) {
        return Ok(first);
    }

    if let Some(first) = first_urdf_recursive(Path::new("examples")) {
        return Ok(first);
    }

    for dir in [Path::new("model")] {
        if let Some(first) = first_urdf_in_dir(dir) {
            return Ok(first);
        }
    }

    Err("No URDF path provided and no .urdf file found in ./models, ./examples, or ./model".into())
}

fn project_config_for_input_path(path: Option<&Path>) -> Option<ProjectConfig> {
    let manifest_path = match path {
        Some(path) => project_manifest_for_input_path(path)?,
        None => project_manifest_for_input_path(Path::new("."))?,
    };
    project_config_from_manifest(&manifest_path)
}

#[derive(Clone)]
struct LinkVisual {
    origin_xyz: [f32; 3],
    origin_rpy: [f32; 3],
    color_rgb: [u8; 3],
    geometry: VisualGeometry,
}

#[derive(Clone)]
enum VisualGeometry {
    Mesh { src: String, scale: [f32; 3] },
    Box { size: [f32; 3] },
    Cylinder { radius: f32, length: f32 },
    Sphere { radius: f32 },
}

#[derive(Clone)]
struct LinkDef {
    name: String,
    visuals: Vec<LinkVisual>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum UrdfJointType {
    Fixed,
    Revolute,
    Continuous,
    Prismatic,
    Other,
}

#[derive(Clone)]
struct JointDef {
    name: String,
    joint_type: UrdfJointType,
    parent: String,
    child: String,
    origin_xyz: [f32; 3],
    origin_rpy: [f32; 3],
    axis: [f32; 3],
    lower: Option<f32>,
    upper: Option<f32>,
}

#[derive(Clone)]
struct RobotModel {
    links: HashMap<String, LinkDef>,
    children_by_parent: HashMap<String, Vec<usize>>,
    joints: Vec<JointDef>,
    roots: Vec<String>,
    movable_joint_indices: Vec<usize>,
}

fn parse_vec3(input: Option<&str>) -> [f32; 3] {
    let mut out = [0.0f32, 0.0, 0.0];
    let Some(raw) = input else {
        return out;
    };
    for (index, part) in raw.split_whitespace().take(3).enumerate() {
        if let Ok(value) = part.parse::<f32>() {
            out[index] = value;
        }
    }
    out
}

fn parse_vec3_or(input: Option<&str>, default: [f32; 3]) -> [f32; 3] {
    match input {
        Some(raw) => {
            let parsed = parse_vec3(Some(raw));
            if parsed == [0.0, 0.0, 0.0] && default != [0.0, 0.0, 0.0] {
                default
            } else {
                parsed
            }
        }
        None => default,
    }
}

fn parse_f32_or(input: Option<&str>, default: f32) -> f32 {
    match input.and_then(|s| s.parse::<f32>().ok()) {
        Some(value) => value,
        None => default,
    }
}

fn parse_color_rgb(visual_node: roxmltree::Node<'_, '_>) -> [u8; 3] {
    let rgba = visual_node
        .children()
        .find(|n| n.has_tag_name("material"))
        .and_then(|mat| mat.children().find(|n| n.has_tag_name("color")))
        .and_then(|color| color.attribute("rgba"));

    let Some(rgba) = rgba else {
        return [190, 190, 205];
    };
    let parts: Vec<f32> = rgba
        .split_whitespace()
        .filter_map(|part| part.parse::<f32>().ok())
        .collect();
    if parts.len() < 3 {
        return [190, 190, 205];
    }

    let to_u8 = |v: f32| -> u8 { (v.clamp(0.0, 1.0) * 255.0).round() as u8 };
    [to_u8(parts[0]), to_u8(parts[1]), to_u8(parts[2])]
}

fn parse_joint_type(input: &str) -> UrdfJointType {
    match input {
        "fixed" => UrdfJointType::Fixed,
        "revolute" => UrdfJointType::Revolute,
        "continuous" => UrdfJointType::Continuous,
        "prismatic" => UrdfJointType::Prismatic,
        _ => UrdfJointType::Other,
    }
}

fn path_to_assets_url(path: &Path) -> Option<String> {
    let mut saw_assets = false;
    let mut rest = Vec::<String>::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => {
                let text = part.to_string_lossy().to_string();
                if !saw_assets {
                    if text == "assets" {
                        saw_assets = true;
                    }
                    continue;
                }
                rest.push(text);
            }
            Component::CurDir => {}
            Component::ParentDir => return None,
            Component::RootDir | Component::Prefix(_) => {}
        }
    }

    if !saw_assets || rest.is_empty() {
        None
    } else {
        Some(format!("/assets/{}", rest.join("/")))
    }
}

fn path_to_workspace_url(path: &Path, workspace_root: &Path) -> Option<String> {
    let relative = path.strip_prefix(workspace_root).ok()?;
    let mut rest = Vec::<String>::new();
    for component in relative.components() {
        match component {
            Component::Normal(part) => rest.push(part.to_string_lossy().to_string()),
            Component::CurDir => {}
            Component::ParentDir => return None,
            Component::RootDir | Component::Prefix(_) => return None,
        }
    }

    if rest.is_empty() {
        None
    } else {
        Some(format!("/fs/{}", rest.join("/")))
    }
}

fn parse_mesh_src(urdf_dir: &Path, workspace_root: &Path, filename: &str) -> Option<String> {
    let mut package_path: Option<PathBuf> = None;
    let trimmed = if let Some(without_scheme) = filename.strip_prefix("package://") {
        let mut parts = without_scheme.splitn(2, '/');
        let _package_name = parts.next();
        if let Some(rest) = parts.next() {
            package_path = Some(PathBuf::from(rest));
            rest
        } else {
            without_scheme
        }
    } else {
        filename
    };

    let mesh_path = Path::new(trimmed);
    let mut candidates = Vec::<PathBuf>::new();
    if mesh_path.is_absolute() {
        candidates.push(mesh_path.to_path_buf());
    } else {
        candidates.push(urdf_dir.join(mesh_path));
        if let Some(file_name) = mesh_path.file_name() {
            candidates.push(urdf_dir.join(file_name));
            candidates.push(urdf_dir.join("meshes").join(file_name));
        }
        if let Some(parent) = urdf_dir.parent() {
            candidates.push(parent.join(mesh_path));
        }
    }

    if let Some(package_rel) = package_path {
        candidates.push(urdf_dir.join(&package_rel));
        if let Some(parent) = urdf_dir.parent() {
            candidates.push(parent.join(&package_rel));
        }
        if let Some(file_name) = package_rel.file_name() {
            candidates.push(urdf_dir.join(file_name));
        }
    }

    for candidate in candidates {
        if let Ok(canonical) = std::fs::canonicalize(&candidate) {
            if let Some(url) = path_to_assets_url(&canonical) {
                return Some(url);
            }
            if let Some(url) = path_to_workspace_url(&canonical, workspace_root) {
                return Some(url);
            }
        } else if candidate.exists() {
            if let Some(url) = path_to_assets_url(&candidate) {
                return Some(url);
            }
            if let Some(url) = path_to_workspace_url(&candidate, workspace_root) {
                return Some(url);
            }
        }
    }

    None
}

fn parse_robot_from_xml(
    xml: &str,
    urdf_dir: &Path,
    workspace_root: &Path,
) -> Result<RobotModel, String> {
    let doc = Document::parse(xml).map_err(|err| format!("failed to parse URDF XML: {err}"))?;

    let mut links: HashMap<String, LinkDef> = HashMap::new();
    for link_node in doc.descendants().filter(|n| n.has_tag_name("link")) {
        let Some(name) = link_node.attribute("name") else {
            continue;
        };

        let mut visuals = Vec::new();
        for visual_node in link_node.children().filter(|n| n.has_tag_name("visual")) {
            let origin_node = visual_node.children().find(|n| n.has_tag_name("origin"));
            let origin_xyz = parse_vec3(origin_node.and_then(|n| n.attribute("xyz")));
            let origin_rpy = parse_vec3(origin_node.and_then(|n| n.attribute("rpy")));
            let color_rgb = parse_color_rgb(visual_node);

            let geometry_node = visual_node.children().find(|n| n.has_tag_name("geometry"));
            let Some(geometry_node) = geometry_node else {
                continue;
            };

            let geometry = if let Some(mesh_node) =
                geometry_node.children().find(|n| n.has_tag_name("mesh"))
            {
                let Some(mesh_filename) = mesh_node.attribute("filename") else {
                    continue;
                };
                let mesh_scale = parse_vec3_or(mesh_node.attribute("scale"), [1.0, 1.0, 1.0]);
                let Some(mesh_src) = parse_mesh_src(urdf_dir, workspace_root, mesh_filename) else {
                    continue;
                };
                VisualGeometry::Mesh {
                    src: mesh_src,
                    scale: mesh_scale,
                }
            } else if let Some(box_node) = geometry_node.children().find(|n| n.has_tag_name("box"))
            {
                VisualGeometry::Box {
                    size: parse_vec3_or(box_node.attribute("size"), [0.05, 0.05, 0.05]),
                }
            } else if let Some(cylinder_node) = geometry_node
                .children()
                .find(|n| n.has_tag_name("cylinder"))
            {
                VisualGeometry::Cylinder {
                    radius: parse_f32_or(cylinder_node.attribute("radius"), 0.05),
                    length: parse_f32_or(cylinder_node.attribute("length"), 0.1),
                }
            } else if let Some(sphere_node) =
                geometry_node.children().find(|n| n.has_tag_name("sphere"))
            {
                VisualGeometry::Sphere {
                    radius: parse_f32_or(sphere_node.attribute("radius"), 0.05),
                }
            } else {
                continue;
            };

            visuals.push(LinkVisual {
                origin_xyz,
                origin_rpy,
                color_rgb,
                geometry,
            });
        }

        links.insert(
            name.to_string(),
            LinkDef {
                name: name.to_string(),
                visuals,
            },
        );
    }

    let mut joints = Vec::new();
    for joint_node in doc.descendants().filter(|n| n.has_tag_name("joint")) {
        let Some(name) = joint_node.attribute("name") else {
            continue;
        };
        let joint_type = parse_joint_type(joint_node.attribute("type").unwrap_or("fixed"));
        let parent = joint_node
            .children()
            .find(|n| n.has_tag_name("parent"))
            .and_then(|n| n.attribute("link"))
            .unwrap_or("world")
            .to_string();
        let Some(child) = joint_node
            .children()
            .find(|n| n.has_tag_name("child"))
            .and_then(|n| n.attribute("link"))
            .map(|s| s.to_string())
        else {
            continue;
        };
        let origin_node = joint_node.children().find(|n| n.has_tag_name("origin"));
        let origin_xyz = parse_vec3(origin_node.and_then(|n| n.attribute("xyz")));
        let origin_rpy = parse_vec3(origin_node.and_then(|n| n.attribute("rpy")));
        let axis = parse_vec3(
            joint_node
                .children()
                .find(|n| n.has_tag_name("axis"))
                .and_then(|n| n.attribute("xyz")),
        );

        let limit_node = joint_node.children().find(|n| n.has_tag_name("limit"));
        let lower = limit_node
            .and_then(|n| n.attribute("lower"))
            .and_then(|s| s.parse::<f32>().ok());
        let upper = limit_node
            .and_then(|n| n.attribute("upper"))
            .and_then(|s| s.parse::<f32>().ok());

        joints.push(JointDef {
            name: name.to_string(),
            joint_type,
            parent,
            child,
            origin_xyz,
            origin_rpy,
            axis,
            lower,
            upper,
        });
    }

    let mut children_by_parent: HashMap<String, Vec<usize>> = HashMap::new();
    let mut has_parent: HashSet<String> = HashSet::new();
    for (joint_index, joint) in joints.iter().enumerate() {
        children_by_parent
            .entry(joint.parent.clone())
            .or_default()
            .push(joint_index);
        has_parent.insert(joint.child.clone());
    }

    let mut roots: Vec<String> = links
        .keys()
        .filter(|name| !has_parent.contains(*name))
        .cloned()
        .collect();
    roots.sort();
    if let Some(world_children) = children_by_parent.get("world") {
        for idx in world_children {
            if !roots.iter().any(|name| name == &joints[*idx].child) {
                roots.push(joints[*idx].child.clone());
            }
        }
    }

    let movable_joint_indices = joints
        .iter()
        .enumerate()
        .filter_map(|(index, joint)| {
            (joint.joint_type == UrdfJointType::Revolute
                || joint.joint_type == UrdfJointType::Continuous
                || joint.joint_type == UrdfJointType::Prismatic)
                .then_some(index)
        })
        .collect();

    Ok(RobotModel {
        links,
        children_by_parent,
        joints,
        roots,
        movable_joint_indices,
    })
}

fn parse_robot(urdf_path: &Path, workspace_root: &Path) -> Result<RobotModel, String> {
    let xml = std::fs::read_to_string(urdf_path)
        .map_err(|err| format!("failed to read URDF {}: {err}", urdf_path.display()))?;
    let urdf_dir = urdf_path.parent().unwrap_or(Path::new("."));
    parse_robot_from_xml(&xml, urdf_dir, workspace_root)
}

fn reload_urdf_state(state: &mut UrdfViewerState) {
    if state.urdf_path.is_none() {
        state.urdf_path = resolve_urdf_path(None).ok();
    }

    let Some(path) = state.urdf_path.as_ref() else {
        state.status = "No URDF found in ./models, ./examples, or ./model".to_string();
        state.robot = None;
        return;
    };

    match parse_robot(path, &state.workspace_root) {
        Ok(robot) => {
            let mut next_joint_values = vec![0; robot.joints.len()];
            for (dst, src) in next_joint_values.iter_mut().zip(state.joint_values.iter()) {
                *dst = *src;
            }
            state.joint_values = next_joint_values;
            state.status = format!(
                "Loaded {} ({} links, {} joints)",
                path.display(),
                robot.links.len(),
                robot.joints.len()
            );
            state.robot = Some(robot);
        }
        Err(err) => {
            state.status = format!("Failed to load {}: {}", path.display(), err);
            state.robot = None;
        }
    }
}

pub(crate) fn project_joint_name<'a>(
    project_config: Option<&'a ProjectConfig>,
    urdf_joint_name: &str,
) -> Option<&'a str> {
    project_config?
        .robots
        .iter()
        .find_map(|robot| robot.joint_names.get(urdf_joint_name).map(String::as_str))
}

pub(crate) fn joint_display_label(
    project_config: Option<&ProjectConfig>,
    urdf_joint_name: &str,
) -> String {
    match project_joint_name(project_config, urdf_joint_name) {
        Some(name) if name != urdf_joint_name => format!("{name} ({urdf_joint_name})"),
        _ => urdf_joint_name.to_string(),
    }
}

fn axis_to_euler(axis: [f32; 3], value: f32) -> [f32; 3] {
    let eps = 0.2;
    if axis[0].abs() > 1.0 - eps && axis[1].abs() < eps && axis[2].abs() < eps {
        return [value * axis[0].signum(), 0.0, 0.0];
    }
    if axis[1].abs() > 1.0 - eps && axis[0].abs() < eps && axis[2].abs() < eps {
        return [0.0, value * axis[1].signum(), 0.0];
    }
    if axis[2].abs() > 1.0 - eps && axis[0].abs() < eps && axis[1].abs() < eps {
        return [0.0, 0.0, value * axis[2].signum()];
    }
    [0.0, 0.0, value]
}

fn urdf_slider_value_to_units(slider_value: i32) -> f32 {
    slider_value as f32 / URDF_VALUE_SCALE
}

fn urdf_joint_slider_range(joint: &JointDef) -> (i32, i32) {
    match joint.joint_type {
        UrdfJointType::Revolute => {
            let min = joint.lower.unwrap_or(-std::f32::consts::PI);
            let max = joint.upper.unwrap_or(std::f32::consts::PI);
            (
                (min * URDF_VALUE_SCALE) as i32,
                (max * URDF_VALUE_SCALE) as i32,
            )
        }
        UrdfJointType::Continuous => (
            (-std::f32::consts::PI * URDF_VALUE_SCALE) as i32,
            (std::f32::consts::PI * URDF_VALUE_SCALE) as i32,
        ),
        UrdfJointType::Prismatic => {
            let min = joint.lower.unwrap_or(-0.05);
            let max = joint.upper.unwrap_or(0.05);
            (
                (min * URDF_VALUE_SCALE) as i32,
                (max * URDF_VALUE_SCALE) as i32,
            )
        }
        _ => (0, 0),
    }
}

fn urdf_value_to_joint_units(joint: &JointDef, slider_value: i32) -> f32 {
    match joint.joint_type {
        UrdfJointType::Revolute | UrdfJointType::Continuous | UrdfJointType::Prismatic => {
            urdf_slider_value_to_units(slider_value)
        }
        _ => 0.0,
    }
}

fn urdf_joint_type_name(joint_type: UrdfJointType) -> &'static str {
    match joint_type {
        UrdfJointType::Fixed => "fixed",
        UrdfJointType::Revolute => "revolute",
        UrdfJointType::Continuous => "continuous",
        UrdfJointType::Prismatic => "prismatic",
        UrdfJointType::Other => "other",
    }
}

fn servo_ticks_to_radians(ticks: i16) -> f32 {
    let ticks = ticks.clamp(0, 4095) as f32;
    (ticks - 2048.0) * (std::f32::consts::TAU / 4096.0)
}

pub(crate) fn servo_ticks_to_slider_value(ticks: i16, min: i32, max: i32) -> i32 {
    let ticks = ticks.clamp(0, 4095) as f32;
    let t = ticks / 4095.0;
    (min as f32 + (max - min) as f32 * t).round() as i32
}

pub(crate) fn slider_value_to_servo_ticks(slider_value: i32, min: i32, max: i32) -> i16 {
    if max <= min {
        return 0;
    }

    let t = (slider_value - min) as f32 / (max - min) as f32;
    (t.clamp(0.0, 1.0) * 4095.0).round() as i16
}

fn urdf_slider_servo_target(
    state: &UrdfViewerState,
    slider_id: u32,
    value: i32,
) -> Option<(u8, i16)> {
    if slider_id < URDF_JOINT_SLIDER_BASE_ID {
        return None;
    }

    let robot = state.robot.as_ref()?;
    let slot = (slider_id - URDF_JOINT_SLIDER_BASE_ID) as usize;
    let joint_index = robot.movable_joint_indices.get(slot)?;
    let (min, max) = urdf_joint_slider_range(&robot.joints[*joint_index]);
    let servo_id = u8::try_from(slot + 1).ok()?;
    Some((servo_id, slider_value_to_servo_ticks(value, min, max)))
}

fn sync_urdf_with_servo_snapshots(state: &mut UrdfViewerState, snapshots: &[FeetechServoSnapshot]) {
    if snapshots.is_empty() {
        return;
    }

    let Some(robot) = state.robot.as_ref() else {
        return;
    };

    if !robot.movable_joint_indices.is_empty() {
        let ranges = robot
            .movable_joint_indices
            .iter()
            .map(|joint_index| {
                let (min, max) = urdf_joint_slider_range(&robot.joints[*joint_index]);
                (*joint_index, min, max)
            })
            .collect::<Vec<_>>();

        let count = snapshots.len().min(ranges.len());
        for slot in 0..count {
            let (joint_index, min, max) = ranges[slot];
            if let Some(value) = state.joint_values.get_mut(joint_index) {
                *value = servo_ticks_to_slider_value(snapshots[slot].present_position, min, max);
            }
        }
        return;
    }

    // Fallback for fixed-joint URDFs: mirror the first servo positions to model pose.
    if let Some(s1) = snapshots.get(0) {
        state.base_rotation_values[2] =
            (servo_ticks_to_radians(s1.present_position) * URDF_VALUE_SCALE).round() as i32;
    }
    if let Some(s2) = snapshots.get(1) {
        state.base_rotation_values[1] =
            (servo_ticks_to_radians(s2.present_position) * URDF_VALUE_SCALE).round() as i32;
    }
    if let Some(s3) = snapshots.get(2) {
        state.base_rotation_values[0] =
            (servo_ticks_to_radians(s3.present_position) * URDF_VALUE_SCALE).round() as i32;
    }
    if let Some(s4) = snapshots.get(3) {
        let z = (servo_ticks_to_radians(s4.present_position) / std::f32::consts::PI
            * URDF_BASE_POSITION_RANGE as f32
            * 0.25)
            .round() as i32;
        state.base_translation_values[2] =
            z.clamp(-URDF_BASE_POSITION_RANGE, URDF_BASE_POSITION_RANGE);
    }
}

fn push_link_visuals(link: &LinkDef, id_gen: &mut u32, children: &mut Vec<ThreeNode>) {
    for visual in &link.visuals {
        *id_gen += 1;
        let mesh_id = *id_gen;
        *id_gen += 1;
        let geom_id = *id_gen;
        *id_gen += 1;
        let mat_id = *id_gen;
        *id_gen += 1;
        let visual_group_id = *id_gen;

        let geometry_node = match &visual.geometry {
            VisualGeometry::Mesh { src, .. } => {
                stl_geometry(geom_id).prop("src", ThreePropValue::String { value: src.clone() })
            }
            VisualGeometry::Box { size } => box_geometry(geom_id)
                .prop("width", ThreePropValue::Number { value: size[0] })
                .prop("height", ThreePropValue::Number { value: size[1] })
                .prop("depth", ThreePropValue::Number { value: size[2] }),
            VisualGeometry::Cylinder { radius, length } => cylinder_geometry(geom_id)
                .prop("radiusTop", ThreePropValue::Number { value: *radius })
                .prop("radiusBottom", ThreePropValue::Number { value: *radius })
                .prop("height", ThreePropValue::Number { value: *length }),
            VisualGeometry::Sphere { radius } => {
                sphere_geometry(geom_id).prop("radius", ThreePropValue::Number { value: *radius })
            }
        };

        let mut mesh_node = mesh(
            mesh_id,
            [
                geometry_node,
                mesh_standard_material(mat_id)
                    .prop(
                        "color",
                        ThreePropValue::Color {
                            r: visual.color_rgb[0],
                            g: visual.color_rgb[1],
                            b: visual.color_rgb[2],
                            a: None,
                        },
                    )
                    .prop("metalness", ThreePropValue::Number { value: 0.15 })
                    .prop("roughness", ThreePropValue::Number { value: 0.7 }),
            ],
        );

        if let VisualGeometry::Cylinder { .. } = &visual.geometry {
            mesh_node = mesh_node.prop(
                "rotation",
                ThreePropValue::Vec3 {
                    x: std::f32::consts::FRAC_PI_2,
                    y: 0.0,
                    z: 0.0,
                },
            );
        }
        if let VisualGeometry::Mesh { scale, .. } = &visual.geometry {
            mesh_node = mesh_node.prop(
                "scale",
                ThreePropValue::Vec3 {
                    x: scale[0],
                    y: scale[1],
                    z: scale[2],
                },
            );
        }

        children.push(
            group(visual_group_id, [mesh_node])
                .prop(
                    "position",
                    ThreePropValue::Vec3 {
                        x: visual.origin_xyz[0],
                        y: visual.origin_xyz[1],
                        z: visual.origin_xyz[2],
                    },
                )
                .prop(
                    "rotation",
                    ThreePropValue::Vec3 {
                        x: visual.origin_rpy[0],
                        y: visual.origin_rpy[1],
                        z: visual.origin_rpy[2],
                    },
                )
                .prop(
                    "rotationOrder",
                    ThreePropValue::String {
                        value: URDF_ROTATION_ORDER.to_string(),
                    },
                ),
        );
    }
}

fn next_three_id(id_gen: &mut u32) -> u32 {
    *id_gen += 1;
    *id_gen
}

fn push_link_cameras(
    project_config: Option<&ProjectConfig>,
    link_name: &str,
    id_gen: &mut u32,
    children: &mut Vec<ThreeNode>,
) {
    let Some(project_config) = project_config else {
        return;
    };

    for camera in project_config
        .scene
        .cameras
        .iter()
        .filter(|camera| camera.mounted_link == link_name)
    {
        let group_id = next_three_id(id_gen);
        let body_id = next_three_id(id_gen);
        let body_geom_id = next_three_id(id_gen);
        let body_mat_id = next_three_id(id_gen);
        let lens_id = next_three_id(id_gen);
        let lens_geom_id = next_three_id(id_gen);
        let lens_mat_id = next_three_id(id_gen);

        let body = mesh(
            body_id,
            [
                box_geometry(body_geom_id)
                    .prop("width", ThreePropValue::Number { value: 0.032 })
                    .prop("height", ThreePropValue::Number { value: 0.022 })
                    .prop("depth", ThreePropValue::Number { value: 0.018 }),
                mesh_standard_material(body_mat_id)
                    .prop(
                        "color",
                        ThreePropValue::Color {
                            r: 43,
                            g: 167,
                            b: 255,
                            a: None,
                        },
                    )
                    .prop("metalness", ThreePropValue::Number { value: 0.18 })
                    .prop("roughness", ThreePropValue::Number { value: 0.45 }),
            ],
        );
        let lens = mesh(
            lens_id,
            [
                cylinder_geometry(lens_geom_id)
                    .prop("radiusTop", ThreePropValue::Number { value: 0.006 })
                    .prop("radiusBottom", ThreePropValue::Number { value: 0.008 })
                    .prop("height", ThreePropValue::Number { value: 0.018 }),
                mesh_standard_material(lens_mat_id)
                    .prop(
                        "color",
                        ThreePropValue::Color {
                            r: 8,
                            g: 17,
                            b: 29,
                            a: None,
                        },
                    )
                    .prop("metalness", ThreePropValue::Number { value: 0.4 })
                    .prop("roughness", ThreePropValue::Number { value: 0.32 }),
            ],
        )
        .prop(
            "position",
            ThreePropValue::Vec3 {
                x: 0.0,
                y: 0.0,
                z: -0.018,
            },
        )
        .prop(
            "rotation",
            ThreePropValue::Vec3 {
                x: std::f32::consts::FRAC_PI_2,
                y: 0.0,
                z: 0.0,
            },
        );

        children.push(
            group(group_id, [body, lens])
                .prop(
                    "name",
                    ThreePropValue::String {
                        value: camera.name.clone(),
                    },
                )
                .prop(
                    "cameraId",
                    ThreePropValue::String {
                        value: camera.id.clone(),
                    },
                )
                .prop("includeInFit", ThreePropValue::Bool { value: false })
                .prop(
                    "position",
                    ThreePropValue::Vec3 {
                        x: camera.position[0],
                        y: camera.position[1],
                        z: camera.position[2],
                    },
                )
                .prop(
                    "rotation",
                    ThreePropValue::Vec3 {
                        x: camera.rotation[0],
                        y: camera.rotation[1],
                        z: camera.rotation[2],
                    },
                )
                .prop(
                    "rotationOrder",
                    ThreePropValue::String {
                        value: URDF_ROTATION_ORDER.to_string(),
                    },
                ),
        );
    }
}

fn build_static_link_subtree(
    robot: &RobotModel,
    link_name: &str,
    id_gen: &mut u32,
    project_config: Option<&ProjectConfig>,
) -> Option<ThreeNode> {
    let link = robot.links.get(link_name)?;
    *id_gen += 1;
    let link_group_id = *id_gen;
    let mut children = Vec::new();
    push_link_visuals(link, id_gen, &mut children);
    push_link_cameras(project_config, link_name, id_gen, &mut children);

    if let Some(child_joint_indices) = robot.children_by_parent.get(link_name) {
        for joint_index in child_joint_indices {
            let joint = &robot.joints[*joint_index];
            *id_gen += 1;
            let origin_group_id = *id_gen;
            *id_gen += 1;
            let motion_group_id = *id_gen;

            let mut motion_group = group(motion_group_id, []);

            if let Some(child_tree) =
                build_static_link_subtree(robot, &joint.child, id_gen, project_config)
            {
                motion_group = motion_group.child(child_tree);
            }

            children.push(
                group(origin_group_id, [motion_group])
                    .prop(
                        "position",
                        ThreePropValue::Vec3 {
                            x: joint.origin_xyz[0],
                            y: joint.origin_xyz[1],
                            z: joint.origin_xyz[2],
                        },
                    )
                    .prop(
                        "rotation",
                        ThreePropValue::Vec3 {
                            x: joint.origin_rpy[0],
                            y: joint.origin_rpy[1],
                            z: joint.origin_rpy[2],
                        },
                    )
                    .prop(
                        "rotationOrder",
                        ThreePropValue::String {
                            value: URDF_ROTATION_ORDER.to_string(),
                        },
                    ),
            );
        }
    }

    Some(group(link_group_id, children).prop(
        "name",
        ThreePropValue::String {
            value: link.name.clone(),
        },
    ))
}

fn transform_vec3_json(value: [f32; 3]) -> serde_json::Value {
    serde_json::json!([value[0], value[1], value[2]])
}

fn transform_axis_angle_json(axis: [f32; 3], angle: f32) -> serde_json::Value {
    let length = (axis[0] * axis[0] + axis[1] * axis[1] + axis[2] * axis[2]).sqrt();
    let axis = if length > 0.0001 {
        [axis[0] / length, axis[1] / length, axis[2] / length]
    } else {
        [0.0, 0.0, 1.0]
    };
    serde_json::json!([axis[0], axis[1], axis[2], angle])
}

fn insert_transform(
    transforms: &mut serde_json::Map<String, serde_json::Value>,
    id: u32,
    position: Option<[f32; 3]>,
    rotation: Option<[f32; 3]>,
    rotation_order: Option<&str>,
) {
    let mut transform = serde_json::Map::new();
    if let Some(position) = position {
        transform.insert("position".to_string(), transform_vec3_json(position));
    }
    if let Some(rotation) = rotation {
        transform.insert("rotation".to_string(), transform_vec3_json(rotation));
    }
    if let Some(rotation_order) = rotation_order {
        transform.insert(
            "rotationOrder".to_string(),
            serde_json::Value::String(rotation_order.to_string()),
        );
    }
    transforms.insert(id.to_string(), serde_json::Value::Object(transform));
}

fn insert_axis_angle_transform(
    transforms: &mut serde_json::Map<String, serde_json::Value>,
    id: u32,
    axis: [f32; 3],
    angle: f32,
) {
    let mut transform = serde_json::Map::new();
    transform.insert(
        "axisAngle".to_string(),
        transform_axis_angle_json(axis, angle),
    );
    transforms.insert(id.to_string(), serde_json::Value::Object(transform));
}

fn collect_dynamic_link_transforms(
    robot: &RobotModel,
    link_name: &str,
    joint_values: &[i32],
    id_gen: &mut u32,
    transforms: &mut serde_json::Map<String, serde_json::Value>,
    project_config: Option<&ProjectConfig>,
) -> Option<()> {
    let link = robot.links.get(link_name)?;
    *id_gen += 1;
    *id_gen += (link.visuals.len() as u32) * 4;
    *id_gen += project_config
        .map(|project_config| {
            project_config
                .scene
                .cameras
                .iter()
                .filter(|camera| camera.mounted_link == link_name)
                .count() as u32
                * 7
        })
        .unwrap_or_default();

    if let Some(child_joint_indices) = robot.children_by_parent.get(link_name) {
        for joint_index in child_joint_indices {
            let joint = &robot.joints[*joint_index];
            *id_gen += 1;
            *id_gen += 1;
            let motion_group_id = *id_gen;

            let joint_value = joint_values.get(*joint_index).copied().unwrap_or(0);
            let unit_value = urdf_value_to_joint_units(joint, joint_value);
            match joint.joint_type {
                UrdfJointType::Revolute | UrdfJointType::Continuous => {
                    insert_axis_angle_transform(
                        transforms,
                        motion_group_id,
                        joint.axis,
                        unit_value,
                    );
                }
                UrdfJointType::Prismatic => {
                    insert_transform(
                        transforms,
                        motion_group_id,
                        Some([
                            joint.axis[0] * unit_value,
                            joint.axis[1] * unit_value,
                            joint.axis[2] * unit_value,
                        ]),
                        None,
                        None,
                    );
                }
                _ => {}
            }

            let _ = collect_dynamic_link_transforms(
                robot,
                &joint.child,
                joint_values,
                id_gen,
                transforms,
                project_config,
            );
        }
    }

    Some(())
}

fn apply_urdf_slider_change(state: &mut UrdfViewerState, slider_id: u32, value: i32) -> bool {
    if slider_id >= URDF_BASE_SLIDER_BASE_ID && slider_id < URDF_BASE_SLIDER_BASE_ID + 6 {
        let slot = (slider_id - URDF_BASE_SLIDER_BASE_ID) as usize;
        if slot < 3 {
            state.base_translation_values[slot] = value;
        } else {
            state.base_rotation_values[slot - 3] = value;
        }
        return true;
    }

    if slider_id >= URDF_JOINT_SLIDER_BASE_ID {
        let Some(robot) = state.robot.as_ref() else {
            return false;
        };
        let slot = (slider_id - URDF_JOINT_SLIDER_BASE_ID) as usize;
        if let Some(joint_index) = robot.movable_joint_indices.get(slot)
            && let Some(v) = state.joint_values.get_mut(*joint_index)
        {
            *v = value;
            return true;
        }
    }

    false
}

fn resolve_project_asset_path(project_config: &ProjectConfig, asset: &str) -> PathBuf {
    let path = Path::new(asset);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        project_config.base_dir.join(path)
    }
}

fn project_asset_url(
    state: &UrdfViewerState,
    project_config: &ProjectConfig,
    asset: &str,
) -> Option<String> {
    let path = resolve_project_asset_path(project_config, asset);
    std::fs::canonicalize(&path)
        .ok()
        .and_then(|canonical| path_to_workspace_url(&canonical, &state.workspace_root))
        .or_else(|| path_to_workspace_url(&path, &state.workspace_root))
}

fn project_scene_object_node(
    state: &UrdfViewerState,
    project_config: &ProjectConfig,
    object: &ProjectSceneObjectConfig,
    index: usize,
) -> Option<ThreeNode> {
    let base_id = PROJECT_SCENE_OBJECT_ID_BASE + index as u32 * PROJECT_SCENE_OBJECT_ID_STRIDE;
    let geometry = project_scene_object_geometry_node(state, project_config, object, base_id + 2)?;
    let mut object_mesh = mesh(
        base_id + 1,
        [
            geometry,
            mesh_standard_material(base_id + 3)
                .prop(
                    "color",
                    ThreePropValue::Color {
                        r: object.color_rgb[0],
                        g: object.color_rgb[1],
                        b: object.color_rgb[2],
                        a: None,
                    },
                )
                .prop("metalness", ThreePropValue::Number { value: 0.1 })
                .prop("roughness", ThreePropValue::Number { value: 0.75 }),
        ],
    );

    if let Some(scale) = object.scale {
        object_mesh = object_mesh.prop(
            "scale",
            ThreePropValue::Vec3 {
                x: scale[0],
                y: scale[1],
                z: scale[2],
            },
        );
    }

    Some(
        group(base_id, [object_mesh])
            .prop(
                "name",
                ThreePropValue::String {
                    value: object.name.clone(),
                },
            )
            .prop(
                "position",
                ThreePropValue::Vec3 {
                    x: object.position[0],
                    y: object.position[1],
                    z: object.position[2],
                },
            )
            .prop(
                "rotation",
                ThreePropValue::Vec3 {
                    x: object.rotation[0],
                    y: object.rotation[1],
                    z: object.rotation[2],
                },
            )
            .prop(
                "includeInFit",
                ThreePropValue::Bool {
                    value: object.include_in_fit,
                },
            ),
    )
}

fn project_scene_object_geometry_node(
    state: &UrdfViewerState,
    project_config: &ProjectConfig,
    object: &ProjectSceneObjectConfig,
    id: u32,
) -> Option<ThreeNode> {
    match &object.geometry {
        ProjectSceneObjectGeometry::Mesh { asset } => {
            let src = project_asset_url(state, project_config, asset)?;
            Some(stl_geometry(id).prop("src", ThreePropValue::String { value: src }))
        }
        ProjectSceneObjectGeometry::Box { size } => Some(
            box_geometry(id)
                .prop("width", ThreePropValue::Number { value: size[0] })
                .prop("height", ThreePropValue::Number { value: size[1] })
                .prop("depth", ThreePropValue::Number { value: size[2] }),
        ),
        ProjectSceneObjectGeometry::Sphere { radius } => {
            Some(sphere_geometry(id).prop("radius", ThreePropValue::Number { value: *radius }))
        }
        ProjectSceneObjectGeometry::Cylinder {
            radius_top,
            radius_bottom,
            height,
        } => Some(
            cylinder_geometry(id)
                .prop("radiusTop", ThreePropValue::Number { value: *radius_top })
                .prop(
                    "radiusBottom",
                    ThreePropValue::Number {
                        value: *radius_bottom,
                    },
                )
                .prop("height", ThreePropValue::Number { value: *height }),
        ),
    }
}

fn project_scene_object_nodes(
    state: &UrdfViewerState,
    project_config: Option<&ProjectConfig>,
) -> Vec<ThreeNode> {
    project_config
        .map(|project_config| {
            project_config
                .scene
                .objects
                .iter()
                .enumerate()
                .filter_map(|(index, object)| {
                    project_scene_object_node(state, project_config, object, index)
                })
                .collect()
        })
        .unwrap_or_default()
}

fn robot_static_scene(
    state: &UrdfViewerState,
    project_config: Option<&ProjectConfig>,
) -> Option<ThreeNode> {
    let robot = state.robot.as_ref()?;
    let mut id_gen = 100;
    let mut model_children: Vec<ThreeNode> = Vec::new();
    for root in &robot.roots {
        if let Some(root_tree) = build_static_link_subtree(robot, root, &mut id_gen, project_config)
        {
            model_children.push(root_tree);
        }
    }

    let urdf_frame_group = group(
        91,
        [group(92, model_children).prop(
            "rotation",
            ThreePropValue::Vec3 {
                x: URDF_TO_VIEW_ROT_X,
                y: 0.0,
                z: 0.0,
            },
        )],
    );
    let mut scene_children = vec![group(ROBOT_BASE_GROUP_ID, [urdf_frame_group])];
    scene_children.extend(project_scene_object_nodes(state, project_config));

    Some(group(STATIC_SCENE_ROOT_ID, scene_children))
}

fn robot_dynamic_state(
    state: &UrdfViewerState,
    base_translation: [f32; 3],
    base_rotation: [f32; 3],
    project_config: Option<&ProjectConfig>,
) -> serde_json::Value {
    let mut transforms = serde_json::Map::new();
    insert_transform(
        &mut transforms,
        ROBOT_BASE_GROUP_ID,
        Some(base_translation),
        Some(base_rotation),
        Some(URDF_ROTATION_ORDER),
    );

    if let Some(robot) = state.robot.as_ref() {
        let mut id_gen = 100;
        for root in &robot.roots {
            let _ = collect_dynamic_link_transforms(
                robot,
                root,
                &state.joint_values,
                &mut id_gen,
                &mut transforms,
                project_config,
            );
        }
    }

    serde_json::json!({
        "schemaVersion": 1,
        "transforms": transforms
    })
}

fn project_scene_objects_key(
    state: &UrdfViewerState,
    project_config: Option<&ProjectConfig>,
) -> String {
    let Some(project_config) = project_config else {
        return String::new();
    };

    project_config
        .scene
        .objects
        .iter()
        .map(|object| {
            let geometry_key = match &object.geometry {
                ProjectSceneObjectGeometry::Mesh { asset } => {
                    let modified = resolve_project_asset_path(project_config, asset)
                        .metadata()
                        .ok()
                        .and_then(|metadata| metadata.modified().ok())
                        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                        .map(|duration| duration.as_millis())
                        .unwrap_or_default();
                    let src = project_asset_url(state, project_config, asset).unwrap_or_default();
                    format!("mesh:{src}:{modified}")
                }
                ProjectSceneObjectGeometry::Box { size } => format!("box:{size:?}"),
                ProjectSceneObjectGeometry::Sphere { radius } => format!("sphere:{radius}"),
                ProjectSceneObjectGeometry::Cylinder {
                    radius_top,
                    radius_bottom,
                    height,
                } => format!("cylinder:{radius_top}:{radius_bottom}:{height}"),
            };
            format!(
                "{}:{}:{:?}:{:?}:{:?}:{}",
                object.id,
                geometry_key,
                object.position,
                object.rotation,
                object.scale,
                object.include_in_fit
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn project_scene_cameras_key(project_config: Option<&ProjectConfig>) -> String {
    let Some(project_config) = project_config else {
        return String::new();
    };

    project_config
        .scene
        .cameras
        .iter()
        .map(|camera| {
            format!(
                "{}:{}:{}:{:?}:{:?}:{}:{}:{:?}",
                camera.id,
                camera.mounted_robot,
                camera.mounted_link,
                camera.position,
                camera.rotation,
                camera.fov_deg,
                camera.rate,
                camera.resolution
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn robot_static_scene_key(
    state: &UrdfViewerState,
    project_config: Option<&ProjectConfig>,
) -> Option<String> {
    let robot = state.robot.as_ref()?;
    let path = state
        .urdf_path
        .as_ref()
        .map(|path| path.display().to_string())
        .unwrap_or_default();
    let modified = state
        .urdf_path
        .as_ref()
        .and_then(|path| path.metadata().ok())
        .and_then(|metadata| metadata.modified().ok())
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis())
        .unwrap_or_default();
    let scene_objects_key = project_scene_objects_key(state, project_config);
    let scene_cameras_key = project_scene_cameras_key(project_config);
    let visual_count = robot
        .links
        .values()
        .map(|link| link.visuals.len())
        .sum::<usize>();
    Some(format!(
        "{path}|{modified}|scene:{scene_objects_key}|cameras:{scene_cameras_key}|{}|{}|{visual_count}",
        robot.links.len(),
        robot.joints.len()
    ))
}

pub(crate) fn robot_scene_props_with_static_scene(
    state: &UrdfViewerState,
    project_config: Option<&ProjectConfig>,
    base_translation: [f32; 3],
    base_rotation: [f32; 3],
    include_static_scene: bool,
) -> serde_json::Value {
    let model_path = state
        .urdf_path
        .as_ref()
        .map(|path| path.display().to_string());
    let model_src = state.urdf_path.as_ref().and_then(|path| {
        std::fs::canonicalize(path)
            .ok()
            .and_then(|canonical| path_to_workspace_url(&canonical, &state.workspace_root))
            .or_else(|| path_to_workspace_url(path, &state.workspace_root))
    });

    let package_roots = state.urdf_path.as_ref().and_then(|path| {
        let urdf_dir = path.parent()?;
        let package_root = urdf_dir.parent()?;
        let name = package_root.file_name()?.to_string_lossy().to_string();
        let url = std::fs::canonicalize(package_root)
            .ok()
            .and_then(|canonical| path_to_workspace_url(&canonical, &state.workspace_root))
            .or_else(|| path_to_workspace_url(package_root, &state.workspace_root))?;
        Some(serde_json::json!({ name: url }))
    });

    let robot_summary = state.robot.as_ref().map(|robot| {
        let movable_joints = robot
            .movable_joint_indices
            .iter()
            .filter_map(|joint_index| {
                let joint = robot.joints.get(*joint_index)?;
                let slider_value = state.joint_values.get(*joint_index).copied().unwrap_or(0);
                Some(serde_json::json!({
                    "name": joint.name,
                    "displayName": joint_display_label(project_config, &joint.name),
                    "semanticName": project_joint_name(project_config, &joint.name),
                    "type": urdf_joint_type_name(joint.joint_type),
                    "value": urdf_value_to_joint_units(joint, slider_value),
                    "axis": joint.axis,
                    "parent": joint.parent,
                    "child": joint.child,
                    "lower": joint.lower,
                    "upper": joint.upper
                }))
            })
            .collect::<Vec<_>>();

        serde_json::json!({
            "linkCount": robot.links.len(),
            "jointCount": robot.joints.len(),
            "rootLinks": robot.roots,
            "movableJoints": movable_joints
        })
    });

    let mut props = serde_json::json!({
        "model": {
            "type": "urdf",
            "path": model_path,
            "src": model_src
        },
        "packageRoots": package_roots.unwrap_or_else(|| serde_json::json!({})),
        "status": state.status,
        "base": {
            "translation": base_translation,
            "rotation": base_rotation
        },
        "robot": robot_summary,
        "staticSceneKey": robot_static_scene_key(state, project_config),
        "dynamicState": robot_dynamic_state(state, base_translation, base_rotation, project_config)
    });

    if include_static_scene && let Some(props) = props.as_object_mut() {
        props.insert(
            "staticScene".to_string(),
            serde_json::json!(robot_static_scene(state, project_config)),
        );
    }

    props
}

fn ui_url(bind_addr: SocketAddr) -> String {
    let host = match bind_addr.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => "127.0.0.1".to_string(),
        IpAddr::V6(ip) if ip.is_unspecified() => "::1".to_string(),
        _ => bind_addr.ip().to_string(),
    };

    if host.contains(':') {
        format!("http://[{host}]:{}", bind_addr.port())
    } else {
        format!("http://{host}:{}", bind_addr.port())
    }
}

#[derive(Debug, Clone)]
struct ProjectLaunch {
    name: String,
    slug: String,
}

fn project_slug_from_name(name: &str) -> String {
    let mut slug = String::new();
    let mut previous_separator = false;

    for character in name.chars() {
        if character.is_ascii_alphanumeric() {
            slug.push(character.to_ascii_lowercase());
            previous_separator = false;
        } else if !previous_separator && !slug.is_empty() {
            slug.push('-');
            previous_separator = true;
        }
    }

    while slug.ends_with('-') {
        slug.pop();
    }

    if slug.is_empty() {
        "project".to_string()
    } else {
        slug
    }
}

fn project_launch_from_manifest(path: &Path) -> Option<ProjectLaunch> {
    if !path.is_file()
        || !path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.eq_ignore_ascii_case("json"))
            .unwrap_or(false)
    {
        return None;
    }

    let json = read_json_file(path).ok()?;
    if json_string_path(&json, &["format"]) != Some(ROBOT_DREAMS_PROJECT_FORMAT) {
        return None;
    }

    let name = json_string_path(&json, &["name"])
        .map(str::to_string)
        .or_else(|| {
            path.file_stem()
                .and_then(|stem| stem.to_str())
                .map(str::to_string)
        })
        .unwrap_or_else(|| "Project".to_string());
    let slug = project_slug_from_name(&name);

    Some(ProjectLaunch { name, slug })
}

fn project_launch_for_input_path(path: &Path) -> Option<ProjectLaunch> {
    if let Some(project_launch) = project_launch_from_manifest(path) {
        return Some(project_launch);
    }

    if path.is_dir() {
        for candidate in [
            path.join("project.json"),
            path.join("robotdreams.json"),
            path.join("robotdreams.example.json"),
        ] {
            if let Some(project_launch) = project_launch_from_manifest(&candidate) {
                return Some(project_launch);
            }
        }
    }

    None
}

fn project_route(project_launch: &ProjectLaunch) -> String {
    format!("/project/{}", project_launch.slug)
}

fn project_url(bind_addr: SocketAddr, project_launch: &ProjectLaunch) -> String {
    format!("{}{}", ui_url(bind_addr), project_route(project_launch))
}

fn project_launch_log_line(bind_addr: SocketAddr, project_launch: &ProjectLaunch) -> String {
    if project_launch.name == project_launch.slug {
        format!("Project URL: {}", project_url(bind_addr, project_launch))
    } else {
        format!(
            "Project URL: {} ({})",
            project_url(bind_addr, project_launch),
            project_launch.name
        )
    }
}

fn log_project_url(bind_addr: SocketAddr, project_launch: Option<&ProjectLaunch>) {
    if let Some(project_launch) = project_launch {
        println!("{}", project_launch_log_line(bind_addr, project_launch));
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(tag = "command", rename_all = "camelCase")]
enum DaemonRequest {
    Open {
        path: String,
    },
    Status,
    ProjectList,
    ProjectState {
        item: Option<String>,
    },
    ProjectCommand {
        target: String,
        action: String,
        payload: Option<serde_json::Value>,
    },
    BusStart,
    BusStop,
    Close,
    Shutdown,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct DaemonResponse {
    ok: bool,
    message: Option<String>,
    project_id: Option<String>,
    url: Option<String>,
    already_open: bool,
    virtual_bus_running: bool,
    virtual_bus_path: Option<String>,
    data: Option<serde_json::Value>,
}

struct DaemonState {
    source_path: PathBuf,
    urdf_path: PathBuf,
    project_config: Option<ProjectConfig>,
    project_id: String,
    url: String,
    virtual_bus: WorkbenchVirtualBusHandle,
}

impl DaemonState {
    fn response(&self, message: Option<String>, already_open: bool) -> DaemonResponse {
        DaemonResponse {
            ok: true,
            message,
            project_id: Some(self.project_id.clone()),
            url: Some(self.url.clone()),
            already_open,
            virtual_bus_running: self.virtual_bus.is_running(),
            virtual_bus_path: self.virtual_bus.path(),
            data: None,
        }
    }

    fn response_with_data(
        &self,
        message: Option<String>,
        already_open: bool,
        data: serde_json::Value,
    ) -> DaemonResponse {
        DaemonResponse {
            ok: true,
            message,
            project_id: Some(self.project_id.clone()),
            url: Some(self.url.clone()),
            already_open,
            virtual_bus_running: self.virtual_bus.is_running(),
            virtual_bus_path: self.virtual_bus.path(),
            data: Some(data),
        }
    }

    fn error(message: impl Into<String>) -> DaemonResponse {
        DaemonResponse {
            ok: false,
            message: Some(message.into()),
            project_id: None,
            url: None,
            already_open: false,
            virtual_bus_running: false,
            virtual_bus_path: None,
            data: None,
        }
    }

    fn controller(&self) -> AppController {
        AppController::new(
            Some(self.urdf_path.clone()),
            self.project_config.clone(),
            self.virtual_bus.clone(),
        )
    }
}

fn canonical_input_path(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

fn project_state_item(state: &serde_json::Value, item: &str) -> Option<serde_json::Value> {
    match item {
        "project" => state.get("project").cloned(),
        "urdf" => state.get("urdf").cloned(),
        "robots" => state.get("robots").cloned(),
        "hardware" => state.get("hardware").cloned(),
        "controls" => state.get("controls").cloned(),
        "virtual-bus" | "virtualBus" => state.get("virtualBus").cloned(),
        "snapshots" => state
            .get("virtualBus")
            .and_then(|bus| bus.get("snapshots"))
            .cloned(),
        _ if item.starts_with("robot:") => {
            let robot_id = item.trim_start_matches("robot:");
            state
                .get("robots")?
                .as_array()?
                .iter()
                .find(|robot| robot.get("id").and_then(|id| id.as_str()) == Some(robot_id))
                .cloned()
        }
        _ if item.starts_with("bus:") => {
            let bus_id = item.trim_start_matches("bus:");
            state
                .get("hardware")?
                .get("buses")?
                .as_array()?
                .iter()
                .find(|bus| bus.get("id").and_then(|id| id.as_str()) == Some(bus_id))
                .cloned()
        }
        _ if item.starts_with("device:") => {
            let mut parts = item.split(':');
            let _prefix = parts.next()?;
            let bus_id = parts.next()?;
            let device_id = parts.next()?;
            let device_id = device_id.parse::<u64>().ok()?;
            let bus = state
                .get("hardware")?
                .get("buses")?
                .as_array()?
                .iter()
                .find(|bus| bus.get("id").and_then(|id| id.as_str()) == Some(bus_id))?;
            bus.get("devices")?
                .as_array()?
                .iter()
                .find(|device| device.get("id").and_then(|id| id.as_u64()) == Some(device_id))
                .cloned()
        }
        _ => None,
    }
}

fn payload_i32(payload: Option<&serde_json::Value>, keys: &[&str]) -> Option<i32> {
    let payload = payload?;
    for key in keys {
        if let Some(value) = payload.get(*key).and_then(|value| value.as_i64())
            && let Ok(value) = i32::try_from(value)
        {
            return Some(value);
        }
    }
    payload.as_i64().and_then(|value| i32::try_from(value).ok())
}

fn servo_target_from_command(
    target: &str,
    payload: Option<&serde_json::Value>,
) -> Option<(u32, i32)> {
    if let Some(servo_id) = target
        .strip_prefix("servo:")
        .and_then(|id| id.parse::<u32>().ok())
    {
        return Some((
            servo_id,
            payload_i32(payload, &["targetPosition", "target", "position"])?,
        ));
    }

    if let Some(rest) = target.strip_prefix("device:") {
        let mut parts = rest.split(':');
        let _bus_id = parts.next()?;
        let servo_id = parts.next()?.parse::<u32>().ok()?;
        return Some((
            servo_id,
            payload_i32(payload, &["targetPosition", "target", "position"])?,
        ));
    }

    None
}

async fn handle_daemon_request(
    state: Arc<AsyncMutex<DaemonState>>,
    request: DaemonRequest,
) -> DaemonResponse {
    match request {
        DaemonRequest::Open { path } => {
            let requested = canonical_input_path(Path::new(&path));
            let state = state.lock().await;
            if requested == state.source_path {
                state.response(Some("project already open".to_string()), true)
            } else {
                DaemonState::error(format!(
                    "daemon is already hosting {}; restart daemon to open {}",
                    state.source_path.display(),
                    requested.display()
                ))
            }
        }
        DaemonRequest::Status => {
            let state = state.lock().await;
            state.response(None, true)
        }
        DaemonRequest::ProjectList => {
            let state = state.lock().await;
            let mut controller = state.controller();
            let data = controller.project_items_json();
            state.response_with_data(None, true, data)
        }
        DaemonRequest::ProjectState { item } => {
            let state = state.lock().await;
            let mut controller = state.controller();
            let project_state = controller.project_state_json();
            let data = if let Some(item) = item {
                match project_state_item(&project_state, &item) {
                    Some(data) => data,
                    None => return DaemonState::error(format!("unknown project item {item}")),
                }
            } else {
                project_state
            };
            state.response_with_data(None, true, data)
        }
        DaemonRequest::ProjectCommand {
            target,
            action,
            payload,
        } => {
            let state = state.lock().await;
            let mut controller = state.controller();
            let result = match (target.as_str(), action.as_str()) {
                ("virtual-bus" | "virtualBus", "start") => controller.start_virtual_bus(),
                ("virtual-bus" | "virtualBus", "stop") => {
                    controller.stop_virtual_bus();
                    Ok(None)
                }
                (target, "start") if target.starts_with("bus:") => {
                    controller.start_virtual_bus_by_id(target.trim_start_matches("bus:"))
                }
                (target, "stop") if target.starts_with("bus:") => {
                    controller.stop_virtual_bus();
                    Ok(None)
                }
                (target, "set-target" | "setTarget") => {
                    if let Some((servo_id, target_position)) =
                        servo_target_from_command(target, payload.as_ref())
                    {
                        controller.set_selected_servo_target(servo_id, target_position);
                        Ok(None)
                    } else {
                        Err(format!(
                            "command {action} requires target servo:<id> or device:<bus>:<id> and payload targetPosition"
                        ))
                    }
                }
                _ => Err(format!("unsupported project command {action} for {target}")),
            };

            match result {
                Ok(path) => {
                    let mut data_controller = state.controller();
                    let project_state = data_controller.project_state_json();
                    let data_item = if target.starts_with("servo:") {
                        "snapshots".to_string()
                    } else if target == "virtualBus" {
                        "virtual-bus".to_string()
                    } else {
                        target.clone()
                    };
                    let data =
                        project_state_item(&project_state, &data_item).unwrap_or(project_state);
                    let message = path
                        .map(|path| format!("virtual bus listening at {path}"))
                        .or_else(|| Some(format!("project command {action} applied to {target}")));
                    state.response_with_data(message, true, data)
                }
                Err(err) => DaemonState::error(err),
            }
        }
        DaemonRequest::BusStart => {
            let state = state.lock().await;
            let mut controller = state.controller();
            match controller.start_virtual_bus() {
                Ok(path) => {
                    let message = path
                        .map(|path| format!("virtual bus listening at {path}"))
                        .unwrap_or_else(|| "no virtual bus configured".to_string());
                    state.response(Some(message), true)
                }
                Err(err) => DaemonState::error(err),
            }
        }
        DaemonRequest::BusStop | DaemonRequest::Close => {
            let state = state.lock().await;
            state.virtual_bus.stop();
            state.response(Some("virtual bus stopped".to_string()), true)
        }
        DaemonRequest::Shutdown => {
            let state = state.lock().await;
            state.virtual_bus.stop();
            let response = state.response(Some("daemon shutting down".to_string()), true);
            tokio::spawn(async {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                std::process::exit(0);
            });
            response
        }
    }
}

#[cfg(unix)]
async fn run_daemon_socket(socket_path: PathBuf, state: Arc<AsyncMutex<DaemonState>>) {
    let _ = std::fs::remove_file(&socket_path);
    let listener = match tokio::net::UnixListener::bind(&socket_path) {
        Ok(listener) => listener,
        Err(err) => {
            eprintln!(
                "failed to bind daemon socket {}: {err}",
                socket_path.display()
            );
            return;
        }
    };

    loop {
        let Ok((stream, _addr)) = listener.accept().await else {
            continue;
        };
        let state = state.clone();
        tokio::spawn(async move {
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            let response = match reader.read_line(&mut line).await {
                Ok(0) => DaemonState::error("empty request"),
                Ok(_) => match serde_json::from_str::<DaemonRequest>(&line) {
                    Ok(request) => handle_daemon_request(state, request).await,
                    Err(err) => DaemonState::error(format!("invalid request: {err}")),
                },
                Err(err) => DaemonState::error(format!("read failed: {err}")),
            };

            let mut stream = reader.into_inner();
            if let Ok(mut raw) = serde_json::to_vec(&response) {
                raw.push(b'\n');
                let _ = stream.write_all(&raw).await;
            }
        });
    }
}

#[cfg(not(unix))]
async fn run_daemon_socket(_socket_path: PathBuf, _state: Arc<AsyncMutex<DaemonState>>) {
    eprintln!("daemon socket is only implemented on Unix");
}

fn ensure_ui_bind_available(bind_addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let listener = std::net::TcpListener::bind(bind_addr).map_err(|err| {
        format!(
            "UI bind failed on {}: {}. Stop the existing process or use `--bind <addr:port>`.",
            bind_addr, err
        )
    })?;
    drop(listener);
    Ok(())
}

async fn run_app(
    path: Option<PathBuf>,
    bind_addr: SocketAddr,
    socket_path: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let project_launch = match path.as_deref() {
        Some(path) => project_launch_for_input_path(path),
        None => project_launch_for_input_path(Path::new(".")),
    };
    let source_path = canonical_input_path(path.as_deref().unwrap_or_else(|| Path::new(".")));
    let project_config = project_config_for_input_path(path.as_deref());
    let urdf_path = resolve_urdf_path(path)?;
    ensure_ui_bind_available(bind_addr)?;

    let browser_url = ui_url(bind_addr);
    println!("RobotDreams app listening on {}", browser_url);
    log_project_url(bind_addr, project_launch.as_ref());
    println!("URDF file: {}", urdf_path.display());
    println!("Press Ctrl-C to stop.");

    let mut wgui = Wgui::new(bind_addr);
    wgui.set_css(ROBOT_DREAMS_CSS);
    let virtual_bus = WorkbenchVirtualBusHandle::new();
    let project_id = project_launch
        .as_ref()
        .map(|project| project.slug.clone())
        .unwrap_or_else(|| "project".to_string());
    let url = project_launch
        .as_ref()
        .map(|project| project_url(bind_addr, project))
        .unwrap_or_else(|| format!("{}/workbench", ui_url(bind_addr)));
    let daemon_state = Arc::new(AsyncMutex::new(DaemonState {
        source_path,
        urdf_path: urdf_path.clone(),
        project_config: project_config.clone(),
        project_id,
        url,
        virtual_bus: virtual_bus.clone(),
    }));
    tokio::spawn(run_daemon_socket(socket_path, daemon_state));
    let robot_scene_virtual_bus = virtual_bus.clone();
    let robot_scene_urdf_path = urdf_path.clone();
    let robot_scene_project_config = project_config.clone();
    wgui.add_custom_component("/project/robot-dreams/robot-scene", move || {
        RobotSceneComponent::new(
            robot_scene_virtual_bus.clone(),
            Some(robot_scene_urdf_path.clone()),
            robot_scene_project_config.clone(),
        )
    });
    wgui.add_page_with("/", || async { ProjectsController::new() });
    wgui.add_page_with("/projects", || async { ProjectsController::new() });

    let workbench_urdf_path = urdf_path.clone();
    let workbench_project_config = project_config.clone();
    let workbench_virtual_bus = virtual_bus.clone();
    wgui.add_page_with("/workbench", move || {
        let urdf_path = workbench_urdf_path.clone();
        let project_config = workbench_project_config.clone();
        let virtual_bus = workbench_virtual_bus.clone();
        async move { AppController::new(Some(urdf_path), project_config, virtual_bus) }
    });

    if let Some(project_launch) = project_launch {
        let route = project_route(&project_launch);
        let launched_virtual_bus = virtual_bus.clone();
        wgui.add_page_with(&route, move || {
            let urdf_path = urdf_path.clone();
            let project_config = project_config.clone();
            let virtual_bus = launched_virtual_bus.clone();
            async move { AppController::new(Some(urdf_path), project_config, virtual_bus) }
        });
    }

    wgui.run().await;

    Ok(())
}

async fn run_headless(args: HeadlessArgs) -> Result<(), Box<dyn std::error::Error>> {
    let project_config = project_config_for_input_path(args.path.as_deref());
    let urdf_path = resolve_urdf_path(args.path)?;
    let virtual_bus = WorkbenchVirtualBusHandle::new();
    let mut controller = AppController::new(Some(urdf_path), project_config, virtual_bus);

    if args.start_virtual_bus {
        match controller.start_virtual_bus()? {
            Some(path) => println!("Virtual servo bus: {path}"),
            None => println!("Virtual servo bus: no virtual bus configured"),
        }
    }

    let seconds = args.seconds.max(0.0);
    let hz = args.hz.max(0.1);
    let tick = std::time::Duration::from_secs_f32(1.0 / hz);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs_f32(seconds);
    let mut ticks = 0_u64;

    while std::time::Instant::now() < deadline {
        controller.tick_headless();
        ticks += 1;
        tokio::time::sleep(tick).await;
    }

    let summary = controller.headless_summary();
    if args.json {
        println!("{}", serde_json::to_string_pretty(&summary)?);
    } else {
        println!("Project: {}", summary.project_name);
        println!("Status: {}", summary.status);
        println!("Loaded: {}", summary.loaded_summary);
        println!("Ticks: {ticks}");
        println!("Virtual bus running: {}", summary.virtual_bus_running);
        if let Some(path) = summary.virtual_bus_path {
            println!("Virtual bus path: {path}");
        }
        println!("Servo snapshots: {}", summary.servo_count);
    }

    controller.stop_virtual_bus();
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let Args {
        path,
        bind,
        socket,
        command,
    } = Args::parse();

    match command {
        Some(Command::Vbus(vbus_args)) => {
            let config = VirtualServoSimConfig {
                first_servo_id: vbus_args.first_servo_id,
                last_servo_id: vbus_args.last_servo_id,
                step_seconds: vbus_args.step_seconds,
                idle_sleep_ms: vbus_args.idle_sleep_ms,
            };
            run_virtual_servo_sim(config)?;
            return Ok(());
        }
        Some(Command::Headless(headless_args)) => {
            run_headless(headless_args).await?;
            return Ok(());
        }
        None => {
            let bind_addr: SocketAddr = bind.parse().map_err(|err| {
                format!(
                    "invalid --bind value '{}': {} (expected host:port)",
                    bind, err
                )
            })?;

            simple_logger::SimpleLogger::new()
                .with_level(LevelFilter::Info)
                .without_timestamps()
                .init()
                .unwrap();

            run_app(path, bind_addr, socket).await?;
            return Ok(());
        }
    }
}
