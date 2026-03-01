#![allow(dead_code)]
#![allow(non_snake_case)]

mod physics;
mod robot_dreams;
mod urdf;

use clap::{Args as ClapArgs, Parser, Subcommand};
use std::collections::{HashMap, HashSet};
#[cfg(unix)]
use std::ffi::CString;
#[cfg(unix)]
use std::io;
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr};
#[cfg(unix)]
use std::os::unix::io::RawFd;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::robot_dreams::{VirtualServoSimConfig, run_virtual_servo_sim};
use log::LevelFilter;
#[cfg(unix)]
use robot_utils::servo::protocol::port_handler::PortHandler;
#[cfg(unix)]
use robot_utils::servo::protocol::virtual_uart::VirtualUartPort;
use robot_utils::servo::sim::{FeetechBusSim, FeetechServoSnapshot};
use roxmltree::Document;
use wgui::*;

const SERVO_COUNT_SLIDER_ID: u32 = 1;
const APPLY_SERVO_COUNT_BUTTON_ID: u32 = 2;
const TARGET_POSITION_SLIDER_ID: u32 = 3;
const TOGGLE_AUTO_MOTION_BUTTON_ID: u32 = 4;
const SERIAL_PORT_INPUT_ID: u32 = 5;
const SERIAL_BAUD_INPUT_ID: u32 = 6;
const TOGGLE_SERIAL_BRIDGE_BUTTON_ID: u32 = 7;
const TOGGLE_VIRTUAL_BUS_BUTTON_ID: u32 = 8;
const URDF_RELOAD_BUTTON_ID: u32 = 1001;
const URDF_JOINT_SLIDER_BASE_ID: u32 = 30_000;
const URDF_BASE_SLIDER_BASE_ID: u32 = 31_000;

const DEFAULT_SERVO_COUNT: i32 = 6;
const DEFAULT_SERIAL_BAUD: &str = "1000000";
const SERIAL_PORT_PLACEHOLDER: &str = "COM6 or /dev/ttyUSB0";
const BUS_SERVICE_INTERVAL_MS: u64 = 5;
const UI_RENDER_INTERVAL_MS: u64 = 50;
const URDF_VALUE_SCALE: f32 = 1000.0;
const URDF_BASE_POSITION_RANGE: i32 = 2000;
const URDF_BASE_ROTATION_MIN: i32 = (-std::f32::consts::PI * URDF_VALUE_SCALE) as i32;
const URDF_BASE_ROTATION_MAX: i32 = (std::f32::consts::PI * URDF_VALUE_SCALE) as i32;
const URDF_ROTATION_ORDER: &str = "ZYX";
const URDF_TO_VIEW_ROT_X: f32 = -std::f32::consts::FRAC_PI_2;

#[derive(Debug, Parser)]
#[command(
    name = "robot_dreams",
    about = "Robot Dreams virtual servo bus UI, bridge, and URDF viewer"
)]
struct Args {
    #[arg(
        long,
        value_name = "PORT",
        help = "Initial serial port path shown in UI (e.g. COM6 or /dev/ttyUSB0)"
    )]
    port: Option<String>,

    #[arg(long, default_value_t = 1_000_000)]
    baud: u32,

    #[arg(long, default_value = "0.0.0.0:12347")]
    bind: String,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run a headless virtual servo bus and print its PTY path.
    Vbus(VbusArgs),

    /// Launch a WGUI URDF viewer for a model file.
    UrdfView(UrdfViewArgs),
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
struct UrdfViewArgs {
    #[arg(
        value_name = "URDF_PATH",
        help = "URDF file path. If omitted, tries ./model/*.urdf then ./puppyarm/model/*.urdf"
    )]
    path: Option<PathBuf>,
}

#[cfg(windows)]
fn default_serial_port() -> &'static str {
    "COM6"
}

#[cfg(not(windows))]
fn default_serial_port() -> &'static str {
    ""
}

struct SerialBridge {
    stop: Arc<AtomicBool>,
    frames_rx: Receiver<Vec<u8>>,
    responses_tx: Sender<Vec<u8>>,
    status: Arc<Mutex<String>>,
    join: Option<thread::JoinHandle<()>>,
}

#[cfg(unix)]
struct VirtualBusBridge {
    stop: Arc<AtomicBool>,
    frames_rx: Receiver<Vec<u8>>,
    responses_tx: Sender<Vec<u8>>,
    status: Arc<Mutex<String>>,
    join: Option<thread::JoinHandle<()>>,
}

#[cfg(unix)]
impl VirtualBusBridge {
    fn start() -> Result<Self, String> {
        let stop = Arc::new(AtomicBool::new(false));
        let status = Arc::new(Mutex::new("Starting virtual bus...".to_string()));
        let (frames_tx, frames_rx) = mpsc::channel::<Vec<u8>>();
        let (responses_tx, responses_rx) = mpsc::channel::<Vec<u8>>();
        let stop_in_thread = Arc::clone(&stop);
        let status_in_thread = Arc::clone(&status);

        let join = thread::spawn(move || {
            run_virtual_bus_bridge(stop_in_thread, status_in_thread, frames_tx, responses_rx);
        });

        Ok(Self {
            stop,
            frames_rx,
            responses_tx,
            status,
            join: Some(join),
        })
    }

    fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }

    fn status(&self) -> String {
        self.status
            .lock()
            .map(|s| s.clone())
            .unwrap_or_else(|_| "status unavailable".to_string())
    }

    fn try_recv_frame(&self) -> Option<Vec<u8>> {
        self.frames_rx.try_recv().ok()
    }

    fn send_response(&self, response: Vec<u8>) {
        let _ = self.responses_tx.send(response);
    }
}

#[cfg(not(unix))]
struct VirtualBusBridge;

#[cfg(not(unix))]
impl VirtualBusBridge {
    fn start() -> Result<Self, String> {
        Err("Virtual bus is only supported on Unix-like systems".to_string())
    }

    fn stop(&mut self) {}

    fn status(&self) -> String {
        "Virtual bus is only supported on Unix-like systems".to_string()
    }

    fn try_recv_frame(&self) -> Option<Vec<u8>> {
        None
    }

    fn send_response(&self, _response: Vec<u8>) {}
}

impl SerialBridge {
    fn start(port_name: String, baudrate: u32) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let status = Arc::new(Mutex::new(format!(
            "Opening {} @ {} baud...",
            port_name, baudrate
        )));
        let (frames_tx, frames_rx) = mpsc::channel::<Vec<u8>>();
        let (responses_tx, responses_rx) = mpsc::channel::<Vec<u8>>();
        let stop_in_thread = Arc::clone(&stop);
        let status_in_thread = Arc::clone(&status);

        let join = thread::spawn(move || {
            run_serial_bridge(
                port_name,
                baudrate,
                stop_in_thread,
                status_in_thread,
                frames_tx,
                responses_rx,
            );
        });

        Self {
            stop,
            frames_rx,
            responses_tx,
            status,
            join: Some(join),
        }
    }

    fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }

    fn status(&self) -> String {
        self.status
            .lock()
            .map(|s| s.clone())
            .unwrap_or_else(|_| "status unavailable".to_string())
    }
}

struct AppState {
    desired_servo_count: i32,
    snapshots: Vec<FeetechServoSnapshot>,
    auto_motion: bool,
    serial_port: String,
    follow_virtual_bus_port: bool,
    serial_baud: String,
    serial_status: String,
    virtual_bus_status: String,
    virtual_bus_running: bool,
    urdf_view: UrdfViewerState,
}

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

fn reload_urdf_state(state: &mut UrdfViewerState) {
    if state.urdf_path.is_none() {
        state.urdf_path = resolve_urdf_path(None).ok();
    }

    let Some(path) = state.urdf_path.as_ref() else {
        state.status = "No URDF found in ./model or ./puppyarm/model".to_string();
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

fn resolve_urdf_path(path: Option<PathBuf>) -> Result<PathBuf, Box<dyn std::error::Error>> {
    if let Some(path) = path {
        return Ok(path);
    }

    for dir in [Path::new("model"), Path::new("puppyarm/model")] {
        let preferred = dir.join("PuppyArm.urdf");
        if preferred.is_file() {
            return Ok(preferred);
        }
        if let Some(first) = first_urdf_in_dir(dir) {
            return Ok(first);
        }
    }

    Err("No URDF path provided and no .urdf file found in ./model or ./puppyarm/model".into())
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

fn servo_ticks_to_radians(ticks: i16) -> f32 {
    let ticks = ticks.clamp(0, 4095) as f32;
    (ticks - 2048.0) * (std::f32::consts::TAU / 4096.0)
}

fn servo_ticks_to_slider_value(ticks: i16, min: i32, max: i32) -> i32 {
    let ticks = ticks.clamp(0, 4095) as f32;
    let t = ticks / 4095.0;
    (min as f32 + (max - min) as f32 * t).round() as i32
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
        state.base_translation_values[2] = z.clamp(-URDF_BASE_POSITION_RANGE, URDF_BASE_POSITION_RANGE);
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

fn build_link_subtree(
    robot: &RobotModel,
    link_name: &str,
    joint_values: &[i32],
    id_gen: &mut u32,
) -> Option<ThreeNode> {
    let link = robot.links.get(link_name)?;
    *id_gen += 1;
    let link_group_id = *id_gen;
    let mut children = Vec::new();
    push_link_visuals(link, id_gen, &mut children);

    if let Some(child_joint_indices) = robot.children_by_parent.get(link_name) {
        for joint_index in child_joint_indices {
            let joint = &robot.joints[*joint_index];
            *id_gen += 1;
            let origin_group_id = *id_gen;
            *id_gen += 1;
            let motion_group_id = *id_gen;

            let mut motion_group = group(motion_group_id, []);
            let joint_value = joint_values.get(*joint_index).copied().unwrap_or(0);
            let unit_value = urdf_value_to_joint_units(joint, joint_value);
            match joint.joint_type {
                UrdfJointType::Revolute | UrdfJointType::Continuous => {
                    let delta = axis_to_euler(joint.axis, unit_value);
                    motion_group = motion_group.prop(
                        "rotation",
                        ThreePropValue::Vec3 {
                            x: delta[0],
                            y: delta[1],
                            z: delta[2],
                        },
                    );
                }
                UrdfJointType::Prismatic => {
                    motion_group = motion_group.prop(
                        "position",
                        ThreePropValue::Vec3 {
                            x: joint.axis[0] * unit_value,
                            y: joint.axis[1] * unit_value,
                            z: joint.axis[2] * unit_value,
                        },
                    );
                }
                _ => {}
            }

            if let Some(child_tree) = build_link_subtree(robot, &joint.child, joint_values, id_gen)
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

fn render_urdf_view(state: &UrdfViewerState) -> Item {
    let file_label = state
        .urdf_path
        .as_ref()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| {
            "(auto-discovery failed: place a .urdf in ./model or ./puppyarm/model)".to_string()
        });
    let base_translation = [
        urdf_slider_value_to_units(state.base_translation_values[0]),
        urdf_slider_value_to_units(state.base_translation_values[1]),
        urdf_slider_value_to_units(state.base_translation_values[2]),
    ];
    let base_rotation = [
        urdf_slider_value_to_units(state.base_rotation_values[0]),
        urdf_slider_value_to_units(state.base_rotation_values[1]),
        urdf_slider_value_to_units(state.base_rotation_values[2]),
    ];

    let mut id_gen = 100;
    let mut scene_children = vec![
        perspective_camera(2)
            .prop(
                "position",
                ThreePropValue::Vec3 {
                    x: 0.28,
                    y: 0.2,
                    z: 0.35,
                },
            )
            .prop(
                "lookAt",
                ThreePropValue::Vec3 {
                    x: -0.05,
                    y: 0.0,
                    z: -0.05,
                },
            )
            .prop("active", ThreePropValue::Bool { value: true }),
        ambient_light(3).prop("intensity", ThreePropValue::Number { value: 0.65 }),
        directional_light(4)
            .prop(
                "position",
                ThreePropValue::Vec3 {
                    x: 0.5,
                    y: 1.0,
                    z: 0.4,
                },
            )
            .prop("intensity", ThreePropValue::Number { value: 1.1 }),
    ];

    if let Some(robot) = state.robot.as_ref() {
        let mut model_children: Vec<ThreeNode> = Vec::new();
        for root in &robot.roots {
            if let Some(root_tree) =
                build_link_subtree(robot, root, &state.joint_values, &mut id_gen)
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
        scene_children.push(
            group(90, [urdf_frame_group])
                .prop(
                    "position",
                    ThreePropValue::Vec3 {
                        x: base_translation[0],
                        y: base_translation[1],
                        z: base_translation[2],
                    },
                )
                .prop(
                    "rotation",
                    ThreePropValue::Vec3 {
                        x: base_rotation[0],
                        y: base_rotation[1],
                        z: base_rotation[2],
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

    let three_panel = three_view(scene(1, scene_children))
        .height(520)
        .border("1px solid #303030")
        .grow(1);

    let mut controls = vec![
        text("URDF viewer").margin_bottom(8),
        text(&format!("Model: {}", file_label)).margin_bottom(8),
        text(&state.status).margin_bottom(8),
        button("Reload URDF")
            .id(URDF_RELOAD_BUTTON_ID)
            .margin_bottom(10),
    ];

    if let Some(robot) = state.robot.as_ref() {
        controls.push(
            text(&format!(
                "Loaded links: {}, joints: {}",
                robot.links.len(),
                robot.joints.len()
            ))
            .margin_bottom(12),
        );
        controls.push(text("Base transform").margin_bottom(6));
        controls.push(text("Base X position"));
        controls.push(
            slider()
                .id(URDF_BASE_SLIDER_BASE_ID)
                .min(-URDF_BASE_POSITION_RANGE)
                .max(URDF_BASE_POSITION_RANGE)
                .ivalue(state.base_translation_values[0])
                .step(1),
        );
        controls.push(
            text(&format!(
                "{:.4}",
                urdf_slider_value_to_units(state.base_translation_values[0])
            ))
            .margin_bottom(4),
        );
        controls.push(text("Base Y position"));
        controls.push(
            slider()
                .id(URDF_BASE_SLIDER_BASE_ID + 1)
                .min(-URDF_BASE_POSITION_RANGE)
                .max(URDF_BASE_POSITION_RANGE)
                .ivalue(state.base_translation_values[1])
                .step(1),
        );
        controls.push(
            text(&format!(
                "{:.4}",
                urdf_slider_value_to_units(state.base_translation_values[1])
            ))
            .margin_bottom(4),
        );
        controls.push(text("Base Z position"));
        controls.push(
            slider()
                .id(URDF_BASE_SLIDER_BASE_ID + 2)
                .min(-URDF_BASE_POSITION_RANGE)
                .max(URDF_BASE_POSITION_RANGE)
                .ivalue(state.base_translation_values[2])
                .step(1),
        );
        controls.push(
            text(&format!(
                "{:.4}",
                urdf_slider_value_to_units(state.base_translation_values[2])
            ))
            .margin_bottom(8),
        );
        controls.push(text("Base roll (X)"));
        controls.push(
            slider()
                .id(URDF_BASE_SLIDER_BASE_ID + 3)
                .min(URDF_BASE_ROTATION_MIN)
                .max(URDF_BASE_ROTATION_MAX)
                .ivalue(state.base_rotation_values[0])
                .step(1),
        );
        controls.push(
            text(&format!(
                "{:.4}",
                urdf_slider_value_to_units(state.base_rotation_values[0])
            ))
            .margin_bottom(4),
        );
        controls.push(text("Base pitch (Y)"));
        controls.push(
            slider()
                .id(URDF_BASE_SLIDER_BASE_ID + 4)
                .min(URDF_BASE_ROTATION_MIN)
                .max(URDF_BASE_ROTATION_MAX)
                .ivalue(state.base_rotation_values[1])
                .step(1),
        );
        controls.push(
            text(&format!(
                "{:.4}",
                urdf_slider_value_to_units(state.base_rotation_values[1])
            ))
            .margin_bottom(4),
        );
        controls.push(text("Base yaw (Z)"));
        controls.push(
            slider()
                .id(URDF_BASE_SLIDER_BASE_ID + 5)
                .min(URDF_BASE_ROTATION_MIN)
                .max(URDF_BASE_ROTATION_MAX)
                .ivalue(state.base_rotation_values[2])
                .step(1),
        );
        controls.push(
            text(&format!(
                "{:.4}",
                urdf_slider_value_to_units(state.base_rotation_values[2])
            ))
            .margin_bottom(8),
        );

        if robot.movable_joint_indices.is_empty() {
            controls
                .push(text("No movable URDF joints detected (all joints are fixed).").margin_bottom(4));
            controls.push(
                text("Using servo-driven base transform fallback for visualization.")
                    .margin_bottom(8),
            );
        }

        for (slider_slot, joint_index) in robot.movable_joint_indices.iter().enumerate() {
            let joint = &robot.joints[*joint_index];
            let slider_id = URDF_JOINT_SLIDER_BASE_ID + slider_slot as u32;
            let value = state.joint_values.get(*joint_index).copied().unwrap_or(0);
            let (min, max) = urdf_joint_slider_range(joint);

            controls.push(text(&joint.name));
            controls.push(
                slider()
                    .id(slider_id)
                    .min(min)
                    .max(max)
                    .ivalue(value)
                    .step(1),
            );
            controls.push(
                text(&format!("{:.4}", urdf_value_to_joint_units(joint, value))).margin_bottom(8),
            );
        }
    } else {
        controls.push(text("No URDF loaded").margin_bottom(8));
    }

    let controls_panel = vstack(controls)
        .width(340)
        .height(520)
        .overflow("scroll")
        .padding(12)
        .border("1px solid #d0d0d0")
        .background_color("#fafafa");

    hstack([three_panel, controls_panel]).spacing(14).into()
}

fn render_servo_state(snapshot: &FeetechServoSnapshot) -> Item {
    let status = if snapshot.moving { "moving" } else { "idle" };
    let torque = if snapshot.torque_enabled { "on" } else { "off" };

    vstack([
        text(&format!("Servo {}", snapshot.id)),
        text(&format!(
            "mode {} | torque {} | status {}",
            snapshot.mode, torque, status
        )),
        text(&format!(
            "position {} | target {} | speed {}",
            snapshot.present_position, snapshot.target_position, snapshot.present_speed
        )),
        text(&format!(
            "load {} | current {} | temp {} C",
            snapshot.present_load, snapshot.current_raw, snapshot.temperature_c
        )),
        slider()
            .id(TARGET_POSITION_SLIDER_ID)
            .inx(snapshot.id as u32)
            .min(0)
            .max(4095)
            .ivalue(snapshot.target_position.clamp(0, 4095) as i32),
    ])
    .spacing(4)
    .padding(12)
    .border("1px solid #d6d6d6")
    .background_color("#f8f9fb")
    .into()
}

fn render(state: &AppState) -> Item {
    vstack([
        text("Robot Dreams Virtual Servo Bus"),
        hstack([
            text("Servo count"),
            slider()
                .id(SERVO_COUNT_SLIDER_ID)
                .min(1)
                .max(32)
                .ivalue(state.desired_servo_count)
                .width(240),
            text(&state.desired_servo_count.to_string()).width(28),
            button("Apply").id(APPLY_SERVO_COUNT_BUTTON_ID),
            button(if state.auto_motion {
                "Disable Auto Motion"
            } else {
                "Enable Auto Motion"
            })
            .id(TOGGLE_AUTO_MOTION_BUTTON_ID),
        ])
        .spacing(10),
        hstack([
            text("Serial port"),
            text_input()
                .id(SERIAL_PORT_INPUT_ID)
                .placeholder(SERIAL_PORT_PLACEHOLDER)
                .svalue(&state.serial_port)
                .width(120),
            text("Baud"),
            text_input()
                .id(SERIAL_BAUD_INPUT_ID)
                .placeholder(DEFAULT_SERIAL_BAUD)
                .svalue(&state.serial_baud)
                .width(110),
            button(if state.serial_status.starts_with("Connected") {
                "Disconnect"
            } else {
                "Connect"
            })
            .id(TOGGLE_SERIAL_BRIDGE_BUTTON_ID),
            button(if state.virtual_bus_running {
                "Stop Virtual Bus"
            } else {
                "Start Virtual Bus"
            })
            .id(TOGGLE_VIRTUAL_BUS_BUTTON_ID),
        ])
        .spacing(10),
        text(&format!("Serial bridge: {}", state.serial_status)),
        text(&format!("Virtual bus: {}", state.virtual_bus_status)),
        text("Connected servos"),
        vstack(state.snapshots.iter().map(render_servo_state)).spacing(8),
        render_urdf_view(&state.urdf_view),
    ])
    .spacing(12)
    .padding(16)
    .background_color("#ffffff")
    .into()
}

fn apply_auto_motion(sim: &mut FeetechBusSim, snapshots: &[FeetechServoSnapshot], t: f32) {
    for (inx, snapshot) in snapshots.iter().enumerate() {
        let phase = t * 1.4 + inx as f32 * 0.8;
        let target = (2048.0 + phase.sin() * 1200.0).round() as i16;
        let _ = sim.set_target_position(snapshot.id, target);
    }
}

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

        frames.push(buffer.drain(0..total_len).collect());
    }

    frames
}

fn format_packet_hex(frame: &[u8]) -> String {
    frame
        .iter()
        .map(|b| format!("{:02X}", b))
        .collect::<Vec<_>>()
        .join(" ")
}

fn log_bus_packet(direction: &str, frame: &[u8]) {
    if frame.is_empty() {
        return;
    }
    let id = frame.get(2).copied().unwrap_or(0);
    let code = frame.get(4).copied().unwrap_or(0);
    log::info!(
        "[{}] id={} code=0x{:02X} len={} bytes={}",
        direction,
        id,
        code,
        frame.len(),
        format_packet_hex(frame)
    );
}

fn log_bus_payload(direction: &str, payload: &[u8]) {
    if payload.is_empty() {
        return;
    }
    let mut tmp = payload.to_vec();
    let frames = extract_frames(&mut tmp);
    if frames.is_empty() {
        log_bus_packet(direction, payload);
    } else {
        for frame in frames {
            log_bus_packet(direction, &frame);
        }
    }
}

enum SerialBridgePort {
    Serial(Box<dyn serialport::SerialPort>),
    #[cfg(unix)]
    UnixRaw(UnixRawSerialBridgePort),
}

#[cfg(unix)]
struct UnixRawSerialBridgePort {
    fd: RawFd,
}

#[cfg(unix)]
impl UnixRawSerialBridgePort {
    fn open(path: &str) -> io::Result<Self> {
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

            Ok(Self { fd })
        }
    }

    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let read_len =
            unsafe { libc::read(self.fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
        if read_len > 0 {
            return Ok(read_len as usize);
        }
        if read_len == 0 {
            return Ok(0);
        }

        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::WouldBlock || err.kind() == io::ErrorKind::Interrupted {
            Ok(0)
        } else {
            Err(err)
        }
    }

    fn write_all(&mut self, data: &[u8]) -> io::Result<()> {
        let mut total = 0usize;
        let started = Instant::now();

        while total < data.len() {
            let written = unsafe {
                libc::write(
                    self.fd,
                    data[total..].as_ptr() as *const libc::c_void,
                    data.len() - total,
                )
            };

            if written > 0 {
                total += written as usize;
                continue;
            }
            if written == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "raw unix write returned 0 bytes",
                ));
            }

            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock || err.kind() == io::ErrorKind::Interrupted {
                if started.elapsed() > Duration::from_millis(50) {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "timed out writing to raw unix serial",
                    ));
                }
                thread::sleep(Duration::from_micros(200));
                continue;
            }
            return Err(err);
        }

        Ok(())
    }
}

#[cfg(unix)]
impl Drop for UnixRawSerialBridgePort {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.fd);
        }
    }
}

impl SerialBridgePort {
    fn open(port_name: &str, baudrate: u32) -> Result<Self, String> {
        match serialport::new(port_name, baudrate)
            .timeout(Duration::from_millis(10))
            .open()
        {
            Ok(port) => Ok(Self::Serial(port)),
            Err(serial_err) => {
                #[cfg(unix)]
                {
                    match UnixRawSerialBridgePort::open(port_name) {
                        Ok(raw) => Ok(Self::UnixRaw(raw)),
                        Err(raw_err) => Err(format!(
                            "Failed to open {} as serial ({}) or raw unix ({})",
                            port_name, serial_err, raw_err
                        )),
                    }
                }
                #[cfg(not(unix))]
                {
                    Err(format!("Failed to open {}: {}", port_name, serial_err))
                }
            }
        }
    }

    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            SerialBridgePort::Serial(port) => port.read(buf),
            #[cfg(unix)]
            SerialBridgePort::UnixRaw(port) => port.read(buf),
        }
    }

    fn write_all(&mut self, data: &[u8]) -> std::io::Result<()> {
        match self {
            SerialBridgePort::Serial(port) => port.write_all(data),
            #[cfg(unix)]
            SerialBridgePort::UnixRaw(port) => port.write_all(data),
        }
    }

    fn transport_name(&self) -> &'static str {
        match self {
            SerialBridgePort::Serial(_) => "serialport",
            #[cfg(unix)]
            SerialBridgePort::UnixRaw(_) => "unix-raw",
        }
    }
}

fn run_serial_bridge(
    port_name: String,
    baudrate: u32,
    stop: Arc<AtomicBool>,
    status: Arc<Mutex<String>>,
    frames_tx: Sender<Vec<u8>>,
    responses_rx: Receiver<Vec<u8>>,
) {
    let mut port = match SerialBridgePort::open(&port_name, baudrate) {
        Ok(port) => port,
        Err(err) => {
            if let Ok(mut s) = status.lock() {
                *s = err;
            }
            return;
        }
    };

    if let Ok(mut s) = status.lock() {
        *s = format!(
            "Connected to {} @ {} baud ({})",
            port_name,
            baudrate,
            port.transport_name()
        );
    }

    let mut buffer: Vec<u8> = Vec::new();
    let mut read_buf = [0u8; 512];

    while !stop.load(Ordering::Relaxed) {
        while let Ok(response) = responses_rx.try_recv() {
            if !response.is_empty() {
                log_bus_payload("serial tx", &response);
                if let Err(err) = port.write_all(&response) {
                    if let Ok(mut s) = status.lock() {
                        *s = format!("Serial write error on {}: {}", port_name, err);
                    }
                    return;
                }
            }
        }

        match port.read(&mut read_buf) {
            Ok(read_len) if read_len > 0 => {
                buffer.extend_from_slice(&read_buf[..read_len]);
                let frames = extract_frames(&mut buffer);
                for frame in frames {
                    log_bus_packet("serial rx", &frame);
                    if frames_tx.send(frame).is_err() {
                        if let Ok(mut s) = status.lock() {
                            *s = "Bridge disconnected from app state".to_string();
                        }
                        return;
                    }
                }
            }
            Ok(_) => {}
            Err(err)
                if err.kind() == std::io::ErrorKind::TimedOut
                    || err.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(err) => {
                if let Ok(mut s) = status.lock() {
                    *s = format!("Serial error on {}: {}", port_name, err);
                }
                return;
            }
        }
    }

    if let Ok(mut s) = status.lock() {
        *s = "Disconnected".to_string();
    }
}

#[cfg(unix)]
fn run_virtual_bus_bridge(
    stop: Arc<AtomicBool>,
    status: Arc<Mutex<String>>,
    frames_tx: Sender<Vec<u8>>,
    responses_rx: Receiver<Vec<u8>>,
) {
    let mut port = match VirtualUartPort::new() {
        Ok(port) => port,
        Err(err) => {
            if let Ok(mut s) = status.lock() {
                *s = format!("Failed to create virtual bus: {}", err);
            }
            return;
        }
    };

    let slave_path = port.slave_path().to_string();
    if let Ok(mut s) = status.lock() {
        *s = format!("Running on {}", slave_path);
    }

    let mut buffer: Vec<u8> = Vec::new();

    while !stop.load(Ordering::Relaxed) {
        let mut had_io = false;

        while let Ok(response) = responses_rx.try_recv() {
            if !response.is_empty() {
                had_io = true;
                log_bus_payload("vbus tx", &response);
                let _ = port.write_port(&response);
            }
        }

        let mut incoming = port.read_port(512);
        if !incoming.is_empty() {
            buffer.append(&mut incoming);
            let frames = extract_frames(&mut buffer);
            for frame in frames {
                log_bus_packet("vbus rx", &frame);
                if frames_tx.send(frame).is_err() {
                    if let Ok(mut s) = status.lock() {
                        *s = "Virtual bus disconnected from app state".to_string();
                    }
                    return;
                }
            }
        } else if !had_io {
            thread::sleep(Duration::from_millis(2));
        }
    }

    if let Ok(mut s) = status.lock() {
        *s = "Stopped".to_string();
    }
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

async fn run_urdf_view(
    urdf_args: UrdfViewArgs,
    bind_addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let urdf_path = resolve_urdf_path(urdf_args.path)?;
    ensure_ui_bind_available(bind_addr)?;

    let browser_url = ui_url(bind_addr);
    println!("Robot Dreams URDF viewer listening on {}", browser_url);
    println!("URDF file: {}", urdf_path.display());
    println!("Press Ctrl-C to stop.");

    let mut state = UrdfViewerState::new(Some(urdf_path));
    reload_urdf_state(&mut state);

    let mut wgui = Wgui::new(bind_addr);
    let mut clients = HashSet::new();

    loop {
        let Some(message) = wgui.next().await else {
            break;
        };

        let client_id = message.client_id;
        match message.event {
            ClientEvent::Disconnected { id: _ } => {
                clients.remove(&client_id);
            }
            ClientEvent::Connected { id: _ } => {
                clients.insert(client_id);
                wgui.render(client_id, render_urdf_view(&state)).await;
            }
            ClientEvent::OnClick(click) => {
                if click.id == URDF_RELOAD_BUTTON_ID {
                    reload_urdf_state(&mut state);
                }
            }
            _ => {}
        }

        for id in &clients {
            wgui.render(*id, render_urdf_view(&state)).await;
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let Args {
        port,
        baud,
        bind,
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
        Some(Command::UrdfView(urdf_args)) => {
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

            run_urdf_view(urdf_args, bind_addr).await?;
            return Ok(());
        }
        None => {}
    }

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

    let mut sim = FeetechBusSim::new();
    sim.set_servo_count(DEFAULT_SERVO_COUNT as u8);

    let initial_port = port.unwrap_or_else(|| default_serial_port().to_string());
    let mut urdf_view = UrdfViewerState::new(resolve_urdf_path(None).ok());
    reload_urdf_state(&mut urdf_view);

    let mut state = AppState {
        desired_servo_count: DEFAULT_SERVO_COUNT,
        snapshots: sim.servo_snapshots(),
        auto_motion: false,
        serial_port: initial_port,
        follow_virtual_bus_port: false,
        serial_baud: baud.to_string(),
        serial_status: "Disconnected".to_string(),
        virtual_bus_status: "Stopped".to_string(),
        virtual_bus_running: false,
        urdf_view,
    };
    let mut serial_bridge: Option<SerialBridge> = None;
    let mut virtual_bus_bridge: Option<VirtualBusBridge> = None;

    let mut wgui = Wgui::new(bind_addr);
    let mut client_ids = HashSet::new();
    let mut ticker = tokio::time::interval(Duration::from_millis(BUS_SERVICE_INTERVAL_MS));
    let mut last_step = Instant::now();
    let motion_start = Instant::now();
    let mut last_render = Instant::now();

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let now = Instant::now();
                let dt = (now - last_step).as_secs_f32();
                last_step = now;

                if state.auto_motion {
                    apply_auto_motion(&mut sim, &state.snapshots, motion_start.elapsed().as_secs_f32());
                }

                if let Some(bridge) = serial_bridge.as_ref() {
                    state.serial_status = bridge.status();
                    while let Ok(frame) = bridge.frames_rx.try_recv() {
                        if let Ok(Some(response)) = sim.handle_frame(&frame) {
                            let _ = bridge.responses_tx.send(response);
                        }
                    }
                }

                if let Some(bridge) = virtual_bus_bridge.as_ref() {
                    state.virtual_bus_status = bridge.status();
                    if state.virtual_bus_status.starts_with("Failed") {
                        state.virtual_bus_running = false;
                    }
                    if let Some(path) = state.virtual_bus_status.strip_prefix("Running on ") {
                        if serial_bridge.is_none() && state.follow_virtual_bus_port {
                            state.serial_port = path.to_string();
                        }
                    }
                    while let Some(frame) = bridge.try_recv_frame() {
                        if let Ok(Some(response)) = sim.handle_frame(&frame) {
                            bridge.send_response(response);
                        }
                    }
                }
                sim.step(dt.max(0.001));
                state.snapshots = sim.servo_snapshots();
                sync_urdf_with_servo_snapshots(&mut state.urdf_view, &state.snapshots);

                if last_render.elapsed() >= Duration::from_millis(UI_RENDER_INTERVAL_MS) {
                    for id in &client_ids {
                        wgui.render(*id, render(&state)).await;
                    }
                    last_render = Instant::now();
                }
            }
            maybe_message = wgui.next() => {
                let Some(message) = maybe_message else {
                    break;
                };

                let client_id = message.client_id;
                match message.event {
                    ClientEvent::Disconnected { id: _ } => {
                        client_ids.remove(&client_id);
                    }
                    ClientEvent::Connected { id: _ } => {
                        client_ids.insert(client_id);
                        wgui.render(client_id, render(&state)).await;
                    }
                    ClientEvent::OnClick(o) => match o.id {
                        APPLY_SERVO_COUNT_BUTTON_ID => {
                            sim.set_servo_count(state.desired_servo_count as u8);
                            state.snapshots = sim.servo_snapshots();
                        }
                        TOGGLE_AUTO_MOTION_BUTTON_ID => {
                            state.auto_motion = !state.auto_motion;
                        }
                        TOGGLE_SERIAL_BRIDGE_BUTTON_ID => {
                            if let Some(mut bridge) = serial_bridge.take() {
                                bridge.stop();
                                state.serial_status = "Disconnected".to_string();
                            } else {
                                let parsed_baud = state.serial_baud.trim().parse::<u32>();
                                if state.serial_port.trim().is_empty() {
                                    state.serial_status = "Set a serial port first (e.g. COM6 or /dev/ttyUSB0)".to_string();
                                } else if state.virtual_bus_running
                                    && state
                                        .virtual_bus_status
                                        .strip_prefix("Running on ")
                                        .map(|path| path == state.serial_port.trim())
                                        .unwrap_or(false)
                                {
                                    state.serial_status =
                                        "Serial bridge disabled: this path is the active virtual bus. Use external client (e.g. puppyarm) to connect."
                                            .to_string();
                                } else if let Ok(baudrate) = parsed_baud {
                                    serial_bridge = Some(SerialBridge::start(
                                        state.serial_port.trim().to_string(),
                                        baudrate,
                                    ));
                                } else {
                                    state.serial_status = "Baud must be an integer (e.g. 1000000)".to_string();
                                }
                            }
                        }
                        TOGGLE_VIRTUAL_BUS_BUTTON_ID => {
                            if state.virtual_bus_running {
                                if let Some(mut bridge) = virtual_bus_bridge.take() {
                                    bridge.stop();
                                }
                                state.virtual_bus_status = "Stopped".to_string();
                                state.virtual_bus_running = false;
                                state.follow_virtual_bus_port = false;
                            } else {
                                if let Some(mut stale_bridge) = virtual_bus_bridge.take() {
                                    stale_bridge.stop();
                                }
                                match VirtualBusBridge::start() {
                                    Ok(bridge) => {
                                        state.virtual_bus_status = bridge.status();
                                        state.virtual_bus_running = true;
                                        state.follow_virtual_bus_port = true;
                                        virtual_bus_bridge = Some(bridge);
                                    }
                                    Err(err) => {
                                        state.virtual_bus_status = err;
                                        state.virtual_bus_running = false;
                                        state.follow_virtual_bus_port = false;
                                    }
                                }
                            }
                        }
                        URDF_RELOAD_BUTTON_ID => {
                            reload_urdf_state(&mut state.urdf_view);
                        }
                        _ => {}
                    },
                    ClientEvent::OnTextChanged(t) => match t.id {
                        SERIAL_PORT_INPUT_ID => {
                            state.serial_port = t.value;
                            state.follow_virtual_bus_port = false;
                        }
                        SERIAL_BAUD_INPUT_ID => {
                            state.serial_baud = t.value;
                        }
                        _ => {}
                    },
                    ClientEvent::OnSliderChange(s) => {
                        if !apply_urdf_slider_change(&mut state.urdf_view, s.id, s.value) {
                            if s.id == SERVO_COUNT_SLIDER_ID {
                                state.desired_servo_count = s.value.clamp(1, 32);
                            }
                            if s.id == TARGET_POSITION_SLIDER_ID {
                                if let Some(inx) = s.inx {
                                    let _ = sim.set_target_position(
                                        inx as u8,
                                        s.value.clamp(0, 4095) as i16,
                                    );
                                    state.snapshots = sim.servo_snapshots();
                                }
                            }
                        }
                    }
                    _ => {}
                }

                for id in &client_ids {
                    wgui.render(*id, render(&state)).await;
                }
            }
        }
    }

    if let Some(mut bridge) = serial_bridge {
        bridge.stop();
    }
    if let Some(mut bridge) = virtual_bus_bridge {
        bridge.stop();
    }

    Ok(())
}
