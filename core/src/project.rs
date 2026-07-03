use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::path::{Path, PathBuf};

use crate::scene_harness::UrdfSceneHarness;

pub const ROBOT_DREAMS_MODEL_FORMAT: &str = "robotdreams.model.v1";
pub const ROBOT_DREAMS_PROJECT_FORMAT: &str = "robotdreams.project.v1";

#[derive(Clone, Debug)]
pub struct ProjectConfig {
    pub format: String,
    pub name: String,
    pub manifest_path: PathBuf,
    pub base_dir: PathBuf,
    pub model_profile_path: Option<String>,
    pub scene: ProjectSceneConfig,
    pub robots: Vec<ProjectRobotConfig>,
    pub hardware: HardwareConfig,
}

#[derive(Clone, Debug, Default)]
pub struct ProjectSceneConfig {
    pub objects: Vec<ProjectSceneObjectConfig>,
    pub cameras: Vec<ProjectCameraConfig>,
}

#[derive(Clone, Debug)]
pub struct ProjectSceneObjectConfig {
    pub id: String,
    pub name: String,
    pub type_name: String,
    pub icon: String,
    pub geometry: ProjectSceneObjectGeometry,
    pub color_rgb: [u8; 3],
    pub position: [f32; 3],
    pub rotation: [f32; 3],
    pub scale: Option<[f32; 3]>,
    pub include_in_fit: bool,
}

#[derive(Clone, Debug)]
pub struct ProjectCameraConfig {
    pub id: String,
    pub name: String,
    pub type_name: String,
    pub icon: String,
    pub mounted_robot: String,
    pub mounted_link: String,
    pub position: [f32; 3],
    pub rotation: [f32; 3],
    pub fov_deg: f32,
    pub rate: String,
    pub resolution: Option<[u32; 2]>,
}

#[derive(Clone, Debug)]
pub enum ProjectSceneObjectGeometry {
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
pub struct ProjectRobotConfig {
    pub id: String,
    pub name: String,
    pub model: ProjectRobotModelConfig,
    pub joint_names: HashMap<String, String>,
    pub base_translation: [f32; 3],
    pub base_rotation: [f32; 3],
}

#[derive(Clone, Debug)]
pub struct ProjectRobotModelConfig {
    pub type_name: String,
    pub path: String,
}

#[derive(Clone, Debug, Default)]
pub struct HardwareConfig {
    pub buses: Vec<BusConfig>,
}

#[derive(Clone, Debug)]
pub struct BusConfig {
    pub id: String,
    pub name: String,
    pub transport: BusTransportConfig,
    pub protocol: String,
    pub devices: Vec<DeviceConfig>,
}

#[derive(Clone, Debug)]
pub struct BusTransportConfig {
    pub type_name: String,
    pub path: Option<String>,
    pub baud: Option<u32>,
}

#[derive(Clone, Debug)]
pub enum DeviceConfig {
    Servo(ServoDeviceConfig),
    Imu(ImuDeviceConfig),
    IoBoard(IoBoardDeviceConfig),
}

#[derive(Clone, Debug)]
pub struct DeviceMapping {
    pub robot: String,
    pub target: String,
}

#[derive(Clone, Debug)]
pub struct ServoCalibrationConfig {
    pub zero_offset: i16,
    pub direction: i8,
}

#[derive(Clone, Debug)]
pub struct ServoDeviceConfig {
    pub id: u32,
    pub name: String,
    pub profile: String,
    pub drives: Option<DeviceMapping>,
    pub calibration: ServoCalibrationConfig,
}

#[derive(Clone, Debug)]
pub struct ImuDeviceConfig {
    pub id: u32,
    pub name: String,
    pub profile: String,
    pub mounted_on: Option<DeviceMapping>,
}

#[derive(Clone, Debug)]
pub struct IoBoardDeviceConfig {
    pub id: u32,
    pub name: String,
    pub profile: String,
}

#[derive(Clone, Debug)]
pub struct ModelProfile {
    pub format: String,
    pub name: String,
    pub manifest_path: PathBuf,
    pub base_dir: PathBuf,
    pub robot: ProjectRobotConfig,
    pub joint_names: HashMap<String, String>,
    pub tcp: Option<TcpConfig>,
    pub frame_mapping: Option<FrameMappingConfig>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TcpConfig {
    pub link: String,
    pub offset: [f32; 3],
}

#[derive(Clone, Debug, PartialEq)]
pub struct FrameMappingConfig {
    pub core: FrameAxesConfig,
    pub model: FrameAxesConfig,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FrameAxesConfig {
    pub forward_axis: String,
    pub left_axis: String,
    pub up_axis: String,
}

#[derive(Clone, Debug)]
pub struct RobotDreamsModel {
    manifest_path: PathBuf,
    project: Option<ProjectConfig>,
    model_profile: Option<ModelProfile>,
    robots: Vec<LoadedRobotModel>,
}

#[derive(Clone, Debug)]
struct LoadedRobotModel {
    config: ProjectRobotConfig,
    model_profile: Option<ModelProfile>,
    urdf_path: PathBuf,
    harness: UrdfSceneHarness,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ModelEntityKind {
    Robot,
    Joint,
    Link,
    Tcp,
    SceneObject,
    Camera,
    HardwareDevice,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SceneLocation {
    pub position: [f64; 3],
    pub rotation: Option<[f64; 3]>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RobotDreamsEntity {
    pub kind: ModelEntityKind,
    pub id: String,
    pub name: String,
    pub robot_id: Option<String>,
    pub urdf_name: Option<String>,
    pub location: Option<SceneLocation>,
    pub properties: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RobotState {
    pub id: String,
    pub name: String,
    pub base: SceneLocation,
    pub joints: BTreeMap<String, JointState>,
    pub links: BTreeMap<String, LinkState>,
    pub tcp: Option<FrameState>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct JointState {
    pub urdf_name: String,
    pub semantic_name: Option<String>,
    pub position_rad: f64,
    pub location: Option<SceneLocation>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LinkState {
    pub urdf_name: String,
    pub location: Option<SceneLocation>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FrameState {
    pub name: String,
    pub link: String,
    pub location: Option<SceneLocation>,
}

fn normalize_query(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn matches_query(query: &str, candidates: &[String]) -> bool {
    let query = normalize_query(query);
    candidates
        .iter()
        .any(|candidate| normalize_query(candidate) == query)
}

fn string_candidates(values: &[&str]) -> Vec<String> {
    values
        .iter()
        .filter(|value| !value.is_empty())
        .map(|value| (*value).to_string())
        .collect()
}

fn joint_candidates(urdf_name: &str, semantic_name: Option<&str>) -> Vec<String> {
    let mut candidates = string_candidates(&[urdf_name, &format!("{urdf_name} joint")]);
    if let Some(semantic_name) = semantic_name {
        candidates.push(semantic_name.to_string());
        candidates.push(format!("{semantic_name} joint"));
    }
    candidates
}

fn project_model_profile(project: &ProjectConfig) -> Option<ModelProfile> {
    let model_profile_path = project.model_profile_path.as_ref()?;
    load_model_profile(project.base_dir.join(model_profile_path)).ok()
}

fn model_profile_matches_robot(profile: &ModelProfile, robot: &ProjectRobotConfig) -> bool {
    profile.robot.id == robot.id
        || profile.robot.name == robot.name
        || profile.robot.model.path == robot.model.path
}

fn model_profile_for_robot(
    project_model_profile: Option<&ModelProfile>,
    robot: &ProjectRobotConfig,
) -> Option<ModelProfile> {
    project_model_profile
        .filter(|profile| model_profile_matches_robot(profile, robot))
        .or(project_model_profile)
        .cloned()
}

fn model_profile_joint_names(profile: &ModelProfile) -> HashMap<String, String> {
    let mut joint_names = profile.robot.joint_names.clone();
    joint_names.extend(profile.joint_names.clone());
    joint_names
}

fn merge_model_profile_joint_names(
    mut robot: ProjectRobotConfig,
    model_profile: Option<&ModelProfile>,
) -> ProjectRobotConfig {
    let Some(model_profile) =
        model_profile.filter(|profile| model_profile_matches_robot(profile, &robot))
    else {
        return robot;
    };

    let project_joint_names = std::mem::take(&mut robot.joint_names);
    let mut joint_names = model_profile_joint_names(model_profile);
    joint_names.extend(project_joint_names);
    robot.joint_names = joint_names;
    robot
}

fn loaded_robot_from_config(
    project_base_dir: &Path,
    robot: ProjectRobotConfig,
    model_profile: Option<ModelProfile>,
) -> Result<LoadedRobotModel, Box<dyn Error>> {
    if !robot.model.type_name.eq_ignore_ascii_case("urdf") {
        return Err(format!(
            "Robot '{}' uses unsupported model type '{}'",
            robot.id, robot.model.type_name
        )
        .into());
    }

    let urdf_path = project_base_dir.join(&robot.model.path);
    let harness = UrdfSceneHarness::from_urdf_path(&urdf_path)?;
    Ok(LoadedRobotModel {
        config: robot,
        model_profile,
        urdf_path,
        harness,
    })
}

fn loaded_robot_from_profile(profile: ModelProfile) -> Result<LoadedRobotModel, Box<dyn Error>> {
    let base_dir = profile.base_dir.clone();
    loaded_robot_from_config(&base_dir, profile.robot.clone(), Some(profile))
}

fn load_project_model(project: ProjectConfig) -> Result<RobotDreamsModel, Box<dyn Error>> {
    let model_profile = project_model_profile(&project);
    let robot_configs = if project.robots.is_empty() {
        model_profile
            .as_ref()
            .map(|profile| vec![profile.robot.clone()])
            .unwrap_or_default()
    } else {
        project.robots.clone()
    };

    let mut robots = Vec::new();
    for robot in robot_configs {
        let robot_model_profile = model_profile_for_robot(model_profile.as_ref(), &robot);
        let robot = merge_model_profile_joint_names(robot, robot_model_profile.as_ref());
        robots.push(loaded_robot_from_config(
            &project.base_dir,
            robot,
            robot_model_profile,
        )?);
    }

    Ok(RobotDreamsModel {
        manifest_path: project.manifest_path.clone(),
        project: Some(project),
        model_profile,
        robots,
    })
}

fn load_profile_model(profile: ModelProfile) -> Result<RobotDreamsModel, Box<dyn Error>> {
    Ok(RobotDreamsModel {
        manifest_path: profile.manifest_path.clone(),
        project: None,
        model_profile: Some(profile.clone()),
        robots: vec![loaded_robot_from_profile(profile)?],
    })
}

fn transform_project_point(point: [f64; 3], translation: [f32; 3], rotation: [f32; 3]) -> [f64; 3] {
    vec_add(
        transform_vector(rpy_rotation(f32_vec3_to_f64(rotation)), point),
        f32_vec3_to_f64(translation),
    )
}

fn scene_location(
    position: [f64; 3],
    rotation: Option<[f32; 3]>,
    base_translation: [f32; 3],
    base_rotation: [f32; 3],
) -> SceneLocation {
    SceneLocation {
        position: transform_project_point(position, base_translation, base_rotation),
        rotation: rotation.map(f32_vec3_to_f64),
    }
}

fn f32_vec3_to_f64(value: [f32; 3]) -> [f64; 3] {
    [value[0] as f64, value[1] as f64, value[2] as f64]
}

fn rpy_rotation(rpy: [f64; 3]) -> [[f64; 3]; 3] {
    let (roll, pitch, yaw) = (rpy[0], rpy[1], rpy[2]);
    mat_mul(mat_mul(rot_z(yaw), rot_y(pitch)), rot_x(roll))
}

fn transform_vector(rotation: [[f64; 3]; 3], vector: [f64; 3]) -> [f64; 3] {
    [
        rotation[0][0] * vector[0] + rotation[0][1] * vector[1] + rotation[0][2] * vector[2],
        rotation[1][0] * vector[0] + rotation[1][1] * vector[1] + rotation[1][2] * vector[2],
        rotation[2][0] * vector[0] + rotation[2][1] * vector[1] + rotation[2][2] * vector[2],
    ]
}

fn rot_x(angle: f64) -> [[f64; 3]; 3] {
    let cos = angle.cos();
    let sin = angle.sin();
    [[1.0, 0.0, 0.0], [0.0, cos, -sin], [0.0, sin, cos]]
}

fn rot_y(angle: f64) -> [[f64; 3]; 3] {
    let cos = angle.cos();
    let sin = angle.sin();
    [[cos, 0.0, sin], [0.0, 1.0, 0.0], [-sin, 0.0, cos]]
}

fn rot_z(angle: f64) -> [[f64; 3]; 3] {
    let cos = angle.cos();
    let sin = angle.sin();
    [[cos, -sin, 0.0], [sin, cos, 0.0], [0.0, 0.0, 1.0]]
}

fn mat_mul(left: [[f64; 3]; 3], right: [[f64; 3]; 3]) -> [[f64; 3]; 3] {
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

fn vec_add(left: [f64; 3], right: [f64; 3]) -> [f64; 3] {
    [left[0] + right[0], left[1] + right[1], left[2] + right[2]]
}

fn robot_entity(robot: &LoadedRobotModel) -> RobotDreamsEntity {
    let mut properties = BTreeMap::new();
    properties.insert(
        "urdfPath".to_string(),
        robot.urdf_path.display().to_string(),
    );
    RobotDreamsEntity {
        kind: ModelEntityKind::Robot,
        id: robot.config.id.clone(),
        name: robot.config.name.clone(),
        robot_id: Some(robot.config.id.clone()),
        urdf_name: None,
        location: Some(scene_location(
            [0.0, 0.0, 0.0],
            Some(robot.config.base_rotation),
            robot.config.base_translation,
            [0.0, 0.0, 0.0],
        )),
        properties,
    }
}

fn joint_entity(robot: &LoadedRobotModel, joint: &urdf_rs::Joint) -> RobotDreamsEntity {
    let semantic_name = robot.config.joint_names.get(&joint.name);
    let mut properties = BTreeMap::new();
    properties.insert("childLink".to_string(), joint.child.link.clone());
    properties.insert("parentLink".to_string(), joint.parent.link.clone());
    properties.insert("type".to_string(), format!("{:?}", joint.joint_type));
    if let Some(semantic_name) = semantic_name {
        properties.insert("semanticName".to_string(), semantic_name.clone());
    }

    RobotDreamsEntity {
        kind: ModelEntityKind::Joint,
        id: format!("{}:joint:{}", robot.config.id, joint.name),
        name: semantic_name.cloned().unwrap_or_else(|| joint.name.clone()),
        robot_id: Some(robot.config.id.clone()),
        urdf_name: Some(joint.name.clone()),
        location: robot
            .harness
            .link_origin_world(&joint.child.link)
            .map(|position| {
                scene_location(
                    position,
                    None,
                    robot.config.base_translation,
                    robot.config.base_rotation,
                )
            }),
        properties,
    }
}

fn link_entity(robot: &LoadedRobotModel, link_name: &str) -> RobotDreamsEntity {
    RobotDreamsEntity {
        kind: ModelEntityKind::Link,
        id: format!("{}:link:{link_name}", robot.config.id),
        name: link_name.to_string(),
        robot_id: Some(robot.config.id.clone()),
        urdf_name: Some(link_name.to_string()),
        location: robot.harness.link_origin_world(link_name).map(|position| {
            scene_location(
                position,
                None,
                robot.config.base_translation,
                robot.config.base_rotation,
            )
        }),
        properties: BTreeMap::new(),
    }
}

fn tcp_entity(robot: &LoadedRobotModel, tcp: &TcpConfig) -> RobotDreamsEntity {
    let mut properties = BTreeMap::new();
    properties.insert("link".to_string(), tcp.link.clone());
    properties.insert("offset".to_string(), format!("{:?}", tcp.offset));
    RobotDreamsEntity {
        kind: ModelEntityKind::Tcp,
        id: format!("{}:tcp", robot.config.id),
        name: "tcp".to_string(),
        robot_id: Some(robot.config.id.clone()),
        urdf_name: Some(tcp.link.clone()),
        location: robot
            .harness
            .link_point_world(
                &tcp.link,
                [
                    tcp.offset[0] as f64,
                    tcp.offset[1] as f64,
                    tcp.offset[2] as f64,
                ],
            )
            .map(|position| {
                scene_location(
                    position,
                    None,
                    robot.config.base_translation,
                    robot.config.base_rotation,
                )
            }),
        properties,
    }
}

fn robot_base_location(robot: &LoadedRobotModel) -> SceneLocation {
    scene_location(
        [0.0, 0.0, 0.0],
        Some(robot.config.base_rotation),
        robot.config.base_translation,
        [0.0, 0.0, 0.0],
    )
}

fn robot_link_location(robot: &LoadedRobotModel, link_name: &str) -> Option<SceneLocation> {
    robot.harness.link_origin_world(link_name).map(|position| {
        scene_location(
            position,
            None,
            robot.config.base_translation,
            robot.config.base_rotation,
        )
    })
}

fn robot_joint_location(robot: &LoadedRobotModel, joint: &urdf_rs::Joint) -> Option<SceneLocation> {
    robot_link_location(robot, &joint.child.link)
}

fn robot_tcp_location(robot: &LoadedRobotModel, tcp: &TcpConfig) -> Option<SceneLocation> {
    robot
        .harness
        .link_point_world(
            &tcp.link,
            [
                tcp.offset[0] as f64,
                tcp.offset[1] as f64,
                tcp.offset[2] as f64,
            ],
        )
        .map(|position| {
            scene_location(
                position,
                None,
                robot.config.base_translation,
                robot.config.base_rotation,
            )
        })
}

fn joint_state_key(urdf_name: &str, semantic_name: Option<&str>) -> String {
    semantic_name.unwrap_or(urdf_name).to_string()
}

fn joint_state(robot: &LoadedRobotModel, joint: &urdf_rs::Joint) -> JointState {
    JointState {
        urdf_name: joint.name.clone(),
        semantic_name: robot.config.joint_names.get(&joint.name).cloned(),
        position_rad: robot.harness.joint_angle(&joint.name),
        location: robot_joint_location(robot, joint),
    }
}

fn link_state(robot: &LoadedRobotModel, link_name: &str) -> LinkState {
    LinkState {
        urdf_name: link_name.to_string(),
        location: robot_link_location(robot, link_name),
    }
}

fn tcp_state(robot: &LoadedRobotModel) -> Option<FrameState> {
    let tcp = robot
        .model_profile
        .as_ref()
        .and_then(|profile| profile.tcp.as_ref())?;
    Some(FrameState {
        name: "tcp".to_string(),
        link: tcp.link.clone(),
        location: robot_tcp_location(robot, tcp),
    })
}

fn robot_state(robot: &LoadedRobotModel) -> RobotState {
    let joints = robot
        .harness
        .robot()
        .joints
        .iter()
        .map(|joint| {
            let semantic_name = robot
                .config
                .joint_names
                .get(&joint.name)
                .map(String::as_str);
            (
                joint_state_key(&joint.name, semantic_name),
                joint_state(robot, joint),
            )
        })
        .collect();
    let links = robot
        .harness
        .robot()
        .links
        .iter()
        .map(|link| (link.name.clone(), link_state(robot, &link.name)))
        .collect();

    RobotState {
        id: robot.config.id.clone(),
        name: robot.config.name.clone(),
        base: robot_base_location(robot),
        joints,
        links,
        tcp: tcp_state(robot),
    }
}

fn scene_object_entity(object: &ProjectSceneObjectConfig) -> RobotDreamsEntity {
    let mut properties = BTreeMap::new();
    properties.insert("type".to_string(), object.type_name.clone());
    properties.insert("icon".to_string(), object.icon.clone());
    RobotDreamsEntity {
        kind: ModelEntityKind::SceneObject,
        id: object.id.clone(),
        name: object.name.clone(),
        robot_id: None,
        urdf_name: None,
        location: Some(SceneLocation {
            position: f32_vec3_to_f64(object.position),
            rotation: Some(f32_vec3_to_f64(object.rotation)),
        }),
        properties,
    }
}

fn camera_entity(camera: &ProjectCameraConfig) -> RobotDreamsEntity {
    let mut properties = BTreeMap::new();
    properties.insert("mountedRobot".to_string(), camera.mounted_robot.clone());
    properties.insert("mountedLink".to_string(), camera.mounted_link.clone());
    properties.insert("type".to_string(), camera.type_name.clone());
    RobotDreamsEntity {
        kind: ModelEntityKind::Camera,
        id: camera.id.clone(),
        name: camera.name.clone(),
        robot_id: Some(camera.mounted_robot.clone()),
        urdf_name: Some(camera.mounted_link.clone()),
        location: Some(SceneLocation {
            position: f32_vec3_to_f64(camera.position),
            rotation: Some(f32_vec3_to_f64(camera.rotation)),
        }),
        properties,
    }
}

fn hardware_device_entity(bus: &BusConfig, device: &DeviceConfig) -> RobotDreamsEntity {
    match device {
        DeviceConfig::Servo(servo) => {
            let mut properties = BTreeMap::new();
            properties.insert("bus".to_string(), bus.id.clone());
            properties.insert("profile".to_string(), servo.profile.clone());
            if let Some(drives) = &servo.drives {
                properties.insert("drivesRobot".to_string(), drives.robot.clone());
                properties.insert("drivesJoint".to_string(), drives.target.clone());
            }
            RobotDreamsEntity {
                kind: ModelEntityKind::HardwareDevice,
                id: format!("{}:servo:{}", bus.id, servo.id),
                name: servo.name.clone(),
                robot_id: servo.drives.as_ref().map(|drives| drives.robot.clone()),
                urdf_name: servo.drives.as_ref().map(|drives| drives.target.clone()),
                location: None,
                properties,
            }
        }
        DeviceConfig::Imu(imu) => {
            let mut properties = BTreeMap::new();
            properties.insert("bus".to_string(), bus.id.clone());
            properties.insert("profile".to_string(), imu.profile.clone());
            RobotDreamsEntity {
                kind: ModelEntityKind::HardwareDevice,
                id: format!("{}:imu:{}", bus.id, imu.id),
                name: imu.name.clone(),
                robot_id: imu
                    .mounted_on
                    .as_ref()
                    .map(|mounted_on| mounted_on.robot.clone()),
                urdf_name: imu
                    .mounted_on
                    .as_ref()
                    .map(|mounted_on| mounted_on.target.clone()),
                location: None,
                properties,
            }
        }
        DeviceConfig::IoBoard(io_board) => {
            let mut properties = BTreeMap::new();
            properties.insert("bus".to_string(), bus.id.clone());
            properties.insert("profile".to_string(), io_board.profile.clone());
            RobotDreamsEntity {
                kind: ModelEntityKind::HardwareDevice,
                id: format!("{}:io_board:{}", bus.id, io_board.id),
                name: io_board.name.clone(),
                robot_id: None,
                urdf_name: None,
                location: None,
                properties,
            }
        }
    }
}

impl RobotDreamsModel {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Box<dyn Error>> {
        let path = path.as_ref();
        if let Some(project_manifest) = project_manifest_for_input_path(path) {
            let project = project_config_from_manifest(&project_manifest).ok_or_else(|| {
                format!(
                    "{} is not a RobotDreams project",
                    project_manifest.display()
                )
            })?;
            return load_project_model(project);
        }

        load_profile_model(load_model_profile(path)?)
    }

    pub fn manifest_path(&self) -> &Path {
        &self.manifest_path
    }

    pub fn project(&self) -> Option<&ProjectConfig> {
        self.project.as_ref()
    }

    pub fn model_profile(&self) -> Option<&ModelProfile> {
        self.model_profile.as_ref()
    }

    pub fn named(&self, name: &str) -> Option<RobotDreamsEntity> {
        for robot in &self.robots {
            if matches_query(
                name,
                &string_candidates(&[&robot.config.id, &robot.config.name]),
            ) {
                return Some(robot_entity(robot));
            }

            if matches_query(name, &string_candidates(&["tcp", "tool center point"]))
                && let Some(tcp) = robot
                    .model_profile
                    .as_ref()
                    .and_then(|profile| profile.tcp.as_ref())
            {
                return Some(tcp_entity(robot, tcp));
            }

            for joint in &robot.harness.robot().joints {
                let semantic_name = robot
                    .config
                    .joint_names
                    .get(&joint.name)
                    .map(String::as_str);
                if matches_query(name, &joint_candidates(&joint.name, semantic_name)) {
                    return Some(joint_entity(robot, joint));
                }
            }

            for link in &robot.harness.robot().links {
                if matches_query(
                    name,
                    &string_candidates(&[&link.name, &format!("{} link", link.name)]),
                ) {
                    return Some(link_entity(robot, &link.name));
                }
            }
        }

        if let Some(project) = &self.project {
            for object in &project.scene.objects {
                if matches_query(name, &string_candidates(&[&object.id, &object.name])) {
                    return Some(scene_object_entity(object));
                }
            }

            for camera in &project.scene.cameras {
                if matches_query(name, &string_candidates(&[&camera.id, &camera.name])) {
                    return Some(camera_entity(camera));
                }
            }

            for bus in &project.hardware.buses {
                for device in &bus.devices {
                    let entity = hardware_device_entity(bus, device);
                    if matches_query(name, &string_candidates(&[&entity.id, &entity.name])) {
                        return Some(entity);
                    }
                }
            }
        }

        None
    }

    pub fn location_of(&self, name: &str) -> Option<SceneLocation> {
        self.named(name)?.location
    }

    pub fn robot_state(&self, robot_id_or_name: &str) -> Option<RobotState> {
        self.robots
            .iter()
            .find(|robot| {
                matches_query(
                    robot_id_or_name,
                    &string_candidates(&[&robot.config.id, &robot.config.name]),
                )
            })
            .map(robot_state)
    }

    pub fn set_joint_angle(
        &mut self,
        name: impl AsRef<str>,
        radians: f64,
    ) -> Result<(), Box<dyn Error>> {
        let name = name.as_ref();
        for robot in &mut self.robots {
            for joint in &robot.harness.robot().joints {
                let semantic_name = robot
                    .config
                    .joint_names
                    .get(&joint.name)
                    .map(String::as_str);
                if matches_query(name, &joint_candidates(&joint.name, semantic_name)) {
                    robot.harness.set_joint_angle(joint.name.clone(), radians);
                    return Ok(());
                }
            }
        }

        Err(format!("No joint named '{name}' found").into())
    }
}

pub fn project_config_from_manifest(path: &Path) -> Option<ProjectConfig> {
    let json = read_json_file(path).ok()?;
    parse_project_config(&json, path.to_path_buf())
}

pub fn model_profile_from_manifest(path: &Path) -> Option<ModelProfile> {
    let json = read_json_file(path).ok()?;
    parse_model_profile(&json, path.to_path_buf())
}

pub fn load_model_profile(path: impl AsRef<Path>) -> Result<ModelProfile, Box<dyn Error>> {
    let manifest = resolve_model_profile_manifest(path.as_ref())?;
    model_profile_from_manifest(&manifest)
        .ok_or_else(|| format!("{} is not a RobotDreams model profile", manifest.display()).into())
}

pub fn project_manifest_for_input_path(path: &Path) -> Option<PathBuf> {
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
        for candidate in [path.join("project.json"), path.join("robotdreams.json")] {
            if project_config_from_manifest(&candidate).is_some() {
                return Some(candidate);
            }
        }
    }

    None
}

pub fn project_config_for_input_path(path: Option<&Path>) -> Option<ProjectConfig> {
    let manifest_path = path.and_then(project_manifest_for_input_path)?;
    project_config_from_manifest(&manifest_path)
}

pub fn resolve_urdf_path(path: Option<PathBuf>) -> Result<PathBuf, Box<dyn Error>> {
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

pub fn resolve_model_profile_manifest(path: &Path) -> Result<PathBuf, Box<dyn Error>> {
    if path.is_dir() {
        for candidate in [
            path.join("robotdreams.json"),
            path.join("model/robotdreams.json"),
            path.join("robotdreams.example.json"),
        ] {
            if model_profile_from_manifest(&candidate).is_some() {
                return Ok(candidate);
            }
        }
    }

    if path.is_file() {
        let json = read_json_file(path)?;
        if json_string_path(&json, &["format"]) == Some(ROBOT_DREAMS_MODEL_FORMAT) {
            return Ok(path.to_path_buf());
        }
        if json_string_path(&json, &["format"]) == Some(ROBOT_DREAMS_PROJECT_FORMAT)
            && let Some(model_profile) = json_string_path(&json, &["modelProfile"])
        {
            return resolve_model_profile_manifest(&path_base(path).join(model_profile));
        }
    }

    Err(format!("{} is not a RobotDreams model profile", path.display()).into())
}

pub fn resolve_robot_input_path(path: PathBuf) -> Result<PathBuf, Box<dyn Error>> {
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

pub fn resolve_json_urdf_path(path: &Path) -> Result<PathBuf, Box<dyn Error>> {
    let json = read_json_file(path)?;
    let base = path_base(path);

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

    if json_string_path(&json, &["format"]) == Some(ROBOT_DREAMS_MODEL_FORMAT)
        && let Some(urdf_path) = json_string_path(&json, &["robot", "model", "path"])
    {
        return Ok(base.join(urdf_path));
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

fn parse_model_profile(value: &serde_json::Value, manifest_path: PathBuf) -> Option<ModelProfile> {
    let format = json_string_path(value, &["format"])?;
    if format != ROBOT_DREAMS_MODEL_FORMAT {
        return None;
    }

    let base_dir = path_base(&manifest_path);
    let name = json_string_path(value, &["name"])
        .unwrap_or("RobotDreams Model")
        .to_string();
    let robot = parse_model_profile_robot_config(value, &name)?;
    let joint_names = parse_project_joint_names(value);

    Some(ModelProfile {
        format: format.to_string(),
        name,
        manifest_path,
        base_dir,
        robot,
        joint_names,
        tcp: parse_tcp_config(value),
        frame_mapping: parse_frame_mapping_config(value),
    })
}

fn parse_model_profile_robot_config(
    value: &serde_json::Value,
    name: &str,
) -> Option<ProjectRobotConfig> {
    let robot = value.get("robot")?;
    let joint_names = parse_project_joint_names(value);
    Some(ProjectRobotConfig {
        id: json_string_path(robot, &["id"]).unwrap_or(name).to_string(),
        name: json_string_path(robot, &["name"])
            .unwrap_or(name)
            .to_string(),
        model: parse_project_robot_model_config(robot)?,
        joint_names,
        base_translation: json_vec3_path(robot, &["base", "translation"])
            .or_else(|| json_vec3_path(robot, &["base", "position"]))
            .unwrap_or([0.0, 0.0, 0.0]),
        base_rotation: json_vec3_path(robot, &["base", "rotation"]).unwrap_or([0.0, 0.0, 0.0]),
    })
}

fn parse_project_config(
    value: &serde_json::Value,
    manifest_path: PathBuf,
) -> Option<ProjectConfig> {
    let format = json_string_path(value, &["format"])?;
    if format != ROBOT_DREAMS_PROJECT_FORMAT {
        return None;
    }

    let base_dir = path_base(&manifest_path);
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
        manifest_path,
        base_dir,
        model_profile_path: json_string_path(value, &["modelProfile"]).map(str::to_string),
        scene: parse_project_scene_config(value),
        robots,
        hardware: parse_hardware_config(value),
    })
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

fn read_json_file(path: &Path) -> Result<serde_json::Value, Box<dyn Error>> {
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

fn parse_tcp_config(value: &serde_json::Value) -> Option<TcpConfig> {
    Some(TcpConfig {
        link: json_string_path(value, &["tcp", "link"])?.to_string(),
        offset: json_vec3_path(value, &["tcp", "offset"]).unwrap_or([0.0, 0.0, 0.0]),
    })
}

fn parse_frame_mapping_config(value: &serde_json::Value) -> Option<FrameMappingConfig> {
    Some(FrameMappingConfig {
        core: parse_frame_axes_config(value, &["frameMapping", "core"])?,
        model: parse_frame_axes_config(value, &["frameMapping", "model"])?,
    })
}

fn parse_frame_axes_config(value: &serde_json::Value, path: &[&str]) -> Option<FrameAxesConfig> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }

    Some(FrameAxesConfig {
        forward_axis: json_string_path(current, &["forwardAxis"])?.to_string(),
        left_axis: json_string_path(current, &["leftAxis"])?.to_string(),
        up_axis: json_string_path(current, &["upAxis"])?.to_string(),
    })
}

fn path_base(path: &Path) -> PathBuf {
    path.parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn project_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("core crate has workspace parent")
            .to_path_buf()
    }

    fn puppyarm_project_path() -> PathBuf {
        project_root().join("examples/puppyarm/project.json")
    }

    fn puppyarm_model_profile_path() -> PathBuf {
        project_root().join("examples/puppyarm/model/robotdreams.json")
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

    fn assert_close(left: f64, right: f64) {
        assert!(
            (left - right).abs() < 1.0e-6,
            "expected {left} close to {right}"
        );
    }

    fn distance(left: [f64; 3], right: [f64; 3]) -> f64 {
        let dx = left[0] - right[0];
        let dy = left[1] - right[1];
        let dz = left[2] - right[2];
        (dx * dx + dy * dy + dz * dz).sqrt()
    }

    #[test]
    fn model_opens_project_and_queries_named_entities() {
        let model = RobotDreamsModel::open(puppyarm_project_path()).expect("load PuppyArm project");

        let wrist = model.named("wrist joint").expect("wrist joint entity");
        assert_eq!(wrist.kind, ModelEntityKind::Joint);
        assert_eq!(wrist.robot_id.as_deref(), Some("puppyarm"));
        assert_eq!(wrist.urdf_name.as_deref(), Some("revolute_1_1"));
        assert_eq!(
            wrist.properties.get("semanticName").map(String::as_str),
            Some("wrist")
        );
        assert!(wrist.location.is_some());

        let trashbin = model.named("trashbin").expect("trashbin entity");
        assert_eq!(trashbin.kind, ModelEntityKind::SceneObject);
        let trashbin_location = trashbin.location.expect("trashbin scene location");
        assert_close(trashbin_location.position[0], 0.38);
        assert_close(trashbin_location.position[1], 0.0);
        assert_close(trashbin_location.position[2], -0.28);

        let robot = model.named("puppyarm").expect("robot entity");
        assert_eq!(robot.kind, ModelEntityKind::Robot);
        let robot_location = robot.location.expect("robot scene location");
        assert_close(robot_location.position[0], 0.0);
        assert_close(robot_location.position[1], 0.154023);
        assert_close(robot_location.position[2], 0.0);
    }

    #[test]
    fn project_uses_model_profile_joint_names_when_robot_omits_them() {
        let project_path = write_temp_project(
            "profile-joint-names",
            serde_json::json!({
                "format": ROBOT_DREAMS_PROJECT_FORMAT,
                "name": "Profile Joint Names",
                "modelProfile": puppyarm_model_profile_path(),
                "robots": [
                    {
                        "id": "puppyarm",
                        "name": "PuppyArm",
                        "model": {
                            "type": "urdf",
                            "path": puppyarm_urdf_path()
                        }
                    }
                ]
            }),
        );

        let model = RobotDreamsModel::open(&project_path).expect("load temp project");
        let wrist = model.named("wrist joint").expect("profile wrist joint");
        assert_eq!(wrist.kind, ModelEntityKind::Joint);
        assert_eq!(wrist.urdf_name.as_deref(), Some("revolute_1_1"));

        let _ = std::fs::remove_dir_all(project_path.parent().expect("temp project parent"));
    }

    #[test]
    fn project_joint_names_override_model_profile_joint_names() {
        let project_path = write_temp_project(
            "project-joint-name-override",
            serde_json::json!({
                "format": ROBOT_DREAMS_PROJECT_FORMAT,
                "name": "Project Joint Name Override",
                "modelProfile": puppyarm_model_profile_path(),
                "robots": [
                    {
                        "id": "puppyarm",
                        "name": "PuppyArm",
                        "model": {
                            "type": "urdf",
                            "path": puppyarm_urdf_path()
                        },
                        "jointNames": {
                            "revolute_1_1": "hand"
                        }
                    }
                ]
            }),
        );

        let model = RobotDreamsModel::open(&project_path).expect("load temp project");
        let hand = model.named("hand joint").expect("project hand joint");
        assert_eq!(hand.kind, ModelEntityKind::Joint);
        assert_eq!(hand.urdf_name.as_deref(), Some("revolute_1_1"));
        assert!(model.named("wrist joint").is_none());

        let yaw = model.named("yaw joint").expect("profile yaw joint");
        assert_eq!(yaw.urdf_name.as_deref(), Some("revolute_2_1"));

        let _ = std::fs::remove_dir_all(project_path.parent().expect("temp project parent"));
    }

    #[test]
    fn model_queries_tcp_and_updates_joint_dependent_location() {
        let mut model =
            RobotDreamsModel::open(puppyarm_project_path()).expect("load PuppyArm project");

        let tcp = model.named("tcp").expect("tcp entity");
        assert_eq!(tcp.kind, ModelEntityKind::Tcp);
        assert_eq!(
            tcp.properties.get("link").map(String::as_str),
            Some("part_1_1")
        );
        let first_location = tcp.location.expect("tcp scene location").position;

        model
            .set_joint_angle("yaw joint", 0.5)
            .expect("set semantic yaw joint angle");
        let moved_location = model
            .location_of("tcp")
            .expect("tcp scene location after joint update")
            .position;

        assert!(
            distance(first_location, moved_location) > 1.0e-6,
            "expected TCP location to change after yaw joint update"
        );
    }
}
