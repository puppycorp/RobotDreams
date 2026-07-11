use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::path::{Path, PathBuf};

use crate::scene_graph::{
    CameraDistortion, CameraIntrinsics, CameraProjection, CameraSensorEffects, EnvironmentSettings,
    LightKind, ReflectionProbeSettings, RenderSettings, ToneMapping,
};
use crate::scene_harness::{LinkPose, UrdfSceneHarness};

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
    pub lights: Vec<ProjectLightConfig>,
    pub render_settings: Option<RenderSettings>,
    pub reflection_probes: Vec<ReflectionProbeSettings>,
}

#[derive(Clone, Debug)]
pub struct ProjectLightConfig {
    pub id: String,
    pub name: String,
    pub kind: LightKind,
    pub color_rgb: [u8; 3],
    pub intensity: f32,
    pub position: [f32; 3],
    pub rotation: [f32; 3],
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
    pub projection: CameraProjection,
    pub rate: String,
    pub resolution: Option<[u32; 2]>,
    pub intrinsics: Option<CameraIntrinsics>,
    pub distortion: Option<CameraDistortion>,
    pub depth_range_m: Option<[f32; 2]>,
    pub sensor_effects: Option<CameraSensorEffects>,
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
    pub model_transformation: Option<ModelTransformationConfig>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ModelTransformationConfig {
    pub translation: [f32; 3],
    pub rotation: [f32; 3],
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
    DcMotor(DcMotorDeviceConfig),
    Imu(ImuDeviceConfig),
    IoBoard(IoBoardDeviceConfig),
}

#[derive(Clone, Debug)]
pub struct DeviceMapping {
    pub robot: String,
    pub target: String,
}

#[derive(Clone, Debug)]
pub struct SteeringMapping {
    pub robot: String,
    pub joints: Vec<String>,
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
    pub steers: Option<SteeringMapping>,
    pub calibration: ServoCalibrationConfig,
}

#[derive(Clone, Debug)]
pub struct DcMotorCalibrationConfig {
    pub direction: i8,
    pub max_speed_mps: f32,
}

#[derive(Clone, Debug)]
pub struct DcMotorDeviceConfig {
    pub id: u32,
    pub name: String,
    pub profile: String,
    pub drives: Option<DeviceMapping>,
    pub calibration: DcMotorCalibrationConfig,
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
    pub frames: Vec<ModelFrameConfig>,
    pub frame_mapping: Option<FrameMappingConfig>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TcpConfig {
    pub link: String,
    pub offset: [f32; 3],
}

#[derive(Clone, Debug, PartialEq)]
pub struct ModelFrameConfig {
    pub id: String,
    pub name: String,
    pub relative_to: String,
    pub translation_m: [f64; 3],
    pub rotation_rpy_rad: [f64; 3],
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
    Frame,
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
    pub frames: BTreeMap<String, ResolvedFrameState>,
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

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RigidTransform {
    pub translation_m: [f64; 3],
    pub rotation: [[f64; 3]; 3],
}

impl RigidTransform {
    pub fn identity() -> Self {
        Self {
            translation_m: [0.0, 0.0, 0.0],
            rotation: [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]],
        }
    }

    pub fn from_translation_rpy(translation_m: [f64; 3], rotation_rpy_rad: [f64; 3]) -> Self {
        Self {
            translation_m,
            rotation: rpy_rotation(rotation_rpy_rad),
        }
    }

    pub fn compose(self, child: Self) -> Self {
        Self {
            translation_m: self.transform_point(child.translation_m),
            rotation: mat_mul(self.rotation, child.rotation),
        }
    }

    pub fn inverse(self) -> Self {
        let rotation = transpose_matrix(self.rotation);
        Self {
            translation_m: negate_vec3(transform_vector(rotation, self.translation_m)),
            rotation,
        }
    }

    pub fn transform_point(self, point_m: [f64; 3]) -> [f64; 3] {
        vec_add(transform_vector(self.rotation, point_m), self.translation_m)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedFrameState {
    pub id: String,
    pub name: String,
    pub relative_to: Option<String>,
    pub relative_transform: RigidTransform,
    pub world_transform: RigidTransform,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RobotVisualMesh {
    pub robot_id: String,
    pub link_name: String,
    pub name: String,
    pub asset: PathBuf,
    pub scale: [f32; 3],
    pub translation: [f32; 3],
    pub rotation_matrix: [[f32; 3]; 3],
}

#[derive(Clone, Debug, PartialEq)]
pub struct RobotVisualTransform {
    pub translation: [f32; 3],
    pub rotation_matrix: [[f32; 3]; 3],
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

fn project_model_profile(project: &ProjectConfig) -> Result<Option<ModelProfile>, Box<dyn Error>> {
    let Some(model_profile_path) = project.model_profile_path.as_ref() else {
        return Ok(None);
    };
    load_model_profile(project.base_dir.join(model_profile_path)).map(Some)
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
    if robot.model.model_transformation.is_none() {
        robot.model.model_transformation = model_profile.robot.model.model_transformation;
    }
    robot
}

fn model_frame_configs(robot: &LoadedRobotModel) -> &[ModelFrameConfig] {
    robot
        .model_profile
        .as_ref()
        .map(|profile| profile.frames.as_slice())
        .unwrap_or_default()
}

fn resolve_robot_frames(
    robot: &LoadedRobotModel,
) -> Result<BTreeMap<String, ResolvedFrameState>, String> {
    let configs = model_frame_configs(robot);
    let mut config_by_name = BTreeMap::new();
    for config in configs {
        if config.id == "base" {
            return Err(
                "model profile frame 'base' redefines the reserved semantic base".to_string(),
            );
        }
        if config.id.trim().is_empty() || config.name.trim().is_empty() {
            return Err("model profile frame names must not be empty".to_string());
        }
        if config_by_name.insert(config.id.as_str(), config).is_some() {
            return Err(format!(
                "model profile frame '{}' is defined more than once",
                config.id
            ));
        }
        if !config.translation_m.iter().all(|value| value.is_finite())
            || !config
                .rotation_rpy_rad
                .iter()
                .all(|value| value.is_finite())
        {
            return Err(format!(
                "model profile frame '{}' has non-finite translation or rotation",
                config.name
            ));
        }
    }

    let base_transform = RigidTransform::from_translation_rpy(
        f32_vec3_to_f64(robot.config.base_translation),
        f32_vec3_to_f64(robot.config.base_rotation),
    );
    let mut resolved = BTreeMap::new();
    resolved.insert(
        "base".to_string(),
        ResolvedFrameState {
            id: "base".to_string(),
            name: "base".to_string(),
            relative_to: None,
            relative_transform: RigidTransform::identity(),
            world_transform: base_transform,
        },
    );
    let mut resolving = Vec::new();
    for name in config_by_name.keys().copied().collect::<Vec<_>>() {
        resolve_model_frame(name, &config_by_name, &mut resolved, &mut resolving)?;
    }
    Ok(resolved)
}

fn resolve_model_frame(
    name: &str,
    configs: &BTreeMap<&str, &ModelFrameConfig>,
    resolved: &mut BTreeMap<String, ResolvedFrameState>,
    resolving: &mut Vec<String>,
) -> Result<(), String> {
    if resolved.contains_key(name) {
        return Ok(());
    }
    let config = configs
        .get(name)
        .copied()
        .ok_or_else(|| format!("model profile frame '{name}' is not defined"))?;
    if let Some(cycle_start) = resolving.iter().position(|frame| frame == name) {
        let mut cycle = resolving[cycle_start..].to_vec();
        cycle.push(name.to_string());
        return Err(format!(
            "model profile frames contain a cycle: {}",
            cycle.join(" -> ")
        ));
    }

    resolving.push(name.to_string());
    if !resolved.contains_key(&config.relative_to) {
        if !configs.contains_key(config.relative_to.as_str()) {
            return Err(format!(
                "model profile frame '{}' refers to unknown parent '{}'",
                config.id, config.relative_to
            ));
        }
        resolve_model_frame(&config.relative_to, configs, resolved, resolving)?;
    }
    let parent = resolved
        .get(&config.relative_to)
        .expect("configured frame parent resolved")
        .world_transform;
    let local = RigidTransform::from_translation_rpy(config.translation_m, config.rotation_rpy_rad);
    resolved.insert(
        config.id.clone(),
        ResolvedFrameState {
            id: config.id.clone(),
            name: config.name.clone(),
            relative_to: Some(config.relative_to.clone()),
            relative_transform: local,
            world_transform: parent.compose(local),
        },
    );
    resolving.pop();
    Ok(())
}

fn model_transformation(robot: &LoadedRobotModel) -> ModelTransformationConfig {
    robot.config.model.model_transformation.unwrap_or_default()
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
    let loaded = LoadedRobotModel {
        config: robot,
        model_profile,
        urdf_path,
        harness,
    };
    resolve_robot_frames(&loaded).map_err(|err| -> Box<dyn Error> { err.into() })?;
    Ok(loaded)
}

fn loaded_robot_from_profile(profile: ModelProfile) -> Result<LoadedRobotModel, Box<dyn Error>> {
    let base_dir = profile.base_dir.clone();
    loaded_robot_from_config(&base_dir, profile.robot.clone(), Some(profile))
}

fn load_project_model(project: ProjectConfig) -> Result<RobotDreamsModel, Box<dyn Error>> {
    let model_profile = project_model_profile(&project)?;
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
    model_transformation: ModelTransformationConfig,
) -> SceneLocation {
    let model_position = transform_project_point(
        position,
        model_transformation.translation,
        model_transformation.rotation,
    );
    SceneLocation {
        position: transform_project_point(model_position, base_translation, base_rotation),
        rotation: rotation.map(f32_vec3_to_f64),
    }
}

fn f32_vec3_to_f64(value: [f32; 3]) -> [f64; 3] {
    [value[0] as f64, value[1] as f64, value[2] as f64]
}

fn f64_vec3_to_f32(value: [f64; 3]) -> [f32; 3] {
    [value[0] as f32, value[1] as f32, value[2] as f32]
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

fn transform_point_matrix(transform: ([f64; 3], [[f64; 3]; 3]), point: [f64; 3]) -> [f64; 3] {
    vec_add(transform_vector(transform.1, point), transform.0)
}

fn transform_then(
    left: ([f64; 3], [[f64; 3]; 3]),
    right: ([f64; 3], [[f64; 3]; 3]),
) -> ([f64; 3], [[f64; 3]; 3]) {
    (
        transform_point_matrix(left, right.0),
        mat_mul(left.1, right.1),
    )
}

fn f64_matrix_to_f32(value: [[f64; 3]; 3]) -> [[f32; 3]; 3] {
    [
        [value[0][0] as f32, value[0][1] as f32, value[0][2] as f32],
        [value[1][0] as f32, value[1][1] as f32, value[1][2] as f32],
        [value[2][0] as f32, value[2][1] as f32, value[2][2] as f32],
    ]
}

fn resolve_mesh_path(urdf_path: &Path, filename: &str) -> PathBuf {
    if let Some(rest) = filename.strip_prefix("package://") {
        let mut parts = rest.splitn(2, '/');
        let package = parts.next().unwrap_or_default();
        let relative = parts.next().unwrap_or_default();
        let urdf_dir = urdf_path.parent().unwrap_or_else(|| Path::new(""));
        let package_root = urdf_dir
            .ancestors()
            .find(|path| path.file_name().and_then(|name| name.to_str()) == Some(package))
            .unwrap_or_else(|| urdf_dir.parent().unwrap_or(urdf_dir));
        return package_root.join(relative);
    }
    let path = Path::new(filename);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        urdf_path
            .parent()
            .unwrap_or_else(|| Path::new(""))
            .join(path)
    }
}

fn robot_base_transform(robot: &LoadedRobotModel) -> ([f64; 3], [[f64; 3]; 3]) {
    (
        f32_vec3_to_f64(robot.config.base_translation),
        rpy_rotation(f32_vec3_to_f64(robot.config.base_rotation)),
    )
}

fn model_transform(robot: &LoadedRobotModel) -> ([f64; 3], [[f64; 3]; 3]) {
    let transform = model_transformation(robot);
    (
        f32_vec3_to_f64(transform.translation),
        rpy_rotation(f32_vec3_to_f64(transform.rotation)),
    )
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

fn transpose_matrix(matrix: [[f64; 3]; 3]) -> [[f64; 3]; 3] {
    [
        [matrix[0][0], matrix[1][0], matrix[2][0]],
        [matrix[0][1], matrix[1][1], matrix[2][1]],
        [matrix[0][2], matrix[1][2], matrix[2][2]],
    ]
}

fn vec_add(left: [f64; 3], right: [f64; 3]) -> [f64; 3] {
    [left[0] + right[0], left[1] + right[1], left[2] + right[2]]
}

fn negate_vec3(value: [f64; 3]) -> [f64; 3] {
    [-value[0], -value[1], -value[2]]
}

fn matrix_to_rpy(matrix: [[f64; 3]; 3]) -> [f64; 3] {
    let pitch = (-matrix[2][0]).asin();
    let cos_pitch = pitch.cos();
    if cos_pitch.abs() > 1.0e-6 {
        [
            matrix[2][1].atan2(matrix[2][2]),
            pitch,
            matrix[1][0].atan2(matrix[0][0]),
        ]
    } else {
        [0.0, pitch, (-matrix[0][1]).atan2(matrix[1][1])]
    }
}

fn scene_location_from_transform(transform: RigidTransform) -> SceneLocation {
    SceneLocation {
        position: transform.translation_m,
        rotation: Some(matrix_to_rpy(transform.rotation)),
    }
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
            ModelTransformationConfig::default(),
        )),
        properties,
    }
}

fn frame_entity(robot: &LoadedRobotModel, frame: &ResolvedFrameState) -> RobotDreamsEntity {
    let mut properties = BTreeMap::new();
    if let Some(relative_to) = &frame.relative_to {
        properties.insert("relativeTo".to_string(), relative_to.clone());
    }
    RobotDreamsEntity {
        kind: ModelEntityKind::Frame,
        id: format!("{}:frame:{}", robot.config.id, frame.id),
        name: frame.name.clone(),
        robot_id: Some(robot.config.id.clone()),
        urdf_name: None,
        location: Some(scene_location_from_transform(frame.world_transform)),
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
                    model_transformation(robot),
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
                model_transformation(robot),
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
                    model_transformation(robot),
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
        ModelTransformationConfig::default(),
    )
}

fn robot_link_location(robot: &LoadedRobotModel, link_name: &str) -> Option<SceneLocation> {
    robot.harness.link_origin_world(link_name).map(|position| {
        scene_location(
            position,
            None,
            robot.config.base_translation,
            robot.config.base_rotation,
            model_transformation(robot),
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
                model_transformation(robot),
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

fn robot_visual_link_transform(
    robot_transform: ([f64; 3], [[f64; 3]; 3]),
    link_poses: &HashMap<String, LinkPose>,
    link_name: &str,
) -> Option<([f64; 3], [[f64; 3]; 3])> {
    let link_pose = link_poses.get(link_name)?;
    let link_transform = (link_pose.translation, link_pose.rotation);
    Some(transform_then(robot_transform, link_transform))
}

fn robot_visual_meshes(robot: &LoadedRobotModel) -> Vec<RobotVisualMesh> {
    let base_transform = robot_base_transform(robot);
    let model_transform = model_transform(robot);
    let robot_transform = transform_then(base_transform, model_transform);
    let link_poses = robot.harness.link_poses_world();
    let mut meshes = Vec::new();

    for link in &robot.harness.robot().links {
        let Some(link_transform) =
            robot_visual_link_transform(robot_transform, &link_poses, &link.name)
        else {
            continue;
        };

        for (index, visual) in link.visual.iter().enumerate() {
            let urdf_rs::Geometry::Mesh { filename, scale } = &visual.geometry else {
                continue;
            };
            let visual_transform = (*visual.origin.xyz, rpy_rotation(*visual.origin.rpy));
            let transform = transform_then(link_transform, visual_transform);
            let scale = scale
                .map(|scale| f64_vec3_to_f32(*scale))
                .unwrap_or([1.0, 1.0, 1.0]);
            meshes.push(RobotVisualMesh {
                robot_id: robot.config.id.clone(),
                link_name: link.name.clone(),
                name: visual
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("{} visual {index}", link.name)),
                asset: resolve_mesh_path(&robot.urdf_path, filename),
                scale,
                translation: f64_vec3_to_f32(transform.0),
                rotation_matrix: f64_matrix_to_f32(transform.1),
            });
        }
    }

    meshes
}

fn robot_visual_transforms(robot: &LoadedRobotModel) -> Vec<RobotVisualTransform> {
    let base_transform = robot_base_transform(robot);
    let model_transform = model_transform(robot);
    let robot_transform = transform_then(base_transform, model_transform);
    let link_poses = robot.harness.link_poses_world();
    let mut transforms = Vec::new();

    for link in &robot.harness.robot().links {
        let Some(link_transform) =
            robot_visual_link_transform(robot_transform, &link_poses, &link.name)
        else {
            continue;
        };

        for visual in &link.visual {
            if !matches!(&visual.geometry, urdf_rs::Geometry::Mesh { .. }) {
                continue;
            }
            let visual_transform = (*visual.origin.xyz, rpy_rotation(*visual.origin.rpy));
            let transform = transform_then(link_transform, visual_transform);
            transforms.push(RobotVisualTransform {
                translation: f64_vec3_to_f32(transform.0),
                rotation_matrix: f64_matrix_to_f32(transform.1),
            });
        }
    }

    transforms
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
        frames: resolve_robot_frames(robot).expect("loaded robot frames validated"),
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
        DeviceConfig::DcMotor(motor) => {
            let mut properties = BTreeMap::new();
            properties.insert("bus".to_string(), bus.id.clone());
            properties.insert("profile".to_string(), motor.profile.clone());
            properties.insert(
                "direction".to_string(),
                motor.calibration.direction.to_string(),
            );
            properties.insert(
                "maxSpeedMps".to_string(),
                format!("{:.3}", motor.calibration.max_speed_mps),
            );
            if let Some(drives) = &motor.drives {
                properties.insert("drivesRobot".to_string(), drives.robot.clone());
                properties.insert("drivesWheel".to_string(), drives.target.clone());
            }
            RobotDreamsEntity {
                kind: ModelEntityKind::HardwareDevice,
                id: format!("{}:dc_motor:{}", bus.id, motor.id),
                name: motor.name.clone(),
                robot_id: motor.drives.as_ref().map(|drives| drives.robot.clone()),
                urdf_name: motor.drives.as_ref().map(|drives| drives.target.clone()),
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

            if let Ok(frames) = resolve_robot_frames(robot) {
                for frame in frames.values() {
                    if matches_query(
                        name,
                        &string_candidates(&[
                            &frame.id,
                            &frame.name,
                            &format!("{} frame", frame.id),
                            &format!("{} frame", frame.name),
                        ]),
                    ) {
                        return Some(frame_entity(robot, frame));
                    }
                }
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

    pub fn frame_state(
        &self,
        robot_id_or_name: &str,
        frame_name: &str,
    ) -> Option<ResolvedFrameState> {
        self.robot_state(robot_id_or_name)
            .and_then(|robot| robot.frames.get(frame_name).cloned())
    }

    pub fn robot_base_yaw(&self, robot_id_or_name: &str) -> Option<f64> {
        self.robots
            .iter()
            .find(|robot| {
                matches_query(
                    robot_id_or_name,
                    &string_candidates(&[&robot.config.id, &robot.config.name]),
                )
            })
            .map(|robot| f64::from(robot.config.base_rotation[2]))
    }

    pub fn robot_states(&self) -> Vec<RobotState> {
        self.robots.iter().map(robot_state).collect()
    }

    pub fn robot_visual_meshes(&self) -> Vec<RobotVisualMesh> {
        self.robots.iter().flat_map(robot_visual_meshes).collect()
    }

    pub fn robot_visual_transforms(&self) -> Vec<RobotVisualTransform> {
        self.robots
            .iter()
            .flat_map(robot_visual_transforms)
            .collect()
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

    pub fn move_robot_base_flat(
        &mut self,
        robot_id_or_name: &str,
        dx: f64,
        dy: f64,
        dyaw: f64,
    ) -> bool {
        let Some(robot) = self.robots.iter_mut().find(|robot| {
            matches_query(
                robot_id_or_name,
                &string_candidates(&[&robot.config.id, &robot.config.name]),
            )
        }) else {
            return false;
        };

        robot.config.base_translation[0] += dx as f32;
        robot.config.base_translation[1] += dy as f32;
        robot.config.base_rotation[2] += dyaw as f32;
        true
    }
}

pub fn project_config_from_manifest(path: &Path) -> Option<ProjectConfig> {
    let json = read_json_file(path).ok()?;
    parse_project_config(&json, path.to_path_buf())
}

pub fn model_profile_from_manifest(path: &Path) -> Option<ModelProfile> {
    let json = read_json_file(path).ok()?;
    parse_model_profile(&json, path.to_path_buf())
        .ok()
        .flatten()
}

pub fn load_model_profile(path: impl AsRef<Path>) -> Result<ModelProfile, Box<dyn Error>> {
    let manifest = resolve_model_profile_manifest(path.as_ref())?;
    let json = read_json_file(&manifest)?;
    parse_model_profile(&json, manifest.clone())?
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

fn parse_model_frames(value: &serde_json::Value) -> Result<Vec<ModelFrameConfig>, String> {
    let Some(frames) = value.get("frames") else {
        return Ok(Vec::new());
    };
    let frames = frames.as_object().ok_or_else(|| {
        "model profile 'frames' must be an object keyed by semantic frame id".to_string()
    })?;
    let mut parsed = Vec::new();
    for (id, frame) in frames {
        let entry = format!("model profile frame '{id}'");
        if id.trim().is_empty() {
            return Err("model profile frame ids must not be empty".to_string());
        }
        let name = json_string_path(frame, &["name"])
            .ok_or_else(|| format!("{entry} must provide a string 'name'"))?;
        let relative_to = json_string_path(frame, &["relativeTo"])
            .ok_or_else(|| format!("{entry} must provide a string 'relativeTo'"))?;
        let translation = json_vec3_path(frame, &["translation"]).ok_or_else(|| {
            format!("{entry} must provide a three-number 'translation' in meters")
        })?;
        let rotation = json_vec3_path(frame, &["rotation"]).ok_or_else(|| {
            format!("{entry} must provide a three-number RPY 'rotation' in radians")
        })?;
        if !translation.iter().all(|value| value.is_finite())
            || !rotation.iter().all(|value| value.is_finite())
        {
            return Err(format!("{entry} has non-finite translation or rotation"));
        }
        parsed.push(ModelFrameConfig {
            id: id.clone(),
            name: name.to_string(),
            relative_to: relative_to.to_string(),
            translation_m: f32_vec3_to_f64(translation),
            rotation_rpy_rad: f32_vec3_to_f64(rotation),
        });
    }
    Ok(parsed)
}

fn parse_model_profile(
    value: &serde_json::Value,
    manifest_path: PathBuf,
) -> Result<Option<ModelProfile>, Box<dyn Error>> {
    let Some(format) = json_string_path(value, &["format"]) else {
        return Ok(None);
    };
    if format != ROBOT_DREAMS_MODEL_FORMAT {
        return Ok(None);
    }

    let base_dir = path_base(&manifest_path);
    let name = json_string_path(value, &["name"])
        .unwrap_or("RobotDreams Model")
        .to_string();
    let Some(robot) = parse_model_profile_robot_config(value, &name) else {
        return Ok(None);
    };
    let joint_names = parse_project_joint_names(value);

    Ok(Some(ModelProfile {
        format: format.to_string(),
        name,
        manifest_path,
        base_dir,
        robot,
        joint_names,
        tcp: parse_tcp_config(value),
        frames: parse_model_frames(value).map_err(|err| -> Box<dyn Error> { err.into() })?,
        frame_mapping: parse_frame_mapping_config(value),
    }))
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

fn json_vec2_f32_path(value: &serde_json::Value, path: &[&str]) -> Option<[f32; 2]> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    let values = current.as_array()?;
    if values.len() != 2 {
        return None;
    }
    Some([values[0].as_f64()? as f32, values[1].as_f64()? as f32])
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

fn parse_camera_intrinsics(value: &serde_json::Value) -> Option<CameraIntrinsics> {
    let fx = json_f32_path(value, &["intrinsics", "fx"])
        .or_else(|| json_f32_path(value, &["camera", "intrinsics", "fx"]))?;
    let fy = json_f32_path(value, &["intrinsics", "fy"])
        .or_else(|| json_f32_path(value, &["camera", "intrinsics", "fy"]))?;
    let cx = json_f32_path(value, &["intrinsics", "cx"])
        .or_else(|| json_f32_path(value, &["camera", "intrinsics", "cx"]))?;
    let cy = json_f32_path(value, &["intrinsics", "cy"])
        .or_else(|| json_f32_path(value, &["camera", "intrinsics", "cy"]))?;
    Some(CameraIntrinsics {
        fx,
        fy,
        cx,
        cy,
        skew: json_f32_path(value, &["intrinsics", "skew"])
            .or_else(|| json_f32_path(value, &["camera", "intrinsics", "skew"]))
            .unwrap_or(0.0),
    })
}

fn parse_camera_projection(value: &serde_json::Value) -> CameraProjection {
    let projection = value.get("projection").or_else(|| {
        value
            .get("camera")
            .and_then(|camera| camera.get("projection"))
    });
    let projection_type = projection
        .and_then(|projection| projection.as_str())
        .or_else(|| {
            projection.and_then(|projection| {
                projection
                    .get("type")
                    .and_then(|projection_type| projection_type.as_str())
            })
        })
        .unwrap_or("perspective");
    if !projection_type.eq_ignore_ascii_case("orthographic") {
        return CameraProjection::Perspective;
    }

    let projection_object = projection.and_then(|projection| projection.as_object());
    let size_m = projection_object
        .and_then(|_| json_f32_path(value, &["projection", "sizeM"]))
        .or_else(|| projection_object.and_then(|_| json_f32_path(value, &["projection", "size"])))
        .or_else(|| json_f32_path(value, &["orthographicSizeM"]))
        .or_else(|| json_f32_path(value, &["orthographicSize"]))
        .or_else(|| json_f32_path(value, &["camera", "orthographicSizeM"]))
        .or_else(|| json_f32_path(value, &["camera", "orthographicSize"]))
        .unwrap_or(1.0)
        .max(1.0e-6);
    CameraProjection::Orthographic { size_m }
}

fn parse_camera_distortion(value: &serde_json::Value) -> Option<CameraDistortion> {
    let distortion = value.get("distortion").or_else(|| {
        value
            .get("camera")
            .and_then(|camera| camera.get("distortion"))
    })?;
    Some(CameraDistortion {
        k1: json_f32_path(distortion, &["k1"]).unwrap_or(0.0),
        k2: json_f32_path(distortion, &["k2"]).unwrap_or(0.0),
        p1: json_f32_path(distortion, &["p1"]).unwrap_or(0.0),
        p2: json_f32_path(distortion, &["p2"]).unwrap_or(0.0),
        k3: json_f32_path(distortion, &["k3"]).unwrap_or(0.0),
    })
}

fn parse_camera_sensor_effects(value: &serde_json::Value) -> Option<CameraSensorEffects> {
    let sensor = value
        .get("sensor")
        .or_else(|| value.get("camera").and_then(|camera| camera.get("sensor")))
        .or_else(|| value.get("effects"))
        .or_else(|| value.get("camera").and_then(|camera| camera.get("effects")))?;
    Some(CameraSensorEffects {
        exposure: json_f32_path(sensor, &["exposure"]).unwrap_or(1.0),
        gamma: json_f32_path(sensor, &["gamma"]).unwrap_or(1.0),
        rgb_noise_stddev: json_f32_path(sensor, &["rgbNoiseStddev"])
            .or_else(|| json_f32_path(sensor, &["rgbNoiseStddevRgb"]))
            .unwrap_or(0.0),
        depth_noise_stddev_m: json_f32_path(sensor, &["depthNoiseStddevM"]).unwrap_or(0.0),
        depth_quantization_m: json_f32_path(sensor, &["depthQuantizationM"]).unwrap_or(0.0),
        noise_seed: json_u32_path(sensor, &["noiseSeed"]).unwrap_or(0),
    })
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

fn parse_steering_mapping(value: &serde_json::Value) -> Option<SteeringMapping> {
    let mapping = value.get("steers").or_else(|| value.get("steering"))?;
    let robot = json_string_path(mapping, &["robot"])
        .unwrap_or("puppybot")
        .to_string();
    let joints = mapping
        .get("joints")
        .and_then(|joints| joints.as_array())
        .map(|joints| {
            joints
                .iter()
                .filter_map(|joint| joint.as_str().map(str::to_string))
                .collect::<Vec<_>>()
        })
        .or_else(|| json_string_path(mapping, &["joint"]).map(|joint| vec![joint.to_string()]))?;
    if joints.is_empty() {
        return None;
    }
    Some(SteeringMapping { robot, joints })
}

fn parse_servo_calibration(value: &serde_json::Value) -> ServoCalibrationConfig {
    ServoCalibrationConfig {
        zero_offset: json_i16_path(value, &["calibration", "zeroOffset"]).unwrap_or(2048),
        direction: json_i8_path(value, &["calibration", "direction"]).unwrap_or(1),
    }
}

fn parse_dc_motor_calibration(value: &serde_json::Value) -> DcMotorCalibrationConfig {
    DcMotorCalibrationConfig {
        direction: json_i8_path(value, &["calibration", "direction"]).unwrap_or(1),
        max_speed_mps: json_f32_path(value, &["calibration", "maxSpeedMps"]).unwrap_or(0.4),
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
            steers: parse_steering_mapping(value),
            calibration: parse_servo_calibration(value),
        })),
        "dcMotor" | "dc_motor" | "hbridgeMotor" | "hbridge_motor" => {
            Some(DeviceConfig::DcMotor(DcMotorDeviceConfig {
                id,
                name,
                profile,
                drives: parse_device_mapping(value, "drives", "wheel"),
                calibration: parse_dc_motor_calibration(value),
            }))
        }
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
        projection: parse_camera_projection(value),
        rate: json_string_path(value, &["rate"])
            .unwrap_or("30 Hz")
            .to_string(),
        resolution: json_vec2_u32_path(value, &["resolution"])
            .or_else(|| json_vec2_u32_path(value, &["camera", "resolution"])),
        intrinsics: parse_camera_intrinsics(value),
        distortion: parse_camera_distortion(value),
        depth_range_m: json_vec2_f32_path(value, &["depthRangeM"])
            .or_else(|| json_vec2_f32_path(value, &["camera", "depthRangeM"]))
            .or_else(|| json_vec2_f32_path(value, &["depth", "rangeM"])),
        sensor_effects: parse_camera_sensor_effects(value),
    })
}

fn parse_project_reflection_probe_settings(
    value: &serde_json::Value,
) -> Option<ReflectionProbeSettings> {
    Some(ReflectionProbeSettings {
        map: json_string_path(value, &["map"])
            .or_else(|| json_string_path(value, &["path"]))
            .or_else(|| json_string_path(value, &["asset"]))?
            .to_string(),
        rotation_deg: json_f32_path(value, &["rotationDeg"]).unwrap_or(0.0),
        intensity: json_f32_path(value, &["intensity"]).unwrap_or(1.0),
        ambient_intensity: json_f32_path(value, &["ambientIntensity"]).unwrap_or(0.35),
        position: json_vec3_path(value, &["position"])
            .or_else(|| json_vec3_path(value, &["transform", "position"])),
        box_size_m: json_vec3_path(value, &["boxSizeM"])
            .or_else(|| json_vec3_path(value, &["box", "sizeM"]))
            .or_else(|| json_vec3_path(value, &["box", "size"])),
        influence_radius_m: json_f32_path(value, &["influenceRadiusM"])
            .or_else(|| json_f32_path(value, &["radiusM"])),
        falloff_power: json_f32_path(value, &["falloffPower"]),
    })
}

fn parse_project_environment_settings(value: &serde_json::Value) -> EnvironmentSettings {
    EnvironmentSettings {
        sky_top_rgb: json_color_path(value, &["skyTopRgb"]).unwrap_or([96, 160, 255]),
        sky_horizon_rgb: json_color_path(value, &["skyHorizonRgb"]).unwrap_or([180, 205, 235]),
        ground_rgb: json_color_path(value, &["groundRgb"]).unwrap_or([70, 72, 68]),
        map: json_string_path(value, &["map"]).map(str::to_string),
        map_rotation_deg: json_f32_path(value, &["mapRotationDeg"]).unwrap_or(0.0),
        intensity: json_f32_path(value, &["intensity"]).unwrap_or(1.0),
        ambient_intensity: json_f32_path(value, &["ambientIntensity"]).unwrap_or(0.35),
    }
}

fn parse_project_tone_mapping(value: &serde_json::Value) -> Option<ToneMapping> {
    match value.as_str()? {
        "Linear" | "linear" => Some(ToneMapping::Linear),
        "Reinhard" | "reinhard" => Some(ToneMapping::Reinhard),
        "Aces" | "ACES" | "aces" => Some(ToneMapping::Aces),
        _ => None,
    }
}

fn parse_project_render_settings(value: &serde_json::Value) -> RenderSettings {
    let mut settings = RenderSettings::default();
    if let Some(background_rgb) = json_color_path(value, &["backgroundRgb"]) {
        settings.background_rgb = background_rgb;
    }
    if let Some(ambient_rgb) = json_color_path(value, &["ambientRgb"]) {
        settings.ambient_rgb = ambient_rgb;
    }
    if let Some(ambient_intensity) = json_f32_path(value, &["ambientIntensity"]) {
        settings.ambient_intensity = ambient_intensity;
    }
    if let Some(samples) = json_u32_path(value, &["debugRgbSamplesPerPixel"]) {
        settings.debug_rgb_samples_per_pixel = samples.max(1);
    }
    if let Some(samples) = json_u32_path(value, &["ambientOcclusionSamples"]) {
        settings.ambient_occlusion_samples = samples;
    }
    if let Some(radius) = json_f32_path(value, &["ambientOcclusionRadiusM"]) {
        settings.ambient_occlusion_radius_m = radius.max(0.0);
    }
    if let Some(intensity) = json_f32_path(value, &["ambientOcclusionIntensity"]) {
        settings.ambient_occlusion_intensity = intensity.max(0.0);
    }
    if let Some(samples) = json_u32_path(value, &["indirectDiffuseSamples"]) {
        settings.indirect_diffuse_samples = samples;
    }
    if let Some(radius) = json_f32_path(value, &["indirectDiffuseRadiusM"]) {
        settings.indirect_diffuse_radius_m = radius.max(0.0);
    }
    if let Some(intensity) = json_f32_path(value, &["indirectDiffuseIntensity"]) {
        settings.indirect_diffuse_intensity = intensity.max(0.0);
    }
    if let Some(bounces) = json_u32_path(value, &["indirectDiffuseBounces"]) {
        settings.indirect_diffuse_bounces = bounces;
    }
    if let Some(samples) = json_u32_path(value, &["softShadowSamples"]) {
        settings.soft_shadow_samples = samples.max(1);
    }
    if let Some(radius) = json_f32_path(value, &["softShadowRadiusM"]) {
        settings.soft_shadow_radius_m = radius.max(0.0);
    }
    if let Some(samples) = json_u32_path(value, &["areaLightSamples"]) {
        settings.area_light_samples = samples.max(1);
    }
    if let Some(samples) = json_u32_path(value, &["roughTransmissionSamples"]) {
        settings.rough_transmission_samples = samples.max(1);
    }
    if let Some(samples) = json_u32_path(value, &["roughReflectionSamples"]) {
        settings.rough_reflection_samples = samples.max(1);
    }
    if let Some(bounces) = json_u32_path(value, &["specularReflectionBounces"]) {
        settings.specular_reflection_bounces = bounces;
    }
    if let Some(variant) = json_string_path(value, &["gltfMaterialVariant"]) {
        settings.gltf_material_variant = Some(variant.to_string());
    }
    if let Some(environment) = value.get("environment") {
        settings.environment = Some(parse_project_environment_settings(environment));
    }
    settings.reflection_probe = value
        .get("reflectionProbe")
        .and_then(parse_project_reflection_probe_settings);
    settings.reflection_probes = value
        .get("reflectionProbes")
        .and_then(|reflection_probes| reflection_probes.as_array())
        .map(|reflection_probes| {
            reflection_probes
                .iter()
                .filter_map(parse_project_reflection_probe_settings)
                .collect()
        })
        .unwrap_or_default();
    if let Some(white_balance_rgb) = json_vec3_path(value, &["whiteBalanceRgb"]) {
        settings.white_balance_rgb = white_balance_rgb;
    }
    settings.color_temperature_kelvin = json_f32_path(value, &["colorTemperatureKelvin"]);
    if let Some(tone_mapping) = value
        .get("toneMapping")
        .and_then(parse_project_tone_mapping)
    {
        settings.tone_mapping = tone_mapping;
    }
    if let Some(tone_exposure) = json_f32_path(value, &["toneExposure"]) {
        settings.tone_exposure = tone_exposure;
    }
    settings
}

fn parse_project_light_config(value: &serde_json::Value) -> Option<ProjectLightConfig> {
    let id = json_string_path(value, &["id"])?.to_string();
    let name = json_string_path(value, &["name"])
        .map(str::to_string)
        .unwrap_or_else(|| id.clone());
    let kind_name = json_string_path(value, &["kind"])
        .or_else(|| json_string_path(value, &["light", "kind"]))
        .or_else(|| json_string_path(value, &["type"]))
        .unwrap_or("directional");
    let direction = json_vec3_path(value, &["direction"])
        .or_else(|| json_vec3_path(value, &["light", "direction"]))
        .unwrap_or([0.0, 0.0, -1.0]);
    let range_m = json_f32_path(value, &["rangeM"]).or_else(|| json_f32_path(value, &["range"]));
    let angular_radius_deg = json_f32_path(value, &["angularRadiusDeg"])
        .or_else(|| json_f32_path(value, &["light", "angularRadiusDeg"]))
        .or_else(|| json_f32_path(value, &["sunAngularRadiusDeg"]))
        .unwrap_or(0.0);
    let kind = match kind_name {
        "point" | "Point" => LightKind::Point { range_m },
        "spot" | "Spot" => LightKind::Spot {
            direction,
            inner_cone_deg: json_f32_path(value, &["innerConeDeg"])
                .or_else(|| json_f32_path(value, &["light", "innerConeDeg"]))
                .unwrap_or(15.0),
            outer_cone_deg: json_f32_path(value, &["outerConeDeg"])
                .or_else(|| json_f32_path(value, &["light", "outerConeDeg"]))
                .unwrap_or(35.0),
            range_m,
        },
        "directional" | "Directional" | "sun" | "Sun" | "light" => LightKind::Directional {
            direction,
            angular_radius_deg,
        },
        _ => LightKind::Directional {
            direction,
            angular_radius_deg,
        },
    };

    Some(ProjectLightConfig {
        id,
        name,
        kind,
        color_rgb: json_color_path(value, &["color"])
            .or_else(|| json_color_path(value, &["light", "color"]))
            .unwrap_or([255, 255, 255]),
        intensity: json_f32_path(value, &["intensity"])
            .or_else(|| json_f32_path(value, &["light", "intensity"]))
            .unwrap_or(1.0),
        position: json_vec3_path(value, &["position"])
            .or_else(|| json_vec3_path(value, &["transform", "position"]))
            .unwrap_or([0.0, 0.0, 0.0]),
        rotation: json_vec3_path(value, &["rotation"])
            .or_else(|| json_vec3_path(value, &["transform", "rotation"]))
            .unwrap_or([0.0, 0.0, 0.0]),
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
    let lights = value
        .get("scene")
        .and_then(|scene| scene.get("lights"))
        .and_then(|lights| lights.as_array())
        .map(|lights| {
            lights
                .iter()
                .filter_map(parse_project_light_config)
                .collect()
        })
        .unwrap_or_default();
    let render_settings = value
        .get("scene")
        .and_then(|scene| scene.get("renderSettings"))
        .map(parse_project_render_settings);
    let reflection_probes = value
        .get("scene")
        .and_then(|scene| scene.get("reflectionProbes"))
        .and_then(|reflection_probes| reflection_probes.as_array())
        .map(|reflection_probes| {
            reflection_probes
                .iter()
                .filter_map(parse_project_reflection_probe_settings)
                .collect()
        })
        .unwrap_or_default();
    ProjectSceneConfig {
        objects,
        cameras,
        lights,
        render_settings,
        reflection_probes,
    }
}

fn parse_project_robot_model_config(value: &serde_json::Value) -> Option<ProjectRobotModelConfig> {
    let model = value.get("model")?;
    Some(ProjectRobotModelConfig {
        type_name: json_string_path(model, &["type"])
            .unwrap_or("urdf")
            .to_string(),
        path: json_string_path(model, &["path"])?.to_string(),
        model_transformation: parse_model_transformation_config(model),
    })
}

fn parse_model_transformation_config(
    value: &serde_json::Value,
) -> Option<ModelTransformationConfig> {
    let transform = value.get("modelTransformation")?;
    Some(ModelTransformationConfig {
        translation: json_vec3_path(transform, &["translation"])
            .or_else(|| json_vec3_path(transform, &["position"]))
            .unwrap_or([0.0, 0.0, 0.0]),
        rotation: json_vec3_path(transform, &["rotation"]).unwrap_or([0.0, 0.0, 0.0]),
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

    fn robot_config_with_model_transformation(
        model_transformation: Option<ModelTransformationConfig>,
    ) -> ProjectRobotConfig {
        ProjectRobotConfig {
            id: "robot".to_string(),
            name: "Robot".to_string(),
            model: ProjectRobotModelConfig {
                type_name: "urdf".to_string(),
                path: "robot.urdf".to_string(),
                model_transformation,
            },
            joint_names: HashMap::new(),
            base_translation: [0.0, 0.0, 0.0],
            base_rotation: [0.0, 0.0, 0.0],
        }
    }

    fn model_profile_with_robot(robot: ProjectRobotConfig) -> ModelProfile {
        ModelProfile {
            format: ROBOT_DREAMS_MODEL_FORMAT.to_string(),
            name: "Robot".to_string(),
            manifest_path: PathBuf::from("robotdreams.json"),
            base_dir: PathBuf::from("."),
            robot,
            joint_names: HashMap::new(),
            tcp: None,
            frames: Vec::new(),
            frame_mapping: None,
        }
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
    fn model_profile_frames_use_keyed_semantic_ids_and_resolve_against_base() {
        let model = RobotDreamsModel::open(puppyarm_project_path()).expect("load PuppyArm project");
        let robot = model.robot_state("puppyarm").expect("PuppyArm state");
        let base = robot.frames.get("base").expect("built-in base frame");
        let arm_base = robot
            .frames
            .get("armBase")
            .expect("configured arm base frame");

        assert_eq!(base.relative_to, None);
        assert_eq!(arm_base.id, "armBase");
        assert_eq!(arm_base.name, "Arm Base");
        assert_eq!(arm_base.relative_to.as_deref(), Some("base"));
        assert_eq!(arm_base.relative_transform, RigidTransform::identity());
        assert_eq!(arm_base.world_transform, base.world_transform);

        let entity = model.named("armBase").expect("semantic frame entity");
        assert_eq!(entity.kind, ModelEntityKind::Frame);
        assert_eq!(entity.name, "Arm Base");
        assert_eq!(
            model.frame_state("PuppyArm", "armBase"),
            Some(arm_base.clone())
        );
    }

    #[test]
    fn rigid_transforms_compose_invert_and_transform_points() {
        let parent = RigidTransform::from_translation_rpy([1.0, 2.0, 3.0], [0.0, 0.0, 0.5]);
        let child = RigidTransform::from_translation_rpy([0.5, 0.0, 0.0], [0.0, 0.0, -0.5]);
        let world = parent.compose(child);
        let point = [0.2, -0.4, 0.7];

        let transformed = world.transform_point(point);
        let recovered = world.inverse().transform_point(transformed);
        for (actual, expected) in recovered.into_iter().zip(point) {
            assert_close(actual, expected);
        }
        assert_close(world.rotation[0][0], 1.0);
        assert_close(world.rotation[1][1], 1.0);
        assert_close(world.rotation[2][2], 1.0);
    }

    #[test]
    fn model_profile_rejects_invalid_frame_definitions() {
        let manifest = puppyarm_model_profile_path();
        let mut malformed = read_json_file(&manifest).expect("read PuppyArm model profile");
        malformed["frames"] = serde_json::json!({"armBase": {"name": "Arm Base"}});
        let error =
            parse_model_profile(&malformed, manifest.clone()).expect_err("reject malformed frame");
        assert!(error.to_string().contains("relativeTo"));
        let malformed_path = std::env::temp_dir().join(format!(
            "robotdreams-malformed-frames-{}.json",
            std::process::id()
        ));
        std::fs::write(
            &malformed_path,
            serde_json::to_vec(&malformed).expect("serialize malformed profile"),
        )
        .expect("write malformed profile");
        let error =
            load_model_profile(&malformed_path).expect_err("model load rejects malformed frame");
        assert!(error.to_string().contains("relativeTo"));
        let _ = std::fs::remove_file(malformed_path);

        let mut unknown_parent = read_json_file(&manifest).expect("read PuppyArm model profile");
        unknown_parent["frames"] = serde_json::json!({
            "armBase": {
                "name": "Arm Base",
                "relativeTo": "missing",
                "translation": [0.0, 0.0, 0.0],
                "rotation": [0.0, 0.0, 0.0]
            }
        });
        let profile = parse_model_profile(&unknown_parent, manifest.clone())
            .expect("parse profile")
            .expect("model profile");
        let error = loaded_robot_from_profile(profile).expect_err("reject unknown parent");
        assert!(error.to_string().contains("unknown parent 'missing'"));

        let mut cycle = read_json_file(&manifest).expect("read PuppyArm model profile");
        cycle["frames"] = serde_json::json!({
            "armBase": {
                "name": "Arm Base",
                "relativeTo": "tool",
                "translation": [0.0, 0.0, 0.0],
                "rotation": [0.0, 0.0, 0.0]
            },
            "tool": {
                "name": "Tool",
                "relativeTo": "armBase",
                "translation": [0.0, 0.0, 0.0],
                "rotation": [0.0, 0.0, 0.0]
            }
        });
        let profile = parse_model_profile(&cycle, manifest)
            .expect("parse profile")
            .expect("model profile");
        let error = loaded_robot_from_profile(profile).expect_err("reject frame cycle");
        assert!(error.to_string().contains("cycle"));

        let mut base_redefinition =
            read_json_file(&puppyarm_model_profile_path()).expect("read PuppyArm model profile");
        base_redefinition["frames"] = serde_json::json!({
            "base": {
                "name": "Base",
                "relativeTo": "base",
                "translation": [0.0, 0.0, 0.0],
                "rotation": [0.0, 0.0, 0.0]
            }
        });
        let profile = parse_model_profile(&base_redefinition, puppyarm_model_profile_path())
            .expect("parse profile")
            .expect("model profile");
        let error = loaded_robot_from_profile(profile).expect_err("reject base redefinition");
        assert!(error.to_string().contains("reserved semantic base"));

        let mut nonfinite = parse_model_profile(
            &read_json_file(&puppyarm_model_profile_path()).expect("read PuppyArm model profile"),
            puppyarm_model_profile_path(),
        )
        .expect("parse profile")
        .expect("model profile");
        nonfinite.frames[0].translation_m[0] = f64::NAN;
        let error = loaded_robot_from_profile(nonfinite).expect_err("reject non-finite frame");
        assert!(error.to_string().contains("non-finite"));
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
    fn project_camera_parses_calibration_fields() {
        let project_path = write_temp_project(
            "camera-calibration",
            serde_json::json!({
                "format": ROBOT_DREAMS_PROJECT_FORMAT,
                "name": "Camera Calibration",
                "modelProfile": puppyarm_model_profile_path(),
                "scene": {
                    "cameras": [{
                        "id": "tcp_camera",
                        "name": "TCP Camera",
                        "mountedOn": {
                            "robot": "puppyarm",
                            "link": "part_1_4"
                        },
                        "projection": {
                            "type": "orthographic",
                            "sizeM": 0.75
                        },
                        "resolution": [640, 480],
                        "intrinsics": {
                            "fx": 410.0,
                            "fy": 420.0,
                            "cx": 319.5,
                            "cy": 239.5,
                            "skew": 0.25
                        },
                        "distortion": {
                            "k1": 0.1,
                            "k2": 0.2,
                            "p1": 0.3,
                            "p2": 0.4,
                            "k3": 0.5
                        },
                        "depthRangeM": [0.15, 3.5],
                        "sensor": {
                            "exposure": 1.5,
                            "gamma": 2.2,
                            "rgbNoiseStddev": 4.0,
                            "depthNoiseStddevM": 0.02,
                            "depthQuantizationM": 0.005,
                            "noiseSeed": 123
                        }
                    }]
                },
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
        let camera = &model.project().unwrap().scene.cameras[0];

        assert_eq!(camera.intrinsics.unwrap().fx, 410.0);
        assert_eq!(
            camera.projection,
            CameraProjection::Orthographic { size_m: 0.75 }
        );
        assert_eq!(camera.intrinsics.unwrap().skew, 0.25);
        assert_eq!(camera.distortion.unwrap().k3, 0.5);
        assert_eq!(camera.depth_range_m, Some([0.15, 3.5]));
        assert_eq!(camera.sensor_effects.unwrap().exposure, 1.5);
        assert_eq!(camera.sensor_effects.unwrap().depth_quantization_m, 0.005);
        assert_eq!(camera.sensor_effects.unwrap().noise_seed, 123);

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
    fn robot_model_transformation_parses_when_present() {
        let robot = parse_project_robot_config(&serde_json::json!({
            "id": "robot",
            "name": "Robot",
            "model": {
                "type": "urdf",
                "path": "robot.urdf",
                "modelTransformation": {
                    "translation": [1.0, 2.0, 3.0],
                    "rotation": [0.1, 0.2, 0.3]
                }
            }
        }))
        .expect("parse robot");

        let transform = robot.model.model_transformation.expect("model transform");
        assert_eq!(transform.translation, [1.0, 2.0, 3.0]);
        assert_eq!(transform.rotation, [0.1, 0.2, 0.3]);
    }

    #[test]
    fn project_scene_reflection_probes_parse_from_scene_config() {
        let project = parse_project_config(
            &serde_json::json!({
                "format": ROBOT_DREAMS_PROJECT_FORMAT,
                "name": "Probe Project",
                "scene": {
                    "reflectionProbes": [
                        {
                            "map": "assets/studio.hdr",
                            "rotationDeg": 90.0,
                            "intensity": 1.5,
                            "ambientIntensity": 0.25,
                            "position": [1.0, 2.0, 3.0],
                            "boxSizeM": [4.0, 5.0, 6.0],
                            "influenceRadiusM": 7.0,
                            "falloffPower": 2.0
                        }
                    ]
                }
            }),
            PathBuf::from("/tmp/robotdreams/probe-project.json"),
        )
        .expect("parse project");

        assert_eq!(project.scene.reflection_probes.len(), 1);
        let probe = &project.scene.reflection_probes[0];
        assert_eq!(probe.map, "assets/studio.hdr");
        assert_eq!(probe.rotation_deg, 90.0);
        assert_eq!(probe.intensity, 1.5);
        assert_eq!(probe.ambient_intensity, 0.25);
        assert_eq!(probe.position, Some([1.0, 2.0, 3.0]));
        assert_eq!(probe.box_size_m, Some([4.0, 5.0, 6.0]));
        assert_eq!(probe.influence_radius_m, Some(7.0));
        assert_eq!(probe.falloff_power, Some(2.0));
    }

    #[test]
    fn project_scene_render_settings_and_lights_parse_from_scene_config() {
        let project = parse_project_config(
            &serde_json::json!({
                "format": ROBOT_DREAMS_PROJECT_FORMAT,
                "name": "Lit Project",
                "scene": {
                    "renderSettings": {
                        "backgroundRgb": "#010203",
                        "ambientRgb": [4, 5, 6],
                        "ambientIntensity": 0.7,
                        "environment": {
                            "skyTopRgb": "#102030",
                            "skyHorizonRgb": [40, 50, 60],
                            "groundRgb": [70, 80, 90],
                            "map": "assets/studio.hdr",
                            "mapRotationDeg": 15.0,
                            "intensity": 1.2,
                            "ambientIntensity": 0.4
                        },
                        "whiteBalanceRgb": [1.0, 0.9, 0.8],
                        "toneMapping": "Aces",
                        "toneExposure": 1.5,
                        "debugRgbSamplesPerPixel": 8,
                        "ambientOcclusionSamples": 12,
                        "ambientOcclusionRadiusM": 0.5,
                        "ambientOcclusionIntensity": 0.8,
                        "indirectDiffuseSamples": 6,
                        "indirectDiffuseRadiusM": 1.2,
                        "indirectDiffuseIntensity": 0.45,
                        "indirectDiffuseBounces": 2,
                        "softShadowSamples": 8,
                        "softShadowRadiusM": 0.2,
                        "areaLightSamples": 10,
                        "roughTransmissionSamples": 11,
                        "roughReflectionSamples": 7,
                        "specularReflectionBounces": 3,
                        "gltfMaterialVariant": "dirty"
                    },
                    "lights": [
                        {
                            "id": "sun",
                            "name": "Sun",
                            "kind": "directional",
                            "direction": [0.0, -1.0, -1.0],
                            "angularRadiusDeg": 0.53,
                            "color": "#ffffff",
                            "intensity": 2.5
                        },
                        {
                            "id": "lamp",
                            "kind": "point",
                            "position": [1.0, 2.0, 3.0],
                            "rangeM": 4.0,
                            "color": [255, 200, 120]
                        }
                    ]
                }
            }),
            PathBuf::from("/tmp/robotdreams/lit-project.json"),
        )
        .expect("parse project");

        let settings = project
            .scene
            .render_settings
            .as_ref()
            .expect("render settings");
        assert_eq!(settings.background_rgb, [1, 2, 3]);
        assert_eq!(settings.ambient_rgb, [4, 5, 6]);
        assert_eq!(settings.ambient_intensity, 0.7);
        assert_eq!(
            settings
                .environment
                .as_ref()
                .and_then(|environment| environment.map.as_deref()),
            Some("assets/studio.hdr")
        );
        assert_eq!(settings.tone_mapping, ToneMapping::Aces);
        assert_eq!(settings.tone_exposure, 1.5);
        assert_eq!(settings.debug_rgb_samples_per_pixel, 8);
        assert_eq!(settings.ambient_occlusion_samples, 12);
        assert_eq!(settings.ambient_occlusion_radius_m, 0.5);
        assert_eq!(settings.ambient_occlusion_intensity, 0.8);
        assert_eq!(settings.indirect_diffuse_samples, 6);
        assert_eq!(settings.indirect_diffuse_radius_m, 1.2);
        assert_eq!(settings.indirect_diffuse_intensity, 0.45);
        assert_eq!(settings.indirect_diffuse_bounces, 2);
        assert_eq!(settings.soft_shadow_samples, 8);
        assert_eq!(settings.soft_shadow_radius_m, 0.2);
        assert_eq!(settings.area_light_samples, 10);
        assert_eq!(settings.rough_transmission_samples, 11);
        assert_eq!(settings.rough_reflection_samples, 7);
        assert_eq!(settings.specular_reflection_bounces, 3);
        assert_eq!(settings.gltf_material_variant.as_deref(), Some("dirty"));

        assert_eq!(project.scene.lights.len(), 2);
        assert_eq!(project.scene.lights[0].id, "sun");
        assert_eq!(project.scene.lights[0].intensity, 2.5);
        assert!(matches!(
            project.scene.lights[0].kind,
            LightKind::Directional {
                angular_radius_deg,
                ..
            } if angular_radius_deg == 0.53
        ));
        assert_eq!(project.scene.lights[1].position, [1.0, 2.0, 3.0]);
        assert!(matches!(
            project.scene.lights[1].kind,
            LightKind::Point { range_m: Some(4.0) }
        ));
    }

    #[test]
    fn project_robot_inherits_model_profile_transformation_when_omitted() {
        let project_robot = robot_config_with_model_transformation(None);
        let profile_transform = ModelTransformationConfig {
            translation: [0.0, 0.0, 0.0],
            rotation: [0.0, 0.0, std::f32::consts::PI],
        };
        let profile_robot = robot_config_with_model_transformation(Some(profile_transform));
        let profile = model_profile_with_robot(profile_robot);

        let merged = merge_model_profile_joint_names(project_robot, Some(&profile));

        assert_eq!(merged.model.model_transformation, Some(profile_transform));
    }

    #[test]
    fn project_robot_model_transformation_overrides_profile_default() {
        let project_transform = ModelTransformationConfig {
            translation: [1.0, 0.0, 0.0],
            rotation: [0.0, 0.0, 0.0],
        };
        let profile_transform = ModelTransformationConfig {
            translation: [0.0, 0.0, 0.0],
            rotation: [0.0, 0.0, std::f32::consts::PI],
        };
        let project_robot = robot_config_with_model_transformation(Some(project_transform));
        let profile_robot = robot_config_with_model_transformation(Some(profile_transform));
        let profile = model_profile_with_robot(profile_robot);

        let merged = merge_model_profile_joint_names(project_robot, Some(&profile));

        assert_eq!(merged.model.model_transformation, Some(project_transform));
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
