use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Transform {
    pub translation: [f32; 3],
    pub rotation: [f32; 3],
    pub rotation_matrix: Option<[[f32; 3]; 3]>,
}

impl Transform {
    pub fn translated(translation: [f32; 3]) -> Self {
        Self {
            translation,
            rotation: [0.0, 0.0, 0.0],
            rotation_matrix: None,
        }
    }

    pub fn matrix(translation: [f32; 3], rotation_matrix: [[f32; 3]; 3]) -> Self {
        Self {
            translation,
            rotation: matrix_to_rpy(rotation_matrix),
            rotation_matrix: Some(rotation_matrix),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EntityId(pub String);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EntityMetadata {
    pub id: EntityId,
    pub name: String,
    pub kind: String,
    pub robot_id: Option<String>,
    pub link_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Geometry {
    Box {
        size: [f32; 3],
    },
    Sphere {
        radius: f32,
    },
    Cylinder {
        radius: f32,
        height: f32,
    },
    MeshBounds {
        size: [f32; 3],
        asset: String,
    },
    MeshAsset {
        asset: String,
        scale: [f32; 3],
        bounds: Option<GeometryBounds>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct GeometryBounds {
    pub min: [f32; 3],
    pub max: [f32; 3],
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Material {
    pub color_rgb: [u8; 3],
}

fn matrix_to_rpy(matrix: [[f32; 3]; 3]) -> [f32; 3] {
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

impl Default for Material {
    fn default() -> Self {
        Self {
            color_rgb: [180, 190, 205],
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SceneNodeKind {
    Group,
    Mesh {
        geometry: Geometry,
        material: Material,
    },
    Camera(CameraSpec),
    Light(LightSpec),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SceneNode {
    pub entity: EntityId,
    pub name: String,
    pub transform: Transform,
    pub kind: SceneNodeKind,
    #[serde(default = "default_include_in_fit")]
    pub include_in_fit: bool,
    /// Whether this render node should become a collider when exported to a
    /// PGE world. Visual debug helpers must remain out of the physics scene.
    #[serde(default = "default_include_in_physics")]
    pub include_in_physics: bool,
    pub children: Vec<SceneNode>,
}

impl SceneNode {
    pub fn group(entity: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            entity: EntityId(entity.into()),
            name: name.into(),
            transform: Transform::default(),
            kind: SceneNodeKind::Group,
            include_in_fit: true,
            include_in_physics: true,
            children: Vec::new(),
        }
    }

    pub fn mesh(
        entity: impl Into<String>,
        name: impl Into<String>,
        geometry: Geometry,
        material: Material,
        transform: Transform,
    ) -> Self {
        Self {
            entity: EntityId(entity.into()),
            name: name.into(),
            transform,
            kind: SceneNodeKind::Mesh { geometry, material },
            include_in_fit: true,
            include_in_physics: true,
            children: Vec::new(),
        }
    }

    pub fn camera(entity: impl Into<String>, name: impl Into<String>, camera: CameraSpec) -> Self {
        Self {
            entity: EntityId(entity.into()),
            name: name.into(),
            transform: camera.transform,
            kind: SceneNodeKind::Camera(camera),
            include_in_fit: true,
            include_in_physics: true,
            children: Vec::new(),
        }
    }

    pub fn light(entity: impl Into<String>, name: impl Into<String>, light: LightSpec) -> Self {
        Self {
            entity: EntityId(entity.into()),
            name: name.into(),
            transform: light.transform,
            kind: SceneNodeKind::Light(light),
            include_in_fit: true,
            include_in_physics: true,
            children: Vec::new(),
        }
    }
}

fn default_include_in_fit() -> bool {
    true
}

fn default_include_in_physics() -> bool {
    true
}

impl Default for SceneNode {
    fn default() -> Self {
        Self::group("world", "World")
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CameraSpec {
    pub id: String,
    pub name: String,
    pub transform: Transform,
    pub fov_deg: f32,
    #[serde(default)]
    pub projection: CameraProjection,
    pub resolution: [u32; 2],
    pub intrinsics: Option<CameraIntrinsics>,
    pub distortion: Option<CameraDistortion>,
    pub depth_range_m: Option<[f32; 2]>,
    pub sensor_effects: Option<CameraSensorEffects>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum CameraProjection {
    #[default]
    Perspective,
    Orthographic {
        size_m: f32,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct CameraIntrinsics {
    pub fx: f32,
    pub fy: f32,
    pub cx: f32,
    pub cy: f32,
    pub skew: f32,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct CameraDistortion {
    pub k1: f32,
    pub k2: f32,
    pub p1: f32,
    pub p2: f32,
    pub k3: f32,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct CameraSensorEffects {
    pub exposure: f32,
    pub gamma: f32,
    pub rgb_noise_stddev: f32,
    pub depth_noise_stddev_m: f32,
    #[serde(default)]
    pub depth_quantization_m: f32,
    pub noise_seed: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LightSpec {
    pub id: String,
    pub name: String,
    pub transform: Transform,
    pub kind: LightKind,
    pub color_rgb: [u8; 3],
    pub intensity: f32,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum LightKind {
    Directional {
        direction: [f32; 3],
        #[serde(default)]
        angular_radius_deg: f32,
    },
    Point {
        range_m: Option<f32>,
    },
    Spot {
        direction: [f32; 3],
        inner_cone_deg: f32,
        outer_cone_deg: f32,
        range_m: Option<f32>,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SceneGraph {
    pub root: SceneNode,
    pub entities: BTreeMap<EntityId, EntityMetadata>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub render_settings: Option<RenderSettings>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reflection_probes: Vec<ReflectionProbeSettings>,
}

impl SceneGraph {
    pub fn empty() -> Self {
        Self {
            root: SceneNode::group("world", "World"),
            entities: BTreeMap::new(),
            render_settings: None,
            reflection_probes: Vec::new(),
        }
    }

    pub fn add_entity(&mut self, metadata: EntityMetadata) {
        self.entities.insert(metadata.id.clone(), metadata);
    }
}

pub const AUTO_CAMERA_ID: &str = "__auto_camera";

pub fn prepare_observation_scene(
    mut scene: SceneGraph,
    request: &ObservationRequest,
) -> SceneGraph {
    if request.camera_id.as_deref() == Some(AUTO_CAMERA_ID) {
        add_auto_camera(&mut scene, request.resolution);
    }
    scene
}

pub fn add_auto_camera(scene: &mut SceneGraph, resolution: [u32; 2]) {
    let (min, max) = scene_bounds(scene).unwrap_or(([-0.5, -0.5, -0.1], [0.5, 0.5, 0.5]));
    let center = scale(add(min, max), 0.5);
    let radius = distance(min, max).max(0.25) * 0.5;
    let fov_deg = 50.0_f32;
    let distance = (radius / (fov_deg.to_radians() * 0.5).tan()).max(0.8);
    let up_axis = scene_floor_up_axis(scene).unwrap_or(2);
    let eye_offset = auto_camera_eye_offset(up_axis);
    let eye = add(center, scale(eye_offset, distance * 1.35));
    let transform = look_at_transform(eye, center, axis_vector(up_axis));
    let camera = CameraSpec {
        id: AUTO_CAMERA_ID.to_string(),
        name: "Auto Camera".to_string(),
        transform,
        fov_deg,
        projection: CameraProjection::Perspective,
        resolution,
        intrinsics: None,
        distortion: None,
        depth_range_m: None,
        sensor_effects: None,
    };
    let entity = EntityId(format!("camera:{AUTO_CAMERA_ID}"));
    scene.add_entity(EntityMetadata {
        id: entity.clone(),
        name: "Auto Camera".to_string(),
        kind: "camera".to_string(),
        robot_id: None,
        link_name: None,
    });
    scene
        .root
        .children
        .push(SceneNode::camera(entity.0, "Auto Camera", camera));
}

pub fn camera_intrinsics_for_resolution(
    camera: &CameraSpec,
    resolution: [u32; 2],
) -> CameraIntrinsics {
    let width = resolution[0].max(1) as f32;
    let height = resolution[1].max(1) as f32;
    if let Some(intrinsics) = camera.intrinsics {
        let native_width = camera.resolution[0].max(1) as f32;
        let native_height = camera.resolution[1].max(1) as f32;
        let sx = width / native_width;
        let sy = height / native_height;
        return CameraIntrinsics {
            fx: intrinsics.fx * sx,
            fy: intrinsics.fy * sy,
            cx: intrinsics.cx * sx,
            cy: intrinsics.cy * sy,
            skew: intrinsics.skew * sx,
        };
    }

    let fov_y = camera.fov_deg.to_radians();
    let fy = (height * 0.5) / (fov_y * 0.5).tan();
    CameraIntrinsics {
        fx: fy,
        fy,
        cx: (width - 1.0) * 0.5,
        cy: (height - 1.0) * 0.5,
        skew: 0.0,
    }
}

pub fn transform_matrix(transform: Transform) -> [[f32; 4]; 4] {
    let rotation = rotation_matrix(transform);
    [
        [
            rotation[0][0],
            rotation[0][1],
            rotation[0][2],
            transform.translation[0],
        ],
        [
            rotation[1][0],
            rotation[1][1],
            rotation[1][2],
            transform.translation[1],
        ],
        [
            rotation[2][0],
            rotation[2][1],
            rotation[2][2],
            transform.translation[2],
        ],
        [0.0, 0.0, 0.0, 1.0],
    ]
}

pub fn scene_bounds(scene: &SceneGraph) -> Option<([f32; 3], [f32; 3])> {
    let mut bounds = BoundsBuilder::default();
    collect_scene_bounds(&scene.root, Transform::default(), &mut bounds);
    bounds.finish()
}

fn collect_scene_bounds(node: &SceneNode, parent: Transform, bounds: &mut BoundsBuilder) {
    if !node.include_in_fit {
        return;
    }
    let transform = compose_transform(parent, node.transform);
    if let SceneNodeKind::Mesh { geometry, .. } = &node.kind {
        add_geometry_bounds(geometry, transform, bounds);
    }
    for child in &node.children {
        collect_scene_bounds(child, transform, bounds);
    }
}

fn add_geometry_bounds(geometry: &Geometry, transform: Transform, bounds: &mut BoundsBuilder) {
    match geometry {
        Geometry::Box { size } | Geometry::MeshBounds { size, .. } => {
            if is_large_thin_slab(*size) {
                return;
            }
            add_box_bounds(*size, transform, bounds);
        }
        Geometry::Sphere { radius } => {
            let diameter = radius * 2.0;
            add_box_bounds([diameter, diameter, diameter], transform, bounds);
        }
        Geometry::Cylinder { radius, height } => {
            add_box_bounds([radius * 2.0, radius * 2.0, *height], transform, bounds);
        }
        Geometry::MeshAsset {
            scale,
            bounds: mesh_bounds,
            ..
        } => {
            if let Some(mesh_bounds) = mesh_bounds {
                add_bounds(*mesh_bounds, transform, bounds);
            } else {
                let size = [
                    scale[0].abs().max(1.0) * 0.08,
                    scale[1].abs().max(1.0) * 0.08,
                    scale[2].abs().max(1.0) * 0.08,
                ];
                add_box_bounds(size, transform, bounds);
            }
        }
    }
}

fn is_large_thin_slab(size: [f32; 3]) -> bool {
    thin_slab_axis(size).is_some()
}

fn thin_slab_axis(size: [f32; 3]) -> Option<usize> {
    let mut sorted = size.map(f32::abs);
    sorted.sort_by(|left, right| left.total_cmp(right));
    if sorted[0] > 0.05 || sorted[1] <= 2.0 || sorted[2] <= 2.0 {
        return None;
    }
    size.iter()
        .enumerate()
        .min_by(|(_, left), (_, right)| left.abs().total_cmp(&right.abs()))
        .map(|(axis, _)| axis)
}

fn add_bounds(mesh_bounds: GeometryBounds, transform: Transform, bounds: &mut BoundsBuilder) {
    for x in [mesh_bounds.min[0], mesh_bounds.max[0]] {
        for y in [mesh_bounds.min[1], mesh_bounds.max[1]] {
            for z in [mesh_bounds.min[2], mesh_bounds.max[2]] {
                bounds.add_point(transform_point(transform, [x, y, z]));
            }
        }
    }
}

fn add_box_bounds(size: [f32; 3], transform: Transform, bounds: &mut BoundsBuilder) {
    let half = [size[0] * 0.5, size[1] * 0.5, size[2] * 0.5];
    for x in [-half[0], half[0]] {
        for y in [-half[1], half[1]] {
            for z in [-half[2], half[2]] {
                bounds.add_point(transform_point(transform, [x, y, z]));
            }
        }
    }
}

#[derive(Default)]
struct BoundsBuilder {
    min: Option<[f32; 3]>,
    max: Option<[f32; 3]>,
}

impl BoundsBuilder {
    fn add_point(&mut self, point: [f32; 3]) {
        match (&mut self.min, &mut self.max) {
            (Some(min), Some(max)) => {
                for axis in 0..3 {
                    min[axis] = min[axis].min(point[axis]);
                    max[axis] = max[axis].max(point[axis]);
                }
            }
            _ => {
                self.min = Some(point);
                self.max = Some(point);
            }
        }
    }

    fn finish(self) -> Option<([f32; 3], [f32; 3])> {
        Some((self.min?, self.max?))
    }
}

#[derive(Default)]
struct FloorAxisCandidate {
    area: f32,
    axis: Option<usize>,
}

impl FloorAxisCandidate {
    fn add(&mut self, area: f32, axis: usize) {
        if area > self.area {
            self.area = area;
            self.axis = Some(axis);
        }
    }
}

fn scene_floor_up_axis(scene: &SceneGraph) -> Option<usize> {
    let mut candidate = FloorAxisCandidate::default();
    collect_scene_floor_up_axis(&scene.root, Transform::default(), &mut candidate);
    candidate.axis
}

fn collect_scene_floor_up_axis(
    node: &SceneNode,
    parent: Transform,
    candidate: &mut FloorAxisCandidate,
) {
    let transform = compose_transform(parent, node.transform);
    if let SceneNodeKind::Mesh { geometry, .. } = &node.kind {
        add_geometry_floor_up_axis(geometry, transform, candidate);
    }
    for child in &node.children {
        collect_scene_floor_up_axis(child, transform, candidate);
    }
}

fn add_geometry_floor_up_axis(
    geometry: &Geometry,
    transform: Transform,
    candidate: &mut FloorAxisCandidate,
) {
    let Some((size, local_axis)) = geometry_slab_size_and_axis(geometry) else {
        return;
    };
    let world_normal = mat_vec_mul(rotation_matrix(transform), axis_vector(local_axis));
    let world_axis = largest_abs_axis(world_normal);
    candidate.add(slab_area(size, local_axis), world_axis);
}

fn geometry_slab_size_and_axis(geometry: &Geometry) -> Option<([f32; 3], usize)> {
    let size = match geometry {
        Geometry::Box { size } | Geometry::MeshBounds { size, .. } => *size,
        Geometry::MeshAsset {
            bounds: Some(bounds),
            ..
        } => sub(bounds.max, bounds.min),
        _ => return None,
    };
    thin_slab_axis(size).map(|axis| (size, axis))
}

fn slab_area(size: [f32; 3], thin_axis: usize) -> f32 {
    size.iter()
        .enumerate()
        .filter(|(axis, _)| *axis != thin_axis)
        .map(|(_, value)| value.abs())
        .product()
}

fn largest_abs_axis(value: [f32; 3]) -> usize {
    let mut axis = 0;
    for candidate in 1..3 {
        if value[candidate].abs() > value[axis].abs() {
            axis = candidate;
        }
    }
    axis
}

fn auto_camera_eye_offset(up_axis: usize) -> [f32; 3] {
    match up_axis {
        0 => normalize([0.50, -0.85, -0.65]),
        1 => normalize([-0.85, 0.50, -0.65]),
        _ => normalize([-0.85, -0.65, 0.50]),
    }
}

fn axis_vector(axis: usize) -> [f32; 3] {
    match axis {
        0 => [1.0, 0.0, 0.0],
        1 => [0.0, 1.0, 0.0],
        _ => [0.0, 0.0, 1.0],
    }
}

fn look_at_transform(eye: [f32; 3], target: [f32; 3], world_up: [f32; 3]) -> Transform {
    let forward = normalize(sub(target, eye));
    let mut left = cross(world_up, forward);
    if length(left) < 1.0e-5 {
        left = [0.0, 1.0, 0.0];
    }
    left = normalize(left);
    let up = normalize(cross(forward, left));
    Transform::matrix(
        eye,
        [
            [forward[0], left[0], up[0]],
            [forward[1], left[1], up[1]],
            [forward[2], left[2], up[2]],
        ],
    )
}

fn compose_transform(parent: Transform, child: Transform) -> Transform {
    let parent_rotation = rotation_matrix(parent);
    let child_rotation = rotation_matrix(child);
    let rotation_matrix = mat_mul(parent_rotation, child_rotation);
    Transform {
        translation: add(
            parent.translation,
            mat_vec_mul(parent_rotation, child.translation),
        ),
        rotation: add(parent.rotation, child.rotation),
        rotation_matrix: Some(rotation_matrix),
    }
}

fn transform_point(transform: Transform, point: [f32; 3]) -> [f32; 3] {
    add(
        transform.translation,
        mat_vec_mul(rotation_matrix(transform), point),
    )
}

fn rotation_matrix(transform: Transform) -> [[f32; 3]; 3] {
    transform
        .rotation_matrix
        .unwrap_or_else(|| rpy_matrix(transform.rotation))
}

fn rpy_matrix(rpy: [f32; 3]) -> [[f32; 3]; 3] {
    let basis_x = rotate(rpy, [1.0, 0.0, 0.0]);
    let basis_y = rotate(rpy, [0.0, 1.0, 0.0]);
    let basis_z = rotate(rpy, [0.0, 0.0, 1.0]);
    [
        [basis_x[0], basis_y[0], basis_z[0]],
        [basis_x[1], basis_y[1], basis_z[1]],
        [basis_x[2], basis_y[2], basis_z[2]],
    ]
}

fn rotate(rpy: [f32; 3], vector: [f32; 3]) -> [f32; 3] {
    let [roll, pitch, yaw] = rpy;
    let (sr, cr) = roll.sin_cos();
    let (sp, cp) = pitch.sin_cos();
    let (sy, cy) = yaw.sin_cos();
    let x1 = vector[0];
    let y1 = cr * vector[1] - sr * vector[2];
    let z1 = sr * vector[1] + cr * vector[2];
    let x2 = cp * x1 + sp * z1;
    let y2 = y1;
    let z2 = -sp * x1 + cp * z1;
    [cy * x2 - sy * y2, sy * x2 + cy * y2, z2]
}

fn mat_mul(left: [[f32; 3]; 3], right: [[f32; 3]; 3]) -> [[f32; 3]; 3] {
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

fn mat_vec_mul(matrix: [[f32; 3]; 3], vector: [f32; 3]) -> [f32; 3] {
    [
        matrix[0][0] * vector[0] + matrix[0][1] * vector[1] + matrix[0][2] * vector[2],
        matrix[1][0] * vector[0] + matrix[1][1] * vector[1] + matrix[1][2] * vector[2],
        matrix[2][0] * vector[0] + matrix[2][1] * vector[1] + matrix[2][2] * vector[2],
    ]
}

fn add(left: [f32; 3], right: [f32; 3]) -> [f32; 3] {
    [left[0] + right[0], left[1] + right[1], left[2] + right[2]]
}

fn sub(left: [f32; 3], right: [f32; 3]) -> [f32; 3] {
    [left[0] - right[0], left[1] - right[1], left[2] - right[2]]
}

fn scale(value: [f32; 3], factor: f32) -> [f32; 3] {
    [value[0] * factor, value[1] * factor, value[2] * factor]
}

fn cross(left: [f32; 3], right: [f32; 3]) -> [f32; 3] {
    [
        left[1] * right[2] - left[2] * right[1],
        left[2] * right[0] - left[0] * right[2],
        left[0] * right[1] - left[1] * right[0],
    ]
}

fn dot(left: [f32; 3], right: [f32; 3]) -> f32 {
    left[0] * right[0] + left[1] * right[1] + left[2] * right[2]
}

fn length(value: [f32; 3]) -> f32 {
    dot(value, value).sqrt()
}

fn normalize(value: [f32; 3]) -> [f32; 3] {
    let len = length(value);
    if len <= f32::EPSILON {
        return [1.0, 0.0, 0.0];
    }
    [value[0] / len, value[1] / len, value[2] / len]
}

fn distance(left: [f32; 3], right: [f32; 3]) -> f32 {
    length(sub(left, right))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObservationView {
    State,
    DebugRgb,
    Depth,
    Segmentation,
    Normal,
    Albedo,
    MaterialProperties,
    WorldPosition,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SegmentationPolicy {
    pub min_alpha: f32,
}

impl Default for SegmentationPolicy {
    fn default() -> Self {
        Self { min_alpha: 0.0 }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShutterPolicy {
    pub exposure_sec: f32,
    pub samples: u32,
    #[serde(default)]
    pub mode: ShutterMode,
    #[serde(default)]
    pub readout_sec: f32,
}

impl Default for ShutterPolicy {
    fn default() -> Self {
        Self {
            exposure_sec: 0.0,
            samples: 1,
            mode: ShutterMode::Global,
            readout_sec: 0.0,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ShutterMode {
    #[default]
    Global,
    RollingTopToBottom,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ToneMapping {
    Linear,
    Reinhard,
    Aces,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RenderSettings {
    pub background_rgb: [u8; 3],
    pub ambient_rgb: [u8; 3],
    pub ambient_intensity: f32,
    #[serde(default = "default_debug_rgb_samples_per_pixel")]
    pub debug_rgb_samples_per_pixel: u32,
    #[serde(default)]
    pub ambient_occlusion_samples: u32,
    #[serde(default = "default_ambient_occlusion_radius_m")]
    pub ambient_occlusion_radius_m: f32,
    #[serde(default = "default_ambient_occlusion_intensity")]
    pub ambient_occlusion_intensity: f32,
    #[serde(default)]
    pub indirect_diffuse_samples: u32,
    #[serde(default = "default_indirect_diffuse_radius_m")]
    pub indirect_diffuse_radius_m: f32,
    #[serde(default = "default_indirect_diffuse_intensity")]
    pub indirect_diffuse_intensity: f32,
    #[serde(default = "default_indirect_diffuse_bounces")]
    pub indirect_diffuse_bounces: u32,
    #[serde(default = "default_soft_shadow_samples")]
    pub soft_shadow_samples: u32,
    #[serde(default)]
    pub soft_shadow_radius_m: f32,
    #[serde(default = "default_area_light_samples")]
    pub area_light_samples: u32,
    #[serde(default = "default_rough_transmission_samples")]
    pub rough_transmission_samples: u32,
    #[serde(default = "default_rough_reflection_samples")]
    pub rough_reflection_samples: u32,
    #[serde(default = "default_specular_reflection_bounces")]
    pub specular_reflection_bounces: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gltf_material_variant: Option<String>,
    pub environment: Option<EnvironmentSettings>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reflection_probe: Option<ReflectionProbeSettings>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reflection_probes: Vec<ReflectionProbeSettings>,
    pub white_balance_rgb: [f32; 3],
    pub color_temperature_kelvin: Option<f32>,
    pub tone_mapping: ToneMapping,
    pub tone_exposure: f32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnvironmentSettings {
    pub sky_top_rgb: [u8; 3],
    pub sky_horizon_rgb: [u8; 3],
    pub ground_rgb: [u8; 3],
    pub map: Option<String>,
    pub map_rotation_deg: f32,
    pub intensity: f32,
    pub ambient_intensity: f32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReflectionProbeSettings {
    pub map: String,
    pub rotation_deg: f32,
    pub intensity: f32,
    pub ambient_intensity: f32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position: Option<[f32; 3]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub box_size_m: Option<[f32; 3]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub influence_radius_m: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub falloff_power: Option<f32>,
}

impl Default for RenderSettings {
    fn default() -> Self {
        Self {
            background_rgb: [8, 8, 8],
            ambient_rgb: [255, 255, 255],
            ambient_intensity: 0.30,
            debug_rgb_samples_per_pixel: default_debug_rgb_samples_per_pixel(),
            ambient_occlusion_samples: 0,
            ambient_occlusion_radius_m: default_ambient_occlusion_radius_m(),
            ambient_occlusion_intensity: default_ambient_occlusion_intensity(),
            indirect_diffuse_samples: 0,
            indirect_diffuse_radius_m: default_indirect_diffuse_radius_m(),
            indirect_diffuse_intensity: default_indirect_diffuse_intensity(),
            indirect_diffuse_bounces: default_indirect_diffuse_bounces(),
            soft_shadow_samples: default_soft_shadow_samples(),
            soft_shadow_radius_m: 0.0,
            area_light_samples: default_area_light_samples(),
            rough_transmission_samples: default_rough_transmission_samples(),
            rough_reflection_samples: default_rough_reflection_samples(),
            specular_reflection_bounces: default_specular_reflection_bounces(),
            gltf_material_variant: None,
            environment: None,
            reflection_probe: None,
            reflection_probes: Vec::new(),
            white_balance_rgb: [1.0, 1.0, 1.0],
            color_temperature_kelvin: None,
            tone_mapping: ToneMapping::Linear,
            tone_exposure: 1.0,
        }
    }
}

fn default_debug_rgb_samples_per_pixel() -> u32 {
    1
}

fn default_ambient_occlusion_radius_m() -> f32 {
    0.25
}

fn default_ambient_occlusion_intensity() -> f32 {
    1.0
}

fn default_indirect_diffuse_radius_m() -> f32 {
    1.0
}

fn default_indirect_diffuse_intensity() -> f32 {
    1.0
}

fn default_indirect_diffuse_bounces() -> u32 {
    1
}

fn default_soft_shadow_samples() -> u32 {
    1
}

fn default_area_light_samples() -> u32 {
    4
}

fn default_rough_transmission_samples() -> u32 {
    5
}

fn default_rough_reflection_samples() -> u32 {
    1
}

fn default_specular_reflection_bounces() -> u32 {
    1
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ObservationRequest {
    pub camera_id: Option<String>,
    pub views: Vec<ObservationView>,
    pub resolution: [u32; 2],
    pub segmentation_policy: Option<SegmentationPolicy>,
    pub shutter_policy: Option<ShutterPolicy>,
    pub render_settings: Option<RenderSettings>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ObservationMetadata {
    pub timestamp_sec: f64,
    pub camera_id: Option<String>,
    pub camera_pose: Option<Transform>,
    pub camera_projection: Option<CameraProjection>,
    pub camera_intrinsics: Option<CameraIntrinsics>,
    pub camera_distortion: Option<CameraDistortion>,
    pub camera_extrinsics_matrix: Option<[[f32; 4]; 4]>,
    pub depth_range_m: Option<[f32; 2]>,
    pub sensor_effects: Option<CameraSensorEffects>,
    pub resolution: [u32; 2],
    pub views: Vec<ObservationView>,
    pub segmentation_policy: Option<SegmentationPolicy>,
    pub shutter_policy: Option<ShutterPolicy>,
    pub render_settings: Option<RenderSettings>,
    pub entities: BTreeMap<EntityId, EntityMetadata>,
}

#[cfg(test)]
mod tests {
    use super::{
        AUTO_CAMERA_ID, Geometry, GeometryBounds, Material, RenderSettings, SceneGraph, SceneNode,
        SceneNodeKind, ShutterMode, ShutterPolicy, Transform, add_auto_camera, scene_bounds,
    };

    #[test]
    fn mesh_asset_bounds_use_real_asset_extents_when_present() {
        let mut scene = SceneGraph::empty();
        scene.root.children.push(SceneNode::mesh(
            "mesh",
            "Mesh",
            Geometry::MeshAsset {
                asset: "mesh.gltf".to_string(),
                scale: [1.0, 1.0, 1.0],
                bounds: Some(GeometryBounds {
                    min: [-2.0, -0.5, -0.25],
                    max: [3.0, 1.5, 0.75],
                }),
            },
            Material::default(),
            Transform::default(),
        ));

        let (min, max) = scene_bounds(&scene).expect("scene bounds");

        assert_eq!(min, [-2.0, -0.5, -0.25]);
        assert_eq!(max, [3.0, 1.5, 0.75]);
    }

    #[test]
    fn scene_bounds_skip_nodes_excluded_from_fit() {
        let mut scene = SceneGraph::empty();
        let mut floor = SceneNode::mesh(
            "floor",
            "Floor",
            Geometry::Box {
                size: [5.0, 0.02, 5.0],
            },
            Material::default(),
            Transform::default(),
        );
        floor.include_in_fit = false;
        scene.root.children.push(floor);
        scene.root.children.push(SceneNode::mesh(
            "box",
            "Box",
            Geometry::Box {
                size: [0.4, 0.2, 0.3],
            },
            Material::default(),
            Transform::translated([0.5, 0.0, 0.2]),
        ));

        let (min, max) = scene_bounds(&scene).expect("scene bounds");

        assert_vec3_near(min, [0.3, -0.1, 0.05]);
        assert_vec3_near(max, [0.7, 0.1, 0.35]);
    }

    #[test]
    fn scene_bounds_skip_large_thin_slab_on_any_axis() {
        let mut scene = SceneGraph::empty();
        scene.root.children.push(SceneNode::mesh(
            "floor",
            "Floor",
            Geometry::Box {
                size: [5.0, 0.02, 5.0],
            },
            Material::default(),
            Transform::default(),
        ));

        assert_eq!(scene_bounds(&scene), None);
    }

    #[test]
    fn auto_camera_uses_excluded_floor_slab_normal_as_up_axis() {
        let mut scene = SceneGraph::empty();
        let mut floor = SceneNode::mesh(
            "floor",
            "Floor",
            Geometry::Box {
                size: [5.0, 0.02, 5.0],
            },
            Material::default(),
            Transform::translated([0.0, -0.011, 0.0]),
        );
        floor.include_in_fit = false;
        scene.root.children.push(floor);
        scene.root.children.push(SceneNode::mesh(
            "box",
            "Box",
            Geometry::Box {
                size: [0.4, 0.2, 0.3],
            },
            Material::default(),
            Transform::translated([0.0, 0.1, 0.0]),
        ));

        add_auto_camera(&mut scene, [320, 180]);

        let camera = scene
            .root
            .children
            .iter()
            .find_map(|node| match &node.kind {
                SceneNodeKind::Camera(camera) if camera.id == AUTO_CAMERA_ID => Some(camera),
                _ => None,
            })
            .expect("auto camera");

        assert!(
            camera.transform.translation[1] > 0.1,
            "auto camera should stand above the Y-up floor plane, got {:?}",
            camera.transform.translation
        );
    }

    fn assert_vec3_near(actual: [f32; 3], expected: [f32; 3]) {
        for axis in 0..3 {
            assert!(
                (actual[axis] - expected[axis]).abs() < 1.0e-6,
                "axis {axis}: expected {}, got {}",
                expected[axis],
                actual[axis]
            );
        }
    }

    #[test]
    fn shutter_policy_deserializes_legacy_global_shape() {
        let policy: ShutterPolicy =
            serde_json::from_str(r#"{"exposureSec":0.02,"samples":3}"#).expect("shutter policy");

        assert_eq!(policy.exposure_sec, 0.02);
        assert_eq!(policy.samples, 3);
        assert_eq!(policy.mode, ShutterMode::Global);
        assert_eq!(policy.readout_sec, 0.0);
    }

    #[test]
    fn render_settings_deserializes_reflection_probe_list() {
        let settings: RenderSettings = serde_json::from_str(
            r#"{
                "backgroundRgb":[8,8,8],
                "ambientRgb":[255,255,255],
                "ambientIntensity":0.3,
                "reflectionProbes":[{
                    "map":"left.hdr",
                    "rotationDeg":0.0,
                    "intensity":1.0,
                    "ambientIntensity":0.2,
                    "position":[-1.0,0.0,0.0],
                    "boxSizeM":[2.0,2.0,2.0],
                    "influenceRadiusM":3.0,
                    "falloffPower":2.0
                }],
                "whiteBalanceRgb":[1.0,1.0,1.0],
                "toneMapping":"Linear",
                "toneExposure":1.0,
                "debugRgbSamplesPerPixel":4,
                "ambientOcclusionSamples":8,
                "ambientOcclusionRadiusM":0.75,
                "ambientOcclusionIntensity":0.6,
                "indirectDiffuseSamples":12,
                "indirectDiffuseRadiusM":1.5,
                "indirectDiffuseIntensity":0.4,
                "indirectDiffuseBounces":3,
                "softShadowSamples":8,
                "softShadowRadiusM":0.12,
                "areaLightSamples":12,
                "roughTransmissionSamples":9,
                "roughReflectionSamples":7,
                "specularReflectionBounces":3,
                "gltfMaterialVariant":"clean"
            }"#,
        )
        .expect("render settings");

        assert_eq!(settings.debug_rgb_samples_per_pixel, 4);
        assert_eq!(settings.ambient_occlusion_samples, 8);
        assert_eq!(settings.ambient_occlusion_radius_m, 0.75);
        assert_eq!(settings.ambient_occlusion_intensity, 0.6);
        assert_eq!(settings.indirect_diffuse_samples, 12);
        assert_eq!(settings.indirect_diffuse_radius_m, 1.5);
        assert_eq!(settings.indirect_diffuse_intensity, 0.4);
        assert_eq!(settings.indirect_diffuse_bounces, 3);
        assert_eq!(settings.soft_shadow_samples, 8);
        assert_eq!(settings.soft_shadow_radius_m, 0.12);
        assert_eq!(settings.area_light_samples, 12);
        assert_eq!(settings.rough_transmission_samples, 9);
        assert_eq!(settings.rough_reflection_samples, 7);
        assert_eq!(settings.specular_reflection_bounces, 3);
        assert_eq!(settings.gltf_material_variant.as_deref(), Some("clean"));
        assert!(settings.reflection_probe.is_none());
        assert_eq!(settings.reflection_probes.len(), 1);
        assert_eq!(settings.reflection_probes[0].map, "left.hdr");
        assert_eq!(
            settings.reflection_probes[0].position,
            Some([-1.0, 0.0, 0.0])
        );

        let legacy: RenderSettings = serde_json::from_str(
            r#"{
                "backgroundRgb":[8,8,8],
                "ambientRgb":[255,255,255],
                "ambientIntensity":0.3,
                "whiteBalanceRgb":[1.0,1.0,1.0],
                "toneMapping":"Linear",
                "toneExposure":1.0
            }"#,
        )
        .expect("legacy render settings");
        assert_eq!(legacy.debug_rgb_samples_per_pixel, 1);
        assert_eq!(legacy.ambient_occlusion_samples, 0);
        assert_eq!(legacy.ambient_occlusion_radius_m, 0.25);
        assert_eq!(legacy.ambient_occlusion_intensity, 1.0);
        assert_eq!(legacy.indirect_diffuse_samples, 0);
        assert_eq!(legacy.indirect_diffuse_radius_m, 1.0);
        assert_eq!(legacy.indirect_diffuse_intensity, 1.0);
        assert_eq!(legacy.indirect_diffuse_bounces, 1);
        assert_eq!(legacy.soft_shadow_samples, 1);
        assert_eq!(legacy.soft_shadow_radius_m, 0.0);
        assert_eq!(legacy.area_light_samples, 4);
        assert_eq!(legacy.rough_transmission_samples, 5);
        assert_eq!(legacy.rough_reflection_samples, 1);
        assert_eq!(legacy.specular_reflection_bounces, 1);
        assert!(legacy.gltf_material_variant.is_none());
        assert!(legacy.reflection_probes.is_empty());
    }
}
