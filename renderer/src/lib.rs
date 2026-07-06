use std::collections::BTreeMap;
use std::f32::consts::PI;
use std::io::Cursor;
use std::path::Path;
use std::sync::{Arc, Mutex};

use gltf::mesh::Mode;
use image::ImageReader;
use rayon::prelude::*;
use robotdreams_core::RobotDreamsSnapshot;
use robotdreams_core::scene_graph::{
    CameraDistortion, CameraProjection, CameraSensorEffects, CameraSpec, EntityId,
    EnvironmentSettings, Geometry, LightKind, ObservationMetadata, ObservationRequest,
    ObservationView, ReflectionProbeSettings, RenderSettings, SceneGraph, SceneNode, SceneNodeKind,
    SegmentationPolicy, ShutterMode, ShutterPolicy, ToneMapping, Transform,
    camera_intrinsics_for_resolution, transform_matrix,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FrameKind {
    DebugRgb,
    Depth,
    Segmentation,
    Normal,
    Albedo,
    MaterialProperties,
    WorldPosition,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FrameBuffer {
    pub kind: FrameKind,
    pub width: u32,
    pub height: u32,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RenderOutput {
    pub metadata: ObservationMetadata,
    pub frames: Vec<FrameBuffer>,
    pub state: Option<RobotDreamsSnapshot>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SceneGraphSample {
    pub scene: SceneGraph,
    pub state: Option<RobotDreamsSnapshot>,
}

#[derive(Clone, Debug)]
pub struct NativeRenderer {
    far_m: f32,
    mesh_cache: MeshGeometryCache,
    environment_cache: EnvironmentTextureCache,
    environment_brdf_lut: Arc<EnvironmentBrdfLut>,
}

type MeshGeometryCache = Arc<Mutex<BTreeMap<MeshCacheKey, Arc<RenderGeometry>>>>;
type EnvironmentTextureCache = Arc<Mutex<BTreeMap<String, Arc<EnvironmentTexture>>>>;
const TRANSMISSION_REFRACTION_BOUNCES: u8 = 4;
const CLEARCOAT_DIELECTRIC_F0: f32 = 0.04;
const EMISSIVE_TRIANGLE_LIGHT_SAMPLES: u32 = 8;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct MeshCacheKey {
    asset: String,
    scale_bits: [u32; 3],
    animation_time_millis: i64,
    material_variant: Option<String>,
}

#[derive(Clone, Debug)]
struct RenderObject {
    entity_u32: u32,
    geometry: Arc<RenderGeometry>,
    color_rgb: [u8; 3],
    transform: Transform,
    bounds: Option<([f32; 3], [f32; 3])>,
}

#[derive(Clone, Debug)]
struct RenderSceneSample {
    timestamp_sec: f32,
    camera: Option<CameraSpec>,
    objects: Vec<RenderObject>,
    object_bvh: Arc<ObjectBvh>,
    lights: Vec<RenderLight>,
}

#[derive(Clone, Debug)]
struct PreparedScene {
    camera: Option<CameraSpec>,
    objects: Vec<RenderObject>,
    object_bvh: Arc<ObjectBvh>,
    lights: Vec<RenderLight>,
    scene_lights: Vec<RenderLight>,
    state: Option<RobotDreamsSnapshot>,
    entities: BTreeMap<EntityId, robotdreams_core::scene_graph::EntityMetadata>,
}

#[derive(Clone, Debug, Default)]
struct ObjectBvh {
    nodes: Vec<ObjectBvhNode>,
    root: Option<usize>,
    unbounded_indices: Vec<usize>,
}

#[derive(Clone, Debug)]
struct ObjectBvhNode {
    bounds_min: [f32; 3],
    bounds_max: [f32; 3],
    left: Option<usize>,
    right: Option<usize>,
    object_indices: Vec<usize>,
}

#[derive(Clone, Copy, Debug)]
struct ObjectBvhPrimitive {
    object_index: usize,
    bounds_min: [f32; 3],
    bounds_max: [f32; 3],
    centroid: [f32; 3],
}

#[derive(Clone, Copy, Debug)]
struct SceneIntersector<'a> {
    objects: &'a [RenderObject],
    object_bvh: &'a ObjectBvh,
}

#[derive(Clone, Copy, Debug)]
struct RenderLight {
    kind: RenderLightKind,
    color_rgb: [f32; 3],
    intensity: f32,
}

#[derive(Clone, Copy, Debug)]
enum RenderLightKind {
    Directional {
        direction: [f32; 3],
        angular_radius_deg: f32,
    },
    Point {
        position: [f32; 3],
        range_m: Option<f32>,
    },
    Spot {
        position: [f32; 3],
        direction: [f32; 3],
        inner_cos: f32,
        outer_cos: f32,
        range_m: Option<f32>,
    },
    AreaTriangle {
        a: [f32; 3],
        b: [f32; 3],
        c: [f32; 3],
        normal: [f32; 3],
        double_sided: bool,
    },
}

#[derive(Clone, Debug)]
enum RenderGeometry {
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
    Triangles {
        bounds_min: [f32; 3],
        bounds_max: [f32; 3],
        triangles: Vec<Triangle>,
        textures: Vec<TextureImage>,
        lights: Vec<RenderLight>,
        bvh: Vec<BvhNode>,
    },
}

#[derive(Clone, Copy, Debug)]
struct VertexColorSample {
    rgba: [u8; 4],
    linear_rgba: [f32; 4],
}

#[derive(Clone, Copy, Debug)]
struct Triangle {
    a: [f32; 3],
    b: [f32; 3],
    c: [f32; 3],
    normal_a: [f32; 3],
    normal_b: [f32; 3],
    normal_c: [f32; 3],
    color_rgba: [u8; 4],
    color_linear_rgb: [f32; 3],
    color_alpha: f32,
    vertex_color_a: Option<[u8; 4]>,
    vertex_color_b: Option<[u8; 4]>,
    vertex_color_c: Option<[u8; 4]>,
    vertex_color_linear_a: Option<[f32; 4]>,
    vertex_color_linear_b: Option<[f32; 4]>,
    vertex_color_linear_c: Option<[f32; 4]>,
    texcoord_a: Option<[f32; 2]>,
    texcoord_b: Option<[f32; 2]>,
    texcoord_c: Option<[f32; 2]>,
    texcoord1_a: Option<[f32; 2]>,
    texcoord1_b: Option<[f32; 2]>,
    texcoord1_c: Option<[f32; 2]>,
    tangent_a: Option<[f32; 4]>,
    tangent_b: Option<[f32; 4]>,
    tangent_c: Option<[f32; 4]>,
    texture_index: Option<usize>,
    texture_texcoord: u32,
    texture_transform: TextureTransform2D,
    metallic_factor: f32,
    roughness_factor: f32,
    metallic_roughness_texture_index: Option<usize>,
    metallic_roughness_texture_texcoord: u32,
    metallic_roughness_texture_transform: TextureTransform2D,
    specular_glossiness_texture_index: Option<usize>,
    specular_glossiness_texture_texcoord: u32,
    specular_glossiness_texture_transform: TextureTransform2D,
    ior: f32,
    transmission_factor: f32,
    transmission_texture_index: Option<usize>,
    transmission_texture_texcoord: u32,
    transmission_texture_transform: TextureTransform2D,
    diffuse_transmission_factor: f32,
    diffuse_transmission_texture_index: Option<usize>,
    diffuse_transmission_texture_texcoord: u32,
    diffuse_transmission_texture_transform: TextureTransform2D,
    diffuse_transmission_color_factor: [f32; 3],
    diffuse_transmission_color_texture_index: Option<usize>,
    diffuse_transmission_color_texture_texcoord: u32,
    diffuse_transmission_color_texture_transform: TextureTransform2D,
    dispersion: f32,
    volume_thickness_factor: f32,
    volume_thickness_texture_index: Option<usize>,
    volume_thickness_texture_texcoord: u32,
    volume_thickness_texture_transform: TextureTransform2D,
    volume_attenuation_distance: f32,
    volume_attenuation_color: [f32; 3],
    clearcoat_factor: f32,
    clearcoat_texture_index: Option<usize>,
    clearcoat_texture_texcoord: u32,
    clearcoat_texture_transform: TextureTransform2D,
    clearcoat_roughness_factor: f32,
    clearcoat_roughness_texture_index: Option<usize>,
    clearcoat_roughness_texture_texcoord: u32,
    clearcoat_roughness_texture_transform: TextureTransform2D,
    clearcoat_normal_texture_index: Option<usize>,
    clearcoat_normal_texture_texcoord: u32,
    clearcoat_normal_texture_transform: TextureTransform2D,
    clearcoat_normal_scale: f32,
    sheen_color_factor: [f32; 3],
    sheen_color_texture_index: Option<usize>,
    sheen_color_texture_texcoord: u32,
    sheen_color_texture_transform: TextureTransform2D,
    sheen_roughness_factor: f32,
    sheen_roughness_texture_index: Option<usize>,
    sheen_roughness_texture_texcoord: u32,
    sheen_roughness_texture_transform: TextureTransform2D,
    anisotropy_strength: f32,
    anisotropy_rotation: f32,
    anisotropy_texture_index: Option<usize>,
    anisotropy_texture_texcoord: u32,
    anisotropy_texture_transform: TextureTransform2D,
    iridescence_factor: f32,
    iridescence_texture_index: Option<usize>,
    iridescence_texture_texcoord: u32,
    iridescence_texture_transform: TextureTransform2D,
    iridescence_ior: f32,
    iridescence_thickness_minimum_nm: f32,
    iridescence_thickness_maximum_nm: f32,
    iridescence_thickness_texture_index: Option<usize>,
    iridescence_thickness_texture_texcoord: u32,
    iridescence_thickness_texture_transform: TextureTransform2D,
    specular_factor: f32,
    specular_texture_index: Option<usize>,
    specular_texture_texcoord: u32,
    specular_texture_transform: TextureTransform2D,
    specular_color_factor: [f32; 3],
    specular_color_texture_index: Option<usize>,
    specular_color_texture_texcoord: u32,
    specular_color_texture_transform: TextureTransform2D,
    normal_texture_index: Option<usize>,
    normal_texture_texcoord: u32,
    normal_texture_transform: TextureTransform2D,
    normal_scale: f32,
    emissive_rgb: [f32; 3],
    emissive_texture_index: Option<usize>,
    emissive_texture_texcoord: u32,
    emissive_texture_transform: TextureTransform2D,
    occlusion_texture_index: Option<usize>,
    occlusion_texture_texcoord: u32,
    occlusion_texture_transform: TextureTransform2D,
    occlusion_strength: f32,
    unlit: bool,
    alpha_cutoff: Option<f32>,
    alpha_mode: MaterialAlphaMode,
    double_sided: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MaterialAlphaMode {
    Opaque,
    Mask,
    Blend,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct TextureTransform2D {
    offset: [f32; 2],
    rotation: f32,
    scale: [f32; 2],
}

impl Default for TextureTransform2D {
    fn default() -> Self {
        Self {
            offset: [0.0, 0.0],
            rotation: 0.0,
            scale: [1.0, 1.0],
        }
    }
}

impl TextureTransform2D {
    fn apply(self, texcoord: [f32; 2]) -> [f32; 2] {
        let scaled = [texcoord[0] * self.scale[0], texcoord[1] * self.scale[1]];
        let (sin, cos) = self.rotation.sin_cos();
        [
            self.offset[0] + cos * scaled[0] - sin * scaled[1],
            self.offset[1] + sin * scaled[0] + cos * scaled[1],
        ]
    }
}

#[derive(Clone, Debug)]
struct TextureImage {
    width: u32,
    height: u32,
    rgba: Vec<u8>,
    wrap_s: TextureWrap,
    wrap_t: TextureWrap,
    filter: TextureFilter,
}

#[derive(Clone, Debug)]
struct EnvironmentTexture {
    width: u32,
    height: u32,
    rgb: Vec<[f32; 3]>,
    roughness_mips: Vec<EnvironmentTextureMip>,
    diffuse_irradiance: Option<EnvironmentTextureMip>,
}

#[derive(Clone, Debug)]
struct EnvironmentTextureMip {
    width: u32,
    height: u32,
    rgb: Vec<[f32; 3]>,
}

#[derive(Clone, Debug)]
struct PreparedRenderSettings {
    settings: RenderSettings,
    environment_map: Option<Arc<EnvironmentTexture>>,
    reflection_probes: Vec<PreparedReflectionProbe>,
    environment_brdf_lut: Arc<EnvironmentBrdfLut>,
}

#[derive(Clone, Debug)]
struct PreparedReflectionProbe {
    settings: ReflectionProbeSettings,
    texture: Arc<EnvironmentTexture>,
}

#[cfg(test)]
impl PreparedRenderSettings {
    fn from_settings(settings: RenderSettings) -> Self {
        Self {
            settings,
            environment_map: None,
            reflection_probes: Vec::new(),
            environment_brdf_lut: Arc::new(build_environment_brdf_lut(32)),
        }
    }
}

#[derive(Clone, Debug)]
struct EnvironmentBrdfLut {
    size: u32,
    values: Vec<[f32; 2]>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TextureWrap {
    Repeat,
    ClampToEdge,
    MirroredRepeat,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TextureFilter {
    Nearest,
    Linear,
}

#[derive(Clone, Copy, Debug)]
struct BvhNode {
    bounds_min: [f32; 3],
    bounds_max: [f32; 3],
    first: usize,
    len: usize,
    left: Option<usize>,
    right: Option<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TriangleIntersectionMode {
    VisibleSurface,
    RefractiveBoundary,
}

#[derive(Clone, Copy, Debug)]
struct Ray {
    origin: [f32; 3],
    dir: [f32; 3],
}

#[derive(Clone, Copy, Debug)]
struct Hit {
    t: f32,
    entity_u32: u32,
    #[allow(dead_code)]
    color_rgb: [u8; 3],
    color_linear_rgb: [f32; 3],
    emission_rgb: [f32; 3],
    alpha: f32,
    transmission_filter_rgb: [f32; 3],
    diffuse_transmission: f32,
    diffuse_transmission_color: [f32; 3],
    dispersion: f32,
    volume_thickness_m: f32,
    volume_attenuation_distance_m: f32,
    volume_attenuation_color: [f32; 3],
    occlusion: f32,
    metallic: f32,
    roughness: f32,
    ior: f32,
    clearcoat_factor: f32,
    clearcoat_roughness: f32,
    clearcoat_normal: [f32; 3],
    sheen_color: [f32; 3],
    sheen_roughness: f32,
    anisotropy_strength: f32,
    anisotropy_direction: [f32; 3],
    iridescence_factor: f32,
    iridescence_thickness_nm: f32,
    iridescence_ior: f32,
    specular_factor: f32,
    specular_color: [f32; 3],
    unlit: bool,
    point: [f32; 3],
    normal: [f32; 3],
    view_dir: [f32; 3],
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct TransmissionMedium {
    entity_u32: u32,
    ior: f32,
    dispersion: f32,
    volume_attenuation_distance_m: f32,
    volume_attenuation_color: [f32; 3],
}

impl TransmissionMedium {
    fn from_hit(hit: &Hit) -> Self {
        Self {
            entity_u32: hit.entity_u32,
            ior: hit.ior.max(1.0),
            dispersion: hit.dispersion.max(0.0),
            volume_attenuation_distance_m: hit.volume_attenuation_distance_m,
            volume_attenuation_color: hit.volume_attenuation_color,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct TriangleHit {
    t: f32,
    color_rgb: [u8; 3],
    color_linear_rgb: [f32; 3],
    emission_rgb: [f32; 3],
    alpha: f32,
    transmission_filter_rgb: [f32; 3],
    diffuse_transmission: f32,
    diffuse_transmission_color: [f32; 3],
    dispersion: f32,
    volume_thickness_m: f32,
    volume_attenuation_distance_m: f32,
    volume_attenuation_color: [f32; 3],
    occlusion: f32,
    metallic: f32,
    roughness: f32,
    ior: f32,
    clearcoat_factor: f32,
    clearcoat_roughness: f32,
    clearcoat_normal: [f32; 3],
    sheen_color: [f32; 3],
    sheen_roughness: f32,
    anisotropy_strength: f32,
    anisotropy_direction: [f32; 3],
    iridescence_factor: f32,
    iridescence_thickness_nm: f32,
    iridescence_ior: f32,
    specular_factor: f32,
    specular_color: [f32; 3],
    unlit: bool,
    normal: [f32; 3],
}

#[derive(Clone, Debug)]
struct MeshData {
    triangles: Vec<Triangle>,
    textures: Vec<TextureImage>,
    lights: Vec<RenderLight>,
}

struct GltfLoadContext<'a> {
    document: &'a gltf::Document,
    node_world_transforms: BTreeMap<usize, [[f32; 4]; 4]>,
    animation_overrides: BTreeMap<usize, AnimatedNodeTransform>,
    buffers: &'a [gltf::buffer::Data],
    material_variant: Option<&'a str>,
    material_variant_names: BTreeMap<usize, String>,
}

struct SkinContext {
    joint_matrices: Vec<[[f32; 4]; 4]>,
}

#[derive(Clone, Debug, Default)]
struct AnimatedNodeTransform {
    translation: Option<[f32; 3]>,
    rotation: Option<[f32; 4]>,
    scale: Option<[f32; 3]>,
    weights: Option<Vec<f32>>,
}

#[derive(Clone, Copy, Debug)]
struct CubicVec3Keyframe {
    in_tangent: [f32; 3],
    value: [f32; 3],
    out_tangent: [f32; 3],
}

#[derive(Clone, Copy, Debug)]
struct CubicQuatKeyframe {
    in_tangent: [f32; 4],
    value: [f32; 4],
    out_tangent: [f32; 4],
}

#[derive(Clone, Copy, Debug)]
struct CubicMorphWeightsKeyframe<'a> {
    in_tangent: &'a [f32],
    value: &'a [f32],
    out_tangent: &'a [f32],
}

impl ObjectBvh {
    fn from_objects(objects: &[RenderObject]) -> Self {
        let mut bounded = Vec::new();
        let mut unbounded_indices = Vec::new();
        for (object_index, object) in objects.iter().enumerate() {
            let Some((bounds_min, bounds_max)) = object.bounds else {
                unbounded_indices.push(object_index);
                continue;
            };
            if !bounds_are_finite((bounds_min, bounds_max)) {
                unbounded_indices.push(object_index);
                continue;
            }
            bounded.push(ObjectBvhPrimitive {
                object_index,
                bounds_min,
                bounds_max,
                centroid: scale(add(bounds_min, bounds_max), 0.5),
            });
        }

        let mut nodes = Vec::new();
        let root = build_object_bvh_node(&mut nodes, &mut bounded);
        Self {
            nodes,
            root,
            unbounded_indices,
        }
    }

    fn for_each_candidate<F>(&self, ray: &Ray, mut visit: F)
    where
        F: FnMut(usize),
    {
        for object_index in &self.unbounded_indices {
            visit(*object_index);
        }
        let Some(root) = self.root else {
            return;
        };
        let mut stack = vec![root];
        while let Some(node_index) = stack.pop() {
            let node = &self.nodes[node_index];
            if intersect_aabb(ray, node.bounds_min, node.bounds_max).is_none() {
                continue;
            }
            for object_index in &node.object_indices {
                visit(*object_index);
            }
            if let Some(left) = node.left {
                stack.push(left);
            }
            if let Some(right) = node.right {
                stack.push(right);
            }
        }
    }
}

impl<'a> SceneIntersector<'a> {
    fn new(objects: &'a [RenderObject], object_bvh: &'a ObjectBvh) -> Self {
        Self {
            objects,
            object_bvh,
        }
    }

    fn hits_with_mode(&self, ray: &Ray, triangle_mode: TriangleIntersectionMode) -> Vec<Hit> {
        let mut hits = Vec::new();
        self.object_bvh.for_each_candidate(ray, |object_index| {
            if let Some(object) = self.objects.get(object_index) {
                hits.extend(intersect_object_hits_with_mode(ray, object, triangle_mode));
            }
        });
        hits
    }

    fn hits(&self, ray: &Ray) -> Vec<Hit> {
        self.hits_with_mode(ray, TriangleIntersectionMode::VisibleSurface)
    }

    fn closest_hit_with_mode(
        &self,
        ray: &Ray,
        triangle_mode: TriangleIntersectionMode,
    ) -> Option<Hit> {
        let mut nearest = None;
        self.object_bvh.for_each_candidate(ray, |object_index| {
            let Some(object) = self.objects.get(object_index) else {
                return;
            };
            let Some(hit) = intersect_object_closest_hit_with_mode(ray, object, triangle_mode)
            else {
                return;
            };
            if nearest
                .as_ref()
                .is_none_or(|nearest_hit: &Hit| hit.t < nearest_hit.t)
            {
                nearest = Some(hit);
            }
        });
        nearest
    }

    fn closest_hit(&self, ray: &Ray) -> Option<Hit> {
        self.closest_hit_with_mode(ray, TriangleIntersectionMode::VisibleSurface)
    }
}

impl NativeRenderer {
    pub fn new() -> Self {
        Self {
            far_m: 10.0,
            mesh_cache: Arc::new(Mutex::new(BTreeMap::new())),
            environment_cache: Arc::new(Mutex::new(BTreeMap::new())),
            environment_brdf_lut: Arc::new(build_environment_brdf_lut(32)),
        }
    }

    pub fn render(
        &self,
        scene: &SceneGraph,
        state: Option<RobotDreamsSnapshot>,
        request: &ObservationRequest,
    ) -> Result<RenderOutput, String> {
        let render_settings = render_settings_for_scene(scene, request);
        let material_variant = render_settings
            .as_ref()
            .and_then(|settings| settings.gltf_material_variant.as_deref());
        let prepared = self.prepare_scene(scene, state, request, material_variant)?;
        let temporal_samples = if uses_temporal_samples(request) {
            let animation_time_sec = prepared
                .state
                .as_ref()
                .map(|state| state.clock_sec as f32)
                .unwrap_or(0.0);
            let entity_ids = entity_u32_map(scene);
            self.shutter_samples_for_scene(
                scene,
                &entity_ids,
                &prepared,
                animation_time_sec,
                request.resolution,
                request.shutter_policy,
                material_variant,
            )?
        } else {
            Vec::new()
        };
        self.render_prepared(&prepared, &temporal_samples, request, render_settings)
    }

    pub fn render_scene_samples(
        &self,
        samples: &[SceneGraphSample],
        request: &ObservationRequest,
    ) -> Result<RenderOutput, String> {
        let primary_sample = samples
            .last()
            .ok_or_else(|| "render_scene_samples requires at least one sample".to_string())?;
        let render_settings = render_settings_for_scene(&primary_sample.scene, request);
        let material_variant = render_settings
            .as_ref()
            .and_then(|settings| settings.gltf_material_variant.as_deref());
        let mut prepared = Vec::new();
        for sample in samples {
            prepared.push(self.prepare_scene(
                &sample.scene,
                sample.state.clone(),
                request,
                material_variant,
            )?);
        }
        let primary = prepared
            .last()
            .ok_or_else(|| "render_scene_samples requires at least one sample".to_string())?;
        let temporal_samples = if uses_temporal_samples(request) {
            self.temporal_samples_for_scene_samples(samples, request, material_variant)?
        } else {
            Vec::new()
        };
        self.render_prepared(primary, &temporal_samples, request, render_settings)
    }

    fn prepare_scene(
        &self,
        scene: &SceneGraph,
        state: Option<RobotDreamsSnapshot>,
        request: &ObservationRequest,
        material_variant: Option<&str>,
    ) -> Result<PreparedScene, String> {
        let camera = if request.views.iter().any(requires_camera) {
            let camera_id = request
                .camera_id
                .as_deref()
                .ok_or_else(|| "visual observation views require camera_id".to_string())?;
            Some(
                find_camera(&scene.root, camera_id)
                    .ok_or_else(|| format!("camera '{camera_id}' not found in scene graph"))?,
            )
        } else {
            None
        };
        let entity_ids = entity_u32_map(scene);
        let animation_time_sec = state
            .as_ref()
            .map(|state| state.clock_sec as f32)
            .unwrap_or(0.0);
        let objects = render_objects(
            &scene.root,
            Transform::default(),
            &entity_ids,
            &self.mesh_cache,
            animation_time_sec,
            material_variant,
        )?;
        let scene_lights = render_lights(&scene.root, Transform::default());
        let lights = lights_for_objects(&scene_lights, &objects);
        let object_bvh = Arc::new(ObjectBvh::from_objects(&objects));
        Ok(PreparedScene {
            camera,
            objects,
            object_bvh,
            lights,
            scene_lights,
            state,
            entities: scene.entities.clone(),
        })
    }

    fn render_prepared(
        &self,
        prepared: &PreparedScene,
        temporal_samples: &[RenderSceneSample],
        request: &ObservationRequest,
        render_settings: Option<RenderSettings>,
    ) -> Result<RenderOutput, String> {
        let resolution = request.resolution;
        let mut frames = Vec::new();

        for view in &request.views {
            match view {
                ObservationView::State => {}
                ObservationView::DebugRgb => {
                    frames.push(self.render_debug_rgb(
                        resolution,
                        temporal_samples,
                        request.shutter_policy,
                        render_settings.clone().unwrap_or_default(),
                    )?);
                }
                ObservationView::Depth => {
                    let camera = prepared.camera.as_ref().expect("camera checked");
                    frames.push(if temporal_samples.is_empty() {
                        self.render_depth(
                            camera,
                            resolution,
                            &prepared.objects,
                            prepared.object_bvh.as_ref(),
                        )
                    } else {
                        self.render_depth_shutter(
                            resolution,
                            temporal_samples,
                            request.shutter_policy,
                        )
                    });
                }
                ObservationView::Segmentation => {
                    let camera = prepared.camera.as_ref().expect("camera checked");
                    frames.push(if temporal_samples.is_empty() {
                        self.render_segmentation(
                            camera,
                            resolution,
                            &prepared.objects,
                            prepared.object_bvh.as_ref(),
                            request.segmentation_policy,
                        )
                    } else {
                        self.render_segmentation_shutter(
                            resolution,
                            temporal_samples,
                            request.shutter_policy,
                            request.segmentation_policy,
                        )
                    });
                }
                ObservationView::Normal => {
                    let camera = prepared.camera.as_ref().expect("camera checked");
                    frames.push(if temporal_samples.is_empty() {
                        self.render_normal(
                            camera,
                            resolution,
                            &prepared.objects,
                            prepared.object_bvh.as_ref(),
                        )?
                    } else {
                        self.render_normal_shutter(
                            resolution,
                            temporal_samples,
                            request.shutter_policy,
                        )?
                    });
                }
                ObservationView::Albedo => {
                    let camera = prepared.camera.as_ref().expect("camera checked");
                    frames.push(if temporal_samples.is_empty() {
                        self.render_albedo(
                            camera,
                            resolution,
                            &prepared.objects,
                            prepared.object_bvh.as_ref(),
                        )?
                    } else {
                        self.render_albedo_shutter(
                            resolution,
                            temporal_samples,
                            request.shutter_policy,
                        )?
                    });
                }
                ObservationView::MaterialProperties => {
                    let camera = prepared.camera.as_ref().expect("camera checked");
                    frames.push(if temporal_samples.is_empty() {
                        self.render_material_properties(
                            camera,
                            resolution,
                            &prepared.objects,
                            prepared.object_bvh.as_ref(),
                        )?
                    } else {
                        self.render_material_properties_shutter(
                            resolution,
                            temporal_samples,
                            request.shutter_policy,
                        )?
                    });
                }
                ObservationView::WorldPosition => {
                    let camera = prepared.camera.as_ref().expect("camera checked");
                    frames.push(if temporal_samples.is_empty() {
                        self.render_world_position(
                            camera,
                            resolution,
                            &prepared.objects,
                            prepared.object_bvh.as_ref(),
                        )
                    } else {
                        self.render_world_position_shutter(
                            resolution,
                            temporal_samples,
                            request.shutter_policy,
                        )
                    });
                }
            }
        }

        Ok(RenderOutput {
            metadata: ObservationMetadata {
                timestamp_sec: prepared
                    .state
                    .as_ref()
                    .map(|state| state.clock_sec)
                    .unwrap_or(0.0),
                camera_id: prepared.camera.as_ref().map(|camera| camera.id.clone()),
                camera_pose: prepared.camera.as_ref().map(|camera| camera.transform),
                camera_projection: prepared.camera.as_ref().map(|camera| camera.projection),
                camera_intrinsics: prepared
                    .camera
                    .as_ref()
                    .map(|camera| camera_intrinsics_for_resolution(camera, resolution)),
                camera_distortion: prepared
                    .camera
                    .as_ref()
                    .and_then(|camera| camera.distortion),
                camera_extrinsics_matrix: prepared
                    .camera
                    .as_ref()
                    .map(|camera| transform_matrix(camera.transform)),
                depth_range_m: prepared
                    .camera
                    .as_ref()
                    .and_then(|camera| camera.depth_range_m),
                sensor_effects: prepared
                    .camera
                    .as_ref()
                    .and_then(|camera| camera.sensor_effects),
                resolution,
                views: request.views.clone(),
                segmentation_policy: request.segmentation_policy,
                shutter_policy: request.shutter_policy,
                render_settings,
                entities: prepared.entities.clone(),
            },
            frames,
            state: prepared.state.clone(),
        })
    }

    fn render_debug_rgb(
        &self,
        resolution: [u32; 2],
        samples: &[RenderSceneSample],
        shutter_policy: Option<ShutterPolicy>,
        settings: RenderSettings,
    ) -> Result<FrameBuffer, String> {
        let settings = self.prepare_render_settings(settings)?;
        let width = resolution[0];
        let height = resolution[1];
        let mut rgb = vec![8_u8; (width * height * 3) as usize];
        rgb.par_chunks_mut(3)
            .enumerate()
            .for_each(|(index, pixel)| {
                let x = (index as u32) % width;
                let y = (index as u32) / width;
                if y < height {
                    let color = self.trace_debug_rgb_shutter(
                        resolution,
                        x,
                        y,
                        samples,
                        shutter_policy,
                        &settings,
                    );
                    let color = apply_rgb_sensor_effects(
                        color,
                        samples
                            .last()
                            .and_then(|sample| sample.camera.clone())
                            .and_then(|camera| camera.sensor_effects),
                        x,
                        y,
                    );
                    pixel.copy_from_slice(&color);
                }
            });
        Ok(FrameBuffer {
            kind: FrameKind::DebugRgb,
            width,
            height,
            bytes: encode_png_rgb(width, height, &rgb)?,
        })
    }

    fn prepare_render_settings(
        &self,
        settings: RenderSettings,
    ) -> Result<PreparedRenderSettings, String> {
        let environment_map = settings
            .environment
            .as_ref()
            .and_then(|environment| environment.map.as_ref())
            .map(|path| self.load_environment_map(path))
            .transpose()?;
        let mut reflection_probes = Vec::new();
        if let Some(probe) = settings.reflection_probe.as_ref() {
            reflection_probes.push(PreparedReflectionProbe {
                settings: probe.clone(),
                texture: self.load_environment_map(&probe.map)?,
            });
        }
        for probe in &settings.reflection_probes {
            reflection_probes.push(PreparedReflectionProbe {
                settings: probe.clone(),
                texture: self.load_environment_map(&probe.map)?,
            });
        }
        Ok(PreparedRenderSettings {
            settings,
            environment_map,
            reflection_probes,
            environment_brdf_lut: self.environment_brdf_lut.clone(),
        })
    }

    fn load_environment_map(&self, path: &str) -> Result<Arc<EnvironmentTexture>, String> {
        if let Some(texture) = self.environment_cache.lock().unwrap().get(path).cloned() {
            return Ok(texture);
        }
        let texture = Arc::new(load_environment_texture(Path::new(path))?);
        self.environment_cache
            .lock()
            .unwrap()
            .insert(path.to_string(), texture.clone());
        Ok(texture)
    }

    fn shutter_samples_for_scene(
        &self,
        scene: &SceneGraph,
        entity_ids: &BTreeMap<EntityId, u32>,
        center: &PreparedScene,
        center_time_sec: f32,
        resolution: [u32; 2],
        policy: Option<ShutterPolicy>,
        material_variant: Option<&str>,
    ) -> Result<Vec<RenderSceneSample>, String> {
        let sample_times = shutter_render_sample_times(center_time_sec, resolution[1], policy);
        sample_times
            .into_iter()
            .map(|sample_time_sec| {
                if (sample_time_sec - center_time_sec).abs() <= f32::EPSILON {
                    return Ok(RenderSceneSample {
                        timestamp_sec: center_time_sec,
                        camera: center.camera.clone(),
                        objects: center.objects.to_vec(),
                        object_bvh: center.object_bvh.clone(),
                        lights: center.lights.to_vec(),
                    });
                }
                let objects = render_objects(
                    &scene.root,
                    Transform::default(),
                    entity_ids,
                    &self.mesh_cache,
                    sample_time_sec,
                    material_variant,
                )?;
                let lights = lights_for_objects(&center.scene_lights, &objects);
                let camera = center
                    .camera
                    .as_ref()
                    .and_then(|camera| find_camera(&scene.root, &camera.id))
                    .or_else(|| center.camera.clone());
                Ok(RenderSceneSample {
                    timestamp_sec: sample_time_sec,
                    camera,
                    object_bvh: Arc::new(ObjectBvh::from_objects(&objects)),
                    objects,
                    lights,
                })
            })
            .collect()
    }

    fn temporal_samples_for_scene_samples(
        &self,
        samples: &[SceneGraphSample],
        request: &ObservationRequest,
        material_variant: Option<&str>,
    ) -> Result<Vec<RenderSceneSample>, String> {
        if samples.is_empty() {
            return Ok(Vec::new());
        }
        let frame_time_sec = scene_graph_sample_time(samples.last().expect("checked samples"));
        let sample_times = request
            .shutter_policy
            .map(|policy| {
                shutter_render_sample_times(frame_time_sec, request.resolution[1], Some(policy))
            })
            .unwrap_or_else(|| {
                samples
                    .iter()
                    .map(scene_graph_sample_time)
                    .collect::<Vec<_>>()
            });
        sample_times
            .into_iter()
            .map(|sample_time_sec| {
                let sample = interpolate_scene_graph_sample(samples, sample_time_sec);
                let prepared =
                    self.prepare_scene(&sample.scene, sample.state, request, material_variant)?;
                Ok(RenderSceneSample {
                    timestamp_sec: sample_time_sec,
                    camera: prepared.camera,
                    object_bvh: prepared.object_bvh,
                    objects: prepared.objects,
                    lights: prepared.lights,
                })
            })
            .collect()
    }

    fn trace_debug_rgb_shutter(
        &self,
        resolution: [u32; 2],
        x: u32,
        y: u32,
        samples: &[RenderSceneSample],
        shutter_policy: Option<ShutterPolicy>,
        settings: &PreparedRenderSettings,
    ) -> [u8; 3] {
        if samples.is_empty() {
            return settings.settings.background_rgb;
        }
        if samples.len() == 1 {
            let sample = &samples[0];
            let Some(camera) = sample.camera.as_ref() else {
                return settings.settings.background_rgb;
            };
            let intersector = SceneIntersector::new(&sample.objects, sample.object_bvh.as_ref());
            return self.trace_debug_rgb_sampled_with_intersector(
                camera,
                resolution,
                x,
                y,
                &intersector,
                &sample.lights,
                settings,
            );
        }

        let sample_indexes =
            shutter_sample_indexes_for_pixel(samples, resolution, y, shutter_policy);
        let mut sum = [0_u32; 3];
        for index in sample_indexes.iter().copied() {
            let sample = &samples[index];
            let Some(camera) = sample.camera.as_ref() else {
                continue;
            };
            let intersector = SceneIntersector::new(&sample.objects, sample.object_bvh.as_ref());
            let color = self.trace_debug_rgb_sampled_with_intersector(
                camera,
                resolution,
                x,
                y,
                &intersector,
                &sample.lights,
                settings,
            );
            for channel in 0..3 {
                sum[channel] += u32::from(color[channel]);
            }
        }
        let count = sample_indexes.len() as u32;
        [
            ((sum[0] + count / 2) / count) as u8,
            ((sum[1] + count / 2) / count) as u8,
            ((sum[2] + count / 2) / count) as u8,
        ]
    }

    fn render_depth(
        &self,
        camera: &CameraSpec,
        resolution: [u32; 2],
        objects: &[RenderObject],
        object_bvh: &ObjectBvh,
    ) -> FrameBuffer {
        let width = resolution[0];
        let height = resolution[1];
        let mut bytes = vec![0_u8; (width * height * 4) as usize];
        bytes
            .par_chunks_mut(4)
            .enumerate()
            .for_each(|(index, pixel)| {
                let x = (index as u32) % width;
                let y = (index as u32) / width;
                let intersector = SceneIntersector::new(objects, object_bvh);
                let depth = self
                    .trace_with_intersector(camera, resolution, x, y, &intersector)
                    .map(|hit| hit.t)
                    .unwrap_or(f32::INFINITY);
                let depth = apply_depth_sensor_effects(depth, camera.sensor_effects, x, y);
                pixel.copy_from_slice(&depth.to_le_bytes());
            });
        FrameBuffer {
            kind: FrameKind::Depth,
            width,
            height,
            bytes,
        }
    }

    fn render_depth_shutter(
        &self,
        resolution: [u32; 2],
        samples: &[RenderSceneSample],
        shutter_policy: Option<ShutterPolicy>,
    ) -> FrameBuffer {
        let width = resolution[0];
        let height = resolution[1];
        let sensor_effects = samples
            .last()
            .and_then(|sample| sample.camera.as_ref())
            .and_then(|camera| camera.sensor_effects);
        let mut bytes = vec![0_u8; (width * height * 4) as usize];
        bytes
            .par_chunks_mut(4)
            .enumerate()
            .for_each(|(index, pixel)| {
                let x = (index as u32) % width;
                let y = (index as u32) / width;
                let depth = self.trace_depth_shutter(resolution, x, y, samples, shutter_policy);
                let depth = apply_depth_sensor_effects(depth, sensor_effects, x, y);
                pixel.copy_from_slice(&depth.to_le_bytes());
            });
        FrameBuffer {
            kind: FrameKind::Depth,
            width,
            height,
            bytes,
        }
    }

    fn render_segmentation(
        &self,
        camera: &CameraSpec,
        resolution: [u32; 2],
        objects: &[RenderObject],
        object_bvh: &ObjectBvh,
        policy: Option<SegmentationPolicy>,
    ) -> FrameBuffer {
        let width = resolution[0];
        let height = resolution[1];
        let mut bytes = vec![0_u8; (width * height * 4) as usize];
        bytes
            .par_chunks_mut(4)
            .enumerate()
            .for_each(|(index, pixel)| {
                let x = (index as u32) % width;
                let y = (index as u32) / width;
                let intersector = SceneIntersector::new(objects, object_bvh);
                let id = self
                    .trace_segmentation_with_intersector(
                        camera,
                        resolution,
                        x,
                        y,
                        &intersector,
                        policy,
                    )
                    .map(|hit| hit.entity_u32)
                    .unwrap_or(0);
                pixel.copy_from_slice(&id.to_le_bytes());
            });
        FrameBuffer {
            kind: FrameKind::Segmentation,
            width,
            height,
            bytes,
        }
    }

    fn render_segmentation_shutter(
        &self,
        resolution: [u32; 2],
        samples: &[RenderSceneSample],
        shutter_policy: Option<ShutterPolicy>,
        policy: Option<SegmentationPolicy>,
    ) -> FrameBuffer {
        let width = resolution[0];
        let height = resolution[1];
        let mut bytes = vec![0_u8; (width * height * 4) as usize];
        bytes
            .par_chunks_mut(4)
            .enumerate()
            .for_each(|(index, pixel)| {
                let x = (index as u32) % width;
                let y = (index as u32) / width;
                let id = self.trace_segmentation_shutter(
                    resolution,
                    x,
                    y,
                    samples,
                    shutter_policy,
                    policy,
                );
                pixel.copy_from_slice(&id.to_le_bytes());
            });
        FrameBuffer {
            kind: FrameKind::Segmentation,
            width,
            height,
            bytes,
        }
    }

    fn render_normal(
        &self,
        camera: &CameraSpec,
        resolution: [u32; 2],
        objects: &[RenderObject],
        object_bvh: &ObjectBvh,
    ) -> Result<FrameBuffer, String> {
        let width = resolution[0];
        let height = resolution[1];
        let mut rgb = vec![0_u8; (width * height * 3) as usize];
        rgb.par_chunks_mut(3)
            .enumerate()
            .for_each(|(index, pixel)| {
                let x = (index as u32) % width;
                let y = (index as u32) / width;
                let intersector = SceneIntersector::new(objects, object_bvh);
                let normal = self
                    .trace_with_intersector(camera, resolution, x, y, &intersector)
                    .map(|hit| hit.normal);
                pixel.copy_from_slice(&encode_normal_rgb(normal));
            });
        Ok(FrameBuffer {
            kind: FrameKind::Normal,
            width,
            height,
            bytes: encode_png_rgb(width, height, &rgb)?,
        })
    }

    fn render_normal_shutter(
        &self,
        resolution: [u32; 2],
        samples: &[RenderSceneSample],
        shutter_policy: Option<ShutterPolicy>,
    ) -> Result<FrameBuffer, String> {
        let width = resolution[0];
        let height = resolution[1];
        let mut rgb = vec![0_u8; (width * height * 3) as usize];
        rgb.par_chunks_mut(3)
            .enumerate()
            .for_each(|(index, pixel)| {
                let x = (index as u32) % width;
                let y = (index as u32) / width;
                let normal = self.trace_normal_shutter(resolution, x, y, samples, shutter_policy);
                pixel.copy_from_slice(&encode_normal_rgb(normal));
            });
        Ok(FrameBuffer {
            kind: FrameKind::Normal,
            width,
            height,
            bytes: encode_png_rgb(width, height, &rgb)?,
        })
    }

    fn render_albedo(
        &self,
        camera: &CameraSpec,
        resolution: [u32; 2],
        objects: &[RenderObject],
        object_bvh: &ObjectBvh,
    ) -> Result<FrameBuffer, String> {
        let width = resolution[0];
        let height = resolution[1];
        let mut rgb = vec![0_u8; (width * height * 3) as usize];
        rgb.par_chunks_mut(3)
            .enumerate()
            .for_each(|(index, pixel)| {
                let x = (index as u32) % width;
                let y = (index as u32) / width;
                let intersector = SceneIntersector::new(objects, object_bvh);
                let albedo = self
                    .trace_with_intersector(camera, resolution, x, y, &intersector)
                    .map(|hit| hit.color_rgb)
                    .unwrap_or([0, 0, 0]);
                pixel.copy_from_slice(&albedo);
            });
        Ok(FrameBuffer {
            kind: FrameKind::Albedo,
            width,
            height,
            bytes: encode_png_rgb(width, height, &rgb)?,
        })
    }

    fn render_albedo_shutter(
        &self,
        resolution: [u32; 2],
        samples: &[RenderSceneSample],
        shutter_policy: Option<ShutterPolicy>,
    ) -> Result<FrameBuffer, String> {
        let width = resolution[0];
        let height = resolution[1];
        let mut rgb = vec![0_u8; (width * height * 3) as usize];
        rgb.par_chunks_mut(3)
            .enumerate()
            .for_each(|(index, pixel)| {
                let x = (index as u32) % width;
                let y = (index as u32) / width;
                pixel.copy_from_slice(&self.trace_albedo_shutter(
                    resolution,
                    x,
                    y,
                    samples,
                    shutter_policy,
                ));
            });
        Ok(FrameBuffer {
            kind: FrameKind::Albedo,
            width,
            height,
            bytes: encode_png_rgb(width, height, &rgb)?,
        })
    }

    fn render_material_properties(
        &self,
        camera: &CameraSpec,
        resolution: [u32; 2],
        objects: &[RenderObject],
        object_bvh: &ObjectBvh,
    ) -> Result<FrameBuffer, String> {
        let width = resolution[0];
        let height = resolution[1];
        let mut rgb = vec![0_u8; (width * height * 3) as usize];
        rgb.par_chunks_mut(3)
            .enumerate()
            .for_each(|(index, pixel)| {
                let x = (index as u32) % width;
                let y = (index as u32) / width;
                let intersector = SceneIntersector::new(objects, object_bvh);
                let material = self
                    .trace_with_intersector(camera, resolution, x, y, &intersector)
                    .map(|hit| encode_material_properties_rgb(&hit))
                    .unwrap_or([0, 0, 0]);
                pixel.copy_from_slice(&material);
            });
        Ok(FrameBuffer {
            kind: FrameKind::MaterialProperties,
            width,
            height,
            bytes: encode_png_rgb(width, height, &rgb)?,
        })
    }

    fn render_material_properties_shutter(
        &self,
        resolution: [u32; 2],
        samples: &[RenderSceneSample],
        shutter_policy: Option<ShutterPolicy>,
    ) -> Result<FrameBuffer, String> {
        let width = resolution[0];
        let height = resolution[1];
        let mut rgb = vec![0_u8; (width * height * 3) as usize];
        rgb.par_chunks_mut(3)
            .enumerate()
            .for_each(|(index, pixel)| {
                let x = (index as u32) % width;
                let y = (index as u32) / width;
                pixel.copy_from_slice(&self.trace_material_properties_shutter(
                    resolution,
                    x,
                    y,
                    samples,
                    shutter_policy,
                ));
            });
        Ok(FrameBuffer {
            kind: FrameKind::MaterialProperties,
            width,
            height,
            bytes: encode_png_rgb(width, height, &rgb)?,
        })
    }

    fn render_world_position(
        &self,
        camera: &CameraSpec,
        resolution: [u32; 2],
        objects: &[RenderObject],
        object_bvh: &ObjectBvh,
    ) -> FrameBuffer {
        let width = resolution[0];
        let height = resolution[1];
        let mut bytes = vec![0_u8; (width * height * 12) as usize];
        bytes
            .par_chunks_mut(12)
            .enumerate()
            .for_each(|(index, pixel)| {
                let x = (index as u32) % width;
                let y = (index as u32) / width;
                let intersector = SceneIntersector::new(objects, object_bvh);
                let position = self
                    .trace_with_intersector(camera, resolution, x, y, &intersector)
                    .map(|hit| hit.point)
                    .unwrap_or([f32::NAN, f32::NAN, f32::NAN]);
                write_vec3_f32le(pixel, position);
            });
        FrameBuffer {
            kind: FrameKind::WorldPosition,
            width,
            height,
            bytes,
        }
    }

    fn render_world_position_shutter(
        &self,
        resolution: [u32; 2],
        samples: &[RenderSceneSample],
        shutter_policy: Option<ShutterPolicy>,
    ) -> FrameBuffer {
        let width = resolution[0];
        let height = resolution[1];
        let mut bytes = vec![0_u8; (width * height * 12) as usize];
        bytes
            .par_chunks_mut(12)
            .enumerate()
            .for_each(|(index, pixel)| {
                let x = (index as u32) % width;
                let y = (index as u32) / width;
                write_vec3_f32le(
                    pixel,
                    self.trace_world_position_shutter(resolution, x, y, samples, shutter_policy),
                );
            });
        FrameBuffer {
            kind: FrameKind::WorldPosition,
            width,
            height,
            bytes,
        }
    }

    #[allow(dead_code)]
    fn trace_segmentation(
        &self,
        camera: &CameraSpec,
        resolution: [u32; 2],
        x: u32,
        y: u32,
        objects: &[RenderObject],
        policy: Option<SegmentationPolicy>,
    ) -> Option<Hit> {
        let object_bvh = ObjectBvh::from_objects(objects);
        let intersector = SceneIntersector::new(objects, &object_bvh);
        self.trace_segmentation_with_intersector(camera, resolution, x, y, &intersector, policy)
    }

    fn trace_segmentation_with_intersector(
        &self,
        camera: &CameraSpec,
        resolution: [u32; 2],
        x: u32,
        y: u32,
        intersector: &SceneIntersector<'_>,
        policy: Option<SegmentationPolicy>,
    ) -> Option<Hit> {
        let ray = camera_ray(camera, resolution, x, y);
        let min_alpha = policy.unwrap_or_default().min_alpha.clamp(0.0, 1.0);
        intersector
            .hits(&ray)
            .into_iter()
            .filter(|hit| hit.t > 0.0 && hit.t <= self.far_m)
            .filter(|hit| depth_range_contains(camera.depth_range_m, hit.t))
            .filter(|hit| hit.alpha >= min_alpha)
            .min_by(|left, right| left.t.total_cmp(&right.t))
    }

    fn trace_depth_shutter(
        &self,
        resolution: [u32; 2],
        x: u32,
        y: u32,
        samples: &[RenderSceneSample],
        shutter_policy: Option<ShutterPolicy>,
    ) -> f32 {
        if samples.is_empty() {
            return f32::INFINITY;
        }

        let sample_indexes =
            shutter_sample_indexes_for_pixel(samples, resolution, y, shutter_policy);
        let mut sum = 0.0_f32;
        let mut count = 0_u32;
        for index in sample_indexes {
            let sample = &samples[index];
            let Some(camera) = sample.camera.as_ref() else {
                continue;
            };
            let intersector = SceneIntersector::new(&sample.objects, sample.object_bvh.as_ref());
            let depth = self
                .trace_with_intersector(camera, resolution, x, y, &intersector)
                .map(|hit| hit.t)
                .unwrap_or(f32::INFINITY);
            if depth.is_finite() {
                sum += depth;
                count += 1;
            }
        }
        if count == 0 {
            f32::INFINITY
        } else {
            sum / count as f32
        }
    }

    fn trace_segmentation_shutter(
        &self,
        resolution: [u32; 2],
        x: u32,
        y: u32,
        samples: &[RenderSceneSample],
        shutter_policy: Option<ShutterPolicy>,
        policy: Option<SegmentationPolicy>,
    ) -> u32 {
        if samples.is_empty() {
            return 0;
        }

        let sample_indexes =
            shutter_sample_indexes_for_pixel(samples, resolution, y, shutter_policy);
        let mut counts = BTreeMap::<u32, (u32, usize)>::new();
        for (order, index) in sample_indexes.iter().copied().enumerate() {
            let sample = &samples[index];
            let id = sample
                .camera
                .as_ref()
                .and_then(|camera| {
                    let intersector =
                        SceneIntersector::new(&sample.objects, sample.object_bvh.as_ref());
                    self.trace_segmentation_with_intersector(
                        camera,
                        resolution,
                        x,
                        y,
                        &intersector,
                        policy,
                    )
                })
                .map(|hit| hit.entity_u32)
                .unwrap_or(0);
            let entry = counts.entry(id).or_insert((0, order));
            entry.0 += 1;
            entry.1 = order;
        }
        counts
            .into_iter()
            .max_by(|(_, left), (_, right)| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)))
            .map(|(id, _)| id)
            .unwrap_or(0)
    }

    fn trace_normal_shutter(
        &self,
        resolution: [u32; 2],
        x: u32,
        y: u32,
        samples: &[RenderSceneSample],
        shutter_policy: Option<ShutterPolicy>,
    ) -> Option<[f32; 3]> {
        if samples.is_empty() {
            return None;
        }

        let sample_indexes =
            shutter_sample_indexes_for_pixel(samples, resolution, y, shutter_policy);
        let mut sum = [0.0_f32; 3];
        let mut count = 0_u32;
        for index in sample_indexes {
            let sample = &samples[index];
            let Some(camera) = sample.camera.as_ref() else {
                continue;
            };
            let intersector = SceneIntersector::new(&sample.objects, sample.object_bvh.as_ref());
            let Some(hit) = self.trace_with_intersector(camera, resolution, x, y, &intersector)
            else {
                continue;
            };
            for axis in 0..3 {
                sum[axis] += hit.normal[axis];
            }
            count += 1;
        }
        (count > 0).then(|| normalize(scale(sum, 1.0 / count as f32)))
    }

    fn trace_albedo_shutter(
        &self,
        resolution: [u32; 2],
        x: u32,
        y: u32,
        samples: &[RenderSceneSample],
        shutter_policy: Option<ShutterPolicy>,
    ) -> [u8; 3] {
        if samples.is_empty() {
            return [0, 0, 0];
        }

        let sample_indexes =
            shutter_sample_indexes_for_pixel(samples, resolution, y, shutter_policy);
        let mut sum = [0_u32; 3];
        let mut count = 0_u32;
        for index in sample_indexes {
            let sample = &samples[index];
            let Some(camera) = sample.camera.as_ref() else {
                continue;
            };
            let intersector = SceneIntersector::new(&sample.objects, sample.object_bvh.as_ref());
            let Some(hit) = self.trace_with_intersector(camera, resolution, x, y, &intersector)
            else {
                continue;
            };
            for channel in 0..3 {
                sum[channel] += u32::from(hit.color_rgb[channel]);
            }
            count += 1;
        }
        if count == 0 {
            [0, 0, 0]
        } else {
            [
                ((sum[0] + count / 2) / count) as u8,
                ((sum[1] + count / 2) / count) as u8,
                ((sum[2] + count / 2) / count) as u8,
            ]
        }
    }

    fn trace_material_properties_shutter(
        &self,
        resolution: [u32; 2],
        x: u32,
        y: u32,
        samples: &[RenderSceneSample],
        shutter_policy: Option<ShutterPolicy>,
    ) -> [u8; 3] {
        if samples.is_empty() {
            return [0, 0, 0];
        }

        let sample_indexes =
            shutter_sample_indexes_for_pixel(samples, resolution, y, shutter_policy);
        let mut sum = [0_u32; 3];
        let mut count = 0_u32;
        for index in sample_indexes {
            let sample = &samples[index];
            let Some(camera) = sample.camera.as_ref() else {
                continue;
            };
            let intersector = SceneIntersector::new(&sample.objects, sample.object_bvh.as_ref());
            let Some(hit) = self.trace_with_intersector(camera, resolution, x, y, &intersector)
            else {
                continue;
            };
            let encoded = encode_material_properties_rgb(&hit);
            for channel in 0..3 {
                sum[channel] += u32::from(encoded[channel]);
            }
            count += 1;
        }
        if count == 0 {
            [0, 0, 0]
        } else {
            [
                ((sum[0] + count / 2) / count) as u8,
                ((sum[1] + count / 2) / count) as u8,
                ((sum[2] + count / 2) / count) as u8,
            ]
        }
    }

    fn trace_world_position_shutter(
        &self,
        resolution: [u32; 2],
        x: u32,
        y: u32,
        samples: &[RenderSceneSample],
        shutter_policy: Option<ShutterPolicy>,
    ) -> [f32; 3] {
        if samples.is_empty() {
            return [f32::NAN, f32::NAN, f32::NAN];
        }

        let sample_indexes =
            shutter_sample_indexes_for_pixel(samples, resolution, y, shutter_policy);
        let mut sum = [0.0_f32; 3];
        let mut count = 0_u32;
        for index in sample_indexes {
            let sample = &samples[index];
            let Some(camera) = sample.camera.as_ref() else {
                continue;
            };
            let intersector = SceneIntersector::new(&sample.objects, sample.object_bvh.as_ref());
            let Some(hit) = self.trace_with_intersector(camera, resolution, x, y, &intersector)
            else {
                continue;
            };
            for axis in 0..3 {
                sum[axis] += hit.point[axis];
            }
            count += 1;
        }
        if count == 0 {
            [f32::NAN, f32::NAN, f32::NAN]
        } else {
            scale(sum, 1.0 / count as f32)
        }
    }

    #[allow(dead_code)]
    fn trace(
        &self,
        camera: &CameraSpec,
        resolution: [u32; 2],
        x: u32,
        y: u32,
        objects: &[RenderObject],
    ) -> Option<Hit> {
        let object_bvh = ObjectBvh::from_objects(objects);
        let intersector = SceneIntersector::new(objects, &object_bvh);
        self.trace_with_intersector(camera, resolution, x, y, &intersector)
    }

    fn trace_with_intersector(
        &self,
        camera: &CameraSpec,
        resolution: [u32; 2],
        x: u32,
        y: u32,
        intersector: &SceneIntersector<'_>,
    ) -> Option<Hit> {
        let ray = camera_ray(camera, resolution, x, y);
        intersector
            .closest_hit(&ray)
            .into_iter()
            .filter(|hit| hit.t > 0.0 && hit.t <= self.far_m)
            .filter(|hit| depth_range_contains(camera.depth_range_m, hit.t))
            .next()
    }

    #[allow(dead_code)]
    fn trace_debug_rgb(
        &self,
        camera: &CameraSpec,
        resolution: [u32; 2],
        x: u32,
        y: u32,
        objects: &[RenderObject],
        lights: &[RenderLight],
        settings: &PreparedRenderSettings,
    ) -> [u8; 3] {
        let object_bvh = ObjectBvh::from_objects(objects);
        let intersector = SceneIntersector::new(objects, &object_bvh);
        self.trace_debug_rgb_at_subpixel_with_intersector(
            camera,
            resolution,
            x,
            y,
            [0.5, 0.5],
            &intersector,
            lights,
            settings,
        )
    }

    #[allow(dead_code)]
    fn trace_debug_rgb_sampled(
        &self,
        camera: &CameraSpec,
        resolution: [u32; 2],
        x: u32,
        y: u32,
        objects: &[RenderObject],
        lights: &[RenderLight],
        settings: &PreparedRenderSettings,
    ) -> [u8; 3] {
        let object_bvh = ObjectBvh::from_objects(objects);
        let intersector = SceneIntersector::new(objects, &object_bvh);
        self.trace_debug_rgb_sampled_with_intersector(
            camera,
            resolution,
            x,
            y,
            &intersector,
            lights,
            settings,
        )
    }

    fn trace_debug_rgb_sampled_with_intersector(
        &self,
        camera: &CameraSpec,
        resolution: [u32; 2],
        x: u32,
        y: u32,
        intersector: &SceneIntersector<'_>,
        lights: &[RenderLight],
        settings: &PreparedRenderSettings,
    ) -> [u8; 3] {
        let sample_count = settings.settings.debug_rgb_samples_per_pixel.clamp(1, 64);
        if sample_count == 1 {
            return self.trace_debug_rgb_at_subpixel_with_intersector(
                camera,
                resolution,
                x,
                y,
                [0.5, 0.5],
                intersector,
                lights,
                settings,
            );
        }

        let mut sum = [0_u32; 3];
        for sample_index in 0..sample_count {
            let offset = subpixel_sample_offset(sample_index, sample_count);
            let color = self.trace_debug_rgb_at_subpixel_with_intersector(
                camera,
                resolution,
                x,
                y,
                offset,
                intersector,
                lights,
                settings,
            );
            for channel in 0..3 {
                sum[channel] += u32::from(color[channel]);
            }
        }
        [
            ((sum[0] + sample_count / 2) / sample_count) as u8,
            ((sum[1] + sample_count / 2) / sample_count) as u8,
            ((sum[2] + sample_count / 2) / sample_count) as u8,
        ]
    }

    #[allow(dead_code)]
    fn trace_debug_rgb_at_subpixel(
        &self,
        camera: &CameraSpec,
        resolution: [u32; 2],
        x: u32,
        y: u32,
        subpixel_offset: [f32; 2],
        objects: &[RenderObject],
        lights: &[RenderLight],
        settings: &PreparedRenderSettings,
    ) -> [u8; 3] {
        let object_bvh = ObjectBvh::from_objects(objects);
        let intersector = SceneIntersector::new(objects, &object_bvh);
        self.trace_debug_rgb_at_subpixel_with_intersector(
            camera,
            resolution,
            x,
            y,
            subpixel_offset,
            &intersector,
            lights,
            settings,
        )
    }

    fn trace_debug_rgb_at_subpixel_with_intersector(
        &self,
        camera: &CameraSpec,
        resolution: [u32; 2],
        x: u32,
        y: u32,
        subpixel_offset: [f32; 2],
        intersector: &SceneIntersector<'_>,
        lights: &[RenderLight],
        settings: &PreparedRenderSettings,
    ) -> [u8; 3] {
        let ray = camera_ray_at_pixel(
            camera,
            resolution,
            x as f32 + subpixel_offset[0].clamp(0.0, 1.0),
            y as f32 + subpixel_offset[1].clamp(0.0, 1.0),
        );
        let mut hits = intersector
            .hits(&ray)
            .into_iter()
            .filter(|hit| hit.t > 0.0 && hit.t <= self.far_m)
            .filter(|hit| depth_range_contains(camera.depth_range_m, hit.t))
            .collect::<Vec<_>>();
        hits.sort_by(|left, right| left.t.total_cmp(&right.t));
        composite_debug_rgb_hits_with_intersector(
            &hits,
            intersector,
            lights,
            settings,
            environment_background_rgb(ray.dir, settings),
        )
    }
}

impl Default for NativeRenderer {
    fn default() -> Self {
        Self::new()
    }
}

fn requires_camera(view: &ObservationView) -> bool {
    !matches!(view, ObservationView::State)
}

fn uses_temporal_samples(request: &ObservationRequest) -> bool {
    request
        .views
        .iter()
        .any(|view| matches!(view, ObservationView::DebugRgb))
        || request.shutter_policy.is_some()
            && request.views.iter().any(|view| {
                matches!(
                    view,
                    ObservationView::Depth
                        | ObservationView::Segmentation
                        | ObservationView::Normal
                        | ObservationView::Albedo
                        | ObservationView::MaterialProperties
                        | ObservationView::WorldPosition
                )
            })
}

fn render_settings_for_scene(
    scene: &SceneGraph,
    request: &ObservationRequest,
) -> Option<RenderSettings> {
    if scene.render_settings.is_none() && scene.reflection_probes.is_empty() {
        return request.render_settings.clone();
    }

    let mut settings = request
        .render_settings
        .clone()
        .or_else(|| scene.render_settings.clone())
        .unwrap_or_default();
    settings
        .reflection_probes
        .extend(scene.reflection_probes.clone());
    Some(settings)
}

fn scene_graph_sample_time(sample: &SceneGraphSample) -> f32 {
    sample
        .state
        .as_ref()
        .map(|state| state.clock_sec as f32)
        .unwrap_or(0.0)
}

fn interpolate_scene_graph_sample(samples: &[SceneGraphSample], time_sec: f32) -> SceneGraphSample {
    let mut ordered = samples.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| {
        scene_graph_sample_time(left).total_cmp(&scene_graph_sample_time(right))
    });

    let Some(first) = ordered.first().copied() else {
        return SceneGraphSample {
            scene: SceneGraph::empty(),
            state: None,
        };
    };
    if time_sec <= scene_graph_sample_time(first) {
        return first.clone();
    }

    let Some(last) = ordered.last().copied() else {
        return first.clone();
    };
    if time_sec >= scene_graph_sample_time(last) {
        return last.clone();
    }

    for pair in ordered.windows(2) {
        let left = pair[0];
        let right = pair[1];
        let left_time = scene_graph_sample_time(left);
        let right_time = scene_graph_sample_time(right);
        if time_sec < left_time || time_sec > right_time {
            continue;
        }
        let span = (right_time - left_time).max(f32::EPSILON);
        let factor = ((time_sec - left_time) / span).clamp(0.0, 1.0);
        let mut state = right.state.clone().or_else(|| left.state.clone());
        if let Some(state) = &mut state {
            state.clock_sec = time_sec as f64;
        }
        return SceneGraphSample {
            scene: interpolate_scene_graph(&left.scene, &right.scene, factor),
            state,
        };
    }

    last.clone()
}

fn interpolate_scene_graph(left: &SceneGraph, right: &SceneGraph, factor: f32) -> SceneGraph {
    let mut right_transforms = BTreeMap::new();
    collect_node_transforms(&right.root, &mut right_transforms);
    let mut scene = left.clone();
    apply_interpolated_node_transforms(&mut scene.root, &right_transforms, factor);
    scene
}

fn collect_node_transforms(node: &SceneNode, transforms: &mut BTreeMap<EntityId, Transform>) {
    transforms.insert(node.entity.clone(), node.transform);
    for child in &node.children {
        collect_node_transforms(child, transforms);
    }
}

fn apply_interpolated_node_transforms(
    node: &mut SceneNode,
    right_transforms: &BTreeMap<EntityId, Transform>,
    factor: f32,
) {
    if let Some(right_transform) = right_transforms.get(&node.entity).copied() {
        node.transform = interpolate_transform(node.transform, right_transform, factor);
        match &mut node.kind {
            SceneNodeKind::Camera(camera) => {
                camera.transform = node.transform;
            }
            SceneNodeKind::Light(light) => {
                light.transform = node.transform;
            }
            SceneNodeKind::Group | SceneNodeKind::Mesh { .. } => {}
        }
    }
    for child in &mut node.children {
        apply_interpolated_node_transforms(child, right_transforms, factor);
    }
}

fn interpolate_transform(left: Transform, right: Transform, factor: f32) -> Transform {
    let factor = factor.clamp(0.0, 1.0);
    Transform {
        translation: lerp_vec3(left.translation, right.translation, factor),
        rotation: lerp_vec3(left.rotation, right.rotation, factor),
        rotation_matrix: None,
    }
}

fn find_camera(node: &SceneNode, camera_id: &str) -> Option<CameraSpec> {
    if let SceneNodeKind::Camera(camera) = &node.kind
        && camera.id == camera_id
    {
        return Some(camera.clone());
    }
    node.children
        .iter()
        .find_map(|child| find_camera(child, camera_id))
}

fn entity_u32_map(scene: &SceneGraph) -> BTreeMap<EntityId, u32> {
    scene
        .entities
        .keys()
        .enumerate()
        .map(|(index, entity)| (entity.clone(), (index + 1) as u32))
        .collect()
}

fn render_object_bounds(
    geometry: &RenderGeometry,
    transform: Transform,
) -> Option<([f32; 3], [f32; 3])> {
    let (local_min, local_max) = geometry_local_bounds(geometry)?;
    Some(transform_bounds(transform, local_min, local_max))
}

fn geometry_local_bounds(geometry: &RenderGeometry) -> Option<([f32; 3], [f32; 3])> {
    let bounds = match geometry {
        RenderGeometry::Box { size } => (
            [-size[0] * 0.5, -size[1] * 0.5, -size[2] * 0.5],
            [size[0] * 0.5, size[1] * 0.5, size[2] * 0.5],
        ),
        RenderGeometry::Sphere { radius } => {
            ([-*radius, -*radius, -*radius], [*radius, *radius, *radius])
        }
        RenderGeometry::Cylinder { radius, height } => (
            [-*radius, -*radius, -*height * 0.5],
            [*radius, *radius, *height * 0.5],
        ),
        RenderGeometry::Triangles {
            bounds_min,
            bounds_max,
            ..
        } => (*bounds_min, *bounds_max),
    };
    bounds_are_finite(bounds).then_some(bounds)
}

fn transform_bounds(
    transform: Transform,
    local_min: [f32; 3],
    local_max: [f32; 3],
) -> ([f32; 3], [f32; 3]) {
    let rotation = rotation_matrix(transform);
    let mut world_min = [f32::INFINITY; 3];
    let mut world_max = [f32::NEG_INFINITY; 3];
    for x in [local_min[0], local_max[0]] {
        for y in [local_min[1], local_max[1]] {
            for z in [local_min[2], local_max[2]] {
                let point = add(transform.translation, mat_vec_mul(rotation, [x, y, z]));
                for axis in 0..3 {
                    world_min[axis] = world_min[axis].min(point[axis]);
                    world_max[axis] = world_max[axis].max(point[axis]);
                }
            }
        }
    }
    (world_min, world_max)
}

fn bounds_are_finite(bounds: ([f32; 3], [f32; 3])) -> bool {
    bounds
        .0
        .iter()
        .chain(bounds.1.iter())
        .all(|value| value.is_finite())
}

fn build_object_bvh_node(
    nodes: &mut Vec<ObjectBvhNode>,
    primitives: &mut [ObjectBvhPrimitive],
) -> Option<usize> {
    if primitives.is_empty() {
        return None;
    }
    let (bounds_min, bounds_max) = object_primitive_range_bounds(primitives);
    if primitives.len() <= 4 {
        let node_index = nodes.len();
        nodes.push(ObjectBvhNode {
            bounds_min,
            bounds_max,
            left: None,
            right: None,
            object_indices: primitives
                .iter()
                .map(|primitive| primitive.object_index)
                .collect(),
        });
        return Some(node_index);
    }

    let (centroid_min, centroid_max) = object_primitive_centroid_range_bounds(primitives);
    let split_axis = largest_axis(sub(centroid_max, centroid_min));
    primitives
        .sort_by(|left, right| left.centroid[split_axis].total_cmp(&right.centroid[split_axis]));
    let mid = primitives.len() / 2;
    let (left_primitives, right_primitives) = primitives.split_at_mut(mid);
    let node_index = nodes.len();
    nodes.push(ObjectBvhNode {
        bounds_min,
        bounds_max,
        left: None,
        right: None,
        object_indices: Vec::new(),
    });
    let left = build_object_bvh_node(nodes, left_primitives);
    let right = build_object_bvh_node(nodes, right_primitives);
    nodes[node_index].left = left;
    nodes[node_index].right = right;
    Some(node_index)
}

fn object_primitive_range_bounds(primitives: &[ObjectBvhPrimitive]) -> ([f32; 3], [f32; 3]) {
    let mut min = [f32::INFINITY; 3];
    let mut max = [f32::NEG_INFINITY; 3];
    for primitive in primitives {
        for axis in 0..3 {
            min[axis] = min[axis].min(primitive.bounds_min[axis]);
            max[axis] = max[axis].max(primitive.bounds_max[axis]);
        }
    }
    (min, max)
}

fn object_primitive_centroid_range_bounds(
    primitives: &[ObjectBvhPrimitive],
) -> ([f32; 3], [f32; 3]) {
    let mut min = [f32::INFINITY; 3];
    let mut max = [f32::NEG_INFINITY; 3];
    for primitive in primitives {
        for axis in 0..3 {
            min[axis] = min[axis].min(primitive.centroid[axis]);
            max[axis] = max[axis].max(primitive.centroid[axis]);
        }
    }
    (min, max)
}

fn render_objects(
    node: &SceneNode,
    parent_transform: Transform,
    entity_ids: &BTreeMap<EntityId, u32>,
    mesh_cache: &MeshGeometryCache,
    animation_time_sec: f32,
    material_variant: Option<&str>,
) -> Result<Vec<RenderObject>, String> {
    let transform = compose_transform(parent_transform, node.transform);
    let mut objects = Vec::new();
    if let SceneNodeKind::Mesh { geometry, material } = &node.kind
        && let Some(entity_u32) = entity_ids.get(&node.entity)
    {
        if let Some(geometry) =
            render_geometry(geometry, mesh_cache, animation_time_sec, material_variant)?
        {
            objects.push(RenderObject {
                entity_u32: *entity_u32,
                bounds: render_object_bounds(geometry.as_ref(), transform),
                geometry,
                color_rgb: material.color_rgb,
                transform,
            });
        }
    }
    for child in &node.children {
        objects.extend(render_objects(
            child,
            transform,
            entity_ids,
            mesh_cache,
            animation_time_sec,
            material_variant,
        )?);
    }
    Ok(objects)
}

fn render_lights(node: &SceneNode, parent_transform: Transform) -> Vec<RenderLight> {
    let transform = compose_transform(parent_transform, node.transform);
    let mut lights = Vec::new();
    if let SceneNodeKind::Light(light) = &node.kind {
        lights.push(render_light(
            light.kind,
            light.color_rgb,
            light.intensity,
            transform,
        ));
    }
    for child in &node.children {
        lights.extend(render_lights(child, transform));
    }
    lights
}

fn render_light(
    kind: LightKind,
    color_rgb: [u8; 3],
    intensity: f32,
    transform: Transform,
) -> RenderLight {
    let rotation = rotation_matrix(transform);
    let kind = match kind {
        LightKind::Directional {
            direction,
            angular_radius_deg,
        } => RenderLightKind::Directional {
            direction: scale(normalize(mat_vec_mul(rotation, direction)), -1.0),
            angular_radius_deg,
        },
        LightKind::Point { range_m } => RenderLightKind::Point {
            position: transform.translation,
            range_m,
        },
        LightKind::Spot {
            direction,
            inner_cone_deg,
            outer_cone_deg,
            range_m,
        } => {
            let inner_cos = inner_cone_deg.to_radians().cos();
            let outer_cos = outer_cone_deg.to_radians().cos();
            RenderLightKind::Spot {
                position: transform.translation,
                direction: normalize(mat_vec_mul(rotation, direction)),
                inner_cos: inner_cos.max(outer_cos),
                outer_cos: inner_cos.min(outer_cos),
                range_m,
            }
        }
    };
    RenderLight {
        kind,
        color_rgb: srgb_u8_to_linear_rgb(color_rgb),
        intensity,
    }
}

fn render_gltf_light(
    light: &gltf::khr_lights_punctual::Light<'_>,
    transform: [[f32; 4]; 4],
) -> RenderLight {
    let color_rgb = light_color_rgb(light.color());
    let intensity = light.intensity().max(0.0);
    let kind = match light.kind() {
        gltf::khr_lights_punctual::Kind::Directional => RenderLightKind::Directional {
            direction: scale(
                normalize(transform_direction4(transform, [0.0, 0.0, -1.0])),
                -1.0,
            ),
            angular_radius_deg: 0.0,
        },
        gltf::khr_lights_punctual::Kind::Point => RenderLightKind::Point {
            position: transform_point4(transform, [0.0, 0.0, 0.0]),
            range_m: light.range(),
        },
        gltf::khr_lights_punctual::Kind::Spot {
            inner_cone_angle,
            outer_cone_angle,
        } => {
            let inner_cos = inner_cone_angle.cos();
            let outer_cos = outer_cone_angle.cos();
            RenderLightKind::Spot {
                position: transform_point4(transform, [0.0, 0.0, 0.0]),
                direction: normalize(transform_direction4(transform, [0.0, 0.0, -1.0])),
                inner_cos: inner_cos.max(outer_cos),
                outer_cos: inner_cos.min(outer_cos),
                range_m: light.range(),
            }
        }
    };
    RenderLight {
        kind,
        color_rgb,
        intensity,
    }
}

fn light_color_rgb(color: [f32; 3]) -> [f32; 3] {
    [color[0].max(0.0), color[1].max(0.0), color[2].max(0.0)]
}

fn render_asset_lights(objects: &[RenderObject]) -> Vec<RenderLight> {
    objects
        .iter()
        .flat_map(|object| match object.geometry.as_ref() {
            RenderGeometry::Triangles {
                triangles,
                textures,
                lights,
                ..
            } => lights
                .iter()
                .copied()
                .chain(emissive_triangle_lights(triangles, textures))
                .map(|light| transform_render_light(light, object.transform))
                .collect::<Vec<_>>(),
            _ => Vec::new(),
        })
        .collect()
}

fn emissive_triangle_lights(triangles: &[Triangle], textures: &[TextureImage]) -> Vec<RenderLight> {
    triangles
        .iter()
        .filter_map(|triangle| emissive_triangle_light(triangle, textures))
        .collect()
}

fn emissive_triangle_light(triangle: &Triangle, textures: &[TextureImage]) -> Option<RenderLight> {
    let emission_rgb = emissive_triangle_average_rgb(triangle, textures);
    let max_emission = emission_rgb.iter().copied().fold(0.0_f32, f32::max);
    if max_emission <= 1.0e-3 {
        return None;
    }
    let area = triangle_area(triangle);
    if area <= 1.0e-8 {
        return None;
    }
    let color = [
        emission_rgb[0] / max_emission,
        emission_rgb[1] / max_emission,
        emission_rgb[2] / max_emission,
    ];
    Some(RenderLight {
        kind: RenderLightKind::AreaTriangle {
            a: triangle.a,
            b: triangle.b,
            c: triangle.c,
            normal: triangle_normal(triangle.a, triangle.b, triangle.c),
            double_sided: triangle.double_sided,
        },
        color_rgb: color,
        intensity: max_emission / 255.0,
    })
}

fn emissive_triangle_average_rgb(triangle: &Triangle, textures: &[TextureImage]) -> [f32; 3] {
    let sample_count = EMISSIVE_TRIANGLE_LIGHT_SAMPLES.max(1);
    let mut sum = [0.0_f32; 3];
    for sample_index in 0..sample_count {
        let barycentric = area_light_sample_barycentric(sample_index, sample_count);
        let sample =
            interpolated_emission_sample(triangle, textures, barycentric[1], barycentric[2]);
        let visibility = interpolated_emissive_visibility_sample(
            triangle,
            textures,
            barycentric[1],
            barycentric[2],
        );
        for channel in 0..3 {
            sum[channel] += sample[channel] * visibility;
        }
    }
    scale(sum, 1.0 / sample_count as f32)
}

fn lights_for_objects(scene_lights: &[RenderLight], objects: &[RenderObject]) -> Vec<RenderLight> {
    let mut lights = scene_lights.to_vec();
    lights.extend(render_asset_lights(objects));
    if lights.is_empty() {
        fallback_lights()
    } else {
        lights
    }
}

fn shutter_sample_times(center_time_sec: f32, policy: Option<ShutterPolicy>) -> Vec<f32> {
    let policy = policy.unwrap_or_default();
    let exposure_sec = policy.exposure_sec.max(0.0);
    let samples = policy.samples.clamp(1, 32);
    if exposure_sec <= f32::EPSILON || samples == 1 {
        return vec![center_time_sec];
    }

    let start_time_sec = center_time_sec - exposure_sec * 0.5;
    let step = exposure_sec / (samples - 1) as f32;
    (0..samples)
        .map(|index| (start_time_sec + step * index as f32).max(0.0))
        .collect()
}

fn shutter_render_sample_times(
    frame_time_sec: f32,
    height: u32,
    policy: Option<ShutterPolicy>,
) -> Vec<f32> {
    let policy = policy.unwrap_or_default();
    if !matches!(policy.mode, ShutterMode::RollingTopToBottom) || policy.readout_sec <= f32::EPSILON
    {
        return shutter_sample_times(frame_time_sec, Some(policy));
    }

    let mut times = Vec::new();
    for y in 0..height.max(1) {
        times.extend(shutter_sample_times(
            shutter_row_center_time_sec(frame_time_sec, y, height, policy),
            Some(policy),
        ));
    }
    times.sort_by(f32::total_cmp);
    times.dedup_by(|left, right| (*left - *right).abs() <= 1.0e-4);
    times
}

fn shutter_row_center_time_sec(
    frame_time_sec: f32,
    y: u32,
    height: u32,
    policy: ShutterPolicy,
) -> f32 {
    if !matches!(policy.mode, ShutterMode::RollingTopToBottom) || policy.readout_sec <= f32::EPSILON
    {
        return frame_time_sec;
    }
    let fraction = if height <= 1 {
        1.0
    } else {
        y.min(height - 1) as f32 / (height - 1) as f32
    };
    (frame_time_sec - policy.readout_sec.max(0.0) * (1.0 - fraction)).max(0.0)
}

fn shutter_sample_indexes_for_pixel(
    samples: &[RenderSceneSample],
    resolution: [u32; 2],
    y: u32,
    policy: Option<ShutterPolicy>,
) -> Vec<usize> {
    if samples.len() <= 1 {
        return (0..samples.len()).collect();
    }
    let Some(policy) = policy else {
        return (0..samples.len()).collect();
    };
    let frame_time_sec = samples
        .last()
        .map(|sample| sample.timestamp_sec)
        .unwrap_or(0.0);
    let sample_times = shutter_sample_times(
        shutter_row_center_time_sec(frame_time_sec, y, resolution[1], policy),
        Some(policy),
    );
    sample_times
        .into_iter()
        .map(|time| nearest_shutter_sample_index(samples, time))
        .collect()
}

fn nearest_shutter_sample_index(samples: &[RenderSceneSample], time_sec: f32) -> usize {
    samples
        .iter()
        .enumerate()
        .min_by(|(_, left), (_, right)| {
            (left.timestamp_sec - time_sec)
                .abs()
                .total_cmp(&(right.timestamp_sec - time_sec).abs())
        })
        .map(|(index, _)| index)
        .unwrap_or(0)
}

fn transform_render_light(light: RenderLight, transform: Transform) -> RenderLight {
    let rotation = rotation_matrix(transform);
    let kind = match light.kind {
        RenderLightKind::Directional {
            direction,
            angular_radius_deg,
        } => RenderLightKind::Directional {
            direction: normalize(mat_vec_mul(rotation, direction)),
            angular_radius_deg,
        },
        RenderLightKind::Point { position, range_m } => RenderLightKind::Point {
            position: add(transform.translation, mat_vec_mul(rotation, position)),
            range_m,
        },
        RenderLightKind::Spot {
            position,
            direction,
            inner_cos,
            outer_cos,
            range_m,
        } => RenderLightKind::Spot {
            position: add(transform.translation, mat_vec_mul(rotation, position)),
            direction: normalize(mat_vec_mul(rotation, direction)),
            inner_cos,
            outer_cos,
            range_m,
        },
        RenderLightKind::AreaTriangle {
            a,
            b,
            c,
            normal,
            double_sided,
        } => RenderLightKind::AreaTriangle {
            a: add(transform.translation, mat_vec_mul(rotation, a)),
            b: add(transform.translation, mat_vec_mul(rotation, b)),
            c: add(transform.translation, mat_vec_mul(rotation, c)),
            normal: normalize(mat_vec_mul(rotation, normal)),
            double_sided,
        },
    };
    RenderLight { kind, ..light }
}

fn render_geometry(
    geometry: &Geometry,
    mesh_cache: &MeshGeometryCache,
    animation_time_sec: f32,
    material_variant: Option<&str>,
) -> Result<Option<Arc<RenderGeometry>>, String> {
    match geometry {
        Geometry::Box { size } | Geometry::MeshBounds { size, .. } => {
            Ok(Some(Arc::new(RenderGeometry::Box { size: *size })))
        }
        Geometry::Sphere { radius } => {
            Ok(Some(Arc::new(RenderGeometry::Sphere { radius: *radius })))
        }
        Geometry::Cylinder { radius, height } => Ok(Some(Arc::new(RenderGeometry::Cylinder {
            radius: *radius,
            height: *height,
        }))),
        Geometry::MeshAsset { asset, scale, .. } => {
            let key = MeshCacheKey {
                asset: asset.clone(),
                scale_bits: scale.map(f32::to_bits),
                animation_time_millis: (animation_time_sec.max(0.0) * 1000.0).round() as i64,
                material_variant: material_variant.map(str::to_string),
            };
            if let Some(cached) = mesh_cache
                .lock()
                .map_err(|_| "mesh geometry cache lock poisoned".to_string())?
                .get(&key)
                .cloned()
            {
                return Ok(Some(cached));
            }

            let mut mesh = load_gltf_mesh_at_time(
                Path::new(asset),
                *scale,
                animation_time_sec,
                material_variant,
            )?;
            let bvh = build_triangle_bvh(&mut mesh.triangles);
            let geometry = mesh_bounds(&mesh.triangles).map(|(bounds_min, bounds_max)| {
                Arc::new(RenderGeometry::Triangles {
                    bounds_min,
                    bounds_max,
                    triangles: mesh.triangles,
                    textures: mesh.textures,
                    lights: mesh.lights,
                    bvh,
                })
            });
            if let Some(geometry) = &geometry {
                mesh_cache
                    .lock()
                    .map_err(|_| "mesh geometry cache lock poisoned".to_string())?
                    .insert(key, geometry.clone());
            }
            Ok(geometry)
        }
    }
}

fn compose_transform(parent: Transform, child: Transform) -> Transform {
    let parent_rotation = rotation_matrix(parent);
    let child_rotation = rotation_matrix(child);
    let rotation_matrix = mat_mul_f32(parent_rotation, child_rotation);
    Transform {
        translation: add(
            parent.translation,
            mat_vec_mul(parent_rotation, child.translation),
        ),
        rotation: add(parent.rotation, child.rotation),
        rotation_matrix: Some(rotation_matrix),
    }
}

fn depth_range_contains(depth_range_m: Option<[f32; 2]>, depth_m: f32) -> bool {
    depth_range_m
        .map(|[near, far]| depth_m >= near.max(0.0) && depth_m <= far.max(near))
        .unwrap_or(true)
}

fn hash_sensor_noise(mut value: u32) -> u32 {
    value ^= value >> 16;
    value = value.wrapping_mul(0x7feb_352d);
    value ^= value >> 15;
    value = value.wrapping_mul(0x846c_a68b);
    value ^ (value >> 16)
}

fn sensor_unit_noise(seed: u32, x: u32, y: u32, channel: u32) -> f32 {
    let mixed = seed
        ^ x.wrapping_mul(0x9e37_79b9)
        ^ y.wrapping_mul(0x85eb_ca6b)
        ^ channel.wrapping_mul(0xc2b2_ae35);
    let hashed = hash_sensor_noise(mixed);
    ((hashed as f32) + 0.5) / ((u32::MAX as f32) + 1.0)
}

fn sensor_gaussian_noise(seed: u32, x: u32, y: u32, channel: u32, stddev: f32) -> f32 {
    if stddev <= 0.0 {
        return 0.0;
    }
    let u1 = sensor_unit_noise(seed, x, y, channel).clamp(1.0e-7, 1.0);
    let u2 = sensor_unit_noise(seed, x, y, channel.wrapping_add(7919));
    let gaussian = (-2.0 * u1.ln()).sqrt() * (std::f32::consts::TAU * u2).cos();
    gaussian * stddev
}

fn apply_rgb_sensor_effects(
    color: [u8; 3],
    sensor_effects: Option<CameraSensorEffects>,
    x: u32,
    y: u32,
) -> [u8; 3] {
    let Some(effects) = sensor_effects else {
        return color;
    };
    let exposure = effects.exposure.max(0.0);
    let gamma = effects.gamma.max(1.0e-6);
    let mut out = [0_u8; 3];
    for channel in 0..3 {
        let linear = (f32::from(color[channel]) / 255.0 * exposure).clamp(0.0, 1.0);
        let gamma_corrected = linear.powf(1.0 / gamma) * 255.0;
        let noisy = gamma_corrected
            + sensor_gaussian_noise(
                effects.noise_seed,
                x,
                y,
                channel as u32,
                effects.rgb_noise_stddev,
            );
        out[channel] = noisy.round().clamp(0.0, 255.0) as u8;
    }
    out
}

fn quantize_depth(depth_m: f32, quantization_m: f32) -> f32 {
    if quantization_m <= 0.0 {
        return depth_m;
    }
    (depth_m / quantization_m).round() * quantization_m
}

fn apply_depth_sensor_effects(
    depth_m: f32,
    sensor_effects: Option<CameraSensorEffects>,
    x: u32,
    y: u32,
) -> f32 {
    if !depth_m.is_finite() {
        return depth_m;
    }
    let Some(effects) = sensor_effects else {
        return depth_m;
    };
    let noisy = depth_m
        + sensor_gaussian_noise(effects.noise_seed, x, y, 101, effects.depth_noise_stddev_m);
    quantize_depth(noisy.max(0.0), effects.depth_quantization_m)
}

#[cfg(test)]
fn distort_normalized_point(point: [f32; 2], distortion: CameraDistortion) -> [f32; 2] {
    let [x, y] = point;
    let r2 = x * x + y * y;
    let r4 = r2 * r2;
    let r6 = r4 * r2;
    let radial = 1.0 + distortion.k1 * r2 + distortion.k2 * r4 + distortion.k3 * r6;
    [
        x * radial + 2.0 * distortion.p1 * x * y + distortion.p2 * (r2 + 2.0 * x * x),
        y * radial + distortion.p1 * (r2 + 2.0 * y * y) + 2.0 * distortion.p2 * x * y,
    ]
}

fn undistort_normalized_point(distorted: [f32; 2], distortion: CameraDistortion) -> [f32; 2] {
    let mut undistorted = distorted;
    for _ in 0..8 {
        let [x, y] = undistorted;
        let r2 = x * x + y * y;
        let r4 = r2 * r2;
        let r6 = r4 * r2;
        let radial =
            (1.0 + distortion.k1 * r2 + distortion.k2 * r4 + distortion.k3 * r6).max(f32::EPSILON);
        let tangential_x = 2.0 * distortion.p1 * x * y + distortion.p2 * (r2 + 2.0 * x * x);
        let tangential_y = distortion.p1 * (r2 + 2.0 * y * y) + 2.0 * distortion.p2 * x * y;
        undistorted = [
            (distorted[0] - tangential_x) / radial,
            (distorted[1] - tangential_y) / radial,
        ];
    }
    undistorted
}

fn camera_ray(camera: &CameraSpec, resolution: [u32; 2], x: u32, y: u32) -> Ray {
    camera_ray_at_pixel(camera, resolution, x as f32 + 0.5, y as f32 + 0.5)
}

fn camera_ray_at_pixel(
    camera: &CameraSpec,
    resolution: [u32; 2],
    pixel_x: f32,
    pixel_y: f32,
) -> Ray {
    if let CameraProjection::Orthographic { size_m } = camera.projection {
        let width = resolution[0].max(1) as f32;
        let height = resolution[1].max(1) as f32;
        let aspect = width / height.max(f32::EPSILON);
        let sensor_x = ((pixel_x / width) - 0.5) * size_m * aspect;
        let sensor_y = ((pixel_y / height) - 0.5) * size_m;
        let [sensor_x, sensor_y] = camera
            .distortion
            .map(|distortion| undistort_normalized_point([sensor_x, sensor_y], distortion))
            .unwrap_or([sensor_x, sensor_y]);
        let rotation = rotation_matrix(camera.transform);
        return Ray {
            origin: add(
                camera.transform.translation,
                mat_vec_mul(rotation, [0.0, -sensor_x, -sensor_y]),
            ),
            dir: normalize(mat_vec_mul(rotation, [1.0, 0.0, 0.0])),
        };
    }

    let intrinsics = camera_intrinsics_for_resolution(camera, resolution);
    let sensor_y = (pixel_y - intrinsics.cy) / intrinsics.fy.max(f32::EPSILON);
    let sensor_x =
        (pixel_x - intrinsics.cx - intrinsics.skew * sensor_y) / intrinsics.fx.max(f32::EPSILON);
    let [sensor_x, sensor_y] = camera
        .distortion
        .map(|distortion| undistort_normalized_point([sensor_x, sensor_y], distortion))
        .unwrap_or([sensor_x, sensor_y]);
    let local = normalize([1.0, -sensor_x, -sensor_y]);
    Ray {
        origin: camera.transform.translation,
        dir: normalize(mat_vec_mul(rotation_matrix(camera.transform), local)),
    }
}

fn subpixel_sample_offset(sample_index: u32, sample_count: u32) -> [f32; 2] {
    if sample_count <= 1 {
        return [0.5, 0.5];
    }
    let grid = (sample_count as f32).sqrt().ceil().max(1.0) as u32;
    let x = sample_index % grid;
    let y = sample_index / grid;
    [
        (x as f32 + 0.5) / grid as f32,
        (y as f32 + 0.5) / grid as f32,
    ]
}

#[allow(dead_code)]
fn intersect_object(ray: &Ray, object: &RenderObject) -> Option<Hit> {
    intersect_object_closest_hit_with_mode(ray, object, TriangleIntersectionMode::VisibleSurface)
}

#[allow(dead_code)]
fn intersect_object_hits(ray: &Ray, object: &RenderObject) -> Vec<Hit> {
    intersect_object_hits_with_mode(ray, object, TriangleIntersectionMode::VisibleSurface)
}

fn intersect_object_hits_with_mode(
    ray: &Ray,
    object: &RenderObject,
    triangle_mode: TriangleIntersectionMode,
) -> Vec<Hit> {
    if let Some((bounds_min, bounds_max)) = object.bounds
        && intersect_aabb(ray, bounds_min, bounds_max).is_none()
    {
        return Vec::new();
    }
    let rotation = rotation_matrix(object.transform);
    let local_origin = inverse_rotate(
        object.transform,
        sub(ray.origin, object.transform.translation),
    );
    let local_dir = normalize(inverse_rotate(object.transform, ray.dir));
    let local_ray = Ray {
        origin: local_origin,
        dir: local_dir,
    };
    match object.geometry.as_ref() {
        RenderGeometry::Box { size } => {
            let Some(t) = intersect_box(&local_ray, *size) else {
                return Vec::new();
            };
            let point = ray_point(&local_ray, t);
            let normal = box_normal(point, *size);
            vec![local_hit_to_world(
                object,
                ray,
                &local_ray,
                LocalHit {
                    t,
                    color_rgb: object.color_rgb,
                    color_linear_rgb: srgb_u8_to_linear_rgb(object.color_rgb),
                    emission_rgb: [0.0, 0.0, 0.0],
                    alpha: 1.0,
                    transmission_filter_rgb: [1.0, 1.0, 1.0],
                    diffuse_transmission: 0.0,
                    diffuse_transmission_color: [1.0, 1.0, 1.0],
                    dispersion: 0.0,
                    volume_thickness_m: 0.0,
                    volume_attenuation_distance_m: f32::INFINITY,
                    volume_attenuation_color: [1.0, 1.0, 1.0],
                    occlusion: 1.0,
                    metallic: 0.0,
                    roughness: 1.0,
                    ior: 1.5,
                    clearcoat_factor: 0.0,
                    clearcoat_roughness: 1.0,
                    clearcoat_normal: normal,
                    sheen_color: [0.0, 0.0, 0.0],
                    sheen_roughness: 1.0,
                    anisotropy_strength: 0.0,
                    anisotropy_direction: [1.0, 0.0, 0.0],
                    iridescence_factor: 0.0,
                    iridescence_thickness_nm: 0.0,
                    iridescence_ior: 1.3,
                    specular_factor: 1.0,
                    specular_color: [1.0, 1.0, 1.0],
                    unlit: false,
                    normal,
                },
                rotation,
            )]
        }
        RenderGeometry::Sphere { radius } => {
            let Some(t) = intersect_sphere(&local_ray, *radius) else {
                return Vec::new();
            };
            let point = ray_point(&local_ray, t);
            let normal = normalize(point);
            vec![local_hit_to_world(
                object,
                ray,
                &local_ray,
                LocalHit {
                    t,
                    color_rgb: object.color_rgb,
                    color_linear_rgb: srgb_u8_to_linear_rgb(object.color_rgb),
                    emission_rgb: [0.0, 0.0, 0.0],
                    alpha: 1.0,
                    transmission_filter_rgb: [1.0, 1.0, 1.0],
                    diffuse_transmission: 0.0,
                    diffuse_transmission_color: [1.0, 1.0, 1.0],
                    dispersion: 0.0,
                    volume_thickness_m: 0.0,
                    volume_attenuation_distance_m: f32::INFINITY,
                    volume_attenuation_color: [1.0, 1.0, 1.0],
                    occlusion: 1.0,
                    metallic: 0.0,
                    roughness: 1.0,
                    ior: 1.5,
                    clearcoat_factor: 0.0,
                    clearcoat_roughness: 1.0,
                    clearcoat_normal: normal,
                    sheen_color: [0.0, 0.0, 0.0],
                    sheen_roughness: 1.0,
                    anisotropy_strength: 0.0,
                    anisotropy_direction: [1.0, 0.0, 0.0],
                    iridescence_factor: 0.0,
                    iridescence_thickness_nm: 0.0,
                    iridescence_ior: 1.3,
                    specular_factor: 1.0,
                    specular_color: [1.0, 1.0, 1.0],
                    unlit: false,
                    normal,
                },
                rotation,
            )]
        }
        RenderGeometry::Cylinder { radius, height } => {
            let Some(t) = intersect_cylinder(&local_ray, *radius, *height) else {
                return Vec::new();
            };
            let point = ray_point(&local_ray, t);
            let normal = cylinder_normal(point, *height);
            vec![local_hit_to_world(
                object,
                ray,
                &local_ray,
                LocalHit {
                    t,
                    color_rgb: object.color_rgb,
                    color_linear_rgb: srgb_u8_to_linear_rgb(object.color_rgb),
                    emission_rgb: [0.0, 0.0, 0.0],
                    alpha: 1.0,
                    transmission_filter_rgb: [1.0, 1.0, 1.0],
                    diffuse_transmission: 0.0,
                    diffuse_transmission_color: [1.0, 1.0, 1.0],
                    dispersion: 0.0,
                    volume_thickness_m: 0.0,
                    volume_attenuation_distance_m: f32::INFINITY,
                    volume_attenuation_color: [1.0, 1.0, 1.0],
                    occlusion: 1.0,
                    metallic: 0.0,
                    roughness: 1.0,
                    ior: 1.5,
                    clearcoat_factor: 0.0,
                    clearcoat_roughness: 1.0,
                    clearcoat_normal: normal,
                    sheen_color: [0.0, 0.0, 0.0],
                    sheen_roughness: 1.0,
                    anisotropy_strength: 0.0,
                    anisotropy_direction: [1.0, 0.0, 0.0],
                    iridescence_factor: 0.0,
                    iridescence_thickness_nm: 0.0,
                    iridescence_ior: 1.3,
                    specular_factor: 1.0,
                    specular_color: [1.0, 1.0, 1.0],
                    unlit: false,
                    normal,
                },
                rotation,
            )]
        }
        RenderGeometry::Triangles {
            bounds_min,
            bounds_max,
            triangles,
            textures,
            bvh,
            ..
        } => {
            if intersect_aabb(&local_ray, *bounds_min, *bounds_max).is_none() {
                return Vec::new();
            }
            intersect_triangles_all_with_mode(&local_ray, triangles, textures, bvh, triangle_mode)
                .into_iter()
                .map(|hit| {
                    local_hit_to_world(object, ray, &local_ray, LocalHit::from(hit), rotation)
                })
                .collect()
        }
    }
}

fn intersect_object_closest_hit_with_mode(
    ray: &Ray,
    object: &RenderObject,
    triangle_mode: TriangleIntersectionMode,
) -> Option<Hit> {
    if let Some((bounds_min, bounds_max)) = object.bounds
        && intersect_aabb(ray, bounds_min, bounds_max).is_none()
    {
        return None;
    }
    let rotation = rotation_matrix(object.transform);
    let local_origin = inverse_rotate(
        object.transform,
        sub(ray.origin, object.transform.translation),
    );
    let local_dir = normalize(inverse_rotate(object.transform, ray.dir));
    let local_ray = Ray {
        origin: local_origin,
        dir: local_dir,
    };
    let local_hit = match object.geometry.as_ref() {
        RenderGeometry::Box { size } => {
            let t = intersect_box(&local_ray, *size)?;
            let point = ray_point(&local_ray, t);
            let normal = box_normal(point, *size);
            LocalHit {
                t,
                color_rgb: object.color_rgb,
                color_linear_rgb: srgb_u8_to_linear_rgb(object.color_rgb),
                emission_rgb: [0.0, 0.0, 0.0],
                alpha: 1.0,
                transmission_filter_rgb: [1.0, 1.0, 1.0],
                diffuse_transmission: 0.0,
                diffuse_transmission_color: [1.0, 1.0, 1.0],
                dispersion: 0.0,
                volume_thickness_m: 0.0,
                volume_attenuation_distance_m: f32::INFINITY,
                volume_attenuation_color: [1.0, 1.0, 1.0],
                occlusion: 1.0,
                metallic: 0.0,
                roughness: 1.0,
                ior: 1.5,
                clearcoat_factor: 0.0,
                clearcoat_roughness: 1.0,
                clearcoat_normal: normal,
                sheen_color: [0.0, 0.0, 0.0],
                sheen_roughness: 1.0,
                anisotropy_strength: 0.0,
                anisotropy_direction: [1.0, 0.0, 0.0],
                iridescence_factor: 0.0,
                iridescence_thickness_nm: 0.0,
                iridescence_ior: 1.3,
                specular_factor: 1.0,
                specular_color: [1.0, 1.0, 1.0],
                unlit: false,
                normal,
            }
        }
        RenderGeometry::Sphere { radius } => {
            let t = intersect_sphere(&local_ray, *radius)?;
            let point = ray_point(&local_ray, t);
            let normal = normalize(point);
            LocalHit {
                t,
                color_rgb: object.color_rgb,
                color_linear_rgb: srgb_u8_to_linear_rgb(object.color_rgb),
                emission_rgb: [0.0, 0.0, 0.0],
                alpha: 1.0,
                transmission_filter_rgb: [1.0, 1.0, 1.0],
                diffuse_transmission: 0.0,
                diffuse_transmission_color: [1.0, 1.0, 1.0],
                dispersion: 0.0,
                volume_thickness_m: 0.0,
                volume_attenuation_distance_m: f32::INFINITY,
                volume_attenuation_color: [1.0, 1.0, 1.0],
                occlusion: 1.0,
                metallic: 0.0,
                roughness: 1.0,
                ior: 1.5,
                clearcoat_factor: 0.0,
                clearcoat_roughness: 1.0,
                clearcoat_normal: normal,
                sheen_color: [0.0, 0.0, 0.0],
                sheen_roughness: 1.0,
                anisotropy_strength: 0.0,
                anisotropy_direction: [1.0, 0.0, 0.0],
                iridescence_factor: 0.0,
                iridescence_thickness_nm: 0.0,
                iridescence_ior: 1.3,
                specular_factor: 1.0,
                specular_color: [1.0, 1.0, 1.0],
                unlit: false,
                normal,
            }
        }
        RenderGeometry::Cylinder { radius, height } => {
            let t = intersect_cylinder(&local_ray, *radius, *height)?;
            let point = ray_point(&local_ray, t);
            let normal = cylinder_normal(point, *height);
            LocalHit {
                t,
                color_rgb: object.color_rgb,
                color_linear_rgb: srgb_u8_to_linear_rgb(object.color_rgb),
                emission_rgb: [0.0, 0.0, 0.0],
                alpha: 1.0,
                transmission_filter_rgb: [1.0, 1.0, 1.0],
                diffuse_transmission: 0.0,
                diffuse_transmission_color: [1.0, 1.0, 1.0],
                dispersion: 0.0,
                volume_thickness_m: 0.0,
                volume_attenuation_distance_m: f32::INFINITY,
                volume_attenuation_color: [1.0, 1.0, 1.0],
                occlusion: 1.0,
                metallic: 0.0,
                roughness: 1.0,
                ior: 1.5,
                clearcoat_factor: 0.0,
                clearcoat_roughness: 1.0,
                clearcoat_normal: normal,
                sheen_color: [0.0, 0.0, 0.0],
                sheen_roughness: 1.0,
                anisotropy_strength: 0.0,
                anisotropy_direction: [1.0, 0.0, 0.0],
                iridescence_factor: 0.0,
                iridescence_thickness_nm: 0.0,
                iridescence_ior: 1.3,
                specular_factor: 1.0,
                specular_color: [1.0, 1.0, 1.0],
                unlit: false,
                normal,
            }
        }
        RenderGeometry::Triangles {
            bounds_min,
            bounds_max,
            triangles,
            textures,
            bvh,
            ..
        } => {
            intersect_aabb(&local_ray, *bounds_min, *bounds_max)?;
            LocalHit::from(intersect_triangles_closest_with_mode(
                &local_ray,
                triangles,
                textures,
                bvh,
                triangle_mode,
            )?)
        }
    };

    Some(local_hit_to_world(
        object, ray, &local_ray, local_hit, rotation,
    ))
}

#[derive(Clone, Copy, Debug)]
struct LocalHit {
    t: f32,
    color_rgb: [u8; 3],
    color_linear_rgb: [f32; 3],
    emission_rgb: [f32; 3],
    alpha: f32,
    transmission_filter_rgb: [f32; 3],
    diffuse_transmission: f32,
    diffuse_transmission_color: [f32; 3],
    dispersion: f32,
    volume_thickness_m: f32,
    volume_attenuation_distance_m: f32,
    volume_attenuation_color: [f32; 3],
    occlusion: f32,
    metallic: f32,
    roughness: f32,
    ior: f32,
    clearcoat_factor: f32,
    clearcoat_roughness: f32,
    clearcoat_normal: [f32; 3],
    sheen_color: [f32; 3],
    sheen_roughness: f32,
    anisotropy_strength: f32,
    anisotropy_direction: [f32; 3],
    iridescence_factor: f32,
    iridescence_thickness_nm: f32,
    iridescence_ior: f32,
    specular_factor: f32,
    specular_color: [f32; 3],
    unlit: bool,
    normal: [f32; 3],
}

impl From<TriangleHit> for LocalHit {
    fn from(hit: TriangleHit) -> Self {
        Self {
            t: hit.t,
            color_rgb: hit.color_rgb,
            color_linear_rgb: hit.color_linear_rgb,
            emission_rgb: hit.emission_rgb,
            alpha: hit.alpha,
            transmission_filter_rgb: hit.transmission_filter_rgb,
            diffuse_transmission: hit.diffuse_transmission,
            diffuse_transmission_color: hit.diffuse_transmission_color,
            dispersion: hit.dispersion,
            volume_thickness_m: hit.volume_thickness_m,
            volume_attenuation_distance_m: hit.volume_attenuation_distance_m,
            volume_attenuation_color: hit.volume_attenuation_color,
            occlusion: hit.occlusion,
            metallic: hit.metallic,
            roughness: hit.roughness,
            ior: hit.ior,
            clearcoat_factor: hit.clearcoat_factor,
            clearcoat_roughness: hit.clearcoat_roughness,
            clearcoat_normal: hit.clearcoat_normal,
            sheen_color: hit.sheen_color,
            sheen_roughness: hit.sheen_roughness,
            anisotropy_strength: hit.anisotropy_strength,
            anisotropy_direction: hit.anisotropy_direction,
            iridescence_factor: hit.iridescence_factor,
            iridescence_thickness_nm: hit.iridescence_thickness_nm,
            iridescence_ior: hit.iridescence_ior,
            specular_factor: hit.specular_factor,
            specular_color: hit.specular_color,
            unlit: hit.unlit,
            normal: hit.normal,
        }
    }
}

fn local_hit_to_world(
    object: &RenderObject,
    ray: &Ray,
    local_ray: &Ray,
    hit: LocalHit,
    rotation: [[f32; 3]; 3],
) -> Hit {
    let local_point = ray_point(local_ray, hit.t);
    Hit {
        t: hit.t,
        entity_u32: object.entity_u32,
        color_rgb: hit.color_rgb,
        color_linear_rgb: hit.color_linear_rgb,
        emission_rgb: hit.emission_rgb,
        alpha: hit.alpha,
        transmission_filter_rgb: hit.transmission_filter_rgb,
        diffuse_transmission: hit.diffuse_transmission,
        diffuse_transmission_color: hit.diffuse_transmission_color,
        dispersion: hit.dispersion,
        volume_thickness_m: hit.volume_thickness_m,
        volume_attenuation_distance_m: hit.volume_attenuation_distance_m,
        volume_attenuation_color: hit.volume_attenuation_color,
        occlusion: hit.occlusion,
        metallic: hit.metallic,
        roughness: hit.roughness,
        ior: hit.ior,
        clearcoat_factor: hit.clearcoat_factor,
        clearcoat_roughness: hit.clearcoat_roughness,
        clearcoat_normal: normalize(mat_vec_mul(rotation, hit.clearcoat_normal)),
        sheen_color: hit.sheen_color,
        sheen_roughness: hit.sheen_roughness,
        anisotropy_strength: hit.anisotropy_strength,
        anisotropy_direction: normalize(mat_vec_mul(rotation, hit.anisotropy_direction)),
        iridescence_factor: hit.iridescence_factor,
        iridescence_thickness_nm: hit.iridescence_thickness_nm,
        iridescence_ior: hit.iridescence_ior,
        specular_factor: hit.specular_factor,
        specular_color: hit.specular_color,
        unlit: hit.unlit,
        point: add(
            object.transform.translation,
            mat_vec_mul(rotation, local_point),
        ),
        normal: normalize(mat_vec_mul(rotation, hit.normal)),
        view_dir: scale(ray.dir, -1.0),
    }
}

#[cfg(test)]
fn load_gltf_mesh(path: &Path, scale: [f32; 3]) -> Result<MeshData, String> {
    load_gltf_mesh_at_time(path, scale, 0.0, None)
}

fn load_gltf_mesh_at_time(
    path: &Path,
    scale: [f32; 3],
    animation_time_sec: f32,
    material_variant: Option<&str>,
) -> Result<MeshData, String> {
    let (document, buffers, images) =
        gltf::import(path).map_err(|err| format!("load mesh {}: {err}", path.display()))?;
    let images = images
        .into_iter()
        .map(texture_image_from_gltf)
        .collect::<Vec<_>>();
    let mut mesh = MeshData {
        triangles: Vec::new(),
        lights: Vec::new(),
        textures: document
            .textures()
            .filter_map(|texture| {
                images
                    .get(texture.source().index())
                    .cloned()
                    .map(|image| texture_image_with_sampler(image, texture.sampler()))
            })
            .collect(),
    };
    let scene = document
        .default_scene()
        .or_else(|| document.scenes().next())
        .ok_or_else(|| format!("mesh {} has no scene", path.display()))?;
    let root_transform = mat4_scale(scale);
    let animation_overrides =
        load_gltf_animation_overrides(&document, &buffers, animation_time_sec.max(0.0));
    let mut node_world_transforms = BTreeMap::new();
    for node in scene.nodes() {
        collect_gltf_node_world_transforms(
            &node,
            root_transform,
            &animation_overrides,
            &mut node_world_transforms,
        );
    }
    let context = GltfLoadContext {
        document: &document,
        node_world_transforms,
        animation_overrides,
        buffers: &buffers,
        material_variant,
        material_variant_names: gltf_material_variant_names(&document),
    };
    for node in scene.nodes() {
        load_gltf_node_data(&node, root_transform, &context, &mut mesh);
    }
    Ok(mesh)
}

fn collect_gltf_node_world_transforms(
    node: &gltf::Node<'_>,
    parent_transform: [[f32; 4]; 4],
    animation_overrides: &BTreeMap<usize, AnimatedNodeTransform>,
    node_world_transforms: &mut BTreeMap<usize, [[f32; 4]; 4]>,
) {
    let node_transform = mat4_mul(
        parent_transform,
        gltf_node_transform_matrix(node, animation_overrides),
    );
    node_world_transforms.insert(node.index(), node_transform);
    for child in node.children() {
        collect_gltf_node_world_transforms(
            &child,
            node_transform,
            animation_overrides,
            node_world_transforms,
        );
    }
}

fn load_gltf_animation_overrides(
    document: &gltf::Document,
    buffers: &[gltf::buffer::Data],
    animation_time_sec: f32,
) -> BTreeMap<usize, AnimatedNodeTransform> {
    let mut overrides: BTreeMap<usize, AnimatedNodeTransform> = BTreeMap::new();
    for animation in document.animations() {
        for channel in animation.channels() {
            let reader = channel.reader(|buffer| buffers.get(buffer.index()).map(|data| &**data));
            let Some(inputs) = reader
                .read_inputs()
                .map(|inputs| inputs.collect::<Vec<_>>())
            else {
                continue;
            };
            let Some(outputs) = reader.read_outputs() else {
                continue;
            };
            let node_index = channel.target().node().index();
            let entry = overrides.entry(node_index).or_default();
            match outputs {
                gltf::animation::util::ReadOutputs::Translations(outputs) => {
                    if let Some(value) = sample_vec3_animation(
                        &inputs,
                        &outputs.collect::<Vec<_>>(),
                        channel.sampler().interpolation(),
                        animation_time_sec,
                    ) {
                        entry.translation = Some(value);
                    }
                }
                gltf::animation::util::ReadOutputs::Rotations(outputs) => {
                    if let Some(value) = sample_quat_animation(
                        &inputs,
                        &outputs.into_f32().collect::<Vec<_>>(),
                        channel.sampler().interpolation(),
                        animation_time_sec,
                    ) {
                        entry.rotation = Some(value);
                    }
                }
                gltf::animation::util::ReadOutputs::Scales(outputs) => {
                    if let Some(value) = sample_vec3_animation(
                        &inputs,
                        &outputs.collect::<Vec<_>>(),
                        channel.sampler().interpolation(),
                        animation_time_sec,
                    ) {
                        entry.scale = Some(value);
                    }
                }
                gltf::animation::util::ReadOutputs::MorphTargetWeights(outputs) => {
                    let outputs = outputs.into_f32().collect::<Vec<_>>();
                    if let Some(value) = sample_morph_weights_animation(
                        &inputs,
                        &outputs,
                        channel.sampler().interpolation(),
                        animation_time_sec,
                    ) {
                        entry.weights = Some(value);
                    }
                }
            }
        }
    }
    overrides
}

fn sample_vec3_animation(
    inputs: &[f32],
    outputs: &[[f32; 3]],
    interpolation: gltf::animation::Interpolation,
    time_sec: f32,
) -> Option<[f32; 3]> {
    let (left, right, factor) = animation_sample_interval(inputs, time_sec)?;
    if matches!(interpolation, gltf::animation::Interpolation::CubicSpline) {
        return sample_cubic_vec3_animation(inputs, outputs, left, right, factor);
    }
    let left_value = animation_vec3_value(outputs, interpolation, left)?;
    let right_value = animation_vec3_value(outputs, interpolation, right)?;
    if matches!(interpolation, gltf::animation::Interpolation::Step) || left == right {
        Some(left_value)
    } else {
        Some(lerp_vec3(left_value, right_value, factor))
    }
}

fn sample_quat_animation(
    inputs: &[f32],
    outputs: &[[f32; 4]],
    interpolation: gltf::animation::Interpolation,
    time_sec: f32,
) -> Option<[f32; 4]> {
    let (left, right, factor) = animation_sample_interval(inputs, time_sec)?;
    if matches!(interpolation, gltf::animation::Interpolation::CubicSpline) {
        return sample_cubic_quat_animation(inputs, outputs, left, right, factor);
    }
    let left_value = normalize_quat(animation_quat_value(outputs, interpolation, left)?);
    let right_value = normalize_quat(animation_quat_value(outputs, interpolation, right)?);
    if matches!(interpolation, gltf::animation::Interpolation::Step) || left == right {
        Some(left_value)
    } else {
        Some(slerp_quat(left_value, right_value, factor))
    }
}

fn sample_morph_weights_animation(
    inputs: &[f32],
    outputs: &[f32],
    interpolation: gltf::animation::Interpolation,
    time_sec: f32,
) -> Option<Vec<f32>> {
    let (left, right, factor) = animation_sample_interval(inputs, time_sec)?;
    let values_per_keyframe = animation_values_per_keyframe(inputs, outputs, interpolation)?;
    if matches!(interpolation, gltf::animation::Interpolation::CubicSpline) {
        return sample_cubic_morph_weights_animation(
            inputs,
            outputs,
            left,
            right,
            factor,
            values_per_keyframe,
        );
    }
    let left_values =
        animation_morph_weights_value(outputs, interpolation, left, values_per_keyframe)?;
    let right_values =
        animation_morph_weights_value(outputs, interpolation, right, values_per_keyframe)?;
    if matches!(interpolation, gltf::animation::Interpolation::Step) || left == right {
        Some(left_values.to_vec())
    } else {
        Some(
            left_values
                .iter()
                .zip(right_values)
                .map(|(left, right)| left + (right - left) * factor)
                .collect(),
        )
    }
}

fn animation_values_per_keyframe(
    inputs: &[f32],
    outputs: &[f32],
    interpolation: gltf::animation::Interpolation,
) -> Option<usize> {
    if inputs.is_empty() {
        return None;
    }
    let divisor = if matches!(interpolation, gltf::animation::Interpolation::CubicSpline) {
        inputs.len() * 3
    } else {
        inputs.len()
    };
    if divisor == 0 || outputs.len() % divisor != 0 {
        return None;
    }
    Some(outputs.len() / divisor)
}

fn sample_cubic_vec3_animation(
    inputs: &[f32],
    outputs: &[[f32; 3]],
    left: usize,
    right: usize,
    factor: f32,
) -> Option<[f32; 3]> {
    let left_value = cubic_vec3_keyframe(outputs, left)?;
    if left == right {
        return Some(left_value.value);
    }
    let right_value = cubic_vec3_keyframe(outputs, right)?;
    let duration = inputs.get(right)? - inputs.get(left)?;
    Some(cubic_vec3(
        left_value.value,
        scale(left_value.out_tangent, duration),
        right_value.value,
        scale(right_value.in_tangent, duration),
        factor,
    ))
}

fn sample_cubic_quat_animation(
    inputs: &[f32],
    outputs: &[[f32; 4]],
    left: usize,
    right: usize,
    factor: f32,
) -> Option<[f32; 4]> {
    let left_value = cubic_quat_keyframe(outputs, left)?;
    if left == right {
        return Some(normalize_quat(left_value.value));
    }
    let right_value = cubic_quat_keyframe(outputs, right)?;
    let duration = inputs.get(right)? - inputs.get(left)?;
    Some(normalize_quat(cubic_quat(
        left_value.value,
        scale_quat(left_value.out_tangent, duration),
        right_value.value,
        scale_quat(right_value.in_tangent, duration),
        factor,
    )))
}

fn sample_cubic_morph_weights_animation(
    inputs: &[f32],
    outputs: &[f32],
    left: usize,
    right: usize,
    factor: f32,
    values_per_keyframe: usize,
) -> Option<Vec<f32>> {
    let left_value = cubic_morph_weights_keyframe(outputs, left, values_per_keyframe)?;
    if left == right {
        return Some(left_value.value.to_vec());
    }
    let right_value = cubic_morph_weights_keyframe(outputs, right, values_per_keyframe)?;
    let duration = inputs.get(right)? - inputs.get(left)?;
    Some(
        left_value
            .value
            .iter()
            .zip(left_value.out_tangent)
            .zip(right_value.value.iter().zip(right_value.in_tangent))
            .map(
                |((left_value, left_tangent), (right_value, right_tangent))| {
                    cubic_scalar(
                        *left_value,
                        *left_tangent * duration,
                        *right_value,
                        *right_tangent * duration,
                        factor,
                    )
                },
            )
            .collect(),
    )
}

fn cubic_vec3_keyframe(outputs: &[[f32; 3]], keyframe_index: usize) -> Option<CubicVec3Keyframe> {
    let offset = keyframe_index * 3;
    Some(CubicVec3Keyframe {
        in_tangent: *outputs.get(offset)?,
        value: *outputs.get(offset + 1)?,
        out_tangent: *outputs.get(offset + 2)?,
    })
}

fn cubic_quat_keyframe(outputs: &[[f32; 4]], keyframe_index: usize) -> Option<CubicQuatKeyframe> {
    let offset = keyframe_index * 3;
    Some(CubicQuatKeyframe {
        in_tangent: *outputs.get(offset)?,
        value: *outputs.get(offset + 1)?,
        out_tangent: *outputs.get(offset + 2)?,
    })
}

fn cubic_morph_weights_keyframe(
    outputs: &[f32],
    keyframe_index: usize,
    values_per_keyframe: usize,
) -> Option<CubicMorphWeightsKeyframe<'_>> {
    let offset = keyframe_index * 3 * values_per_keyframe;
    Some(CubicMorphWeightsKeyframe {
        in_tangent: outputs.get(offset..offset + values_per_keyframe)?,
        value: outputs.get(offset + values_per_keyframe..offset + values_per_keyframe * 2)?,
        out_tangent: outputs
            .get(offset + values_per_keyframe * 2..offset + values_per_keyframe * 3)?,
    })
}

fn cubic_vec3(
    left_value: [f32; 3],
    left_tangent: [f32; 3],
    right_value: [f32; 3],
    right_tangent: [f32; 3],
    factor: f32,
) -> [f32; 3] {
    [
        cubic_scalar(
            left_value[0],
            left_tangent[0],
            right_value[0],
            right_tangent[0],
            factor,
        ),
        cubic_scalar(
            left_value[1],
            left_tangent[1],
            right_value[1],
            right_tangent[1],
            factor,
        ),
        cubic_scalar(
            left_value[2],
            left_tangent[2],
            right_value[2],
            right_tangent[2],
            factor,
        ),
    ]
}

fn cubic_quat(
    left_value: [f32; 4],
    left_tangent: [f32; 4],
    right_value: [f32; 4],
    right_tangent: [f32; 4],
    factor: f32,
) -> [f32; 4] {
    [
        cubic_scalar(
            left_value[0],
            left_tangent[0],
            right_value[0],
            right_tangent[0],
            factor,
        ),
        cubic_scalar(
            left_value[1],
            left_tangent[1],
            right_value[1],
            right_tangent[1],
            factor,
        ),
        cubic_scalar(
            left_value[2],
            left_tangent[2],
            right_value[2],
            right_tangent[2],
            factor,
        ),
        cubic_scalar(
            left_value[3],
            left_tangent[3],
            right_value[3],
            right_tangent[3],
            factor,
        ),
    ]
}

fn cubic_scalar(
    left_value: f32,
    left_tangent: f32,
    right_value: f32,
    right_tangent: f32,
    factor: f32,
) -> f32 {
    let t2 = factor * factor;
    let t3 = t2 * factor;
    (2.0 * t3 - 3.0 * t2 + 1.0) * left_value
        + (t3 - 2.0 * t2 + factor) * left_tangent
        + (-2.0 * t3 + 3.0 * t2) * right_value
        + (t3 - t2) * right_tangent
}

fn animation_sample_interval(inputs: &[f32], time_sec: f32) -> Option<(usize, usize, f32)> {
    if inputs.is_empty() {
        return None;
    }
    if inputs.len() == 1 || time_sec <= inputs[0] {
        return Some((0, 0, 0.0));
    }
    for index in 0..inputs.len() - 1 {
        let left = inputs[index];
        let right = inputs[index + 1];
        if time_sec <= right {
            let factor = if (right - left).abs() <= f32::EPSILON {
                0.0
            } else {
                ((time_sec - left) / (right - left)).clamp(0.0, 1.0)
            };
            return Some((index, index + 1, factor));
        }
    }
    let last = inputs.len() - 1;
    Some((last, last, 0.0))
}

fn animation_vec3_value(
    outputs: &[[f32; 3]],
    interpolation: gltf::animation::Interpolation,
    keyframe_index: usize,
) -> Option<[f32; 3]> {
    let index = if matches!(interpolation, gltf::animation::Interpolation::CubicSpline) {
        keyframe_index * 3 + 1
    } else {
        keyframe_index
    };
    outputs.get(index).copied()
}

fn animation_quat_value(
    outputs: &[[f32; 4]],
    interpolation: gltf::animation::Interpolation,
    keyframe_index: usize,
) -> Option<[f32; 4]> {
    let index = if matches!(interpolation, gltf::animation::Interpolation::CubicSpline) {
        keyframe_index * 3 + 1
    } else {
        keyframe_index
    };
    outputs.get(index).copied()
}

fn animation_morph_weights_value(
    outputs: &[f32],
    interpolation: gltf::animation::Interpolation,
    keyframe_index: usize,
    values_per_keyframe: usize,
) -> Option<&[f32]> {
    let index = if matches!(interpolation, gltf::animation::Interpolation::CubicSpline) {
        keyframe_index * 3 + 1
    } else {
        keyframe_index
    };
    let start = index * values_per_keyframe;
    let end = start + values_per_keyframe;
    outputs.get(start..end)
}

fn gltf_node_transform_matrix(
    node: &gltf::Node<'_>,
    animation_overrides: &BTreeMap<usize, AnimatedNodeTransform>,
) -> [[f32; 4]; 4] {
    let Some(animated) = animation_overrides.get(&node.index()) else {
        return node.transform().matrix();
    };
    let (base_translation, base_rotation, base_scale) = node.transform().decomposed();
    gltf::scene::Transform::Decomposed {
        translation: animated.translation.unwrap_or(base_translation),
        rotation: animated.rotation.unwrap_or(base_rotation),
        scale: animated.scale.unwrap_or(base_scale),
    }
    .matrix()
}

fn load_gltf_node_data(
    node: &gltf::Node<'_>,
    parent_transform: [[f32; 4]; 4],
    context: &GltfLoadContext<'_>,
    mesh_data: &mut MeshData,
) {
    let node_transform = mat4_mul(
        parent_transform,
        gltf_node_transform_matrix(node, &context.animation_overrides),
    );

    if let Some(light) = node.light() {
        mesh_data
            .lights
            .push(render_gltf_light(&light, node_transform));
    }

    if let Some(mesh) = node.mesh() {
        let animated_weights = context
            .animation_overrides
            .get(&node.index())
            .and_then(|animated| animated.weights.as_deref());
        let weights = animated_weights
            .or_else(|| node.weights())
            .or_else(|| mesh.weights())
            .unwrap_or(&[]);
        let skin_context = node
            .skin()
            .as_ref()
            .map(|skin| load_skin_context(skin, context));
        for primitive in mesh.primitives() {
            if !is_renderable_triangle_mode(primitive.mode()) {
                continue;
            }
            load_gltf_primitive_triangles(
                &primitive,
                node_transform,
                context.buffers,
                weights,
                skin_context.as_ref(),
                context,
                &mut mesh_data.triangles,
            );
        }
    }

    for child in node.children() {
        load_gltf_node_data(&child, node_transform, context, mesh_data);
    }
}

fn load_skin_context(skin: &gltf::Skin<'_>, context: &GltfLoadContext<'_>) -> SkinContext {
    let inverse_bind_matrices = skin
        .reader(|buffer| context.buffers.get(buffer.index()).map(|data| &**data))
        .read_inverse_bind_matrices()
        .map(|matrices| matrices.collect::<Vec<_>>())
        .unwrap_or_default();
    let joint_matrices = skin
        .joints()
        .enumerate()
        .map(|(index, joint)| {
            let joint_world = context
                .node_world_transforms
                .get(&joint.index())
                .copied()
                .unwrap_or_else(mat4_identity);
            let inverse_bind = inverse_bind_matrices
                .get(index)
                .copied()
                .unwrap_or_else(mat4_identity);
            mat4_mul(joint_world, inverse_bind)
        })
        .collect();
    SkinContext { joint_matrices }
}

fn gltf_material_variant_names(document: &gltf::Document) -> BTreeMap<usize, String> {
    document
        .extension_value("KHR_materials_variants")
        .and_then(|extension| extension.get("variants"))
        .and_then(|variants| variants.as_array())
        .map(|variants| {
            variants
                .iter()
                .enumerate()
                .filter_map(|(index, variant)| {
                    Some((index, variant.get("name")?.as_str()?.to_string()))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn variant_mapping_matches_selected(
    mapping: &serde_json::Value,
    selected_variant: &str,
    variant_names: &BTreeMap<usize, String>,
) -> bool {
    mapping
        .get("variants")
        .and_then(|variants| variants.as_array())
        .is_some_and(|variants| {
            variants.iter().any(|variant| {
                variant
                    .as_u64()
                    .and_then(|index| usize::try_from(index).ok())
                    .and_then(|index| variant_names.get(&index))
                    .is_some_and(|name| name == selected_variant)
            })
        })
}

fn selected_gltf_primitive_material<'a>(
    document: &'a gltf::Document,
    primitive: &gltf::Primitive<'a>,
    selected_variant: Option<&str>,
    variant_names: &BTreeMap<usize, String>,
) -> gltf::Material<'a> {
    let Some(selected_variant) = selected_variant else {
        return primitive.material();
    };
    let Some(mappings) = primitive
        .extension_value("KHR_materials_variants")
        .and_then(|extension| extension.get("mappings"))
        .and_then(|mappings| mappings.as_array())
    else {
        return primitive.material();
    };

    mappings
        .iter()
        .find(|mapping| variant_mapping_matches_selected(mapping, selected_variant, variant_names))
        .and_then(|mapping| mapping.get("material"))
        .and_then(|material| material.as_u64())
        .and_then(|index| usize::try_from(index).ok())
        .and_then(|index| document.materials().nth(index))
        .unwrap_or_else(|| primitive.material())
}

fn load_gltf_primitive_triangles<'a>(
    primitive: &gltf::Primitive<'a>,
    transform: [[f32; 4]; 4],
    buffers: &[gltf::buffer::Data],
    weights: &[f32],
    skin_context: Option<&SkinContext>,
    context: &GltfLoadContext<'a>,
    triangles: &mut Vec<Triangle>,
) {
    let reader = primitive.reader(|buffer| buffers.get(buffer.index()).map(|data| &**data));
    let Some(positions) = reader.read_positions() else {
        return;
    };
    let mut positions = positions.collect::<Vec<_>>();
    let mut normals = reader
        .read_normals()
        .map(|normals| normals.collect::<Vec<_>>());
    let mut tangents = reader
        .read_tangents()
        .map(|tangents| tangents.collect::<Vec<_>>());
    apply_morph_targets(
        &reader,
        weights,
        &mut positions,
        normals.as_mut(),
        tangents.as_mut(),
    );
    apply_skinning(
        &reader,
        skin_context,
        transform,
        &mut positions,
        normals.as_mut(),
        tangents.as_mut(),
    );
    let positions = positions
        .into_iter()
        .map(|position| transform_point4(transform, position))
        .collect::<Vec<_>>();
    let normals = normals.map(|normals| {
        normals
            .into_iter()
            .map(|normal| normalize(transform_normal4(transform, normal)))
            .collect::<Vec<_>>()
    });
    let tangents = tangents.map(|tangents| {
        tangents
            .into_iter()
            .map(|tangent| {
                let direction = normalize(transform_direction4(
                    transform,
                    [tangent[0], tangent[1], tangent[2]],
                ));
                [direction[0], direction[1], direction[2], tangent[3]]
            })
            .collect::<Vec<_>>()
    });
    let texcoords = reader
        .read_tex_coords(0)
        .map(|texcoords| texcoords.into_f32().collect::<Vec<_>>());
    let texcoords1 = reader
        .read_tex_coords(1)
        .map(|texcoords| texcoords.into_f32().collect::<Vec<_>>());
    let vertex_colors = reader.read_colors(0).map(|colors| {
        colors
            .into_rgba_f32()
            .map(|color| VertexColorSample {
                rgba: material_color_rgba(color),
                linear_rgba: material_color_linear_rgba(color),
            })
            .collect::<Vec<_>>()
    });
    let material = selected_gltf_primitive_material(
        context.document,
        primitive,
        context.material_variant,
        &context.material_variant_names,
    );
    let pbr = material.pbr_metallic_roughness();
    let specular_glossiness = material.extension_value("KHR_materials_pbrSpecularGlossiness");
    let base_color_factor = pbr.base_color_factor();
    let mut color_rgba = material_color_rgba(base_color_factor);
    let mut color_linear_rgb = material_color_linear_rgb(base_color_factor);
    let mut color_alpha = material_color_alpha(base_color_factor);
    let base_color_texture = pbr.base_color_texture();
    let mut texture_index = base_color_texture
        .as_ref()
        .map(|info| info.texture().index());
    let mut texture_texcoord = base_color_texture
        .as_ref()
        .map(texture_info_tex_coord)
        .unwrap_or(0);
    let mut texture_transform = base_color_texture
        .as_ref()
        .map(texture_info_transform)
        .unwrap_or_default();
    let mut metallic_factor = pbr.metallic_factor();
    let mut roughness_factor = pbr.roughness_factor();
    let metallic_roughness_texture = pbr.metallic_roughness_texture();
    let mut metallic_roughness_texture_index = metallic_roughness_texture
        .as_ref()
        .map(|info| info.texture().index());
    let mut metallic_roughness_texture_texcoord = metallic_roughness_texture
        .as_ref()
        .map(texture_info_tex_coord)
        .unwrap_or(0);
    let mut metallic_roughness_texture_transform = metallic_roughness_texture
        .as_ref()
        .map(texture_info_transform)
        .unwrap_or_default();
    let (
        specular_glossiness_texture_index,
        specular_glossiness_texture_texcoord,
        specular_glossiness_texture_transform,
    ) = json_texture_info(specular_glossiness, "specularGlossinessTexture");
    if specular_glossiness.is_some() {
        let diffuse_factor =
            json_object_f32_quad(specular_glossiness, "diffuseFactor", [1.0, 1.0, 1.0, 1.0]);
        color_rgba = material_color_rgba(diffuse_factor);
        color_linear_rgb = material_color_linear_rgb(diffuse_factor);
        color_alpha = material_color_alpha(diffuse_factor);
        let (diffuse_texture_index, diffuse_texture_texcoord, diffuse_texture_transform) =
            json_texture_info(specular_glossiness, "diffuseTexture");
        if diffuse_texture_index.is_some() {
            texture_index = diffuse_texture_index;
            texture_texcoord = diffuse_texture_texcoord;
            texture_transform = diffuse_texture_transform;
        }
        metallic_factor = 0.0;
        roughness_factor =
            1.0 - json_object_f32(specular_glossiness, "glossinessFactor", 1.0).clamp(0.0, 1.0);
        metallic_roughness_texture_index = None;
        metallic_roughness_texture_texcoord = 0;
        metallic_roughness_texture_transform = TextureTransform2D::default();
    }
    let mut ior = material.ior().unwrap_or(1.5).max(1.0);
    let transmission = material.transmission();
    let transmission_factor = transmission
        .as_ref()
        .map(|transmission| transmission.transmission_factor())
        .unwrap_or(0.0);
    let transmission_texture = transmission
        .as_ref()
        .and_then(|transmission| transmission.transmission_texture());
    let transmission_texture_index = transmission_texture
        .as_ref()
        .map(|info| info.texture().index());
    let transmission_texture_texcoord = transmission_texture
        .as_ref()
        .map(texture_info_tex_coord)
        .unwrap_or(0);
    let transmission_texture_transform = transmission_texture
        .as_ref()
        .map(texture_info_transform)
        .unwrap_or_default();
    let diffuse_transmission = material.extension_value("KHR_materials_diffuse_transmission");
    let diffuse_transmission_factor =
        json_object_f32(diffuse_transmission, "diffuseTransmissionFactor", 0.0);
    let (
        diffuse_transmission_texture_index,
        diffuse_transmission_texture_texcoord,
        diffuse_transmission_texture_transform,
    ) = json_texture_info(diffuse_transmission, "diffuseTransmissionTexture");
    let diffuse_transmission_color_factor = json_object_f32_triplet(
        diffuse_transmission,
        "diffuseTransmissionColorFactor",
        [1.0, 1.0, 1.0],
    );
    let (
        diffuse_transmission_color_texture_index,
        diffuse_transmission_color_texture_texcoord,
        diffuse_transmission_color_texture_transform,
    ) = json_texture_info(diffuse_transmission, "diffuseTransmissionColorTexture");
    let dispersion = json_object_f32(
        material.extension_value("KHR_materials_dispersion"),
        "dispersion",
        0.0,
    )
    .max(0.0);
    let volume = material.volume();
    let volume_thickness_factor = volume
        .as_ref()
        .map(|volume| volume.thickness_factor())
        .unwrap_or(0.0);
    let volume_thickness_texture = volume
        .as_ref()
        .and_then(|volume| volume.thickness_texture());
    let volume_thickness_texture_index = volume_thickness_texture
        .as_ref()
        .map(|info| info.texture().index());
    let volume_thickness_texture_texcoord = volume_thickness_texture
        .as_ref()
        .map(texture_info_tex_coord)
        .unwrap_or(0);
    let volume_thickness_texture_transform = volume_thickness_texture
        .as_ref()
        .map(texture_info_transform)
        .unwrap_or_default();
    let volume_attenuation_distance = volume
        .as_ref()
        .map(|volume| volume.attenuation_distance())
        .unwrap_or(f32::INFINITY);
    let volume_attenuation_color = volume
        .as_ref()
        .map(|volume| volume.attenuation_color())
        .unwrap_or([1.0, 1.0, 1.0]);
    let clearcoat = material.extension_value("KHR_materials_clearcoat");
    let clearcoat_factor = json_object_f32(clearcoat, "clearcoatFactor", 0.0);
    let (clearcoat_texture_index, clearcoat_texture_texcoord, clearcoat_texture_transform) =
        json_texture_info(clearcoat, "clearcoatTexture");
    let clearcoat_roughness_factor = json_object_f32(clearcoat, "clearcoatRoughnessFactor", 0.0);
    let (
        clearcoat_roughness_texture_index,
        clearcoat_roughness_texture_texcoord,
        clearcoat_roughness_texture_transform,
    ) = json_texture_info(clearcoat, "clearcoatRoughnessTexture");
    let (
        clearcoat_normal_texture_index,
        clearcoat_normal_texture_texcoord,
        clearcoat_normal_texture_transform,
        clearcoat_normal_scale,
    ) = json_normal_texture_info(clearcoat, "clearcoatNormalTexture");
    let sheen = material.extension_value("KHR_materials_sheen");
    let sheen_color_factor = json_object_f32_triplet(sheen, "sheenColorFactor", [0.0, 0.0, 0.0]);
    let (sheen_color_texture_index, sheen_color_texture_texcoord, sheen_color_texture_transform) =
        json_texture_info(sheen, "sheenColorTexture");
    let sheen_roughness_factor = json_object_f32(sheen, "sheenRoughnessFactor", 0.0);
    let (
        sheen_roughness_texture_index,
        sheen_roughness_texture_texcoord,
        sheen_roughness_texture_transform,
    ) = json_texture_info(sheen, "sheenRoughnessTexture");
    let anisotropy = material.extension_value("KHR_materials_anisotropy");
    let anisotropy_strength = json_object_f32(anisotropy, "anisotropyStrength", 0.0);
    let anisotropy_rotation = json_object_f32(anisotropy, "anisotropyRotation", 0.0);
    let (anisotropy_texture_index, anisotropy_texture_texcoord, anisotropy_texture_transform) =
        json_texture_info(anisotropy, "anisotropyTexture");
    let iridescence = material.extension_value("KHR_materials_iridescence");
    let iridescence_factor = json_object_f32(iridescence, "iridescenceFactor", 0.0);
    let (iridescence_texture_index, iridescence_texture_texcoord, iridescence_texture_transform) =
        json_texture_info(iridescence, "iridescenceTexture");
    let iridescence_ior = json_object_f32(iridescence, "iridescenceIor", 1.3).max(1.0);
    let iridescence_thickness_minimum_nm =
        json_object_f32(iridescence, "iridescenceThicknessMinimum", 100.0).max(0.0);
    let iridescence_thickness_maximum_nm =
        json_object_f32(iridescence, "iridescenceThicknessMaximum", 400.0)
            .max(iridescence_thickness_minimum_nm);
    let (
        iridescence_thickness_texture_index,
        iridescence_thickness_texture_texcoord,
        iridescence_thickness_texture_transform,
    ) = json_texture_info(iridescence, "iridescenceThicknessTexture");
    let specular = material.specular();
    let mut specular_factor = specular
        .as_ref()
        .map(|specular| specular.specular_factor())
        .unwrap_or(1.0);
    let specular_texture = specular
        .as_ref()
        .and_then(|specular| specular.specular_texture());
    let mut specular_texture_index = specular_texture.as_ref().map(|info| info.texture().index());
    let mut specular_texture_texcoord = specular_texture
        .as_ref()
        .map(texture_info_tex_coord)
        .unwrap_or(0);
    let mut specular_texture_transform = specular_texture
        .as_ref()
        .map(texture_info_transform)
        .unwrap_or_default();
    let mut specular_color_factor = specular
        .as_ref()
        .map(|specular| specular.specular_color_factor())
        .unwrap_or([1.0, 1.0, 1.0]);
    let specular_color_texture = specular
        .as_ref()
        .and_then(|specular| specular.specular_color_texture());
    let mut specular_color_texture_index = specular_color_texture
        .as_ref()
        .map(|info| info.texture().index());
    let mut specular_color_texture_texcoord = specular_color_texture
        .as_ref()
        .map(texture_info_tex_coord)
        .unwrap_or(0);
    let mut specular_color_texture_transform = specular_color_texture
        .as_ref()
        .map(texture_info_transform)
        .unwrap_or_default();
    if specular_glossiness.is_some() {
        let legacy_specular =
            json_object_f32_triplet(specular_glossiness, "specularFactor", [1.0, 1.0, 1.0]);
        let max_f0 = legacy_specular
            .iter()
            .copied()
            .fold(0.0_f32, f32::max)
            .clamp(0.0, 0.99);
        ior = ior_from_dielectric_f0(max_f0);
        specular_factor = 1.0;
        specular_texture_index = None;
        specular_texture_texcoord = 0;
        specular_texture_transform = TextureTransform2D::default();
        specular_color_factor = if max_f0 > 1.0e-6 {
            [
                legacy_specular[0] / max_f0,
                legacy_specular[1] / max_f0,
                legacy_specular[2] / max_f0,
            ]
        } else {
            [0.0, 0.0, 0.0]
        };
        if specular_glossiness_texture_index.is_some() {
            specular_color_texture_index = specular_glossiness_texture_index;
            specular_color_texture_texcoord = specular_glossiness_texture_texcoord;
            specular_color_texture_transform = specular_glossiness_texture_transform;
        }
    }
    let emissive_rgb = material_emissive_rgb(
        material.emissive_factor(),
        material.emissive_strength().unwrap_or(1.0),
    );
    let emissive_texture = material.emissive_texture();
    let emissive_texture_index = emissive_texture.as_ref().map(|info| info.texture().index());
    let emissive_texture_texcoord = emissive_texture
        .as_ref()
        .map(texture_info_tex_coord)
        .unwrap_or(0);
    let emissive_texture_transform = emissive_texture
        .as_ref()
        .map(texture_info_transform)
        .unwrap_or_default();
    let occlusion_texture = material.occlusion_texture();
    let occlusion_texture_index = occlusion_texture
        .as_ref()
        .map(|info| info.texture().index());
    let occlusion_texture_texcoord = occlusion_texture
        .as_ref()
        .map(occlusion_texture_tex_coord)
        .unwrap_or(0);
    let occlusion_texture_transform = occlusion_texture
        .as_ref()
        .map(occlusion_texture_transform)
        .unwrap_or_default();
    let occlusion_strength = occlusion_texture
        .as_ref()
        .map(|info| info.strength())
        .unwrap_or(1.0);
    let normal_texture = material.normal_texture();
    let normal_texture_index = normal_texture.as_ref().map(|info| info.texture().index());
    let normal_texture_texcoord = normal_texture
        .as_ref()
        .map(normal_texture_tex_coord)
        .unwrap_or(0);
    let normal_texture_transform = normal_texture
        .as_ref()
        .map(normal_texture_transform)
        .unwrap_or_default();
    let normal_scale = normal_texture
        .as_ref()
        .map(|info| info.scale())
        .unwrap_or(1.0);
    let unlit = material.unlit();
    let alpha_cutoff = material_alpha_cutoff(&material);
    let alpha_mode = material_alpha_mode(&material);
    let double_sided = material.double_sided();

    let vertex_indices = reader
        .read_indices()
        .map(|indices| {
            indices
                .into_u32()
                .filter_map(|index| usize::try_from(index).ok())
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| (0..positions.len()).collect());
    for [a_index, b_index, c_index] in triangle_index_triplets(primitive.mode(), &vertex_indices) {
        if let (Some(a), Some(b), Some(c)) = (
            positions.get(a_index),
            positions.get(b_index),
            positions.get(c_index),
        ) {
            let normals = normals.as_ref().and_then(|normals| {
                Some((
                    *normals.get(a_index)?,
                    *normals.get(b_index)?,
                    *normals.get(c_index)?,
                ))
            });
            let texcoords = texcoords.as_ref().and_then(|texcoords| {
                Some((
                    *texcoords.get(a_index)?,
                    *texcoords.get(b_index)?,
                    *texcoords.get(c_index)?,
                ))
            });
            let texcoords1 = texcoords1.as_ref().and_then(|texcoords| {
                Some((
                    *texcoords.get(a_index)?,
                    *texcoords.get(b_index)?,
                    *texcoords.get(c_index)?,
                ))
            });
            let tangents = tangents.as_ref().and_then(|tangents| {
                Some((
                    *tangents.get(a_index)?,
                    *tangents.get(b_index)?,
                    *tangents.get(c_index)?,
                ))
            });
            let vertex_colors = vertex_colors.as_ref().and_then(|vertex_colors| {
                Some((
                    *vertex_colors.get(a_index)?,
                    *vertex_colors.get(b_index)?,
                    *vertex_colors.get(c_index)?,
                ))
            });
            triangles.push(make_triangle(
                *a,
                *b,
                *c,
                normals,
                color_rgba,
                color_linear_rgb,
                color_alpha,
                vertex_colors,
                texcoords,
                texcoords1,
                tangents,
                texture_index,
                texture_texcoord,
                texture_transform,
                metallic_factor,
                roughness_factor,
                metallic_roughness_texture_index,
                metallic_roughness_texture_texcoord,
                metallic_roughness_texture_transform,
                specular_glossiness_texture_index,
                specular_glossiness_texture_texcoord,
                specular_glossiness_texture_transform,
                ior,
                transmission_factor,
                transmission_texture_index,
                transmission_texture_texcoord,
                transmission_texture_transform,
                diffuse_transmission_factor,
                diffuse_transmission_texture_index,
                diffuse_transmission_texture_texcoord,
                diffuse_transmission_texture_transform,
                diffuse_transmission_color_factor,
                diffuse_transmission_color_texture_index,
                diffuse_transmission_color_texture_texcoord,
                diffuse_transmission_color_texture_transform,
                dispersion,
                volume_thickness_factor,
                volume_thickness_texture_index,
                volume_thickness_texture_texcoord,
                volume_thickness_texture_transform,
                volume_attenuation_distance,
                volume_attenuation_color,
                clearcoat_factor,
                clearcoat_texture_index,
                clearcoat_texture_texcoord,
                clearcoat_texture_transform,
                clearcoat_roughness_factor,
                clearcoat_roughness_texture_index,
                clearcoat_roughness_texture_texcoord,
                clearcoat_roughness_texture_transform,
                clearcoat_normal_texture_index,
                clearcoat_normal_texture_texcoord,
                clearcoat_normal_texture_transform,
                clearcoat_normal_scale,
                sheen_color_factor,
                sheen_color_texture_index,
                sheen_color_texture_texcoord,
                sheen_color_texture_transform,
                sheen_roughness_factor,
                sheen_roughness_texture_index,
                sheen_roughness_texture_texcoord,
                sheen_roughness_texture_transform,
                anisotropy_strength,
                anisotropy_rotation,
                anisotropy_texture_index,
                anisotropy_texture_texcoord,
                anisotropy_texture_transform,
                iridescence_factor,
                iridescence_texture_index,
                iridescence_texture_texcoord,
                iridescence_texture_transform,
                iridescence_ior,
                iridescence_thickness_minimum_nm,
                iridescence_thickness_maximum_nm,
                iridescence_thickness_texture_index,
                iridescence_thickness_texture_texcoord,
                iridescence_thickness_texture_transform,
                specular_factor,
                specular_texture_index,
                specular_texture_texcoord,
                specular_texture_transform,
                specular_color_factor,
                specular_color_texture_index,
                specular_color_texture_texcoord,
                specular_color_texture_transform,
                normal_texture_index,
                normal_texture_texcoord,
                normal_texture_transform,
                normal_scale,
                emissive_rgb,
                emissive_texture_index,
                emissive_texture_texcoord,
                emissive_texture_transform,
                occlusion_texture_index,
                occlusion_texture_texcoord,
                occlusion_texture_transform,
                occlusion_strength,
                unlit,
                alpha_cutoff,
                alpha_mode,
                double_sided,
            ));
        }
    }
}

fn apply_skinning<'a, 's, F>(
    reader: &gltf::mesh::Reader<'a, 's, F>,
    skin_context: Option<&SkinContext>,
    mesh_transform: [[f32; 4]; 4],
    positions: &mut [[f32; 3]],
    mut normals: Option<&mut Vec<[f32; 3]>>,
    mut tangents: Option<&mut Vec<[f32; 4]>>,
) where
    F: Clone + Fn(gltf::Buffer<'a>) -> Option<&'s [u8]>,
{
    let Some(skin_context) = skin_context else {
        return;
    };
    if skin_context.joint_matrices.is_empty() {
        return;
    }
    let Some(joints) = reader.read_joints(0).map(|joints| {
        joints
            .into_u16()
            .map(|joint| {
                [
                    usize::from(joint[0]),
                    usize::from(joint[1]),
                    usize::from(joint[2]),
                    usize::from(joint[3]),
                ]
            })
            .collect::<Vec<_>>()
    }) else {
        return;
    };
    let Some(weights) = reader
        .read_weights(0)
        .map(|weights| weights.into_f32().collect::<Vec<_>>())
    else {
        return;
    };
    let mesh_inverse_transform = mat4_inverse(mesh_transform).unwrap_or_else(mat4_identity);
    let joint_matrices = skin_context
        .joint_matrices
        .iter()
        .map(|matrix| mat4_mul(mesh_inverse_transform, *matrix))
        .collect::<Vec<_>>();

    for (index, position) in positions.iter_mut().enumerate() {
        let Some(joints) = joints.get(index) else {
            continue;
        };
        let Some(weights) = weights.get(index) else {
            continue;
        };
        if let Some(skinned) = skin_point(*position, *joints, *weights, &joint_matrices) {
            *position = skinned;
        }
    }

    if let Some(normals) = normals.as_deref_mut() {
        for (index, normal) in normals.iter_mut().enumerate() {
            let Some(joints) = joints.get(index) else {
                continue;
            };
            let Some(weights) = weights.get(index) else {
                continue;
            };
            if let Some(skinned) = skin_direction(*normal, *joints, *weights, &joint_matrices) {
                *normal = skinned;
            }
        }
    }

    if let Some(tangents) = tangents.as_deref_mut() {
        for (index, tangent) in tangents.iter_mut().enumerate() {
            let Some(joints) = joints.get(index) else {
                continue;
            };
            let Some(weights) = weights.get(index) else {
                continue;
            };
            if let Some(skinned) = skin_direction(
                [tangent[0], tangent[1], tangent[2]],
                *joints,
                *weights,
                &joint_matrices,
            ) {
                tangent[0] = skinned[0];
                tangent[1] = skinned[1];
                tangent[2] = skinned[2];
            }
        }
    }
}

fn skin_point(
    point: [f32; 3],
    joints: [usize; 4],
    weights: [f32; 4],
    joint_matrices: &[[[f32; 4]; 4]],
) -> Option<[f32; 3]> {
    let mut skinned = [0.0; 3];
    let mut total_weight = 0.0_f32;
    for lane in 0..4 {
        let weight = weights[lane];
        if weight.abs() <= f32::EPSILON {
            continue;
        }
        let Some(matrix) = joint_matrices.get(joints[lane]) else {
            continue;
        };
        let transformed = transform_point4(*matrix, point);
        skinned[0] += transformed[0] * weight;
        skinned[1] += transformed[1] * weight;
        skinned[2] += transformed[2] * weight;
        total_weight += weight;
    }
    (total_weight > f32::EPSILON).then_some(skinned)
}

fn skin_direction(
    direction: [f32; 3],
    joints: [usize; 4],
    weights: [f32; 4],
    joint_matrices: &[[[f32; 4]; 4]],
) -> Option<[f32; 3]> {
    let mut skinned = [0.0; 3];
    let mut total_weight = 0.0_f32;
    for lane in 0..4 {
        let weight = weights[lane];
        if weight.abs() <= f32::EPSILON {
            continue;
        }
        let Some(matrix) = joint_matrices.get(joints[lane]) else {
            continue;
        };
        let transformed = transform_direction4(*matrix, direction);
        skinned[0] += transformed[0] * weight;
        skinned[1] += transformed[1] * weight;
        skinned[2] += transformed[2] * weight;
        total_weight += weight;
    }
    (total_weight > f32::EPSILON).then(|| normalize(skinned))
}

fn apply_morph_targets<'a, 's, F>(
    reader: &gltf::mesh::Reader<'a, 's, F>,
    weights: &[f32],
    positions: &mut [[f32; 3]],
    mut normals: Option<&mut Vec<[f32; 3]>>,
    mut tangents: Option<&mut Vec<[f32; 4]>>,
) where
    F: Clone + Fn(gltf::Buffer<'a>) -> Option<&'s [u8]>,
{
    if weights.is_empty() {
        return;
    }

    for (target_index, (target_positions, target_normals, target_tangents)) in
        reader.read_morph_targets().enumerate()
    {
        let weight = weights.get(target_index).copied().unwrap_or(0.0);
        if weight.abs() <= f32::EPSILON {
            continue;
        }

        if let Some(target_positions) = target_positions {
            for (position, delta) in positions.iter_mut().zip(target_positions) {
                position[0] += delta[0] * weight;
                position[1] += delta[1] * weight;
                position[2] += delta[2] * weight;
            }
        }

        if let (Some(normals), Some(target_normals)) = (normals.as_deref_mut(), target_normals) {
            for (normal, delta) in normals.iter_mut().zip(target_normals) {
                normal[0] += delta[0] * weight;
                normal[1] += delta[1] * weight;
                normal[2] += delta[2] * weight;
            }
        }

        if let (Some(tangents), Some(target_tangents)) = (tangents.as_deref_mut(), target_tangents)
        {
            for (tangent, delta) in tangents.iter_mut().zip(target_tangents) {
                tangent[0] += delta[0] * weight;
                tangent[1] += delta[1] * weight;
                tangent[2] += delta[2] * weight;
            }
        }
    }

    if let Some(normals) = normals {
        for normal in normals {
            *normal = normalize(*normal);
        }
    }
    if let Some(tangents) = tangents {
        for tangent in tangents {
            let direction = normalize([tangent[0], tangent[1], tangent[2]]);
            tangent[0] = direction[0];
            tangent[1] = direction[1];
            tangent[2] = direction[2];
        }
    }
}

fn is_renderable_triangle_mode(mode: Mode) -> bool {
    matches!(
        mode,
        Mode::Triangles | Mode::TriangleStrip | Mode::TriangleFan
    )
}

fn triangle_index_triplets(mode: Mode, vertices: &[usize]) -> Vec<[usize; 3]> {
    match mode {
        Mode::Triangles => vertices
            .chunks_exact(3)
            .filter_map(|chunk| nondegenerate_triangle([chunk[0], chunk[1], chunk[2]]))
            .collect(),
        Mode::TriangleStrip => vertices
            .windows(3)
            .enumerate()
            .filter_map(|(index, window)| {
                let triangle = if index % 2 == 0 {
                    [window[0], window[1], window[2]]
                } else {
                    [window[1], window[0], window[2]]
                };
                nondegenerate_triangle(triangle)
            })
            .collect(),
        Mode::TriangleFan => {
            let Some(first) = vertices.first().copied() else {
                return Vec::new();
            };
            vertices[1..]
                .windows(2)
                .filter_map(|window| nondegenerate_triangle([first, window[0], window[1]]))
                .collect()
        }
        _ => Vec::new(),
    }
}

fn nondegenerate_triangle(triangle: [usize; 3]) -> Option<[usize; 3]> {
    if triangle[0] == triangle[1] || triangle[1] == triangle[2] || triangle[0] == triangle[2] {
        None
    } else {
        Some(triangle)
    }
}

fn texture_image_from_gltf(image: gltf::image::Data) -> TextureImage {
    TextureImage {
        width: image.width,
        height: image.height,
        rgba: texture_pixels_to_rgba(image.pixels, image.format),
        wrap_s: TextureWrap::Repeat,
        wrap_t: TextureWrap::Repeat,
        filter: TextureFilter::Nearest,
    }
}

fn texture_image_with_sampler(
    mut image: TextureImage,
    sampler: gltf::texture::Sampler<'_>,
) -> TextureImage {
    image.wrap_s = texture_wrap_from_gltf(sampler.wrap_s());
    image.wrap_t = texture_wrap_from_gltf(sampler.wrap_t());
    image.filter = texture_filter_from_gltf(sampler.mag_filter(), sampler.min_filter());
    image
}

fn texture_wrap_from_gltf(wrap: gltf::texture::WrappingMode) -> TextureWrap {
    match wrap {
        gltf::texture::WrappingMode::ClampToEdge => TextureWrap::ClampToEdge,
        gltf::texture::WrappingMode::MirroredRepeat => TextureWrap::MirroredRepeat,
        gltf::texture::WrappingMode::Repeat => TextureWrap::Repeat,
    }
}

fn texture_filter_from_gltf(
    mag_filter: Option<gltf::texture::MagFilter>,
    min_filter: Option<gltf::texture::MinFilter>,
) -> TextureFilter {
    if mag_filter.is_none() && min_filter.is_none() {
        TextureFilter::Linear
    } else if matches!(mag_filter, Some(gltf::texture::MagFilter::Linear))
        || matches!(
            min_filter,
            Some(
                gltf::texture::MinFilter::Linear
                    | gltf::texture::MinFilter::LinearMipmapNearest
                    | gltf::texture::MinFilter::LinearMipmapLinear
            )
        )
    {
        TextureFilter::Linear
    } else {
        TextureFilter::Nearest
    }
}

fn texture_pixels_to_rgba(pixels: Vec<u8>, format: gltf::image::Format) -> Vec<u8> {
    match format {
        gltf::image::Format::R8 => pixels
            .into_iter()
            .flat_map(|r| [r, r, r, 255])
            .collect::<Vec<_>>(),
        gltf::image::Format::R8G8 => pixels
            .chunks_exact(2)
            .flat_map(|chunk| [chunk[0], chunk[0], chunk[0], chunk[1]])
            .collect::<Vec<_>>(),
        gltf::image::Format::R8G8B8 => pixels
            .chunks_exact(3)
            .flat_map(|chunk| [chunk[0], chunk[1], chunk[2], 255])
            .collect::<Vec<_>>(),
        gltf::image::Format::R8G8B8A8 => pixels,
        gltf::image::Format::R16 => pixels
            .chunks_exact(2)
            .flat_map(|chunk| {
                let r = chunk[1];
                [r, r, r, 255]
            })
            .collect::<Vec<_>>(),
        gltf::image::Format::R16G16 => pixels
            .chunks_exact(4)
            .flat_map(|chunk| {
                let r = chunk[1];
                [r, r, r, chunk[3]]
            })
            .collect::<Vec<_>>(),
        gltf::image::Format::R16G16B16 => pixels
            .chunks_exact(6)
            .flat_map(|chunk| [chunk[1], chunk[3], chunk[5], 255])
            .collect::<Vec<_>>(),
        gltf::image::Format::R16G16B16A16 => pixels
            .chunks_exact(8)
            .flat_map(|chunk| [chunk[1], chunk[3], chunk[5], chunk[7]])
            .collect::<Vec<_>>(),
        gltf::image::Format::R32G32B32FLOAT => pixels
            .chunks_exact(12)
            .flat_map(|chunk| {
                [
                    f32_color_bytes_to_u8(&chunk[0..4]),
                    f32_color_bytes_to_u8(&chunk[4..8]),
                    f32_color_bytes_to_u8(&chunk[8..12]),
                    255,
                ]
            })
            .collect::<Vec<_>>(),
        gltf::image::Format::R32G32B32A32FLOAT => pixels
            .chunks_exact(16)
            .flat_map(|chunk| {
                [
                    f32_color_bytes_to_u8(&chunk[0..4]),
                    f32_color_bytes_to_u8(&chunk[4..8]),
                    f32_color_bytes_to_u8(&chunk[8..12]),
                    f32_color_bytes_to_u8(&chunk[12..16]),
                ]
            })
            .collect::<Vec<_>>(),
    }
}

fn f32_color_bytes_to_u8(bytes: &[u8]) -> u8 {
    let Ok(bytes) = <[u8; 4]>::try_from(bytes) else {
        return 0;
    };
    float_color_to_u8(f32::from_le_bytes(bytes))
}

fn material_color_rgba(base_color: [f32; 4]) -> [u8; 4] {
    [
        float_color_to_u8(base_color[0]),
        float_color_to_u8(base_color[1]),
        float_color_to_u8(base_color[2]),
        float_color_to_u8(base_color[3]),
    ]
}

fn material_color_linear_rgb(base_color: [f32; 4]) -> [f32; 3] {
    [
        base_color[0].clamp(0.0, 1.0),
        base_color[1].clamp(0.0, 1.0),
        base_color[2].clamp(0.0, 1.0),
    ]
}

fn material_color_alpha(base_color: [f32; 4]) -> f32 {
    base_color[3].clamp(0.0, 1.0)
}

fn material_color_linear_rgba(base_color: [f32; 4]) -> [f32; 4] {
    [
        base_color[0].clamp(0.0, 1.0),
        base_color[1].clamp(0.0, 1.0),
        base_color[2].clamp(0.0, 1.0),
        base_color[3].clamp(0.0, 1.0),
    ]
}

fn ior_from_dielectric_f0(f0: f32) -> f32 {
    let sqrt_f0 = f0.clamp(0.0, 0.99).sqrt();
    ((1.0 + sqrt_f0) / (1.0 - sqrt_f0)).max(1.0)
}

fn material_emissive_rgb(base_color: [f32; 3], strength: f32) -> [f32; 3] {
    [
        base_color[0].max(0.0) * strength.max(0.0) * 255.0,
        base_color[1].max(0.0) * strength.max(0.0) * 255.0,
        base_color[2].max(0.0) * strength.max(0.0) * 255.0,
    ]
}

fn texture_info_tex_coord(info: &gltf::texture::Info<'_>) -> u32 {
    info.texture_transform()
        .and_then(|transform| transform.tex_coord())
        .unwrap_or_else(|| info.tex_coord())
}

fn texture_info_transform(info: &gltf::texture::Info<'_>) -> TextureTransform2D {
    info.texture_transform()
        .map(|transform| TextureTransform2D {
            offset: transform.offset(),
            rotation: transform.rotation(),
            scale: transform.scale(),
        })
        .unwrap_or_default()
}

fn normal_texture_tex_coord(info: &gltf::material::NormalTexture<'_>) -> u32 {
    texture_transform_from_extension_value(info.extension_value("KHR_texture_transform"))
        .and_then(|(_, tex_coord)| tex_coord)
        .unwrap_or_else(|| info.tex_coord())
}

fn normal_texture_transform(info: &gltf::material::NormalTexture<'_>) -> TextureTransform2D {
    texture_transform_from_extension_value(info.extension_value("KHR_texture_transform"))
        .map(|(transform, _)| transform)
        .unwrap_or_default()
}

fn occlusion_texture_tex_coord(info: &gltf::material::OcclusionTexture<'_>) -> u32 {
    texture_transform_from_extension_value(info.extension_value("KHR_texture_transform"))
        .and_then(|(_, tex_coord)| tex_coord)
        .unwrap_or_else(|| info.tex_coord())
}

fn occlusion_texture_transform(info: &gltf::material::OcclusionTexture<'_>) -> TextureTransform2D {
    texture_transform_from_extension_value(info.extension_value("KHR_texture_transform"))
        .map(|(transform, _)| transform)
        .unwrap_or_default()
}

fn texture_transform_from_extension_value(
    value: Option<&serde_json::Value>,
) -> Option<(TextureTransform2D, Option<u32>)> {
    let object = value?.as_object()?;
    let offset = json_f32_pair(object.get("offset")).unwrap_or([0.0, 0.0]);
    let rotation = object
        .get("rotation")
        .and_then(|value| value.as_f64())
        .unwrap_or(0.0) as f32;
    let scale = json_f32_pair(object.get("scale")).unwrap_or([1.0, 1.0]);
    let tex_coord = object
        .get("texCoord")
        .and_then(|value| value.as_u64())
        .and_then(|value| u32::try_from(value).ok());
    Some((
        TextureTransform2D {
            offset,
            rotation,
            scale,
        },
        tex_coord,
    ))
}

fn json_object_f32(value: Option<&serde_json::Value>, key: &str, default: f32) -> f32 {
    value
        .and_then(|value| value.as_object())
        .and_then(|object| object.get(key))
        .and_then(|value| value.as_f64())
        .map(|value| value as f32)
        .unwrap_or(default)
}

fn json_object_f32_triplet(
    value: Option<&serde_json::Value>,
    key: &str,
    default: [f32; 3],
) -> [f32; 3] {
    let Some(array) = value
        .and_then(|value| value.as_object())
        .and_then(|object| object.get(key))
        .and_then(|value| value.as_array())
    else {
        return default;
    };
    [
        array
            .first()
            .and_then(|value| value.as_f64())
            .map(|value| value as f32)
            .unwrap_or(default[0]),
        array
            .get(1)
            .and_then(|value| value.as_f64())
            .map(|value| value as f32)
            .unwrap_or(default[1]),
        array
            .get(2)
            .and_then(|value| value.as_f64())
            .map(|value| value as f32)
            .unwrap_or(default[2]),
    ]
}

fn json_object_f32_quad(
    value: Option<&serde_json::Value>,
    key: &str,
    default: [f32; 4],
) -> [f32; 4] {
    let Some(array) = value
        .and_then(|value| value.as_object())
        .and_then(|object| object.get(key))
        .and_then(|value| value.as_array())
    else {
        return default;
    };
    [
        array
            .first()
            .and_then(|value| value.as_f64())
            .map(|value| value as f32)
            .unwrap_or(default[0]),
        array
            .get(1)
            .and_then(|value| value.as_f64())
            .map(|value| value as f32)
            .unwrap_or(default[1]),
        array
            .get(2)
            .and_then(|value| value.as_f64())
            .map(|value| value as f32)
            .unwrap_or(default[2]),
        array
            .get(3)
            .and_then(|value| value.as_f64())
            .map(|value| value as f32)
            .unwrap_or(default[3]),
    ]
}

fn json_texture_info(
    extension: Option<&serde_json::Value>,
    key: &str,
) -> (Option<usize>, u32, TextureTransform2D) {
    let Some(texture_info) = extension
        .and_then(|value| value.as_object())
        .and_then(|object| object.get(key))
        .and_then(|value| value.as_object())
    else {
        return (None, 0, TextureTransform2D::default());
    };
    let texture_index = texture_info
        .get("index")
        .and_then(|value| value.as_u64())
        .and_then(|value| usize::try_from(value).ok());
    let mut texcoord = texture_info
        .get("texCoord")
        .and_then(|value| value.as_u64())
        .and_then(|value| u32::try_from(value).ok())
        .unwrap_or(0);
    let mut transform = TextureTransform2D::default();
    if let Some((texture_transform, transform_texcoord)) = texture_transform_from_extension_value(
        texture_info.get("extensions").and_then(|extensions| {
            extensions
                .as_object()
                .and_then(|extensions| extensions.get("KHR_texture_transform"))
        }),
    ) {
        transform = texture_transform;
        if let Some(transform_texcoord) = transform_texcoord {
            texcoord = transform_texcoord;
        }
    }
    (texture_index, texcoord, transform)
}

fn json_normal_texture_info(
    extension: Option<&serde_json::Value>,
    key: &str,
) -> (Option<usize>, u32, TextureTransform2D, f32) {
    let (texture_index, texcoord, transform) = json_texture_info(extension, key);
    let scale = extension
        .and_then(|value| value.as_object())
        .and_then(|object| object.get(key))
        .and_then(|value| value.as_object())
        .and_then(|texture_info| texture_info.get("scale"))
        .and_then(|value| value.as_f64())
        .map(|value| value as f32)
        .unwrap_or(1.0);
    (texture_index, texcoord, transform, scale)
}

fn json_f32_pair(value: Option<&serde_json::Value>) -> Option<[f32; 2]> {
    let array = value?.as_array()?;
    Some([
        array.first()?.as_f64()? as f32,
        array.get(1)?.as_f64()? as f32,
    ])
}

fn material_alpha_cutoff(material: &gltf::Material<'_>) -> Option<f32> {
    match material.alpha_mode() {
        gltf::material::AlphaMode::Opaque => None,
        gltf::material::AlphaMode::Mask => Some(material.alpha_cutoff().unwrap_or(0.5)),
        gltf::material::AlphaMode::Blend => Some(1.0 / 255.0),
    }
}

fn material_alpha_mode(material: &gltf::Material<'_>) -> MaterialAlphaMode {
    match material.alpha_mode() {
        gltf::material::AlphaMode::Opaque => MaterialAlphaMode::Opaque,
        gltf::material::AlphaMode::Mask => MaterialAlphaMode::Mask,
        gltf::material::AlphaMode::Blend => MaterialAlphaMode::Blend,
    }
}

fn float_color_to_u8(value: f32) -> u8 {
    (value.clamp(0.0, 1.0) * 255.0).round() as u8
}

fn make_triangle(
    a: [f32; 3],
    b: [f32; 3],
    c: [f32; 3],
    normals: Option<([f32; 3], [f32; 3], [f32; 3])>,
    color_rgba: [u8; 4],
    color_linear_rgb: [f32; 3],
    color_alpha: f32,
    vertex_colors: Option<(VertexColorSample, VertexColorSample, VertexColorSample)>,
    texcoords: Option<([f32; 2], [f32; 2], [f32; 2])>,
    texcoords1: Option<([f32; 2], [f32; 2], [f32; 2])>,
    tangents: Option<([f32; 4], [f32; 4], [f32; 4])>,
    texture_index: Option<usize>,
    texture_texcoord: u32,
    texture_transform: TextureTransform2D,
    metallic_factor: f32,
    roughness_factor: f32,
    metallic_roughness_texture_index: Option<usize>,
    metallic_roughness_texture_texcoord: u32,
    metallic_roughness_texture_transform: TextureTransform2D,
    specular_glossiness_texture_index: Option<usize>,
    specular_glossiness_texture_texcoord: u32,
    specular_glossiness_texture_transform: TextureTransform2D,
    ior: f32,
    transmission_factor: f32,
    transmission_texture_index: Option<usize>,
    transmission_texture_texcoord: u32,
    transmission_texture_transform: TextureTransform2D,
    diffuse_transmission_factor: f32,
    diffuse_transmission_texture_index: Option<usize>,
    diffuse_transmission_texture_texcoord: u32,
    diffuse_transmission_texture_transform: TextureTransform2D,
    diffuse_transmission_color_factor: [f32; 3],
    diffuse_transmission_color_texture_index: Option<usize>,
    diffuse_transmission_color_texture_texcoord: u32,
    diffuse_transmission_color_texture_transform: TextureTransform2D,
    dispersion: f32,
    volume_thickness_factor: f32,
    volume_thickness_texture_index: Option<usize>,
    volume_thickness_texture_texcoord: u32,
    volume_thickness_texture_transform: TextureTransform2D,
    volume_attenuation_distance: f32,
    volume_attenuation_color: [f32; 3],
    clearcoat_factor: f32,
    clearcoat_texture_index: Option<usize>,
    clearcoat_texture_texcoord: u32,
    clearcoat_texture_transform: TextureTransform2D,
    clearcoat_roughness_factor: f32,
    clearcoat_roughness_texture_index: Option<usize>,
    clearcoat_roughness_texture_texcoord: u32,
    clearcoat_roughness_texture_transform: TextureTransform2D,
    clearcoat_normal_texture_index: Option<usize>,
    clearcoat_normal_texture_texcoord: u32,
    clearcoat_normal_texture_transform: TextureTransform2D,
    clearcoat_normal_scale: f32,
    sheen_color_factor: [f32; 3],
    sheen_color_texture_index: Option<usize>,
    sheen_color_texture_texcoord: u32,
    sheen_color_texture_transform: TextureTransform2D,
    sheen_roughness_factor: f32,
    sheen_roughness_texture_index: Option<usize>,
    sheen_roughness_texture_texcoord: u32,
    sheen_roughness_texture_transform: TextureTransform2D,
    anisotropy_strength: f32,
    anisotropy_rotation: f32,
    anisotropy_texture_index: Option<usize>,
    anisotropy_texture_texcoord: u32,
    anisotropy_texture_transform: TextureTransform2D,
    iridescence_factor: f32,
    iridescence_texture_index: Option<usize>,
    iridescence_texture_texcoord: u32,
    iridescence_texture_transform: TextureTransform2D,
    iridescence_ior: f32,
    iridescence_thickness_minimum_nm: f32,
    iridescence_thickness_maximum_nm: f32,
    iridescence_thickness_texture_index: Option<usize>,
    iridescence_thickness_texture_texcoord: u32,
    iridescence_thickness_texture_transform: TextureTransform2D,
    specular_factor: f32,
    specular_texture_index: Option<usize>,
    specular_texture_texcoord: u32,
    specular_texture_transform: TextureTransform2D,
    specular_color_factor: [f32; 3],
    specular_color_texture_index: Option<usize>,
    specular_color_texture_texcoord: u32,
    specular_color_texture_transform: TextureTransform2D,
    normal_texture_index: Option<usize>,
    normal_texture_texcoord: u32,
    normal_texture_transform: TextureTransform2D,
    normal_scale: f32,
    emissive_rgb: [f32; 3],
    emissive_texture_index: Option<usize>,
    emissive_texture_texcoord: u32,
    emissive_texture_transform: TextureTransform2D,
    occlusion_texture_index: Option<usize>,
    occlusion_texture_texcoord: u32,
    occlusion_texture_transform: TextureTransform2D,
    occlusion_strength: f32,
    unlit: bool,
    alpha_cutoff: Option<f32>,
    alpha_mode: MaterialAlphaMode,
    double_sided: bool,
) -> Triangle {
    let face_normal = triangle_normal(a, b, c);
    let (normal_a, normal_b, normal_c) = normals.unwrap_or((face_normal, face_normal, face_normal));
    let (
        vertex_color_a,
        vertex_color_b,
        vertex_color_c,
        vertex_color_linear_a,
        vertex_color_linear_b,
        vertex_color_linear_c,
    ) = vertex_colors.map_or((None, None, None, None, None, None), |(a, b, c)| {
        (
            Some(a.rgba),
            Some(b.rgba),
            Some(c.rgba),
            Some(a.linear_rgba),
            Some(b.linear_rgba),
            Some(c.linear_rgba),
        )
    });
    let (texcoord_a, texcoord_b, texcoord_c) =
        texcoords.map_or((None, None, None), |(a, b, c)| (Some(a), Some(b), Some(c)));
    let (texcoord1_a, texcoord1_b, texcoord1_c) =
        texcoords1.map_or((None, None, None), |(a, b, c)| (Some(a), Some(b), Some(c)));
    let (tangent_a, tangent_b, tangent_c) =
        tangents.map_or((None, None, None), |(a, b, c)| (Some(a), Some(b), Some(c)));
    let has_texcoords0 = texcoords.is_some();
    let has_texcoords1 = texcoords1.is_some();
    Triangle {
        a,
        b,
        c,
        normal_a,
        normal_b,
        normal_c,
        color_rgba,
        color_linear_rgb,
        color_alpha,
        vertex_color_a,
        vertex_color_b,
        vertex_color_c,
        vertex_color_linear_a,
        vertex_color_linear_b,
        vertex_color_linear_c,
        texcoord_a,
        texcoord_b,
        texcoord_c,
        texcoord1_a,
        texcoord1_b,
        texcoord1_c,
        tangent_a,
        tangent_b,
        tangent_c,
        texture_index: texture_index
            .filter(|_| has_texcoord_set(has_texcoords0, has_texcoords1, texture_texcoord)),
        texture_texcoord,
        texture_transform,
        metallic_factor,
        roughness_factor,
        metallic_roughness_texture_index: metallic_roughness_texture_index.filter(|_| {
            has_texcoord_set(
                has_texcoords0,
                has_texcoords1,
                metallic_roughness_texture_texcoord,
            )
        }),
        metallic_roughness_texture_texcoord,
        metallic_roughness_texture_transform,
        specular_glossiness_texture_index: specular_glossiness_texture_index.filter(|_| {
            has_texcoord_set(
                has_texcoords0,
                has_texcoords1,
                specular_glossiness_texture_texcoord,
            )
        }),
        specular_glossiness_texture_texcoord,
        specular_glossiness_texture_transform,
        ior,
        transmission_factor,
        transmission_texture_index: transmission_texture_index.filter(|_| {
            has_texcoord_set(
                has_texcoords0,
                has_texcoords1,
                transmission_texture_texcoord,
            )
        }),
        transmission_texture_texcoord,
        transmission_texture_transform,
        diffuse_transmission_factor,
        diffuse_transmission_texture_index: diffuse_transmission_texture_index.filter(|_| {
            has_texcoord_set(
                has_texcoords0,
                has_texcoords1,
                diffuse_transmission_texture_texcoord,
            )
        }),
        diffuse_transmission_texture_texcoord,
        diffuse_transmission_texture_transform,
        diffuse_transmission_color_factor,
        diffuse_transmission_color_texture_index: diffuse_transmission_color_texture_index.filter(
            |_| {
                has_texcoord_set(
                    has_texcoords0,
                    has_texcoords1,
                    diffuse_transmission_color_texture_texcoord,
                )
            },
        ),
        diffuse_transmission_color_texture_texcoord,
        diffuse_transmission_color_texture_transform,
        dispersion,
        volume_thickness_factor,
        volume_thickness_texture_index: volume_thickness_texture_index.filter(|_| {
            has_texcoord_set(
                has_texcoords0,
                has_texcoords1,
                volume_thickness_texture_texcoord,
            )
        }),
        volume_thickness_texture_texcoord,
        volume_thickness_texture_transform,
        volume_attenuation_distance,
        volume_attenuation_color,
        clearcoat_factor,
        clearcoat_texture_index: clearcoat_texture_index.filter(|_| {
            has_texcoord_set(has_texcoords0, has_texcoords1, clearcoat_texture_texcoord)
        }),
        clearcoat_texture_texcoord,
        clearcoat_texture_transform,
        clearcoat_roughness_factor,
        clearcoat_roughness_texture_index: clearcoat_roughness_texture_index.filter(|_| {
            has_texcoord_set(
                has_texcoords0,
                has_texcoords1,
                clearcoat_roughness_texture_texcoord,
            )
        }),
        clearcoat_roughness_texture_texcoord,
        clearcoat_roughness_texture_transform,
        clearcoat_normal_texture_index: clearcoat_normal_texture_index.filter(|_| {
            has_texcoord_set(
                has_texcoords0,
                has_texcoords1,
                clearcoat_normal_texture_texcoord,
            )
        }),
        clearcoat_normal_texture_texcoord,
        clearcoat_normal_texture_transform,
        clearcoat_normal_scale,
        sheen_color_factor,
        sheen_color_texture_index: sheen_color_texture_index.filter(|_| {
            has_texcoord_set(has_texcoords0, has_texcoords1, sheen_color_texture_texcoord)
        }),
        sheen_color_texture_texcoord,
        sheen_color_texture_transform,
        sheen_roughness_factor,
        sheen_roughness_texture_index: sheen_roughness_texture_index.filter(|_| {
            has_texcoord_set(
                has_texcoords0,
                has_texcoords1,
                sheen_roughness_texture_texcoord,
            )
        }),
        sheen_roughness_texture_texcoord,
        sheen_roughness_texture_transform,
        anisotropy_strength,
        anisotropy_rotation,
        anisotropy_texture_index: anisotropy_texture_index.filter(|_| {
            has_texcoord_set(has_texcoords0, has_texcoords1, anisotropy_texture_texcoord)
        }),
        anisotropy_texture_texcoord,
        anisotropy_texture_transform,
        iridescence_factor,
        iridescence_texture_index: iridescence_texture_index.filter(|_| {
            has_texcoord_set(has_texcoords0, has_texcoords1, iridescence_texture_texcoord)
        }),
        iridescence_texture_texcoord,
        iridescence_texture_transform,
        iridescence_ior,
        iridescence_thickness_minimum_nm,
        iridescence_thickness_maximum_nm,
        iridescence_thickness_texture_index: iridescence_thickness_texture_index.filter(|_| {
            has_texcoord_set(
                has_texcoords0,
                has_texcoords1,
                iridescence_thickness_texture_texcoord,
            )
        }),
        iridescence_thickness_texture_texcoord,
        iridescence_thickness_texture_transform,
        specular_factor,
        specular_texture_index: specular_texture_index.filter(|_| {
            has_texcoord_set(has_texcoords0, has_texcoords1, specular_texture_texcoord)
        }),
        specular_texture_texcoord,
        specular_texture_transform,
        specular_color_factor,
        specular_color_texture_index: specular_color_texture_index.filter(|_| {
            has_texcoord_set(
                has_texcoords0,
                has_texcoords1,
                specular_color_texture_texcoord,
            )
        }),
        specular_color_texture_texcoord,
        specular_color_texture_transform,
        normal_texture_index: normal_texture_index
            .filter(|_| has_texcoord_set(has_texcoords0, has_texcoords1, normal_texture_texcoord)),
        normal_texture_texcoord,
        normal_texture_transform,
        normal_scale,
        emissive_rgb,
        emissive_texture_index: emissive_texture_index.filter(|_| {
            has_texcoord_set(has_texcoords0, has_texcoords1, emissive_texture_texcoord)
        }),
        emissive_texture_texcoord,
        emissive_texture_transform,
        occlusion_texture_index: occlusion_texture_index.filter(|_| {
            has_texcoord_set(has_texcoords0, has_texcoords1, occlusion_texture_texcoord)
        }),
        occlusion_texture_texcoord,
        occlusion_texture_transform,
        occlusion_strength,
        unlit,
        alpha_cutoff,
        alpha_mode,
        double_sided,
    }
}

fn has_texcoord_set(has_texcoords0: bool, has_texcoords1: bool, set: u32) -> bool {
    match set {
        0 => has_texcoords0,
        1 => has_texcoords1,
        _ => false,
    }
}

fn triangle_normal(a: [f32; 3], b: [f32; 3], c: [f32; 3]) -> [f32; 3] {
    normalize(cross(sub(b, a), sub(c, a)))
}

fn mesh_bounds(triangles: &[Triangle]) -> Option<([f32; 3], [f32; 3])> {
    let first = triangles.first()?;
    let mut min = first.a;
    let mut max = first.a;
    for triangle in triangles {
        for point in [triangle.a, triangle.b, triangle.c] {
            for axis in 0..3 {
                min[axis] = min[axis].min(point[axis]);
                max[axis] = max[axis].max(point[axis]);
            }
        }
    }
    Some((min, max))
}

fn build_triangle_bvh(triangles: &mut [Triangle]) -> Vec<BvhNode> {
    let mut nodes = Vec::new();
    if !triangles.is_empty() {
        build_triangle_bvh_node(triangles, 0, triangles.len(), &mut nodes);
    }
    nodes
}

fn build_triangle_bvh_node(
    triangles: &mut [Triangle],
    first: usize,
    len: usize,
    nodes: &mut Vec<BvhNode>,
) -> usize {
    let index = nodes.len();
    let (bounds_min, bounds_max) = triangle_range_bounds(&triangles[first..first + len]);
    let (centroid_min, centroid_max) =
        triangle_centroid_range_bounds(&triangles[first..first + len]);
    nodes.push(BvhNode {
        bounds_min,
        bounds_max,
        first,
        len,
        left: None,
        right: None,
    });

    let centroid_extent = sub(centroid_max, centroid_min);
    let split_axis = largest_axis(centroid_extent);
    if len <= 16 || centroid_extent[split_axis] < 1.0e-6 {
        return index;
    }

    triangles[first..first + len].sort_by(|left, right| {
        triangle_centroid(left)[split_axis].total_cmp(&triangle_centroid(right)[split_axis])
    });
    let left_len = len / 2;
    let right_len = len - left_len;
    let left = build_triangle_bvh_node(triangles, first, left_len, nodes);
    let right = build_triangle_bvh_node(triangles, first + left_len, right_len, nodes);
    nodes[index].left = Some(left);
    nodes[index].right = Some(right);
    nodes[index].len = 0;
    nodes[index].first = 0;
    index
}

fn triangle_range_bounds(triangles: &[Triangle]) -> ([f32; 3], [f32; 3]) {
    let mut min = [f32::INFINITY; 3];
    let mut max = [f32::NEG_INFINITY; 3];
    for triangle in triangles {
        for point in [triangle.a, triangle.b, triangle.c] {
            for axis in 0..3 {
                min[axis] = min[axis].min(point[axis]);
                max[axis] = max[axis].max(point[axis]);
            }
        }
    }
    (min, max)
}

fn triangle_centroid_range_bounds(triangles: &[Triangle]) -> ([f32; 3], [f32; 3]) {
    let mut min = [f32::INFINITY; 3];
    let mut max = [f32::NEG_INFINITY; 3];
    for triangle in triangles {
        let centroid = triangle_centroid(triangle);
        for axis in 0..3 {
            min[axis] = min[axis].min(centroid[axis]);
            max[axis] = max[axis].max(centroid[axis]);
        }
    }
    (min, max)
}

fn triangle_centroid(triangle: &Triangle) -> [f32; 3] {
    [
        (triangle.a[0] + triangle.b[0] + triangle.c[0]) / 3.0,
        (triangle.a[1] + triangle.b[1] + triangle.c[1]) / 3.0,
        (triangle.a[2] + triangle.b[2] + triangle.c[2]) / 3.0,
    ]
}

fn largest_axis(value: [f32; 3]) -> usize {
    if value[0] >= value[1] && value[0] >= value[2] {
        0
    } else if value[1] >= value[2] {
        1
    } else {
        2
    }
}

fn intersect_box(ray: &Ray, size: [f32; 3]) -> Option<f32> {
    let min = [-size[0] * 0.5, -size[1] * 0.5, -size[2] * 0.5];
    let max = [size[0] * 0.5, size[1] * 0.5, size[2] * 0.5];
    intersect_aabb(ray, min, max)
}

fn intersect_aabb(ray: &Ray, min: [f32; 3], max: [f32; 3]) -> Option<f32> {
    let mut t_min = f32::NEG_INFINITY;
    let mut t_max = f32::INFINITY;
    for axis in 0..3 {
        if ray.dir[axis].abs() < 1.0e-6 {
            if ray.origin[axis] < min[axis] || ray.origin[axis] > max[axis] {
                return None;
            }
            continue;
        }
        let inv = 1.0 / ray.dir[axis];
        let mut near = (min[axis] - ray.origin[axis]) * inv;
        let mut far = (max[axis] - ray.origin[axis]) * inv;
        if near > far {
            std::mem::swap(&mut near, &mut far);
        }
        t_min = t_min.max(near);
        t_max = t_max.min(far);
        if t_min > t_max {
            return None;
        }
    }
    if t_min >= 0.0 {
        Some(t_min)
    } else if t_max >= 0.0 {
        Some(t_max)
    } else {
        None
    }
}

fn intersect_sphere(ray: &Ray, radius: f32) -> Option<f32> {
    let a = dot(ray.dir, ray.dir);
    let b = 2.0 * dot(ray.origin, ray.dir);
    let c = dot(ray.origin, ray.origin) - radius * radius;
    let disc = b * b - 4.0 * a * c;
    if disc < 0.0 {
        return None;
    }
    let root = disc.sqrt();
    let near = (-b - root) / (2.0 * a);
    let far = (-b + root) / (2.0 * a);
    if near >= 0.0 {
        Some(near)
    } else if far >= 0.0 {
        Some(far)
    } else {
        None
    }
}

fn intersect_cylinder(ray: &Ray, radius: f32, height: f32) -> Option<f32> {
    let a = ray.dir[0] * ray.dir[0] + ray.dir[1] * ray.dir[1];
    let b = 2.0 * (ray.origin[0] * ray.dir[0] + ray.origin[1] * ray.dir[1]);
    let c = ray.origin[0] * ray.origin[0] + ray.origin[1] * ray.origin[1] - radius * radius;
    let half_h = height * 0.5;
    let mut best: Option<f32> = None;

    if a.abs() > 1.0e-6 {
        let disc = b * b - 4.0 * a * c;
        if disc >= 0.0 {
            let root = disc.sqrt();
            for t in [(-b - root) / (2.0 * a), (-b + root) / (2.0 * a)] {
                let z = ray.origin[2] + t * ray.dir[2];
                if t >= 0.0 && z >= -half_h && z <= half_h {
                    best = Some(best.map_or(t, |current| current.min(t)));
                }
            }
        }
    }

    if ray.dir[2].abs() > 1.0e-6 {
        for z in [-half_h, half_h] {
            let t = (z - ray.origin[2]) / ray.dir[2];
            let x = ray.origin[0] + t * ray.dir[0];
            let y = ray.origin[1] + t * ray.dir[1];
            if t >= 0.0 && x * x + y * y <= radius * radius {
                best = Some(best.map_or(t, |current| current.min(t)));
            }
        }
    }

    best
}

fn ray_point(ray: &Ray, t: f32) -> [f32; 3] {
    add(ray.origin, scale(ray.dir, t))
}

fn box_normal(point: [f32; 3], size: [f32; 3]) -> [f32; 3] {
    let half = [size[0] * 0.5, size[1] * 0.5, size[2] * 0.5];
    let distances = [
        (point[0].abs() - half[0]).abs(),
        (point[1].abs() - half[1]).abs(),
        (point[2].abs() - half[2]).abs(),
    ];
    let axis = if distances[0] <= distances[1] && distances[0] <= distances[2] {
        0
    } else if distances[1] <= distances[2] {
        1
    } else {
        2
    };
    let mut normal = [0.0; 3];
    normal[axis] = if point[axis] >= 0.0 { 1.0 } else { -1.0 };
    normal
}

fn cylinder_normal(point: [f32; 3], height: f32) -> [f32; 3] {
    let half_h = height * 0.5;
    if (point[2] - half_h).abs() < 1.0e-4 {
        [0.0, 0.0, 1.0]
    } else if (point[2] + half_h).abs() < 1.0e-4 {
        [0.0, 0.0, -1.0]
    } else {
        normalize([point[0], point[1], 0.0])
    }
}

fn intersect_triangles_all_with_mode(
    ray: &Ray,
    triangles: &[Triangle],
    textures: &[TextureImage],
    bvh: &[BvhNode],
    mode: TriangleIntersectionMode,
) -> Vec<TriangleHit> {
    let mut hits = Vec::new();
    if bvh.is_empty() {
        hits.extend(
            triangles
                .iter()
                .filter_map(|triangle| intersect_triangle_with_mode(ray, triangle, textures, mode))
                .filter(|hit| hit.t >= 0.0),
        );
        hits.sort_by(|left, right| left.t.total_cmp(&right.t));
        return hits;
    }

    let mut stack = vec![0_usize];
    while let Some(node_index) = stack.pop() {
        let Some(node) = bvh.get(node_index) else {
            continue;
        };
        if intersect_aabb(ray, node.bounds_min, node.bounds_max).is_none() {
            continue;
        }

        if let (Some(left), Some(right)) = (node.left, node.right) {
            stack.push(left);
            stack.push(right);
            continue;
        }

        for triangle in &triangles[node.first..node.first + node.len] {
            if let Some(hit) = intersect_triangle_with_mode(ray, triangle, textures, mode) {
                hits.push(hit);
            }
        }
    }
    hits.sort_by(|left, right| left.t.total_cmp(&right.t));
    hits
}

fn intersect_triangles_closest_with_mode(
    ray: &Ray,
    triangles: &[Triangle],
    textures: &[TextureImage],
    bvh: &[BvhNode],
    mode: TriangleIntersectionMode,
) -> Option<TriangleHit> {
    if bvh.is_empty() {
        return triangles
            .iter()
            .filter_map(|triangle| intersect_triangle_with_mode(ray, triangle, textures, mode))
            .filter(|hit| hit.t >= 0.0)
            .min_by(|left, right| left.t.total_cmp(&right.t));
    }

    let mut best: Option<TriangleHit> = None;
    let mut stack = vec![0_usize];
    while let Some(node_index) = stack.pop() {
        let Some(node) = bvh.get(node_index) else {
            continue;
        };
        let Some(bounds_t) = intersect_aabb(ray, node.bounds_min, node.bounds_max) else {
            continue;
        };
        if best.is_some_and(|hit| bounds_t > hit.t) {
            continue;
        }

        if let (Some(left), Some(right)) = (node.left, node.right) {
            let left_t = bvh
                .get(left)
                .and_then(|node| intersect_aabb(ray, node.bounds_min, node.bounds_max));
            let right_t = bvh
                .get(right)
                .and_then(|node| intersect_aabb(ray, node.bounds_min, node.bounds_max));
            match (left_t, right_t) {
                (Some(left_t), Some(right_t)) if left_t < right_t => {
                    stack.push(right);
                    stack.push(left);
                }
                (Some(_), Some(_)) => {
                    stack.push(left);
                    stack.push(right);
                }
                (Some(_), None) => stack.push(left),
                (None, Some(_)) => stack.push(right),
                (None, None) => {}
            }
            continue;
        }

        for triangle in &triangles[node.first..node.first + node.len] {
            if let Some(hit) = intersect_triangle_with_mode(ray, triangle, textures, mode)
                && best.is_none_or(|current| hit.t < current.t)
            {
                best = Some(hit);
            }
        }
    }
    best
}

#[cfg(test)]
fn intersect_triangle(
    ray: &Ray,
    triangle: &Triangle,
    textures: &[TextureImage],
) -> Option<TriangleHit> {
    intersect_triangle_with_mode(
        ray,
        triangle,
        textures,
        TriangleIntersectionMode::VisibleSurface,
    )
}

fn triangle_can_be_refractive_boundary(triangle: &Triangle) -> bool {
    triangle.transmission_factor > 1.0e-6
        || triangle.diffuse_transmission_factor > 1.0e-6
        || triangle.dispersion > 1.0e-6
        || triangle.volume_thickness_factor > 1.0e-6
        || triangle.volume_attenuation_distance.is_finite()
}

fn intersect_triangle_with_mode(
    ray: &Ray,
    triangle: &Triangle,
    textures: &[TextureImage],
    mode: TriangleIntersectionMode,
) -> Option<TriangleHit> {
    let edge1 = sub(triangle.b, triangle.a);
    let edge2 = sub(triangle.c, triangle.a);
    let h = cross(ray.dir, edge2);
    let a = dot(edge1, h);
    if triangle.double_sided {
        if a.abs() < 1.0e-7 {
            return None;
        }
    } else if a <= 1.0e-7
        && !(mode == TriangleIntersectionMode::RefractiveBoundary
            && a < -1.0e-7
            && triangle_can_be_refractive_boundary(triangle))
    {
        return None;
    }
    let f = 1.0 / a;
    let s = sub(ray.origin, triangle.a);
    let u = f * dot(s, h);
    if !(0.0..=1.0).contains(&u) {
        return None;
    }
    let q = cross(s, edge1);
    let v = f * dot(ray.dir, q);
    if v < 0.0 || u + v > 1.0 {
        return None;
    }
    let t = f * dot(edge2, q);
    if t <= 1.0e-6 {
        return None;
    }
    let sample = interpolated_material_sample(triangle, textures, u, v);
    if let Some(cutoff) = triangle.alpha_cutoff
        && sample.alpha < cutoff
    {
        return None;
    }
    let base_normal = interpolated_normal(triangle, u, v);
    let normal = oriented_normal(
        mapped_normal(triangle, textures, u, v, base_normal),
        ray.dir,
        triangle.double_sided,
    );
    let clearcoat_normal = oriented_normal(
        mapped_clearcoat_normal(triangle, textures, u, v, base_normal, normal),
        ray.dir,
        triangle.double_sided,
    );
    Some(TriangleHit {
        t,
        color_rgb: sample.color_rgb,
        color_linear_rgb: sample.color_linear_rgb,
        emission_rgb: sample.emission_rgb,
        alpha: sample.alpha,
        transmission_filter_rgb: sample.transmission_filter_rgb,
        diffuse_transmission: sample.diffuse_transmission,
        diffuse_transmission_color: sample.diffuse_transmission_color,
        dispersion: sample.dispersion,
        volume_thickness_m: sample.volume_thickness_m,
        volume_attenuation_distance_m: sample.volume_attenuation_distance_m,
        volume_attenuation_color: sample.volume_attenuation_color,
        occlusion: sample.occlusion,
        metallic: sample.metallic,
        roughness: sample.roughness,
        ior: triangle.ior,
        clearcoat_factor: sample.clearcoat_factor,
        clearcoat_roughness: sample.clearcoat_roughness,
        clearcoat_normal,
        sheen_color: sample.sheen_color,
        sheen_roughness: sample.sheen_roughness,
        anisotropy_strength: sample.anisotropy_strength,
        anisotropy_direction: interpolated_anisotropy_direction(triangle, textures, u, v, normal),
        iridescence_factor: sample.iridescence_factor,
        iridescence_thickness_nm: sample.iridescence_thickness_nm,
        iridescence_ior: sample.iridescence_ior,
        specular_factor: sample.specular_factor,
        specular_color: sample.specular_color,
        unlit: triangle.unlit,
        normal,
    })
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct MaterialSample {
    color_rgb: [u8; 3],
    color_linear_rgb: [f32; 3],
    emission_rgb: [f32; 3],
    alpha: f32,
    transmission_filter_rgb: [f32; 3],
    diffuse_transmission: f32,
    diffuse_transmission_color: [f32; 3],
    dispersion: f32,
    volume_thickness_m: f32,
    volume_attenuation_distance_m: f32,
    volume_attenuation_color: [f32; 3],
    occlusion: f32,
    metallic: f32,
    roughness: f32,
    ior: f32,
    clearcoat_factor: f32,
    clearcoat_roughness: f32,
    sheen_color: [f32; 3],
    sheen_roughness: f32,
    anisotropy_strength: f32,
    iridescence_factor: f32,
    iridescence_thickness_nm: f32,
    iridescence_ior: f32,
    specular_factor: f32,
    specular_color: [f32; 3],
}

fn interpolated_material_sample(
    triangle: &Triangle,
    textures: &[TextureImage],
    u: f32,
    v: f32,
) -> MaterialSample {
    let vertex_color = interpolated_vertex_color(triangle, u, v).unwrap_or([255, 255, 255, 255]);
    let vertex_linear =
        interpolated_vertex_color_linear(triangle, u, v).unwrap_or([1.0, 1.0, 1.0, 1.0]);
    let base = multiply_rgba(triangle.color_rgba, vertex_color);
    let base_linear = [
        triangle.color_linear_rgb[0] * vertex_linear[0],
        triangle.color_linear_rgb[1] * vertex_linear[1],
        triangle.color_linear_rgb[2] * vertex_linear[2],
    ];
    let base_alpha = triangle.color_alpha * vertex_linear[3];
    let Some(texture_index) = triangle.texture_index else {
        return MaterialSample {
            color_rgb: [base[0], base[1], base[2]],
            color_linear_rgb: base_linear,
            emission_rgb: interpolated_emission_sample(triangle, textures, u, v),
            alpha: interpolated_alpha_sample(triangle, textures, u, v, base_alpha, 1.0),
            transmission_filter_rgb: interpolated_transmission_filter_sample(
                triangle, textures, u, v,
            ),
            diffuse_transmission: interpolated_diffuse_transmission_sample(
                triangle, textures, u, v,
            ),
            diffuse_transmission_color: interpolated_diffuse_transmission_color_sample(
                triangle, textures, u, v,
            ),
            dispersion: triangle.dispersion,
            volume_thickness_m: interpolated_volume_thickness_sample(triangle, textures, u, v),
            volume_attenuation_distance_m: triangle.volume_attenuation_distance,
            volume_attenuation_color: triangle.volume_attenuation_color,
            occlusion: interpolated_occlusion_sample(triangle, textures, u, v),
            metallic: interpolated_metallic_sample(triangle, textures, u, v),
            roughness: interpolated_roughness_sample(triangle, textures, u, v),
            ior: triangle.ior,
            clearcoat_factor: interpolated_clearcoat_factor_sample(triangle, textures, u, v),
            clearcoat_roughness: interpolated_clearcoat_roughness_sample(triangle, textures, u, v),
            sheen_color: interpolated_sheen_color_sample(triangle, textures, u, v),
            sheen_roughness: interpolated_sheen_roughness_sample(triangle, textures, u, v),
            anisotropy_strength: interpolated_anisotropy_strength_sample(triangle, textures, u, v),
            iridescence_factor: interpolated_iridescence_factor_sample(triangle, textures, u, v),
            iridescence_thickness_nm: interpolated_iridescence_thickness_sample(
                triangle, textures, u, v,
            ),
            iridescence_ior: triangle.iridescence_ior,
            specular_factor: interpolated_specular_factor_sample(triangle, textures, u, v),
            specular_color: interpolated_specular_color_sample(triangle, textures, u, v),
        };
    };
    let Some(texture) = textures.get(texture_index) else {
        return MaterialSample {
            color_rgb: [base[0], base[1], base[2]],
            color_linear_rgb: base_linear,
            emission_rgb: interpolated_emission_sample(triangle, textures, u, v),
            alpha: interpolated_alpha_sample(triangle, textures, u, v, base_alpha, 1.0),
            transmission_filter_rgb: interpolated_transmission_filter_sample(
                triangle, textures, u, v,
            ),
            diffuse_transmission: interpolated_diffuse_transmission_sample(
                triangle, textures, u, v,
            ),
            diffuse_transmission_color: interpolated_diffuse_transmission_color_sample(
                triangle, textures, u, v,
            ),
            dispersion: triangle.dispersion,
            volume_thickness_m: interpolated_volume_thickness_sample(triangle, textures, u, v),
            volume_attenuation_distance_m: triangle.volume_attenuation_distance,
            volume_attenuation_color: triangle.volume_attenuation_color,
            occlusion: interpolated_occlusion_sample(triangle, textures, u, v),
            metallic: interpolated_metallic_sample(triangle, textures, u, v),
            roughness: interpolated_roughness_sample(triangle, textures, u, v),
            ior: triangle.ior,
            clearcoat_factor: interpolated_clearcoat_factor_sample(triangle, textures, u, v),
            clearcoat_roughness: interpolated_clearcoat_roughness_sample(triangle, textures, u, v),
            sheen_color: interpolated_sheen_color_sample(triangle, textures, u, v),
            sheen_roughness: interpolated_sheen_roughness_sample(triangle, textures, u, v),
            anisotropy_strength: interpolated_anisotropy_strength_sample(triangle, textures, u, v),
            iridescence_factor: interpolated_iridescence_factor_sample(triangle, textures, u, v),
            iridescence_thickness_nm: interpolated_iridescence_thickness_sample(
                triangle, textures, u, v,
            ),
            iridescence_ior: triangle.iridescence_ior,
            specular_factor: interpolated_specular_factor_sample(triangle, textures, u, v),
            specular_color: interpolated_specular_color_sample(triangle, textures, u, v),
        };
    };
    let Some(texcoord) = interpolated_texture_texcoord(
        triangle,
        u,
        v,
        triangle.texture_texcoord,
        triangle.texture_transform,
    ) else {
        return MaterialSample {
            color_rgb: [base[0], base[1], base[2]],
            color_linear_rgb: base_linear,
            emission_rgb: interpolated_emission_sample(triangle, textures, u, v),
            alpha: interpolated_alpha_sample(triangle, textures, u, v, base_alpha, 1.0),
            transmission_filter_rgb: interpolated_transmission_filter_sample(
                triangle, textures, u, v,
            ),
            diffuse_transmission: interpolated_diffuse_transmission_sample(
                triangle, textures, u, v,
            ),
            diffuse_transmission_color: interpolated_diffuse_transmission_color_sample(
                triangle, textures, u, v,
            ),
            dispersion: triangle.dispersion,
            volume_thickness_m: interpolated_volume_thickness_sample(triangle, textures, u, v),
            volume_attenuation_distance_m: triangle.volume_attenuation_distance,
            volume_attenuation_color: triangle.volume_attenuation_color,
            occlusion: interpolated_occlusion_sample(triangle, textures, u, v),
            metallic: interpolated_metallic_sample(triangle, textures, u, v),
            roughness: interpolated_roughness_sample(triangle, textures, u, v),
            ior: triangle.ior,
            clearcoat_factor: interpolated_clearcoat_factor_sample(triangle, textures, u, v),
            clearcoat_roughness: interpolated_clearcoat_roughness_sample(triangle, textures, u, v),
            sheen_color: interpolated_sheen_color_sample(triangle, textures, u, v),
            sheen_roughness: interpolated_sheen_roughness_sample(triangle, textures, u, v),
            anisotropy_strength: interpolated_anisotropy_strength_sample(triangle, textures, u, v),
            iridescence_factor: interpolated_iridescence_factor_sample(triangle, textures, u, v),
            iridescence_thickness_nm: interpolated_iridescence_thickness_sample(
                triangle, textures, u, v,
            ),
            iridescence_ior: triangle.iridescence_ior,
            specular_factor: interpolated_specular_factor_sample(triangle, textures, u, v),
            specular_color: interpolated_specular_color_sample(triangle, textures, u, v),
        };
    };
    let texture_sample = sample_texture(texture, texcoord);
    let texture_linear = sample_color_texture_linear_rgb(texture, texcoord);
    MaterialSample {
        color_rgb: multiply_rgb(
            [base[0], base[1], base[2]],
            [texture_sample[0], texture_sample[1], texture_sample[2]],
        ),
        color_linear_rgb: [
            base_linear[0] * texture_linear[0],
            base_linear[1] * texture_linear[1],
            base_linear[2] * texture_linear[2],
        ],
        emission_rgb: interpolated_emission_sample(triangle, textures, u, v),
        alpha: interpolated_alpha_sample(
            triangle,
            textures,
            u,
            v,
            base_alpha,
            f32::from(texture_sample[3]) / 255.0,
        ),
        transmission_filter_rgb: interpolated_transmission_filter_sample(triangle, textures, u, v),
        diffuse_transmission: interpolated_diffuse_transmission_sample(triangle, textures, u, v),
        diffuse_transmission_color: interpolated_diffuse_transmission_color_sample(
            triangle, textures, u, v,
        ),
        dispersion: triangle.dispersion,
        volume_thickness_m: interpolated_volume_thickness_sample(triangle, textures, u, v),
        volume_attenuation_distance_m: triangle.volume_attenuation_distance,
        volume_attenuation_color: triangle.volume_attenuation_color,
        occlusion: interpolated_occlusion_sample(triangle, textures, u, v),
        metallic: interpolated_metallic_sample(triangle, textures, u, v),
        roughness: interpolated_roughness_sample(triangle, textures, u, v),
        ior: triangle.ior,
        clearcoat_factor: interpolated_clearcoat_factor_sample(triangle, textures, u, v),
        clearcoat_roughness: interpolated_clearcoat_roughness_sample(triangle, textures, u, v),
        sheen_color: interpolated_sheen_color_sample(triangle, textures, u, v),
        sheen_roughness: interpolated_sheen_roughness_sample(triangle, textures, u, v),
        anisotropy_strength: interpolated_anisotropy_strength_sample(triangle, textures, u, v),
        iridescence_factor: interpolated_iridescence_factor_sample(triangle, textures, u, v),
        iridescence_thickness_nm: interpolated_iridescence_thickness_sample(
            triangle, textures, u, v,
        ),
        iridescence_ior: triangle.iridescence_ior,
        specular_factor: interpolated_specular_factor_sample(triangle, textures, u, v),
        specular_color: interpolated_specular_color_sample(triangle, textures, u, v),
    }
}

fn interpolated_emission_sample(
    triangle: &Triangle,
    textures: &[TextureImage],
    u: f32,
    v: f32,
) -> [f32; 3] {
    let Some(texture_index) = triangle.emissive_texture_index else {
        return triangle.emissive_rgb;
    };
    let Some(texture) = textures.get(texture_index) else {
        return triangle.emissive_rgb;
    };
    let Some(texcoord) = interpolated_texture_texcoord(
        triangle,
        u,
        v,
        triangle.emissive_texture_texcoord,
        triangle.emissive_texture_transform,
    ) else {
        return triangle.emissive_rgb;
    };
    let texture_sample = sample_color_texture_linear_rgb(texture, texcoord);
    multiply_rgb_by_linear_unit(triangle.emissive_rgb, texture_sample)
}

fn interpolated_emissive_visibility_sample(
    triangle: &Triangle,
    textures: &[TextureImage],
    u: f32,
    v: f32,
) -> f32 {
    let vertex_linear =
        interpolated_vertex_color_linear(triangle, u, v).unwrap_or([1.0, 1.0, 1.0, 1.0]);
    let base_alpha = triangle.color_alpha * vertex_linear[3];
    let texture_alpha = triangle
        .texture_index
        .and_then(|texture_index| textures.get(texture_index))
        .and_then(|texture| {
            interpolated_texture_texcoord(
                triangle,
                u,
                v,
                triangle.texture_texcoord,
                triangle.texture_transform,
            )
            .map(|texcoord| f32::from(sample_texture(texture, texcoord)[3]) / 255.0)
        })
        .unwrap_or(1.0);
    interpolated_alpha_sample(triangle, textures, u, v, base_alpha, texture_alpha)
}

fn interpolated_alpha_sample(
    triangle: &Triangle,
    textures: &[TextureImage],
    u: f32,
    v: f32,
    base_alpha: f32,
    texture_alpha: f32,
) -> f32 {
    let transmission = interpolated_transmission_sample(triangle, textures, u, v);
    let source_alpha = (base_alpha * texture_alpha).clamp(0.0, 1.0);
    let material_alpha = match triangle.alpha_mode {
        MaterialAlphaMode::Opaque => 1.0,
        MaterialAlphaMode::Mask => {
            if source_alpha >= triangle.alpha_cutoff.unwrap_or(0.5) {
                1.0
            } else {
                0.0
            }
        }
        MaterialAlphaMode::Blend => source_alpha,
    };
    (material_alpha * (1.0 - transmission)).clamp(0.0, 1.0)
}

fn interpolated_transmission_sample(
    triangle: &Triangle,
    textures: &[TextureImage],
    u: f32,
    v: f32,
) -> f32 {
    let mut transmission = triangle.transmission_factor;
    if let Some(texture_index) = triangle.transmission_texture_index
        && let Some(texture) = textures.get(texture_index)
        && let Some(texcoord) = interpolated_texture_texcoord(
            triangle,
            u,
            v,
            triangle.transmission_texture_texcoord,
            triangle.transmission_texture_transform,
        )
    {
        transmission *= f32::from(sample_texture(texture, texcoord)[0]) / 255.0;
    }
    transmission.clamp(0.0, 1.0)
}

fn interpolated_transmission_filter_sample(
    triangle: &Triangle,
    textures: &[TextureImage],
    u: f32,
    v: f32,
) -> [f32; 3] {
    if interpolated_transmission_sample(triangle, textures, u, v) <= 1.0e-6 {
        return [1.0, 1.0, 1.0];
    }
    let thickness = interpolated_volume_thickness_sample(triangle, textures, u, v);
    if thickness <= 1.0e-6 || !triangle.volume_attenuation_distance.is_finite() {
        return [1.0, 1.0, 1.0];
    }
    let distance = triangle.volume_attenuation_distance.max(1.0e-6);
    let power = thickness / distance;
    [
        triangle.volume_attenuation_color[0]
            .clamp(0.0, 1.0)
            .powf(power),
        triangle.volume_attenuation_color[1]
            .clamp(0.0, 1.0)
            .powf(power),
        triangle.volume_attenuation_color[2]
            .clamp(0.0, 1.0)
            .powf(power),
    ]
}

fn interpolated_diffuse_transmission_sample(
    triangle: &Triangle,
    textures: &[TextureImage],
    u: f32,
    v: f32,
) -> f32 {
    let mut transmission = triangle.diffuse_transmission_factor;
    if let Some(texture_index) = triangle.diffuse_transmission_texture_index
        && let Some(texture) = textures.get(texture_index)
        && let Some(texcoord) = interpolated_texture_texcoord(
            triangle,
            u,
            v,
            triangle.diffuse_transmission_texture_texcoord,
            triangle.diffuse_transmission_texture_transform,
        )
    {
        transmission *= f32::from(sample_texture(texture, texcoord)[3]) / 255.0;
    }
    transmission.clamp(0.0, 1.0)
}

fn interpolated_diffuse_transmission_color_sample(
    triangle: &Triangle,
    textures: &[TextureImage],
    u: f32,
    v: f32,
) -> [f32; 3] {
    let mut color = triangle.diffuse_transmission_color_factor;
    if let Some(texture_index) = triangle.diffuse_transmission_color_texture_index
        && let Some(texture) = textures.get(texture_index)
        && let Some(texcoord) = interpolated_texture_texcoord(
            triangle,
            u,
            v,
            triangle.diffuse_transmission_color_texture_texcoord,
            triangle.diffuse_transmission_color_texture_transform,
        )
    {
        let texture_rgb = sample_color_texture_linear_rgb(texture, texcoord);
        for channel in 0..3 {
            color[channel] *= texture_rgb[channel];
        }
    }
    [
        color[0].clamp(0.0, 1.0),
        color[1].clamp(0.0, 1.0),
        color[2].clamp(0.0, 1.0),
    ]
}

fn interpolated_volume_thickness_sample(
    triangle: &Triangle,
    textures: &[TextureImage],
    u: f32,
    v: f32,
) -> f32 {
    let mut thickness = triangle.volume_thickness_factor.max(0.0);
    if let Some(texture_index) = triangle.volume_thickness_texture_index
        && let Some(texture) = textures.get(texture_index)
        && let Some(texcoord) = interpolated_texture_texcoord(
            triangle,
            u,
            v,
            triangle.volume_thickness_texture_texcoord,
            triangle.volume_thickness_texture_transform,
        )
    {
        thickness *= f32::from(sample_texture(texture, texcoord)[1]) / 255.0;
    }
    thickness.max(0.0)
}

fn interpolated_occlusion_sample(
    triangle: &Triangle,
    textures: &[TextureImage],
    u: f32,
    v: f32,
) -> f32 {
    let Some(texture_index) = triangle.occlusion_texture_index else {
        return 1.0;
    };
    let Some(texture) = textures.get(texture_index) else {
        return 1.0;
    };
    let Some(texcoord) = interpolated_texture_texcoord(
        triangle,
        u,
        v,
        triangle.occlusion_texture_texcoord,
        triangle.occlusion_texture_transform,
    ) else {
        return 1.0;
    };
    let texture_sample = sample_texture(texture, texcoord);
    let occlusion = f32::from(texture_sample[0]) / 255.0;
    1.0 - triangle.occlusion_strength.clamp(0.0, 1.0) * (1.0 - occlusion)
}

fn interpolated_metallic_sample(
    triangle: &Triangle,
    textures: &[TextureImage],
    u: f32,
    v: f32,
) -> f32 {
    let mut metallic = triangle.metallic_factor;
    if let Some(sample) = interpolated_metallic_roughness_texture_sample(triangle, textures, u, v) {
        metallic *= f32::from(sample[2]) / 255.0;
    }
    metallic.clamp(0.0, 1.0)
}

fn interpolated_roughness_sample(
    triangle: &Triangle,
    textures: &[TextureImage],
    u: f32,
    v: f32,
) -> f32 {
    let mut roughness = triangle.roughness_factor;
    if let Some(sample) = interpolated_metallic_roughness_texture_sample(triangle, textures, u, v) {
        roughness *= f32::from(sample[1]) / 255.0;
    }
    if let Some(sample) = interpolated_specular_glossiness_texture_sample(triangle, textures, u, v)
    {
        let glossiness =
            (1.0 - triangle.roughness_factor).clamp(0.0, 1.0) * (f32::from(sample[3]) / 255.0);
        roughness = 1.0 - glossiness;
    }
    roughness.clamp(0.04, 1.0)
}

fn interpolated_clearcoat_factor_sample(
    triangle: &Triangle,
    textures: &[TextureImage],
    u: f32,
    v: f32,
) -> f32 {
    let mut clearcoat = triangle.clearcoat_factor;
    if let Some(texture_index) = triangle.clearcoat_texture_index
        && let Some(texture) = textures.get(texture_index)
        && let Some(texcoord) = interpolated_texture_texcoord(
            triangle,
            u,
            v,
            triangle.clearcoat_texture_texcoord,
            triangle.clearcoat_texture_transform,
        )
    {
        clearcoat *= f32::from(sample_texture(texture, texcoord)[0]) / 255.0;
    }
    clearcoat.clamp(0.0, 1.0)
}

fn interpolated_clearcoat_roughness_sample(
    triangle: &Triangle,
    textures: &[TextureImage],
    u: f32,
    v: f32,
) -> f32 {
    let mut roughness = triangle.clearcoat_roughness_factor;
    if let Some(texture_index) = triangle.clearcoat_roughness_texture_index
        && let Some(texture) = textures.get(texture_index)
        && let Some(texcoord) = interpolated_texture_texcoord(
            triangle,
            u,
            v,
            triangle.clearcoat_roughness_texture_texcoord,
            triangle.clearcoat_roughness_texture_transform,
        )
    {
        roughness *= f32::from(sample_texture(texture, texcoord)[1]) / 255.0;
    }
    roughness.clamp(0.04, 1.0)
}

fn interpolated_sheen_color_sample(
    triangle: &Triangle,
    textures: &[TextureImage],
    u: f32,
    v: f32,
) -> [f32; 3] {
    let mut color = triangle.sheen_color_factor;
    if let Some(texture_index) = triangle.sheen_color_texture_index
        && let Some(texture) = textures.get(texture_index)
        && let Some(texcoord) = interpolated_texture_texcoord(
            triangle,
            u,
            v,
            triangle.sheen_color_texture_texcoord,
            triangle.sheen_color_texture_transform,
        )
    {
        let texture_rgb = sample_color_texture_linear_rgb(texture, texcoord);
        for channel in 0..3 {
            color[channel] *= texture_rgb[channel];
        }
    }
    [
        color[0].clamp(0.0, 1.0),
        color[1].clamp(0.0, 1.0),
        color[2].clamp(0.0, 1.0),
    ]
}

fn interpolated_sheen_roughness_sample(
    triangle: &Triangle,
    textures: &[TextureImage],
    u: f32,
    v: f32,
) -> f32 {
    let mut roughness = triangle.sheen_roughness_factor;
    if let Some(texture_index) = triangle.sheen_roughness_texture_index
        && let Some(texture) = textures.get(texture_index)
        && let Some(texcoord) = interpolated_texture_texcoord(
            triangle,
            u,
            v,
            triangle.sheen_roughness_texture_texcoord,
            triangle.sheen_roughness_texture_transform,
        )
    {
        roughness *= f32::from(sample_texture(texture, texcoord)[3]) / 255.0;
    }
    roughness.clamp(0.04, 1.0)
}

fn interpolated_anisotropy_strength_sample(
    triangle: &Triangle,
    textures: &[TextureImage],
    u: f32,
    v: f32,
) -> f32 {
    let mut strength = triangle.anisotropy_strength;
    if let Some(texture_index) = triangle.anisotropy_texture_index
        && let Some(texture) = textures.get(texture_index)
        && let Some(texcoord) = interpolated_texture_texcoord(
            triangle,
            u,
            v,
            triangle.anisotropy_texture_texcoord,
            triangle.anisotropy_texture_transform,
        )
    {
        strength *= f32::from(sample_texture(texture, texcoord)[2]) / 255.0;
    }
    strength.clamp(0.0, 1.0)
}

fn interpolated_iridescence_factor_sample(
    triangle: &Triangle,
    textures: &[TextureImage],
    u: f32,
    v: f32,
) -> f32 {
    let mut factor = triangle.iridescence_factor;
    if let Some(texture_index) = triangle.iridescence_texture_index
        && let Some(texture) = textures.get(texture_index)
        && let Some(texcoord) = interpolated_texture_texcoord(
            triangle,
            u,
            v,
            triangle.iridescence_texture_texcoord,
            triangle.iridescence_texture_transform,
        )
    {
        factor *= f32::from(sample_texture(texture, texcoord)[0]) / 255.0;
    }
    factor.clamp(0.0, 1.0)
}

fn interpolated_iridescence_thickness_sample(
    triangle: &Triangle,
    textures: &[TextureImage],
    u: f32,
    v: f32,
) -> f32 {
    let min_nm = triangle.iridescence_thickness_minimum_nm.max(0.0);
    let max_nm = triangle
        .iridescence_thickness_maximum_nm
        .max(min_nm)
        .max(0.0);
    let mut amount = 1.0;
    if let Some(texture_index) = triangle.iridescence_thickness_texture_index
        && let Some(texture) = textures.get(texture_index)
        && let Some(texcoord) = interpolated_texture_texcoord(
            triangle,
            u,
            v,
            triangle.iridescence_thickness_texture_texcoord,
            triangle.iridescence_thickness_texture_transform,
        )
    {
        amount = f32::from(sample_texture(texture, texcoord)[1]) / 255.0;
    }
    min_nm + (max_nm - min_nm) * amount.clamp(0.0, 1.0)
}

fn interpolated_anisotropy_direction(
    triangle: &Triangle,
    textures: &[TextureImage],
    u: f32,
    v: f32,
    normal: [f32; 3],
) -> [f32; 3] {
    let n = normalize(normal);
    let tangent = interpolated_tangent(triangle, u, v)
        .map(|tangent| {
            let raw = normalize([tangent[0], tangent[1], tangent[2]]);
            let t = normalize(sub(raw, scale(n, dot(raw, n))));
            let b = scale(normalize(cross(n, t)), tangent[3]);
            (t, b)
        })
        .unwrap_or_else(|| orthonormal_basis(n));
    let mut direction_xy = [
        triangle.anisotropy_rotation.cos(),
        triangle.anisotropy_rotation.sin(),
    ];
    if let Some(texture_index) = triangle.anisotropy_texture_index
        && let Some(texture) = textures.get(texture_index)
        && let Some(texcoord) = interpolated_texture_texcoord(
            triangle,
            u,
            v,
            triangle.anisotropy_texture_texcoord,
            triangle.anisotropy_texture_transform,
        )
    {
        let sample = sample_texture(texture, texcoord);
        let encoded = [
            f32::from(sample[0]) / 255.0 * 2.0 - 1.0,
            f32::from(sample[1]) / 255.0 * 2.0 - 1.0,
        ];
        if encoded[0].abs() + encoded[1].abs() > 1.0e-4 {
            let base = encoded[1].atan2(encoded[0]) + triangle.anisotropy_rotation;
            direction_xy = [base.cos(), base.sin()];
        }
    }
    normalize(add(
        scale(tangent.0, direction_xy[0]),
        scale(tangent.1, direction_xy[1]),
    ))
}

fn interpolated_specular_factor_sample(
    triangle: &Triangle,
    textures: &[TextureImage],
    u: f32,
    v: f32,
) -> f32 {
    let mut specular = triangle.specular_factor;
    if let Some(texture_index) = triangle.specular_texture_index
        && let Some(texture) = textures.get(texture_index)
        && let Some(texcoord) = interpolated_texture_texcoord(
            triangle,
            u,
            v,
            triangle.specular_texture_texcoord,
            triangle.specular_texture_transform,
        )
    {
        specular *= f32::from(sample_texture(texture, texcoord)[3]) / 255.0;
    }
    specular.clamp(0.0, 1.0)
}

fn interpolated_specular_color_sample(
    triangle: &Triangle,
    textures: &[TextureImage],
    u: f32,
    v: f32,
) -> [f32; 3] {
    let mut color = triangle.specular_color_factor;
    if let Some(texture_index) = triangle.specular_color_texture_index
        && let Some(texture) = textures.get(texture_index)
        && let Some(texcoord) = interpolated_texture_texcoord(
            triangle,
            u,
            v,
            triangle.specular_color_texture_texcoord,
            triangle.specular_color_texture_transform,
        )
    {
        let texture_rgb = sample_color_texture_linear_rgb(texture, texcoord);
        for channel in 0..3 {
            color[channel] *= texture_rgb[channel];
        }
    }
    [
        color[0].clamp(0.0, 1.0),
        color[1].clamp(0.0, 1.0),
        color[2].clamp(0.0, 1.0),
    ]
}

fn interpolated_metallic_roughness_texture_sample(
    triangle: &Triangle,
    textures: &[TextureImage],
    u: f32,
    v: f32,
) -> Option<[u8; 4]> {
    let texture_index = triangle.metallic_roughness_texture_index?;
    let texture = textures.get(texture_index)?;
    let texcoord = interpolated_texture_texcoord(
        triangle,
        u,
        v,
        triangle.metallic_roughness_texture_texcoord,
        triangle.metallic_roughness_texture_transform,
    )?;
    Some(sample_texture(texture, texcoord))
}

fn interpolated_specular_glossiness_texture_sample(
    triangle: &Triangle,
    textures: &[TextureImage],
    u: f32,
    v: f32,
) -> Option<[u8; 4]> {
    let texture_index = triangle.specular_glossiness_texture_index?;
    let texture = textures.get(texture_index)?;
    let texcoord = interpolated_texture_texcoord(
        triangle,
        u,
        v,
        triangle.specular_glossiness_texture_texcoord,
        triangle.specular_glossiness_texture_transform,
    )?;
    Some(sample_texture(texture, texcoord))
}

fn interpolated_texcoord(triangle: &Triangle, u: f32, v: f32, set: u32) -> Option<[f32; 2]> {
    let w = 1.0 - u - v;
    let (a, b, c) = match set {
        0 => (
            triangle.texcoord_a?,
            triangle.texcoord_b?,
            triangle.texcoord_c?,
        ),
        1 => (
            triangle.texcoord1_a?,
            triangle.texcoord1_b?,
            triangle.texcoord1_c?,
        ),
        _ => return None,
    };
    Some([
        a[0] * w + b[0] * u + c[0] * v,
        a[1] * w + b[1] * u + c[1] * v,
    ])
}

fn interpolated_texture_texcoord(
    triangle: &Triangle,
    u: f32,
    v: f32,
    set: u32,
    transform: TextureTransform2D,
) -> Option<[f32; 2]> {
    interpolated_texcoord(triangle, u, v, set).map(|texcoord| transform.apply(texcoord))
}

fn interpolated_tangent(triangle: &Triangle, u: f32, v: f32) -> Option<[f32; 4]> {
    let w = 1.0 - u - v;
    let a = triangle.tangent_a?;
    let b = triangle.tangent_b?;
    let c = triangle.tangent_c?;
    let direction = normalize([
        a[0] * w + b[0] * u + c[0] * v,
        a[1] * w + b[1] * u + c[1] * v,
        a[2] * w + b[2] * u + c[2] * v,
    ]);
    let handedness = if a[3] * w + b[3] * u + c[3] * v < 0.0 {
        -1.0
    } else {
        1.0
    };
    Some([direction[0], direction[1], direction[2], handedness])
}

fn derived_triangle_tangent(
    triangle: &Triangle,
    normal: [f32; 3],
    texture_texcoord: u32,
    texture_transform: TextureTransform2D,
) -> Option<[f32; 4]> {
    let (uv_a, uv_b, uv_c) =
        triangle_texture_texcoords(triangle, texture_texcoord, texture_transform)?;
    let edge1 = sub(triangle.b, triangle.a);
    let edge2 = sub(triangle.c, triangle.a);
    let duv1 = [uv_b[0] - uv_a[0], uv_b[1] - uv_a[1]];
    let duv2 = [uv_c[0] - uv_a[0], uv_c[1] - uv_a[1]];
    let determinant = duv1[0] * duv2[1] - duv2[0] * duv1[1];
    if determinant.abs() <= 1.0e-8 {
        return None;
    }
    let inv_det = 1.0 / determinant;
    let raw_tangent = scale(sub(scale(edge1, duv2[1]), scale(edge2, duv1[1])), inv_det);
    let raw_bitangent = scale(sub(scale(edge2, duv1[0]), scale(edge1, duv2[0])), inv_det);
    let n = normalize(normal);
    let tangent = normalize(sub(raw_tangent, scale(n, dot(raw_tangent, n))));
    if dot(tangent, tangent) <= 1.0e-8 {
        return None;
    }
    let handedness = if dot(cross(n, tangent), raw_bitangent) < 0.0 {
        -1.0
    } else {
        1.0
    };
    Some([tangent[0], tangent[1], tangent[2], handedness])
}

fn triangle_texture_texcoords(
    triangle: &Triangle,
    texture_texcoord: u32,
    texture_transform: TextureTransform2D,
) -> Option<([f32; 2], [f32; 2], [f32; 2])> {
    let (a, b, c) = match texture_texcoord {
        0 => (
            triangle.texcoord_a?,
            triangle.texcoord_b?,
            triangle.texcoord_c?,
        ),
        1 => (
            triangle.texcoord1_a?,
            triangle.texcoord1_b?,
            triangle.texcoord1_c?,
        ),
        _ => return None,
    };
    Some((
        texture_transform.apply(a),
        texture_transform.apply(b),
        texture_transform.apply(c),
    ))
}

fn interpolated_vertex_color(triangle: &Triangle, u: f32, v: f32) -> Option<[u8; 4]> {
    let w = 1.0 - u - v;
    let a = triangle.vertex_color_a?;
    let b = triangle.vertex_color_b?;
    let c = triangle.vertex_color_c?;
    Some([
        interpolated_u8_channel(a[0], b[0], c[0], w, u, v),
        interpolated_u8_channel(a[1], b[1], c[1], w, u, v),
        interpolated_u8_channel(a[2], b[2], c[2], w, u, v),
        interpolated_u8_channel(a[3], b[3], c[3], w, u, v),
    ])
}

fn interpolated_vertex_color_linear(triangle: &Triangle, u: f32, v: f32) -> Option<[f32; 4]> {
    let w = 1.0 - u - v;
    let a = triangle.vertex_color_linear_a?;
    let b = triangle.vertex_color_linear_b?;
    let c = triangle.vertex_color_linear_c?;
    Some([
        a[0] * w + b[0] * u + c[0] * v,
        a[1] * w + b[1] * u + c[1] * v,
        a[2] * w + b[2] * u + c[2] * v,
        a[3] * w + b[3] * u + c[3] * v,
    ])
}

fn interpolated_u8_channel(a: u8, b: u8, c: u8, w: f32, u: f32, v: f32) -> u8 {
    (f32::from(a) * w + f32::from(b) * u + f32::from(c) * v)
        .round()
        .clamp(0.0, 255.0) as u8
}

fn sample_texture(texture: &TextureImage, texcoord: [f32; 2]) -> [u8; 4] {
    if texture.width == 0 || texture.height == 0 || texture.rgba.len() < 4 {
        return [255, 255, 255, 255];
    }
    let u = wrap_texture_coord(texcoord[0], texture.wrap_s);
    let v = wrap_texture_coord(texcoord[1], texture.wrap_t);
    if texture.filter == TextureFilter::Linear {
        return sample_texture_linear(texture, u, v);
    }
    sample_texture_nearest(texture, u, v)
}

fn sample_color_texture_linear_rgb(texture: &TextureImage, texcoord: [f32; 2]) -> [f32; 3] {
    if texture.width == 0 || texture.height == 0 || texture.rgba.len() < 4 {
        return [1.0, 1.0, 1.0];
    }
    let u = wrap_texture_coord(texcoord[0], texture.wrap_s);
    let v = wrap_texture_coord(texcoord[1], texture.wrap_t);
    if texture.filter == TextureFilter::Linear {
        return sample_color_texture_linear_filter(texture, u, v);
    }
    let sample = sample_texture_nearest(texture, u, v);
    srgb_u8_to_linear_rgb([sample[0], sample[1], sample[2]])
}

fn sample_texture_nearest(texture: &TextureImage, u: f32, v: f32) -> [u8; 4] {
    let x = (u * texture.width as f32)
        .floor()
        .clamp(0.0, texture.width.saturating_sub(1) as f32) as u32;
    let y = (v * texture.height as f32)
        .floor()
        .clamp(0.0, texture.height.saturating_sub(1) as f32) as u32;
    texture_pixel(texture, x, y)
}

fn sample_color_texture_linear_filter(texture: &TextureImage, u: f32, v: f32) -> [f32; 3] {
    if texture.width == 1 && texture.height == 1 {
        let sample = texture_pixel(texture, 0, 0);
        return srgb_u8_to_linear_rgb([sample[0], sample[1], sample[2]]);
    }
    let x = u * texture.width.saturating_sub(1) as f32;
    let y = v * texture.height.saturating_sub(1) as f32;
    let x0 = x.floor().clamp(0.0, texture.width.saturating_sub(1) as f32) as u32;
    let y0 = y
        .floor()
        .clamp(0.0, texture.height.saturating_sub(1) as f32) as u32;
    let x1 = (x0 + 1).min(texture.width.saturating_sub(1));
    let y1 = (y0 + 1).min(texture.height.saturating_sub(1));
    let tx = x - x0 as f32;
    let ty = y - y0 as f32;
    let c00 = texture_pixel(texture, x0, y0);
    let c10 = texture_pixel(texture, x1, y0);
    let c01 = texture_pixel(texture, x0, y1);
    let c11 = texture_pixel(texture, x1, y1);
    let c00 = srgb_u8_to_linear_rgb([c00[0], c00[1], c00[2]]);
    let c10 = srgb_u8_to_linear_rgb([c10[0], c10[1], c10[2]]);
    let c01 = srgb_u8_to_linear_rgb([c01[0], c01[1], c01[2]]);
    let c11 = srgb_u8_to_linear_rgb([c11[0], c11[1], c11[2]]);
    [
        bilinear_f32(c00[0], c10[0], c01[0], c11[0], tx, ty),
        bilinear_f32(c00[1], c10[1], c01[1], c11[1], tx, ty),
        bilinear_f32(c00[2], c10[2], c01[2], c11[2], tx, ty),
    ]
}

fn sample_texture_linear(texture: &TextureImage, u: f32, v: f32) -> [u8; 4] {
    if texture.width == 1 && texture.height == 1 {
        return texture_pixel(texture, 0, 0);
    }
    let x = u * texture.width.saturating_sub(1) as f32;
    let y = v * texture.height.saturating_sub(1) as f32;
    let x0 = x.floor().clamp(0.0, texture.width.saturating_sub(1) as f32) as u32;
    let y0 = y
        .floor()
        .clamp(0.0, texture.height.saturating_sub(1) as f32) as u32;
    let x1 = (x0 + 1).min(texture.width.saturating_sub(1));
    let y1 = (y0 + 1).min(texture.height.saturating_sub(1));
    let tx = x - x0 as f32;
    let ty = y - y0 as f32;
    let c00 = texture_pixel(texture, x0, y0);
    let c10 = texture_pixel(texture, x1, y0);
    let c01 = texture_pixel(texture, x0, y1);
    let c11 = texture_pixel(texture, x1, y1);
    [
        bilinear_u8(c00[0], c10[0], c01[0], c11[0], tx, ty),
        bilinear_u8(c00[1], c10[1], c01[1], c11[1], tx, ty),
        bilinear_u8(c00[2], c10[2], c01[2], c11[2], tx, ty),
        bilinear_u8(c00[3], c10[3], c01[3], c11[3], tx, ty),
    ]
}

fn texture_pixel(texture: &TextureImage, x: u32, y: u32) -> [u8; 4] {
    let offset = ((y * texture.width + x) * 4) as usize;
    [
        *texture.rgba.get(offset).unwrap_or(&255),
        *texture.rgba.get(offset + 1).unwrap_or(&255),
        *texture.rgba.get(offset + 2).unwrap_or(&255),
        *texture.rgba.get(offset + 3).unwrap_or(&255),
    ]
}

fn decoded_environment_image_rgb(image: image::DynamicImage) -> (u32, u32, Vec<[f32; 3]>) {
    let is_float_radiance = matches!(
        image.color(),
        image::ColorType::Rgb32F | image::ColorType::Rgba32F
    );
    let image = image.to_rgb32f();
    let width = image.width();
    let height = image.height();
    let rgb = image
        .pixels()
        .map(|pixel| {
            let channels = pixel.0;
            if is_float_radiance {
                [
                    channels[0].max(0.0) * 255.0,
                    channels[1].max(0.0) * 255.0,
                    channels[2].max(0.0) * 255.0,
                ]
            } else {
                [
                    srgb_unit_to_linear_unit(channels[0]) * 255.0,
                    srgb_unit_to_linear_unit(channels[1]) * 255.0,
                    srgb_unit_to_linear_unit(channels[2]) * 255.0,
                ]
            }
        })
        .collect::<Vec<_>>();
    (width, height, rgb)
}

fn load_environment_texture(path: &Path) -> Result<EnvironmentTexture, String> {
    let image = ImageReader::open(path)
        .map_err(|err| format!("open environment map {}: {err}", path.display()))?
        .decode()
        .map_err(|err| format!("decode environment map {}: {err}", path.display()))?;
    let (width, height, rgb) = decoded_environment_image_rgb(image);
    let roughness_mips = build_environment_roughness_mips(width, height, &rgb);
    let diffuse_irradiance = build_environment_diffuse_irradiance(width, height, &rgb);
    Ok(EnvironmentTexture {
        width,
        height,
        rgb,
        roughness_mips,
        diffuse_irradiance,
    })
}

fn sample_environment_texture(
    texture: &EnvironmentTexture,
    direction: [f32; 3],
    rotation_deg: f32,
) -> [f32; 3] {
    if texture.width == 0 || texture.height == 0 || texture.rgb.is_empty() {
        return [0.0, 0.0, 0.0];
    }
    let [u, v] = environment_uv_for_direction(direction, rotation_deg);
    sample_environment_texture_uv(texture, u, v)
}

fn sample_environment_texture_uv(texture: &EnvironmentTexture, u: f32, v: f32) -> [f32; 3] {
    if texture.width == 1 && texture.height == 1 {
        return environment_texture_pixel(texture, 0, 0);
    }
    let x = u.rem_euclid(1.0) * texture.width as f32;
    let y = v.clamp(0.0, 1.0) * texture.height.saturating_sub(1) as f32;
    let x0 = x.floor() as u32 % texture.width;
    let x1 = (x0 + 1) % texture.width;
    let y0 = y
        .floor()
        .clamp(0.0, texture.height.saturating_sub(1) as f32) as u32;
    let y1 = (y0 + 1).min(texture.height.saturating_sub(1));
    let tx = x - x.floor();
    let ty = y - y0 as f32;
    let c00 = environment_texture_pixel(texture, x0, y0);
    let c10 = environment_texture_pixel(texture, x1, y0);
    let c01 = environment_texture_pixel(texture, x0, y1);
    let c11 = environment_texture_pixel(texture, x1, y1);
    [
        bilinear_f32(c00[0], c10[0], c01[0], c11[0], tx, ty),
        bilinear_f32(c00[1], c10[1], c01[1], c11[1], tx, ty),
        bilinear_f32(c00[2], c10[2], c01[2], c11[2], tx, ty),
    ]
}

fn environment_texture_pixel(texture: &EnvironmentTexture, x: u32, y: u32) -> [f32; 3] {
    *texture
        .rgb
        .get((y * texture.width + x) as usize)
        .unwrap_or(&[0.0, 0.0, 0.0])
}

fn build_environment_diffuse_irradiance(
    width: u32,
    height: u32,
    rgb: &[[f32; 3]],
) -> Option<EnvironmentTextureMip> {
    if width == 0 || height == 0 || rgb.is_empty() {
        return None;
    }
    let source = EnvironmentTextureMip {
        width,
        height,
        rgb: rgb.to_vec(),
    };
    let out_width = width.min(32).max(1);
    let out_height = height.min(16).max(1);
    let mut irradiance = vec![[0.0; 3]; (out_width * out_height) as usize];
    for y in 0..out_height {
        for x in 0..out_width {
            let u = (x as f32 + 0.5) / out_width as f32;
            let v = if out_height <= 1 {
                0.5
            } else {
                y as f32 / out_height.saturating_sub(1) as f32
            };
            let normal = environment_direction_from_uv(u, v);
            irradiance[(y * out_width + x) as usize] =
                convolve_environment_diffuse_irradiance(&source, normal);
        }
    }
    Some(EnvironmentTextureMip {
        width: out_width,
        height: out_height,
        rgb: irradiance,
    })
}

fn convolve_environment_diffuse_irradiance(
    source: &EnvironmentTextureMip,
    normal: [f32; 3],
) -> [f32; 3] {
    let normal = normalize(normal);
    let (tangent, bitangent) = orthonormal_basis(normal);
    let rings = [
        (0.0_f32, 1.0_f32, 1_usize),
        (35.0_f32.to_radians(), 0.80_f32, 8_usize),
        (65.0_f32.to_radians(), 0.45_f32, 8_usize),
    ];
    let mut rgb = [0.0_f32; 3];
    let mut total = 0.0_f32;
    for (theta, ring_weight, count) in rings {
        let sin_theta = theta.sin();
        let cos_theta = theta.cos().max(0.0);
        for sample in 0..count {
            let phi = if count == 1 {
                0.0
            } else {
                sample as f32 / count as f32 * 2.0 * PI
            };
            let sample_dir = normalize(add(
                scale(normal, cos_theta),
                add(
                    scale(tangent, phi.cos() * sin_theta),
                    scale(bitangent, phi.sin() * sin_theta),
                ),
            ));
            let weight = cos_theta * ring_weight;
            let sample_rgb = sample_environment_mip_direction(source, sample_dir, 0.0);
            for channel in 0..3 {
                rgb[channel] += sample_rgb[channel] * weight;
            }
            total += weight;
        }
    }
    if total <= f32::EPSILON {
        return sample_environment_mip_direction(source, normal, 0.0);
    }
    scale(rgb, 1.0 / total)
}

fn build_environment_roughness_mips(
    width: u32,
    height: u32,
    rgb: &[[f32; 3]],
) -> Vec<EnvironmentTextureMip> {
    let mut mips = Vec::new();
    if width == 0 || height == 0 || rgb.is_empty() {
        return mips;
    }
    let mut current = EnvironmentTextureMip {
        width,
        height,
        rgb: rgb.to_vec(),
    };
    mips.push(current.clone());
    while (current.width > 1 || current.height > 1) && mips.len() < 8 {
        current = downsample_environment_mip(&current);
        mips.push(current.clone());
    }
    mips
}

fn downsample_environment_mip(source: &EnvironmentTextureMip) -> EnvironmentTextureMip {
    let width = (source.width / 2).max(1);
    let height = (source.height / 2).max(1);
    let mut rgb = vec![[0.0; 3]; (width * height) as usize];
    for y in 0..height {
        for x in 0..width {
            let x0 = x * 2;
            let y0 = y * 2;
            let samples = [
                environment_mip_pixel(source, x0, y0),
                environment_mip_pixel(source, x0 + 1, y0),
                environment_mip_pixel(source, x0, y0 + 1),
                environment_mip_pixel(source, x0 + 1, y0 + 1),
            ];
            let mut sum = [0.0_f32; 3];
            for sample in samples {
                for channel in 0..3 {
                    sum[channel] += sample[channel];
                }
            }
            rgb[(y * width + x) as usize] = [sum[0] * 0.25, sum[1] * 0.25, sum[2] * 0.25];
        }
    }
    EnvironmentTextureMip { width, height, rgb }
}

fn sample_prefiltered_environment_texture(
    texture: &EnvironmentTexture,
    direction: [f32; 3],
    rotation_deg: f32,
    roughness: f32,
) -> [f32; 3] {
    if texture.roughness_mips.is_empty() {
        return sample_environment_texture(texture, direction, rotation_deg);
    }
    let [u, v] = environment_uv_for_direction(direction, rotation_deg);
    let roughness = roughness.clamp(0.0, 1.0);
    let mip = roughness * roughness * texture.roughness_mips.len().saturating_sub(1) as f32;
    let low_index = mip.floor() as usize;
    let high_index = (low_index + 1).min(texture.roughness_mips.len() - 1);
    let amount = mip - low_index as f32;
    let low = sample_environment_mip_uv(&texture.roughness_mips[low_index], u, v);
    let high = sample_environment_mip_uv(&texture.roughness_mips[high_index], u, v);
    lerp_vec3(low, high, amount)
}

fn sample_diffuse_environment_texture(
    texture: &EnvironmentTexture,
    normal: [f32; 3],
    rotation_deg: f32,
) -> [f32; 3] {
    texture
        .diffuse_irradiance
        .as_ref()
        .map(|irradiance| sample_environment_mip_direction(irradiance, normal, rotation_deg))
        .unwrap_or_else(|| sample_environment_texture(texture, normal, rotation_deg))
}

fn sample_environment_mip_direction(
    mip: &EnvironmentTextureMip,
    direction: [f32; 3],
    rotation_deg: f32,
) -> [f32; 3] {
    let [u, v] = environment_uv_for_direction(direction, rotation_deg);
    sample_environment_mip_uv(mip, u, v)
}

fn environment_uv_for_direction(direction: [f32; 3], rotation_deg: f32) -> [f32; 2] {
    let direction = normalize(direction);
    let yaw = direction[1].atan2(direction[0]) + rotation_deg.to_radians();
    [
        (yaw / (2.0 * PI) + 0.5).rem_euclid(1.0),
        direction[2].clamp(-1.0, 1.0).acos() / PI,
    ]
}

fn environment_direction_from_uv(u: f32, v: f32) -> [f32; 3] {
    let yaw = (u - 0.5) * 2.0 * PI;
    let theta = v.clamp(0.0, 1.0) * PI;
    let z = theta.cos();
    let radius = theta.sin();
    normalize([yaw.cos() * radius, yaw.sin() * radius, z])
}

fn sample_environment_mip_uv(mip: &EnvironmentTextureMip, u: f32, v: f32) -> [f32; 3] {
    if mip.width == 0 || mip.height == 0 || mip.rgb.is_empty() {
        return [0.0, 0.0, 0.0];
    }
    if mip.width == 1 && mip.height == 1 {
        return environment_mip_pixel(mip, 0, 0);
    }
    let x = u.rem_euclid(1.0) * mip.width as f32;
    let y = v.clamp(0.0, 1.0) * mip.height.saturating_sub(1) as f32;
    let x0 = x.floor() as u32 % mip.width;
    let x1 = (x0 + 1) % mip.width;
    let y0 = y.floor().clamp(0.0, mip.height.saturating_sub(1) as f32) as u32;
    let y1 = (y0 + 1).min(mip.height.saturating_sub(1));
    let tx = x - x.floor();
    let ty = y - y0 as f32;
    let c00 = environment_mip_pixel(mip, x0, y0);
    let c10 = environment_mip_pixel(mip, x1, y0);
    let c01 = environment_mip_pixel(mip, x0, y1);
    let c11 = environment_mip_pixel(mip, x1, y1);
    [
        bilinear_f32(c00[0], c10[0], c01[0], c11[0], tx, ty),
        bilinear_f32(c00[1], c10[1], c01[1], c11[1], tx, ty),
        bilinear_f32(c00[2], c10[2], c01[2], c11[2], tx, ty),
    ]
}

fn environment_mip_pixel(mip: &EnvironmentTextureMip, x: u32, y: u32) -> [f32; 3] {
    if mip.width == 0 || mip.height == 0 {
        return [0.0, 0.0, 0.0];
    }
    let x = x % mip.width;
    let y = y.min(mip.height.saturating_sub(1));
    *mip.rgb
        .get((y * mip.width + x) as usize)
        .unwrap_or(&[0.0, 0.0, 0.0])
}

fn build_environment_brdf_lut(size: u32) -> EnvironmentBrdfLut {
    let size = size.max(2);
    let mut values = Vec::with_capacity((size * size) as usize);
    for y in 0..size {
        let roughness = y as f32 / (size - 1) as f32;
        for x in 0..size {
            let ndotv = x as f32 / (size - 1) as f32;
            values.push(environment_brdf_coefficients(ndotv, roughness));
        }
    }
    EnvironmentBrdfLut { size, values }
}

fn environment_brdf_coefficients(ndotv: f32, roughness: f32) -> [f32; 2] {
    let ndotv = ndotv.clamp(0.0, 1.0);
    let roughness = roughness.clamp(0.0, 1.0);
    let grazing = (1.0 - ndotv).powi(5);
    let smoothness = 1.0 - roughness;
    let visibility = ndotv / (ndotv * (1.0 - roughness * 0.55) + roughness * 0.55 + 1.0e-4);
    let scale = (visibility * (0.20 + 0.80 * smoothness * smoothness)).clamp(0.0, 1.0);
    let bias = (grazing * (0.12 + 0.18 * smoothness)).clamp(0.0, 1.0);
    [scale, bias]
}

fn sample_environment_brdf_lut(lut: &EnvironmentBrdfLut, ndotv: f32, roughness: f32) -> [f32; 2] {
    if lut.size <= 1 || lut.values.is_empty() {
        return environment_brdf_coefficients(ndotv, roughness);
    }
    let x = ndotv.clamp(0.0, 1.0) * (lut.size - 1) as f32;
    let y = roughness.clamp(0.0, 1.0) * (lut.size - 1) as f32;
    let x0 = x.floor() as u32;
    let y0 = y.floor() as u32;
    let x1 = (x0 + 1).min(lut.size - 1);
    let y1 = (y0 + 1).min(lut.size - 1);
    let tx = x - x0 as f32;
    let ty = y - y0 as f32;
    let c00 = environment_brdf_lut_value(lut, x0, y0);
    let c10 = environment_brdf_lut_value(lut, x1, y0);
    let c01 = environment_brdf_lut_value(lut, x0, y1);
    let c11 = environment_brdf_lut_value(lut, x1, y1);
    [
        bilinear_f32(c00[0], c10[0], c01[0], c11[0], tx, ty),
        bilinear_f32(c00[1], c10[1], c01[1], c11[1], tx, ty),
    ]
}

fn environment_brdf_lut_value(lut: &EnvironmentBrdfLut, x: u32, y: u32) -> [f32; 2] {
    *lut.values
        .get((y.min(lut.size - 1) * lut.size + x.min(lut.size - 1)) as usize)
        .unwrap_or(&[1.0, 0.0])
}

fn bilinear_u8(c00: u8, c10: u8, c01: u8, c11: u8, tx: f32, ty: f32) -> u8 {
    let top = f32::from(c00) * (1.0 - tx) + f32::from(c10) * tx;
    let bottom = f32::from(c01) * (1.0 - tx) + f32::from(c11) * tx;
    (top * (1.0 - ty) + bottom * ty).round().clamp(0.0, 255.0) as u8
}

fn bilinear_f32(c00: f32, c10: f32, c01: f32, c11: f32, tx: f32, ty: f32) -> f32 {
    let top = c00 * (1.0 - tx) + c10 * tx;
    let bottom = c01 * (1.0 - tx) + c11 * tx;
    top * (1.0 - ty) + bottom * ty
}

fn wrap_texture_coord(coord: f32, wrap: TextureWrap) -> f32 {
    match wrap {
        TextureWrap::Repeat => coord.rem_euclid(1.0),
        TextureWrap::ClampToEdge => coord.clamp(0.0, 1.0),
        TextureWrap::MirroredRepeat => {
            let mirrored = coord.rem_euclid(2.0);
            if mirrored > 1.0 {
                2.0 - mirrored
            } else {
                mirrored
            }
        }
    }
}

fn multiply_rgb(base: [u8; 3], texture: [u8; 3]) -> [u8; 3] {
    [
        ((base[0] as u16 * texture[0] as u16) / 255) as u8,
        ((base[1] as u16 * texture[1] as u16) / 255) as u8,
        ((base[2] as u16 * texture[2] as u16) / 255) as u8,
    ]
}

fn multiply_rgb_by_linear_unit(base: [f32; 3], texture: [f32; 3]) -> [f32; 3] {
    [
        base[0] * texture[0].clamp(0.0, 1.0),
        base[1] * texture[1].clamp(0.0, 1.0),
        base[2] * texture[2].clamp(0.0, 1.0),
    ]
}

fn multiply_rgba(left: [u8; 4], right: [u8; 4]) -> [u8; 4] {
    [
        ((left[0] as u16 * right[0] as u16) / 255) as u8,
        ((left[1] as u16 * right[1] as u16) / 255) as u8,
        ((left[2] as u16 * right[2] as u16) / 255) as u8,
        ((left[3] as u16 * right[3] as u16) / 255) as u8,
    ]
}

fn interpolated_normal(triangle: &Triangle, u: f32, v: f32) -> [f32; 3] {
    let w = 1.0 - u - v;
    normalize([
        triangle.normal_a[0] * w + triangle.normal_b[0] * u + triangle.normal_c[0] * v,
        triangle.normal_a[1] * w + triangle.normal_b[1] * u + triangle.normal_c[1] * v,
        triangle.normal_a[2] * w + triangle.normal_b[2] * u + triangle.normal_c[2] * v,
    ])
}

fn mapped_normal(
    triangle: &Triangle,
    textures: &[TextureImage],
    u: f32,
    v: f32,
    base_normal: [f32; 3],
) -> [f32; 3] {
    mapped_texture_normal(
        triangle,
        textures,
        u,
        v,
        base_normal,
        triangle.normal_texture_index,
        triangle.normal_texture_texcoord,
        triangle.normal_texture_transform,
        triangle.normal_scale,
    )
}

fn mapped_clearcoat_normal(
    triangle: &Triangle,
    textures: &[TextureImage],
    u: f32,
    v: f32,
    base_normal: [f32; 3],
    fallback_normal: [f32; 3],
) -> [f32; 3] {
    if triangle.clearcoat_normal_texture_index.is_none() {
        return fallback_normal;
    }
    mapped_texture_normal(
        triangle,
        textures,
        u,
        v,
        base_normal,
        triangle.clearcoat_normal_texture_index,
        triangle.clearcoat_normal_texture_texcoord,
        triangle.clearcoat_normal_texture_transform,
        triangle.clearcoat_normal_scale,
    )
}

fn mapped_texture_normal(
    triangle: &Triangle,
    textures: &[TextureImage],
    u: f32,
    v: f32,
    base_normal: [f32; 3],
    texture_index: Option<usize>,
    texture_texcoord: u32,
    texture_transform: TextureTransform2D,
    normal_scale: f32,
) -> [f32; 3] {
    let Some(texture_index) = texture_index else {
        return base_normal;
    };
    let Some(texture) = textures.get(texture_index) else {
        return base_normal;
    };
    let Some(texcoord) =
        interpolated_texture_texcoord(triangle, u, v, texture_texcoord, texture_transform)
    else {
        return base_normal;
    };
    let n = normalize(base_normal);
    let tangent = interpolated_tangent(triangle, u, v)
        .or_else(|| derived_triangle_tangent(triangle, n, texture_texcoord, texture_transform));
    let Some(tangent) = tangent else {
        return base_normal;
    };

    let tangent_normal = normal_texture_sample(texture, texcoord, normal_scale);
    let raw_tangent = normalize([tangent[0], tangent[1], tangent[2]]);
    let t = normalize(sub(raw_tangent, scale(n, dot(raw_tangent, n))));
    let b = scale(normalize(cross(n, t)), tangent[3]);
    normalize(add(
        add(scale(t, tangent_normal[0]), scale(b, tangent_normal[1])),
        scale(n, tangent_normal[2]),
    ))
}

fn normal_texture_sample(
    texture: &TextureImage,
    texcoord: [f32; 2],
    scale_factor: f32,
) -> [f32; 3] {
    let sample = sample_texture(texture, texcoord);
    normalize([
        ((f32::from(sample[0]) / 255.0) * 2.0 - 1.0) * scale_factor,
        ((f32::from(sample[1]) / 255.0) * 2.0 - 1.0) * scale_factor,
        (f32::from(sample[2]) / 255.0) * 2.0 - 1.0,
    ])
}

fn oriented_normal(normal: [f32; 3], ray_dir: [f32; 3], double_sided: bool) -> [f32; 3] {
    if double_sided && dot(normal, ray_dir) > 0.0 {
        scale(normal, -1.0)
    } else {
        normal
    }
}

#[cfg(test)]
fn shade_hit(hit: &Hit, objects: &[RenderObject]) -> [u8; 3] {
    let settings = PreparedRenderSettings::from_settings(RenderSettings::default());
    tone_map_rgb(
        shade_hit_with_lights(hit, objects, &fallback_lights(), &settings),
        &settings.settings,
    )
}

#[allow(dead_code)]
fn shade_hit_local_rgb(
    hit: &Hit,
    objects: &[RenderObject],
    lights: &[RenderLight],
    settings: &PreparedRenderSettings,
) -> [f32; 3] {
    let object_bvh = ObjectBvh::from_objects(objects);
    let intersector = SceneIntersector::new(objects, &object_bvh);
    shade_hit_local_rgb_with_intersector(hit, &intersector, lights, settings)
}

fn shade_hit_local_rgb_with_intersector(
    hit: &Hit,
    intersector: &SceneIntersector<'_>,
    lights: &[RenderLight],
    settings: &PreparedRenderSettings,
) -> [f32; 3] {
    if hit.unlit {
        return [
            hit.color_linear_rgb[0] * 255.0 + hit.emission_rgb[0],
            hit.color_linear_rgb[1] * 255.0 + hit.emission_rgb[1],
            hit.color_linear_rgb[2] * 255.0 + hit.emission_rgb[2],
        ];
    }

    let mut shaded = ambient_debug_rgb_with_intersector(hit, intersector, lights, settings);
    let reflection = environment_reflection_rgb(hit, settings);
    for channel in 0..3 {
        shaded[channel] += reflection[channel];
    }
    for light in lights {
        for sample in light_samples(light, hit.point, settings) {
            let visibility_rgb = shadow_visibility_rgb_with_intersector(
                hit,
                sample.direction,
                sample.max_t,
                intersector,
            );
            let direct = shade_pbr_direct_rgb(hit, &sample, visibility_rgb);
            for channel in 0..3 {
                shaded[channel] += direct[channel];
            }
        }
    }
    for channel in 0..3 {
        shaded[channel] += hit.emission_rgb[channel];
    }
    shaded
}

fn rough_reflection_directions(
    direction: [f32; 3],
    normal: [f32; 3],
    roughness: f32,
    sample_count: u32,
) -> Vec<[f32; 3]> {
    let direction = normalize(direction);
    let normal = normalize(normal);
    let sample_count = sample_count.clamp(1, 64);
    if sample_count == 1 || roughness <= 0.05 {
        return vec![direction];
    }
    let (tangent, bitangent) = tangent_basis(direction);
    let radius = roughness.clamp(0.0, 1.0).powi(2) * 0.45;
    let mut directions = Vec::with_capacity(sample_count as usize);
    directions.push(direction);
    for sample_index in 1..sample_count {
        let index = sample_index as f32;
        let count = sample_count.max(2) as f32;
        let radial = (index / (count - 1.0)).sqrt() * radius;
        let angle = index * 2.399_963_1;
        let sample = normalize(add(
            direction,
            add(
                scale(tangent, radial * angle.cos()),
                scale(bitangent, radial * angle.sin()),
            ),
        ));
        if dot(sample, normal) > 1.0e-4 {
            directions.push(sample);
        }
    }
    directions
}

#[allow(dead_code)]
fn scene_reflection_rgb(
    hit: &Hit,
    objects: &[RenderObject],
    lights: &[RenderLight],
    settings: &PreparedRenderSettings,
    remaining_bounces: u32,
) -> [f32; 3] {
    let object_bvh = ObjectBvh::from_objects(objects);
    let intersector = SceneIntersector::new(objects, &object_bvh);
    scene_reflection_rgb_with_intersector(hit, &intersector, lights, settings, remaining_bounces)
}

fn scene_reflection_rgb_with_intersector(
    hit: &Hit,
    intersector: &SceneIntersector<'_>,
    lights: &[RenderLight],
    settings: &PreparedRenderSettings,
    remaining_bounces: u32,
) -> [f32; 3] {
    if remaining_bounces == 0 {
        return [0.0, 0.0, 0.0];
    }
    let v = normalize(hit.view_dir);
    let mut out = [0.0_f32; 3];

    let roughness = hit.roughness.clamp(0.04, 1.0);
    let gloss = (1.0 - roughness).clamp(0.0, 1.0);
    if gloss > 1.0e-4 {
        let n = normalize(hit.normal);
        let ndotv = dot(n, v).abs().clamp(0.0, 1.0);
        let brdf = sample_environment_brdf_lut(&settings.environment_brdf_lut, ndotv, roughness);
        let f0 = material_f0_rgb(hit);
        let fresnel_tint = iridescence_fresnel_tint_rgb(hit, ndotv);
        let metallic = hit.metallic.clamp(0.0, 1.0);
        let strength = (0.35 + 0.65 * metallic) * gloss * gloss * hit.occlusion.clamp(0.0, 1.0);
        let clearcoat_base_energy = clearcoat_base_scene_reflection_energy(hit, v);
        let strength_rgb = [
            (f0[0] * brdf[0] + brdf[1]).clamp(0.0, 1.0)
                * fresnel_tint[0]
                * strength
                * clearcoat_base_energy,
            (f0[1] * brdf[0] + brdf[1]).clamp(0.0, 1.0)
                * fresnel_tint[1]
                * strength
                * clearcoat_base_energy,
            (f0[2] * brdf[0] + brdf[1]).clamp(0.0, 1.0)
                * fresnel_tint[2]
                * strength
                * clearcoat_base_energy,
        ];
        let base_reflection = trace_scene_reflection_lobe_with_intersector(
            hit,
            intersector,
            lights,
            settings,
            remaining_bounces,
            n,
            roughness,
            strength_rgb,
        );
        for channel in 0..3 {
            out[channel] += base_reflection[channel];
        }
    }

    let clearcoat = hit.clearcoat_factor.clamp(0.0, 1.0);
    if clearcoat > 1.0e-4 {
        let clearcoat_normal = normalize(hit.clearcoat_normal);
        let clearcoat_roughness = hit.clearcoat_roughness.clamp(0.04, 1.0);
        let clearcoat_ndotv = dot(clearcoat_normal, v).abs().clamp(0.0, 1.0);
        let clearcoat_brdf = sample_environment_brdf_lut(
            &settings.environment_brdf_lut,
            clearcoat_ndotv,
            clearcoat_roughness,
        );
        let clearcoat_fresnel =
            (CLEARCOAT_DIELECTRIC_F0 * clearcoat_brdf[0] + clearcoat_brdf[1]).clamp(0.0, 1.0);
        let clearcoat_strength = clearcoat
            * (1.0 - clearcoat_roughness).clamp(0.0, 1.0).sqrt()
            * hit.occlusion.clamp(0.0, 1.0);
        let clearcoat_strength_rgb = [clearcoat_fresnel * clearcoat_strength; 3];
        let clearcoat_reflection = trace_scene_reflection_lobe_with_intersector(
            hit,
            intersector,
            lights,
            settings,
            remaining_bounces,
            clearcoat_normal,
            clearcoat_roughness,
            clearcoat_strength_rgb,
        );
        for channel in 0..3 {
            out[channel] += clearcoat_reflection[channel];
        }
    }

    out
}

fn clearcoat_base_scene_reflection_energy(hit: &Hit, view_dir: [f32; 3]) -> f32 {
    let clearcoat = hit.clearcoat_factor.clamp(0.0, 1.0);
    if clearcoat <= 1.0e-4 {
        return 1.0;
    }
    let clearcoat_normal = normalize(hit.clearcoat_normal);
    let view_cos = dot(clearcoat_normal, normalize(view_dir))
        .abs()
        .clamp(0.0, 1.0);
    clearcoat_base_attenuation(clearcoat, view_cos, None)
}

fn trace_scene_reflection_lobe_with_intersector(
    hit: &Hit,
    intersector: &SceneIntersector<'_>,
    lights: &[RenderLight],
    settings: &PreparedRenderSettings,
    remaining_bounces: u32,
    normal: [f32; 3],
    roughness: f32,
    strength_rgb: [f32; 3],
) -> [f32; 3] {
    let n = normalize(normal);
    let v = normalize(hit.view_dir);
    let incident = scale(v, -1.0);
    let reflection_dir = reflect(incident, n);
    if dot(reflection_dir, n) <= 1.0e-4 {
        return [0.0, 0.0, 0.0];
    }

    let directions = rough_reflection_directions(
        reflection_dir,
        n,
        roughness,
        settings.settings.rough_reflection_samples,
    );
    let mut reflected_rgb = [0.0_f32; 3];
    let sample_count = directions.len().max(1) as f32;
    let mut any_hit = false;
    for direction in directions {
        let ray = Ray {
            origin: add(hit.point, scale(direction, 1.0e-3)),
            dir: direction,
        };
        let Some(reflected_hit) = intersector
            .hits(&ray)
            .into_iter()
            .filter(|sample_hit| sample_hit.t > 1.0e-3)
            .min_by(|left, right| left.t.total_cmp(&right.t))
        else {
            continue;
        };
        let sample_rgb =
            shade_hit_local_rgb_with_intersector(&reflected_hit, intersector, lights, settings);
        for channel in 0..3 {
            reflected_rgb[channel] += sample_rgb[channel];
        }
        if remaining_bounces > 1 {
            let bounce_rgb = scene_reflection_rgb_with_intersector(
                &reflected_hit,
                intersector,
                lights,
                settings,
                remaining_bounces - 1,
            );
            for channel in 0..3 {
                reflected_rgb[channel] += bounce_rgb[channel];
            }
        }
        any_hit = true;
    }
    if !any_hit {
        return [0.0, 0.0, 0.0];
    }
    reflected_rgb = scale(reflected_rgb, 1.0 / sample_count);
    [
        reflected_rgb[0] * strength_rgb[0],
        reflected_rgb[1] * strength_rgb[1],
        reflected_rgb[2] * strength_rgb[2],
    ]
}

#[allow(dead_code)]
fn shade_hit_with_lights(
    hit: &Hit,
    objects: &[RenderObject],
    lights: &[RenderLight],
    settings: &PreparedRenderSettings,
) -> [f32; 3] {
    let object_bvh = ObjectBvh::from_objects(objects);
    let intersector = SceneIntersector::new(objects, &object_bvh);
    shade_hit_with_lights_with_intersector(hit, &intersector, lights, settings)
}

fn shade_hit_with_lights_with_intersector(
    hit: &Hit,
    intersector: &SceneIntersector<'_>,
    lights: &[RenderLight],
    settings: &PreparedRenderSettings,
) -> [f32; 3] {
    let mut shaded = shade_hit_local_rgb_with_intersector(hit, intersector, lights, settings);
    let reflection = scene_reflection_rgb_with_intersector(
        hit,
        intersector,
        lights,
        settings,
        settings.settings.specular_reflection_bounces,
    );
    for channel in 0..3 {
        shaded[channel] += reflection[channel];
    }
    shaded
}

#[derive(Clone, Copy, Debug)]
struct LightSample {
    direction: [f32; 3],
    color_rgb: [f32; 3],
    intensity: f32,
    max_t: Option<f32>,
}

fn light_samples(
    light: &RenderLight,
    point: [f32; 3],
    settings: &PreparedRenderSettings,
) -> Vec<LightSample> {
    match light.kind {
        RenderLightKind::Directional {
            direction,
            angular_radius_deg,
        } => directional_light_samples(light, direction, angular_radius_deg, settings),
        RenderLightKind::Point { position, range_m } => {
            point_light_samples(light, point, position, range_m, settings)
        }
        RenderLightKind::Spot {
            position,
            direction,
            inner_cos,
            outer_cos,
            range_m,
        } => spot_light_samples(
            light, point, position, direction, inner_cos, outer_cos, range_m, settings,
        ),
        RenderLightKind::AreaTriangle {
            a,
            b,
            c,
            normal,
            double_sided,
        } => area_triangle_light_samples(light, point, a, b, c, normal, double_sided, settings),
    }
}

fn directional_light_samples(
    light: &RenderLight,
    direction: [f32; 3],
    angular_radius_deg: f32,
    settings: &PreparedRenderSettings,
) -> Vec<LightSample> {
    let direction = normalize(direction);
    let sample_count = if angular_radius_deg > 1.0e-6 {
        finite_light_sample_count(settings)
    } else {
        1
    };
    let radius = angular_radius_deg.max(0.0).to_radians().tan();
    if sample_count <= 1 || radius <= 1.0e-6 {
        return vec![LightSample {
            direction,
            color_rgb: light.color_rgb,
            intensity: light.intensity.max(0.0),
            max_t: None,
        }];
    }

    let (tangent, bitangent) = tangent_basis(direction);
    let per_sample = light.intensity.max(0.0) / sample_count as f32;
    (0..sample_count)
        .map(|sample_index| {
            let disk = soft_shadow_disk_sample(sample_index, sample_count);
            LightSample {
                direction: normalize(add(
                    direction,
                    add(
                        scale(tangent, disk[0] * radius),
                        scale(bitangent, disk[1] * radius),
                    ),
                )),
                color_rgb: light.color_rgb,
                intensity: per_sample,
                max_t: None,
            }
        })
        .collect()
}

fn point_light_samples(
    light: &RenderLight,
    point: [f32; 3],
    position: [f32; 3],
    range_m: Option<f32>,
    settings: &PreparedRenderSettings,
) -> Vec<LightSample> {
    finite_light_sample_positions(position, point, settings)
        .into_iter()
        .filter_map(|sample_position| {
            let offset = sub(sample_position, point);
            let distance = dot(offset, offset).sqrt();
            if distance <= 1.0e-6 || range_m.is_some_and(|range| distance > range) {
                return None;
            }
            let sample_count = finite_light_sample_count(settings) as f32;
            Some(LightSample {
                direction: scale(offset, 1.0 / distance),
                color_rgb: light.color_rgb,
                intensity: light.intensity.max(0.0) * point_light_attenuation(distance, range_m)
                    / sample_count,
                max_t: Some(distance),
            })
        })
        .collect()
}

fn spot_light_angular_attenuation(cone_cos: f32, inner_cos: f32, outer_cos: f32) -> f32 {
    let scale = 1.0 / (inner_cos - outer_cos).max(0.001);
    let offset = -outer_cos * scale;
    let angular = (cone_cos * scale + offset).clamp(0.0, 1.0);
    angular * angular
}

fn spot_light_samples(
    light: &RenderLight,
    point: [f32; 3],
    position: [f32; 3],
    direction: [f32; 3],
    inner_cos: f32,
    outer_cos: f32,
    range_m: Option<f32>,
    settings: &PreparedRenderSettings,
) -> Vec<LightSample> {
    finite_light_sample_positions(position, point, settings)
        .into_iter()
        .filter_map(|sample_position| {
            let offset = sub(sample_position, point);
            let distance = dot(offset, offset).sqrt();
            if distance <= 1.0e-6 || range_m.is_some_and(|range| distance > range) {
                return None;
            }
            let surface_to_light = scale(offset, 1.0 / distance);
            let light_to_surface = scale(surface_to_light, -1.0);
            let cone_cos = dot(normalize(direction), light_to_surface);
            if cone_cos < outer_cos {
                return None;
            }
            let cone = spot_light_angular_attenuation(cone_cos, inner_cos, outer_cos);
            let sample_count = finite_light_sample_count(settings) as f32;
            Some(LightSample {
                direction: surface_to_light,
                color_rgb: light.color_rgb,
                intensity: light.intensity.max(0.0)
                    * point_light_attenuation(distance, range_m)
                    * cone
                    / sample_count,
                max_t: Some(distance),
            })
        })
        .collect()
}

fn finite_light_sample_positions(
    position: [f32; 3],
    point: [f32; 3],
    settings: &PreparedRenderSettings,
) -> Vec<[f32; 3]> {
    let sample_count = finite_light_sample_count(settings);
    let radius = settings.settings.soft_shadow_radius_m.max(0.0);
    if sample_count <= 1 || radius <= 1.0e-6 {
        return vec![position];
    }

    let center_direction = normalize(sub(position, point));
    let (tangent, bitangent) = tangent_basis(center_direction);
    (0..sample_count)
        .map(|sample_index| {
            let disk = soft_shadow_disk_sample(sample_index, sample_count);
            add(
                position,
                add(
                    scale(tangent, disk[0] * radius),
                    scale(bitangent, disk[1] * radius),
                ),
            )
        })
        .collect()
}

fn finite_light_sample_count(settings: &PreparedRenderSettings) -> u32 {
    settings.settings.soft_shadow_samples.clamp(1, 32)
}

fn soft_shadow_disk_sample(sample_index: u32, sample_count: u32) -> [f32; 2] {
    if sample_count <= 1 {
        return [0.0, 0.0];
    }
    let golden_angle = 2.399_963_1_f32;
    let index = sample_index as f32 + 0.5;
    let radius = (index / sample_count as f32).sqrt();
    let angle = index * golden_angle;
    let (sin, cos) = angle.sin_cos();
    [cos * radius, sin * radius]
}

fn area_light_sample_count(settings: &PreparedRenderSettings) -> u32 {
    settings.settings.area_light_samples.clamp(1, 64)
}

fn area_light_sample_barycentric(sample_index: u32, sample_count: u32) -> [f32; 3] {
    if sample_count <= 1 {
        return [1.0 / 3.0, 1.0 / 3.0, 1.0 / 3.0];
    }
    match sample_index {
        0 => [1.0 / 3.0, 1.0 / 3.0, 1.0 / 3.0],
        1 => [0.60, 0.20, 0.20],
        2 => [0.20, 0.60, 0.20],
        3 => [0.20, 0.20, 0.60],
        _ => {
            let index = sample_index as f32 + 0.5;
            let u = (index * 0.754_877_7).fract();
            let v = (index * 0.569_840_3).fract();
            let sqrt_u = u.sqrt();
            [1.0 - sqrt_u, sqrt_u * (1.0 - v), sqrt_u * v]
        }
    }
}

fn area_triangle_light_samples(
    light: &RenderLight,
    point: [f32; 3],
    a: [f32; 3],
    b: [f32; 3],
    c: [f32; 3],
    normal: [f32; 3],
    double_sided: bool,
    settings: &PreparedRenderSettings,
) -> Vec<LightSample> {
    let area = 0.5 * dot(cross(sub(b, a), sub(c, a)), cross(sub(b, a), sub(c, a))).sqrt();
    if area <= 1.0e-8 {
        return Vec::new();
    }
    let sample_count = area_light_sample_count(settings);
    let normal = normalize(normal);
    let per_sample = light.intensity.max(0.0) * area / sample_count as f32;
    (0..sample_count)
        .map(|sample_index| {
            triangle_barycentric_point(
                a,
                b,
                c,
                area_light_sample_barycentric(sample_index, sample_count),
            )
        })
        .filter_map(|sample_point| {
            let offset = sub(sample_point, point);
            let distance_sq = dot(offset, offset);
            if distance_sq <= 1.0e-8 {
                return None;
            }
            let distance = distance_sq.sqrt();
            let surface_to_light = scale(offset, 1.0 / distance);
            let light_to_surface = scale(surface_to_light, -1.0);
            let emitter_cos = if double_sided {
                dot(normal, light_to_surface).abs()
            } else {
                dot(normal, light_to_surface).max(0.0)
            };
            if emitter_cos <= 1.0e-5 {
                return None;
            }
            Some(LightSample {
                direction: surface_to_light,
                color_rgb: light.color_rgb,
                intensity: per_sample * emitter_cos / distance_sq.max(1.0e-4),
                max_t: Some(distance),
            })
        })
        .collect()
}

fn point_light_attenuation(distance: f32, range_m: Option<f32>) -> f32 {
    let distance_sq = distance.max(1.0e-4).powi(2);
    let distance_falloff = 1.0 / distance_sq;
    if let Some(range) = range_m {
        let range_falloff = if range > 1.0e-6 {
            (1.0 - (distance / range).powi(4)).clamp(0.0, 1.0)
        } else {
            0.0
        };
        distance_falloff * range_falloff
    } else {
        distance_falloff
    }
}

#[allow(dead_code)]
fn ambient_debug_rgb(
    hit: &Hit,
    objects: &[RenderObject],
    lights: &[RenderLight],
    settings: &PreparedRenderSettings,
) -> [f32; 3] {
    let object_bvh = ObjectBvh::from_objects(objects);
    let intersector = SceneIntersector::new(objects, &object_bvh);
    ambient_debug_rgb_with_intersector(hit, &intersector, lights, settings)
}

fn ambient_debug_rgb_with_intersector(
    hit: &Hit,
    intersector: &SceneIntersector<'_>,
    lights: &[RenderLight],
    settings: &PreparedRenderSettings,
) -> [f32; 3] {
    let ambient = settings.settings.ambient_intensity.max(0.0)
        * hit.occlusion.clamp(0.0, 1.0)
        * geometry_ambient_occlusion_with_intersector(hit, intersector, settings);
    let ambient_rgb = srgb_u8_to_linear_rgb(settings.settings.ambient_rgb);
    let reflected_environment_ambient =
        diffuse_environment_radiance_unit_rgb(hit.normal, hit.point, settings);
    let indirect_diffuse =
        indirect_diffuse_unit_rgb_with_intersector(hit, intersector, lights, settings);
    let diffuse_transmission = hit.diffuse_transmission.clamp(0.0, 1.0);
    let transmitted_environment_ambient =
        diffuse_environment_radiance_unit_rgb(scale(hit.normal, -1.0), hit.point, settings);
    [
        hit.color_linear_rgb[0]
            * 255.0
            * ambient
            * (ambient_rgb[0]
                + reflected_environment_ambient[0] * (1.0 - diffuse_transmission)
                + indirect_diffuse[0] * (1.0 - diffuse_transmission)
                + transmitted_environment_ambient[0]
                    * diffuse_transmission
                    * hit.diffuse_transmission_color[0]),
        hit.color_linear_rgb[1]
            * 255.0
            * ambient
            * (ambient_rgb[1]
                + reflected_environment_ambient[1] * (1.0 - diffuse_transmission)
                + indirect_diffuse[1] * (1.0 - diffuse_transmission)
                + transmitted_environment_ambient[1]
                    * diffuse_transmission
                    * hit.diffuse_transmission_color[1]),
        hit.color_linear_rgb[2]
            * 255.0
            * ambient
            * (ambient_rgb[2]
                + reflected_environment_ambient[2] * (1.0 - diffuse_transmission)
                + indirect_diffuse[2] * (1.0 - diffuse_transmission)
                + transmitted_environment_ambient[2]
                    * diffuse_transmission
                    * hit.diffuse_transmission_color[2]),
    ]
}

#[allow(dead_code)]
fn recursive_indirect_diffuse_unit_rgb(
    hit: &Hit,
    objects: &[RenderObject],
    lights: &[RenderLight],
    settings: &PreparedRenderSettings,
    sample_count: u32,
    radius: f32,
    remaining_bounces: u32,
) -> [f32; 3] {
    let object_bvh = ObjectBvh::from_objects(objects);
    let intersector = SceneIntersector::new(objects, &object_bvh);
    recursive_indirect_diffuse_unit_rgb_with_intersector(
        hit,
        &intersector,
        lights,
        settings,
        sample_count,
        radius,
        remaining_bounces,
    )
}

fn recursive_indirect_diffuse_unit_rgb_with_intersector(
    hit: &Hit,
    intersector: &SceneIntersector<'_>,
    lights: &[RenderLight],
    settings: &PreparedRenderSettings,
    sample_count: u32,
    radius: f32,
    remaining_bounces: u32,
) -> [f32; 3] {
    if remaining_bounces == 0 {
        return [0.0, 0.0, 0.0];
    }

    let normal = normalize(hit.normal);
    let (tangent, bitangent) = tangent_basis(normal);
    let origin = add(hit.point, scale(normal, 1.0e-3));
    let mut sum = [0.0_f32; 3];
    for sample_index in 0..sample_count {
        let direction = ambient_occlusion_sample_direction(
            sample_index,
            sample_count,
            normal,
            tangent,
            bitangent,
        );
        let ray = Ray {
            origin,
            dir: direction,
        };
        let Some(sample_hit) = intersector
            .hits(&ray)
            .into_iter()
            .filter(|sample_hit| sample_hit.t > 1.0e-3 && sample_hit.t <= radius)
            .min_by(|left, right| left.t.total_cmp(&right.t))
        else {
            continue;
        };
        let facing = dot(sample_hit.normal, scale(direction, -1.0)).max(0.0);
        let distance_falloff = (1.0 - sample_hit.t / radius).clamp(0.0, 1.0);
        let weight = facing * distance_falloff * sample_hit.alpha.clamp(0.0, 1.0);
        let incoming = indirect_surface_incoming_light_unit_rgb_with_intersector(
            &sample_hit,
            intersector,
            lights,
            settings,
            sample_count,
            radius,
            remaining_bounces,
        );
        for channel in 0..3 {
            let albedo = sample_hit.color_linear_rgb[channel].clamp(0.0, 1.0);
            let source =
                sample_hit.emission_rgb[channel].max(0.0) / 255.0 + albedo * incoming[channel];
            sum[channel] += source * weight;
        }
    }
    [
        sum[0] / sample_count as f32,
        sum[1] / sample_count as f32,
        sum[2] / sample_count as f32,
    ]
}

#[allow(dead_code)]
fn indirect_surface_incoming_light_unit_rgb(
    hit: &Hit,
    objects: &[RenderObject],
    lights: &[RenderLight],
    settings: &PreparedRenderSettings,
    sample_count: u32,
    radius: f32,
    remaining_bounces: u32,
) -> [f32; 3] {
    let object_bvh = ObjectBvh::from_objects(objects);
    let intersector = SceneIntersector::new(objects, &object_bvh);
    indirect_surface_incoming_light_unit_rgb_with_intersector(
        hit,
        &intersector,
        lights,
        settings,
        sample_count,
        radius,
        remaining_bounces,
    )
}

fn indirect_surface_incoming_light_unit_rgb_with_intersector(
    hit: &Hit,
    intersector: &SceneIntersector<'_>,
    lights: &[RenderLight],
    settings: &PreparedRenderSettings,
    sample_count: u32,
    radius: f32,
    remaining_bounces: u32,
) -> [f32; 3] {
    let ambient = settings.settings.ambient_intensity.max(0.0) * hit.occlusion.clamp(0.0, 1.0);
    let ambient_rgb = srgb_u8_to_linear_rgb(settings.settings.ambient_rgb);
    let environment = diffuse_environment_radiance_unit_rgb(hit.normal, hit.point, settings);
    let mut incoming = [
        ambient * (ambient_rgb[0] + environment[0]),
        ambient * (ambient_rgb[1] + environment[1]),
        ambient * (ambient_rgb[2] + environment[2]),
    ];

    for light in lights {
        for sample in light_samples(light, hit.point, settings) {
            let ndotl = dot(hit.normal, sample.direction).max(0.0);
            if ndotl <= 1.0e-5 {
                continue;
            }
            let visibility_rgb = shadow_visibility_rgb_with_intersector(
                hit,
                sample.direction,
                sample.max_t,
                intersector,
            );
            for channel in 0..3 {
                incoming[channel] += sample.color_rgb[channel]
                    * sample.intensity
                    * ndotl
                    * visibility_rgb[channel].max(0.0);
            }
        }
    }

    if remaining_bounces > 1 {
        let bounced = recursive_indirect_diffuse_unit_rgb_with_intersector(
            hit,
            intersector,
            lights,
            settings,
            sample_count,
            radius,
            remaining_bounces - 1,
        );
        for channel in 0..3 {
            incoming[channel] += bounced[channel];
        }
    }

    incoming
}

#[allow(dead_code)]
fn indirect_diffuse_unit_rgb(
    hit: &Hit,
    objects: &[RenderObject],
    lights: &[RenderLight],
    settings: &PreparedRenderSettings,
) -> [f32; 3] {
    let object_bvh = ObjectBvh::from_objects(objects);
    let intersector = SceneIntersector::new(objects, &object_bvh);
    indirect_diffuse_unit_rgb_with_intersector(hit, &intersector, lights, settings)
}

fn indirect_diffuse_unit_rgb_with_intersector(
    hit: &Hit,
    intersector: &SceneIntersector<'_>,
    lights: &[RenderLight],
    settings: &PreparedRenderSettings,
) -> [f32; 3] {
    let sample_count = settings.settings.indirect_diffuse_samples.min(64);
    let radius = settings.settings.indirect_diffuse_radius_m.max(0.0);
    let intensity = settings.settings.indirect_diffuse_intensity.max(0.0);
    let bounces = settings.settings.indirect_diffuse_bounces.min(3);
    if sample_count == 0 || bounces == 0 || radius <= 1.0e-5 || intensity <= 1.0e-5 {
        return [0.0, 0.0, 0.0];
    }

    let gathered = recursive_indirect_diffuse_unit_rgb_with_intersector(
        hit,
        intersector,
        lights,
        settings,
        sample_count,
        radius,
        bounces,
    );
    [
        gathered[0] * intensity,
        gathered[1] * intensity,
        gathered[2] * intensity,
    ]
}

#[allow(dead_code)]
fn geometry_ambient_occlusion(
    hit: &Hit,
    objects: &[RenderObject],
    settings: &PreparedRenderSettings,
) -> f32 {
    let object_bvh = ObjectBvh::from_objects(objects);
    let intersector = SceneIntersector::new(objects, &object_bvh);
    geometry_ambient_occlusion_with_intersector(hit, &intersector, settings)
}

fn geometry_ambient_occlusion_with_intersector(
    hit: &Hit,
    intersector: &SceneIntersector<'_>,
    settings: &PreparedRenderSettings,
) -> f32 {
    let sample_count = settings.settings.ambient_occlusion_samples.min(64);
    let radius = settings.settings.ambient_occlusion_radius_m.max(0.0);
    let intensity = settings.settings.ambient_occlusion_intensity.max(0.0);
    if sample_count == 0 || radius <= 1.0e-5 || intensity <= 1.0e-5 {
        return 1.0;
    }

    let normal = normalize(hit.normal);
    let (tangent, bitangent) = tangent_basis(normal);
    let origin = add(hit.point, scale(normal, 1.0e-3));
    let mut occlusion = 0.0_f32;
    for sample_index in 0..sample_count {
        let direction = ambient_occlusion_sample_direction(
            sample_index,
            sample_count,
            normal,
            tangent,
            bitangent,
        );
        let ray = Ray {
            origin,
            dir: direction,
        };
        let nearest = intersector
            .hits(&ray)
            .into_iter()
            .filter(|sample_hit| sample_hit.t > 1.0e-3 && sample_hit.t <= radius)
            .map(|sample_hit| (1.0 - sample_hit.t / radius).clamp(0.0, 1.0))
            .max_by(|left, right| left.total_cmp(right))
            .unwrap_or(0.0);
        occlusion += nearest;
    }
    let occlusion = occlusion / sample_count as f32;
    (1.0 - occlusion * intensity).clamp(0.0, 1.0)
}

fn tangent_basis(normal: [f32; 3]) -> ([f32; 3], [f32; 3]) {
    let helper = if normal[2].abs() < 0.999 {
        [0.0, 0.0, 1.0]
    } else {
        [0.0, 1.0, 0.0]
    };
    let tangent = normalize(cross(helper, normal));
    let bitangent = normalize(cross(normal, tangent));
    (tangent, bitangent)
}

fn ambient_occlusion_sample_direction(
    sample_index: u32,
    sample_count: u32,
    normal: [f32; 3],
    tangent: [f32; 3],
    bitangent: [f32; 3],
) -> [f32; 3] {
    let u = (sample_index as f32 + 0.5) / sample_count.max(1) as f32;
    let theta = sample_index as f32 * 2.399_963_1;
    let radial = u.sqrt();
    let local_x = theta.cos() * radial;
    let local_y = theta.sin() * radial;
    let local_z = (1.0 - u).sqrt();
    normalize(add(
        add(scale(tangent, local_x), scale(bitangent, local_y)),
        scale(normal, local_z),
    ))
}

fn diffuse_environment_radiance_unit_rgb(
    normal: [f32; 3],
    point: [f32; 3],
    settings: &PreparedRenderSettings,
) -> [f32; 3] {
    let base = diffuse_environment_radiance_unit_rgb_without_probe(normal, settings);
    blend_reflection_probe_diffuse_radiance(base, normal, point, settings)
}

fn diffuse_environment_radiance_unit_rgb_without_probe(
    normal: [f32; 3],
    settings: &PreparedRenderSettings,
) -> [f32; 3] {
    let Some(environment) = settings.settings.environment.as_ref() else {
        return [0.0, 0.0, 0.0];
    };
    let radiance = settings
        .environment_map
        .as_deref()
        .map(|texture| {
            scale(
                sample_diffuse_environment_texture(texture, normal, environment.map_rotation_deg),
                environment.intensity.max(0.0),
            )
        })
        .unwrap_or_else(|| environment_radiance_rgb(normal, settings));
    let rgb = rgb_f32_to_unit(radiance);
    [
        rgb[0] * environment.ambient_intensity.max(0.0),
        rgb[1] * environment.ambient_intensity.max(0.0),
        rgb[2] * environment.ambient_intensity.max(0.0),
    ]
}

fn clearcoat_fresnel(cos_theta: f32) -> f32 {
    CLEARCOAT_DIELECTRIC_F0
        + (1.0 - CLEARCOAT_DIELECTRIC_F0) * (1.0 - cos_theta.clamp(0.0, 1.0)).powi(5)
}

fn clearcoat_base_attenuation(clearcoat: f32, view_cos: f32, light_cos: Option<f32>) -> f32 {
    let clearcoat = clearcoat.clamp(0.0, 1.0);
    if clearcoat <= 1.0e-4 {
        return 1.0;
    }
    let view_transmittance = 1.0 - clearcoat * clearcoat_fresnel(view_cos);
    let light_transmittance = light_cos
        .map(|light_cos| 1.0 - clearcoat * clearcoat_fresnel(light_cos))
        .unwrap_or(1.0);
    (view_transmittance * light_transmittance).clamp(0.0, 1.0)
}

fn environment_reflection_rgb(hit: &Hit, settings: &PreparedRenderSettings) -> [f32; 3] {
    if settings.settings.environment.is_none() && settings.reflection_probes.is_empty() {
        return [0.0, 0.0, 0.0];
    }

    let n = normalize(hit.normal);
    let v = normalize(hit.view_dir);
    let view_incident = scale(v, -1.0);
    let reflection_dir = reflect(view_incident, n);
    let roughness = hit.roughness.clamp(0.04, 1.0);
    let metallic = hit.metallic.clamp(0.0, 1.0);
    let glossy_dir = normalize(lerp_vec3(reflection_dir, n, roughness * roughness * 0.70));
    let environment = rough_environment_radiance_rgb(glossy_dir, n, hit.point, roughness, settings);
    let ndotv = dot(n, v).abs().clamp(0.0, 1.0);
    let brdf = sample_environment_brdf_lut(&settings.environment_brdf_lut, ndotv, roughness);
    let strength = 0.35 + 0.65 * metallic;
    let occlusion = hit.occlusion.clamp(0.0, 1.0);
    let clearcoat = hit.clearcoat_factor.clamp(0.0, 1.0);
    let clearcoat_base_energy = if clearcoat > 1.0e-4 {
        let clearcoat_normal = normalize(hit.clearcoat_normal);
        let clearcoat_ndotv = dot(clearcoat_normal, v).abs().clamp(0.0, 1.0);
        clearcoat_base_attenuation(clearcoat, clearcoat_ndotv, None)
    } else {
        1.0
    };

    let mut out = [0.0_f32; 3];
    let f0 = material_f0_rgb(hit);
    let fresnel_tint = iridescence_fresnel_tint_rgb(hit, ndotv);
    for channel in 0..3 {
        let fresnel = (f0[channel] * brdf[0] + brdf[1]).clamp(0.0, 1.0);
        out[channel] = environment[channel]
            * fresnel
            * fresnel_tint[channel]
            * strength
            * occlusion
            * clearcoat_base_energy;
    }
    let anisotropy = hit.anisotropy_strength.clamp(0.0, 1.0);
    if anisotropy > 1.0e-4 {
        let tangent = normalize(hit.anisotropy_direction);
        let tangent_alignment = dot(reflection_dir, tangent).abs();
        let stretched_dir = normalize(lerp_vec3(reflection_dir, tangent, anisotropy * 0.35));
        let stretched_environment =
            rough_environment_radiance_rgb(stretched_dir, n, hit.point, roughness, settings);
        let anisotropic_strength = anisotropy * tangent_alignment * (1.0 - roughness * 0.5);
        for channel in 0..3 {
            out[channel] = out[channel] * (1.0 - anisotropic_strength * 0.35)
                + stretched_environment[channel]
                    * (f0[channel] * brdf[0] + brdf[1]).clamp(0.0, 1.0)
                    * fresnel_tint[channel]
                    * strength
                    * occlusion
                    * clearcoat_base_energy
                    * anisotropic_strength;
        }
    }
    if clearcoat > 1.0e-4 {
        let clearcoat_normal = normalize(hit.clearcoat_normal);
        let clearcoat_reflection_dir = reflect(view_incident, clearcoat_normal);
        let clearcoat_ndotv = dot(clearcoat_normal, v).abs().clamp(0.0, 1.0);
        let clearcoat_roughness = hit.clearcoat_roughness.clamp(0.04, 1.0);
        let clearcoat_dir = normalize(lerp_vec3(
            clearcoat_reflection_dir,
            clearcoat_normal,
            clearcoat_roughness * clearcoat_roughness * 0.45,
        ));
        let clearcoat_environment = rough_environment_radiance_rgb(
            clearcoat_dir,
            clearcoat_normal,
            hit.point,
            clearcoat_roughness,
            settings,
        );
        let clearcoat_brdf = sample_environment_brdf_lut(
            &settings.environment_brdf_lut,
            clearcoat_ndotv,
            clearcoat_roughness,
        );
        let clearcoat_fresnel = CLEARCOAT_DIELECTRIC_F0 * clearcoat_brdf[0] + clearcoat_brdf[1];
        let clearcoat_strength = clearcoat * (1.0 - clearcoat_roughness).clamp(0.0, 1.0).sqrt();
        for channel in 0..3 {
            out[channel] +=
                clearcoat_environment[channel] * clearcoat_fresnel * clearcoat_strength * occlusion;
        }
    }
    let sheen_strength = hit
        .sheen_color
        .iter()
        .copied()
        .fold(0.0_f32, f32::max)
        .clamp(0.0, 1.0);
    if sheen_strength > 1.0e-4 {
        let sheen_roughness = hit.sheen_roughness.clamp(0.04, 1.0);
        let sheen_environment =
            rough_environment_radiance_rgb(n, n, hit.point, sheen_roughness, settings);
        let grazing = (1.0 - ndotv).powi(2);
        let sheen_weight = grazing * (0.35 + 0.65 * sheen_roughness) * occlusion;
        for channel in 0..3 {
            out[channel] += sheen_environment[channel]
                * hit.sheen_color[channel].clamp(0.0, 1.0)
                * sheen_weight
                * clearcoat_base_energy;
        }
    }
    out
}

fn shade_pbr_direct_rgb(hit: &Hit, light: &LightSample, visibility_rgb: [f32; 3]) -> [f32; 3] {
    let n = normalize(hit.normal);
    let l = normalize(light.direction);
    let v = normalize(hit.view_dir);
    let h = normalize(add(l, v));
    let ndotl_signed = dot(n, l);
    let ndotl = ndotl_signed.max(0.0);
    let transmitted_ndotl = (-ndotl_signed).max(0.0);
    let ndotv = dot(n, v).max(0.0);
    let ndoth = dot(n, h).max(0.0);
    let vdoth = dot(v, h).max(0.0);
    let diffuse_transmission = hit.diffuse_transmission.clamp(0.0, 1.0);
    if ndotl <= 0.0 || ndotv <= 0.0 {
        if diffuse_transmission > 1.0e-4 && transmitted_ndotl > 0.0 && ndotv > 0.0 {
            return shade_diffuse_transmission_direct_rgb(
                hit,
                light,
                visibility_rgb,
                transmitted_ndotl,
            );
        }
        return [0.0, 0.0, 0.0];
    }

    let metallic = hit.metallic.clamp(0.0, 1.0);
    let roughness = hit.roughness.clamp(0.04, 1.0);
    let base_rgb = hit.color_linear_rgb;
    let f0 = material_f0_rgb(hit);
    let mut fresnel = fresnel_schlick_rgb(vdoth, f0);
    let iridescence_tint = iridescence_fresnel_tint_rgb(hit, vdoth);
    for channel in 0..3 {
        fresnel[channel] *= iridescence_tint[channel];
    }
    let distribution = ggx_distribution(ndoth, roughness);
    let geometry = smith_ggx_geometry(ndotv, ndotl, roughness);
    let anisotropy = hit.anisotropy_strength.clamp(0.0, 1.0);
    let anisotropy_shape = if anisotropy > 1.0e-4 {
        let tangent = normalize(hit.anisotropy_direction);
        let bitangent = normalize(cross(n, tangent));
        let hdot_t = dot(h, tangent).abs();
        let hdot_b = dot(h, bitangent).abs();
        (1.0 + anisotropy * hdot_t * hdot_t * 3.0) * (1.0 - anisotropy * hdot_b * hdot_b * 0.65)
    } else {
        1.0
    }
    .max(0.05);
    let denominator = (4.0 * ndotv * ndotl).max(1.0e-5);
    let light_rgb = light.color_rgb;
    let radiance = light.intensity.max(0.0) * ndotl;

    let mut out = [0.0_f32; 3];
    let clearcoat = hit.clearcoat_factor.clamp(0.0, 1.0);
    let base_energy = if clearcoat > 1.0e-4 {
        let clearcoat_normal = normalize(hit.clearcoat_normal);
        let clearcoat_ndotl = dot(clearcoat_normal, l).max(0.0);
        let clearcoat_ndotv = dot(clearcoat_normal, v).max(0.0);
        let clearcoat_ndoth = dot(clearcoat_normal, h).max(0.0);
        let base_energy =
            clearcoat_base_attenuation(clearcoat, clearcoat_ndotv, Some(clearcoat_ndotl));
        if clearcoat_ndotl <= 0.0 || clearcoat_ndotv <= 0.0 {
            base_energy
        } else {
            let clearcoat_roughness = hit.clearcoat_roughness.clamp(0.04, 1.0);
            let clearcoat_distribution = ggx_distribution(clearcoat_ndoth, clearcoat_roughness);
            let clearcoat_geometry =
                smith_ggx_geometry(clearcoat_ndotv, clearcoat_ndotl, clearcoat_roughness);
            let clearcoat_fresnel = clearcoat_fresnel(vdoth);
            let clearcoat_denominator = (4.0 * clearcoat_ndotv * clearcoat_ndotl).max(1.0e-5);
            let clearcoat_radiance = light.intensity.max(0.0) * clearcoat_ndotl;
            let clearcoat_specular =
                clearcoat_distribution * clearcoat_geometry * clearcoat_fresnel
                    / clearcoat_denominator;
            for channel in 0..3 {
                out[channel] += clearcoat_specular
                    * clearcoat
                    * light_rgb[channel]
                    * clearcoat_radiance
                    * visibility_rgb[channel].max(0.0)
                    * 255.0;
            }
            base_energy
        }
    } else {
        1.0
    };
    for channel in 0..3 {
        let specular = distribution * anisotropy_shape * geometry * fresnel[channel] / denominator;
        let diffuse = (1.0 - fresnel[channel])
            * (1.0 - metallic)
            * (1.0 - diffuse_transmission)
            * base_rgb[channel]
            / PI;
        out[channel] += (diffuse + specular)
            * light_rgb[channel]
            * radiance
            * visibility_rgb[channel].max(0.0)
            * 255.0
            * base_energy;
    }
    let sheen_strength = hit
        .sheen_color
        .iter()
        .copied()
        .fold(0.0_f32, f32::max)
        .clamp(0.0, 1.0);
    if sheen_strength > 1.0e-4 {
        let sheen_roughness = hit.sheen_roughness.clamp(0.04, 1.0);
        let grazing_v = (1.0 - ndotv).powf(1.0 + sheen_roughness * 2.0);
        let grazing_l = (1.0 - ndotl).powf(1.0 + sheen_roughness * 2.0);
        let retro = (1.0 - vdoth).powf(1.0 + sheen_roughness);
        let velvet = (0.55 * grazing_v + 0.30 * grazing_l + 0.15 * retro)
            * (0.45 + 0.55 * sheen_roughness)
            * (1.0 - metallic);
        for channel in 0..3 {
            out[channel] += hit.sheen_color[channel].clamp(0.0, 1.0)
                * light_rgb[channel]
                * radiance
                * visibility_rgb[channel].max(0.0)
                * 255.0
                * velvet
                * base_energy;
        }
    }
    out
}

fn shade_transmissive_surface_direct_reflection_rgb(
    hit: &Hit,
    intersector: &SceneIntersector<'_>,
    lights: &[RenderLight],
    settings: &PreparedRenderSettings,
) -> [f32; 3] {
    if hit.unlit || !transmission_background_refracts(hit) {
        return [0.0, 0.0, 0.0];
    }
    let transmission_weight = (1.0 - hit.alpha.clamp(0.0, 1.0)).clamp(0.0, 1.0);
    if transmission_weight <= 1.0e-4 {
        return [0.0, 0.0, 0.0];
    }

    let mut out = [0.0_f32; 3];
    for light in lights {
        for sample in light_samples(light, hit.point, settings) {
            let visibility_rgb = shadow_visibility_rgb_with_intersector(
                hit,
                sample.direction,
                sample.max_t,
                intersector,
            );
            let reflected = shade_transmissive_specular_direct_rgb(hit, &sample, visibility_rgb);
            for channel in 0..3 {
                out[channel] += reflected[channel] * transmission_weight;
            }
        }
    }
    out
}

fn shade_transmissive_specular_direct_rgb(
    hit: &Hit,
    light: &LightSample,
    visibility_rgb: [f32; 3],
) -> [f32; 3] {
    let n = normalize(hit.normal);
    let l = normalize(light.direction);
    let v = normalize(hit.view_dir);
    let h = normalize(add(l, v));
    let ndotl = dot(n, l).max(0.0);
    let ndotv = dot(n, v).max(0.0);
    let ndoth = dot(n, h).max(0.0);
    let vdoth = dot(v, h).max(0.0);
    if ndotl <= 0.0 || ndotv <= 0.0 {
        return [0.0, 0.0, 0.0];
    }

    let roughness = hit.roughness.clamp(0.04, 1.0);
    let distribution = ggx_distribution(ndoth, roughness);
    let geometry = smith_ggx_geometry(ndotv, ndotl, roughness);
    let denominator = (4.0 * ndotv * ndotl).max(1.0e-5);
    let mut fresnel = fresnel_schlick_rgb(vdoth, material_f0_rgb(hit));
    let iridescence_tint = iridescence_fresnel_tint_rgb(hit, vdoth);
    let radiance = light.intensity.max(0.0) * ndotl;

    let mut out = [0.0_f32; 3];
    for channel in 0..3 {
        fresnel[channel] *= iridescence_tint[channel];
        let specular = distribution * geometry * fresnel[channel] / denominator;
        out[channel] = specular
            * light.color_rgb[channel]
            * radiance
            * visibility_rgb[channel].max(0.0)
            * 255.0;
    }
    out
}

fn shade_diffuse_transmission_direct_rgb(
    hit: &Hit,
    light: &LightSample,
    visibility_rgb: [f32; 3],
    transmitted_ndotl: f32,
) -> [f32; 3] {
    let diffuse_transmission = hit.diffuse_transmission.clamp(0.0, 1.0);
    let metallic = hit.metallic.clamp(0.0, 1.0);
    let radiance = light.intensity.max(0.0) * transmitted_ndotl.max(0.0);
    [
        hit.color_linear_rgb[0]
            * hit.diffuse_transmission_color[0]
            * diffuse_transmission
            * (1.0 - metallic)
            * light.color_rgb[0]
            * radiance
            * visibility_rgb[0].max(0.0)
            * 255.0
            / PI,
        hit.color_linear_rgb[1]
            * hit.diffuse_transmission_color[1]
            * diffuse_transmission
            * (1.0 - metallic)
            * light.color_rgb[1]
            * radiance
            * visibility_rgb[1].max(0.0)
            * 255.0
            / PI,
        hit.color_linear_rgb[2]
            * hit.diffuse_transmission_color[2]
            * diffuse_transmission
            * (1.0 - metallic)
            * light.color_rgb[2]
            * radiance
            * visibility_rgb[2].max(0.0)
            * 255.0
            / PI,
    ]
}

fn material_f0_rgb(hit: &Hit) -> [f32; 3] {
    let metallic = hit.metallic.clamp(0.0, 1.0);
    let specular_factor = hit.specular_factor.clamp(0.0, 1.0);
    let dielectric_f0 = dielectric_f0_from_ior(hit.ior);
    let base_rgb = hit.color_linear_rgb;
    [
        mix_f32(
            dielectric_f0 * specular_factor * hit.specular_color[0].clamp(0.0, 1.0),
            base_rgb[0],
            metallic,
        ),
        mix_f32(
            dielectric_f0 * specular_factor * hit.specular_color[1].clamp(0.0, 1.0),
            base_rgb[1],
            metallic,
        ),
        mix_f32(
            dielectric_f0 * specular_factor * hit.specular_color[2].clamp(0.0, 1.0),
            base_rgb[2],
            metallic,
        ),
    ]
}

fn dielectric_f0_from_ior(ior: f32) -> f32 {
    let ior = ior.max(1.0);
    let f0 = (ior - 1.0) / (ior + 1.0);
    (f0 * f0).clamp(0.0, 1.0)
}

#[allow(dead_code)]
fn composite_debug_rgb_hits(
    hits: &[Hit],
    objects: &[RenderObject],
    lights: &[RenderLight],
    settings: &PreparedRenderSettings,
    background_rgb: [f32; 3],
) -> [u8; 3] {
    let object_bvh = ObjectBvh::from_objects(objects);
    let intersector = SceneIntersector::new(objects, &object_bvh);
    composite_debug_rgb_hits_with_intersector(hits, &intersector, lights, settings, background_rgb)
}

fn composite_debug_rgb_hits_with_intersector(
    hits: &[Hit],
    intersector: &SceneIntersector<'_>,
    lights: &[RenderLight],
    settings: &PreparedRenderSettings,
    background_rgb: [f32; 3],
) -> [u8; 3] {
    composite_debug_rgb_hits_with_refraction_and_intersector(
        hits,
        intersector,
        lights,
        settings,
        background_rgb,
        TRANSMISSION_REFRACTION_BOUNCES,
        &[],
    )
}

#[allow(dead_code)]
fn composite_debug_rgb_hits_with_refraction(
    hits: &[Hit],
    objects: &[RenderObject],
    lights: &[RenderLight],
    settings: &PreparedRenderSettings,
    background_rgb: [f32; 3],
    refraction_depth_remaining: u8,
    medium_stack: &[TransmissionMedium],
) -> [u8; 3] {
    let object_bvh = ObjectBvh::from_objects(objects);
    let intersector = SceneIntersector::new(objects, &object_bvh);
    composite_debug_rgb_hits_with_refraction_and_intersector(
        hits,
        &intersector,
        lights,
        settings,
        background_rgb,
        refraction_depth_remaining,
        medium_stack,
    )
}

fn composite_debug_rgb_hits_with_refraction_and_intersector(
    hits: &[Hit],
    intersector: &SceneIntersector<'_>,
    lights: &[RenderLight],
    settings: &PreparedRenderSettings,
    background_rgb: [f32; 3],
    refraction_depth_remaining: u8,
    medium_stack: &[TransmissionMedium],
) -> [u8; 3] {
    let mut color = [0.0_f32; 3];
    let mut transmittance = [1.0_f32, 1.0, 1.0];
    for (index, hit) in hits.iter().enumerate() {
        if transmittance.iter().all(|channel| *channel <= 1.0e-3) {
            break;
        }
        let alpha = hit.alpha.clamp(0.0, 1.0);
        let shaded = shade_hit_with_lights_with_intersector(hit, intersector, lights, settings);
        let transmissive_reflection =
            shade_transmissive_surface_direct_reflection_rgb(hit, intersector, lights, settings);
        for channel in 0..3 {
            color[channel] += transmissive_reflection[channel] * transmittance[channel];
            color[channel] += shaded[channel] * alpha * transmittance[channel];
        }
        let diffuse_transmission = hit.diffuse_transmission.clamp(0.0, 1.0);
        let filter = dispersion_filter(
            hit,
            transmission_filter_for_hit_path(hits, index, medium_stack),
        );
        for channel in 0..3 {
            let pass = (1.0 - alpha)
                + alpha
                    * diffuse_transmission
                    * hit.diffuse_transmission_color[channel].clamp(0.0, 1.0);
            transmittance[channel] *= pass.clamp(0.0, 1.0) * filter[channel].clamp(0.0, 1.0);
        }
    }
    let background_rgb = refracted_background_rgb_with_intersector(
        hits,
        intersector,
        lights,
        settings,
        background_rgb,
        refraction_depth_remaining,
        medium_stack,
    );
    for channel in 0..3 {
        color[channel] += background_rgb[channel] * transmittance[channel];
    }
    tone_map_rgb(color, &settings.settings)
}

fn refraction_surface_normal(incident: [f32; 3], normal: [f32; 3]) -> [f32; 3] {
    let normal = normalize(normal);
    if dot(incident, normal) > 0.0 {
        scale(normal, -1.0)
    } else {
        normal
    }
}

fn transmission_fresnel_rgb(hit: &Hit, incident: [f32; 3], surface_normal: [f32; 3]) -> [f32; 3] {
    let cos_theta = (-dot(normalize(surface_normal), normalize(incident))).clamp(0.0, 1.0);
    let mut fresnel = fresnel_schlick_rgb(cos_theta, material_f0_rgb(hit));
    let tint = iridescence_fresnel_tint_rgb(hit, cos_theta);
    for channel in 0..3 {
        fresnel[channel] = (fresnel[channel] * tint[channel]).clamp(0.0, 1.0);
    }
    fresnel
}

fn channel_dispersion_ior(base_ior: f32, dispersion: f32, channel: usize) -> f32 {
    let base_ior = base_ior.max(1.0);
    let spread = (dispersion.max(0.0) * (base_ior - 1.0) * 0.08).clamp(0.0, 0.35);
    let offset = match channel {
        0 => -spread,
        1 => 0.0,
        _ => spread,
    };
    (base_ior + offset).max(1.0)
}

fn current_transmission_channel_ior(medium_stack: &[TransmissionMedium], channel: usize) -> f32 {
    medium_stack
        .last()
        .map(|medium| channel_dispersion_ior(medium.ior, medium.dispersion, channel))
        .unwrap_or(1.0)
}

#[allow(dead_code)]
fn refracted_transmission_lobe_rgb(
    hit: &Hit,
    incident: [f32; 3],
    surface_normal: [f32; 3],
    objects: &[RenderObject],
    lights: &[RenderLight],
    settings: &PreparedRenderSettings,
    fallback_rgb: [f32; 3],
    refraction_depth_remaining: u8,
    medium_stack: &[TransmissionMedium],
    next_medium_stack: &[TransmissionMedium],
) -> ([f32; 3], [bool; 3]) {
    let object_bvh = ObjectBvh::from_objects(objects);
    let intersector = SceneIntersector::new(objects, &object_bvh);
    refracted_transmission_lobe_rgb_with_intersector(
        hit,
        incident,
        surface_normal,
        &intersector,
        lights,
        settings,
        fallback_rgb,
        refraction_depth_remaining,
        medium_stack,
        next_medium_stack,
    )
}

fn refracted_transmission_lobe_rgb_with_intersector(
    hit: &Hit,
    incident: [f32; 3],
    surface_normal: [f32; 3],
    intersector: &SceneIntersector<'_>,
    lights: &[RenderLight],
    settings: &PreparedRenderSettings,
    fallback_rgb: [f32; 3],
    refraction_depth_remaining: u8,
    medium_stack: &[TransmissionMedium],
    next_medium_stack: &[TransmissionMedium],
) -> ([f32; 3], [bool; 3]) {
    let mut rgb = [0.0_f32; 3];
    let mut total_internal_reflection = [false; 3];
    for channel in 0..3 {
        let eta = current_transmission_channel_ior(medium_stack, channel)
            / current_transmission_channel_ior(next_medium_stack, channel);
        if let Some(direction) = refract(incident, surface_normal, eta) {
            let sample = rough_transmission_lobe_rgb_with_intersector(
                hit,
                direction,
                intersector,
                lights,
                settings,
                fallback_rgb,
                refraction_depth_remaining,
                next_medium_stack,
            );
            rgb[channel] = sample[channel];
        } else {
            total_internal_reflection[channel] = true;
        }
    }
    (rgb, total_internal_reflection)
}

#[allow(dead_code)]
fn refracted_background_rgb(
    hits: &[Hit],
    objects: &[RenderObject],
    lights: &[RenderLight],
    settings: &PreparedRenderSettings,
    fallback_rgb: [f32; 3],
    refraction_depth_remaining: u8,
    medium_stack: &[TransmissionMedium],
) -> [f32; 3] {
    let object_bvh = ObjectBvh::from_objects(objects);
    let intersector = SceneIntersector::new(objects, &object_bvh);
    refracted_background_rgb_with_intersector(
        hits,
        &intersector,
        lights,
        settings,
        fallback_rgb,
        refraction_depth_remaining,
        medium_stack,
    )
}

fn refracted_background_rgb_with_intersector(
    hits: &[Hit],
    intersector: &SceneIntersector<'_>,
    lights: &[RenderLight],
    settings: &PreparedRenderSettings,
    fallback_rgb: [f32; 3],
    refraction_depth_remaining: u8,
    medium_stack: &[TransmissionMedium],
) -> [f32; 3] {
    if refraction_depth_remaining == 0 {
        return fallback_rgb;
    }
    let Some(hit) = hits
        .iter()
        .find(|hit| transmission_background_refracts(hit))
    else {
        return fallback_rgb;
    };
    let incident = scale(hit.view_dir, -1.0);
    let next_medium_stack = next_transmission_medium_stack(medium_stack, hit);
    let surface_normal = refraction_surface_normal(incident, hit.normal);
    let reflection_direction = reflect(incident, surface_normal);
    let reflected_rgb = rough_transmission_lobe_rgb_with_intersector(
        hit,
        reflection_direction,
        intersector,
        lights,
        settings,
        fallback_rgb,
        refraction_depth_remaining - 1,
        medium_stack,
    );
    let (refracted_rgb, total_internal_reflection) =
        refracted_transmission_lobe_rgb_with_intersector(
            hit,
            incident,
            surface_normal,
            intersector,
            lights,
            settings,
            fallback_rgb,
            refraction_depth_remaining - 1,
            medium_stack,
            &next_medium_stack,
        );
    let fresnel = transmission_fresnel_rgb(hit, incident, surface_normal);
    [
        if total_internal_reflection[0] {
            reflected_rgb[0]
        } else {
            mix_f32(refracted_rgb[0], reflected_rgb[0], fresnel[0])
        },
        if total_internal_reflection[1] {
            reflected_rgb[1]
        } else {
            mix_f32(refracted_rgb[1], reflected_rgb[1], fresnel[1])
        },
        if total_internal_reflection[2] {
            reflected_rgb[2]
        } else {
            mix_f32(refracted_rgb[2], reflected_rgb[2], fresnel[2])
        },
    ]
}

#[allow(dead_code)]
fn rough_transmission_lobe_rgb(
    hit: &Hit,
    direction: [f32; 3],
    objects: &[RenderObject],
    lights: &[RenderLight],
    settings: &PreparedRenderSettings,
    fallback_rgb: [f32; 3],
    refraction_depth_remaining: u8,
    medium_stack: &[TransmissionMedium],
) -> [f32; 3] {
    let object_bvh = ObjectBvh::from_objects(objects);
    let intersector = SceneIntersector::new(objects, &object_bvh);
    rough_transmission_lobe_rgb_with_intersector(
        hit,
        direction,
        &intersector,
        lights,
        settings,
        fallback_rgb,
        refraction_depth_remaining,
        medium_stack,
    )
}

fn rough_transmission_lobe_rgb_with_intersector(
    hit: &Hit,
    direction: [f32; 3],
    intersector: &SceneIntersector<'_>,
    lights: &[RenderLight],
    settings: &PreparedRenderSettings,
    fallback_rgb: [f32; 3],
    refraction_depth_remaining: u8,
    medium_stack: &[TransmissionMedium],
) -> [f32; 3] {
    let directions =
        rough_transmission_directions(hit, direction, settings.settings.rough_transmission_samples);
    let mut sum = [0.0_f32; 3];
    for direction in directions.iter().copied() {
        let rgb = refracted_direction_rgb_with_intersector(
            hit,
            direction,
            intersector,
            lights,
            settings,
            fallback_rgb,
            refraction_depth_remaining,
            medium_stack,
        );
        for channel in 0..3 {
            sum[channel] += rgb[channel];
        }
    }
    scale(sum, 1.0 / directions.len().max(1) as f32)
}

#[allow(dead_code)]
fn refracted_direction_rgb(
    hit: &Hit,
    direction: [f32; 3],
    objects: &[RenderObject],
    lights: &[RenderLight],
    settings: &PreparedRenderSettings,
    fallback_rgb: [f32; 3],
    refraction_depth_remaining: u8,
    medium_stack: &[TransmissionMedium],
) -> [f32; 3] {
    let object_bvh = ObjectBvh::from_objects(objects);
    let intersector = SceneIntersector::new(objects, &object_bvh);
    refracted_direction_rgb_with_intersector(
        hit,
        direction,
        &intersector,
        lights,
        settings,
        fallback_rgb,
        refraction_depth_remaining,
        medium_stack,
    )
}

fn refracted_direction_rgb_with_intersector(
    hit: &Hit,
    direction: [f32; 3],
    intersector: &SceneIntersector<'_>,
    lights: &[RenderLight],
    settings: &PreparedRenderSettings,
    fallback_rgb: [f32; 3],
    refraction_depth_remaining: u8,
    medium_stack: &[TransmissionMedium],
) -> [f32; 3] {
    refracted_scene_rgb_with_intersector(
        hit,
        direction,
        intersector,
        lights,
        settings,
        refraction_depth_remaining,
        medium_stack,
    )
    .unwrap_or_else(|| {
        if settings.settings.environment.is_some() {
            environment_background_rgb(direction, settings)
        } else {
            fallback_rgb
        }
    })
}

#[allow(dead_code)]
fn refracted_scene_rgb(
    source_hit: &Hit,
    direction: [f32; 3],
    objects: &[RenderObject],
    lights: &[RenderLight],
    settings: &PreparedRenderSettings,
    refraction_depth_remaining: u8,
    medium_stack: &[TransmissionMedium],
) -> Option<[f32; 3]> {
    let object_bvh = ObjectBvh::from_objects(objects);
    let intersector = SceneIntersector::new(objects, &object_bvh);
    refracted_scene_rgb_with_intersector(
        source_hit,
        direction,
        &intersector,
        lights,
        settings,
        refraction_depth_remaining,
        medium_stack,
    )
}

fn refracted_scene_rgb_with_intersector(
    source_hit: &Hit,
    direction: [f32; 3],
    intersector: &SceneIntersector<'_>,
    lights: &[RenderLight],
    settings: &PreparedRenderSettings,
    refraction_depth_remaining: u8,
    medium_stack: &[TransmissionMedium],
) -> Option<[f32; 3]> {
    let ray = Ray {
        origin: add(source_hit.point, scale(direction, 1.0e-3)),
        dir: direction,
    };
    let mut hits = intersector
        .hits_with_mode(&ray, TriangleIntersectionMode::RefractiveBoundary)
        .into_iter()
        .filter(|hit| hit.t > 1.0e-3)
        .collect::<Vec<_>>();
    hits.sort_by(|left, right| left.t.total_cmp(&right.t));
    if hits.is_empty() {
        return None;
    }
    let first_hit_t = hits[0].t;
    let mut rgb =
        srgb_u8_to_linear_radiance(composite_debug_rgb_hits_with_refraction_and_intersector(
            &hits,
            intersector,
            lights,
            settings,
            environment_background_rgb(direction, settings),
            refraction_depth_remaining,
            medium_stack,
        ));
    let filter = transmission_medium_stack_filter(medium_stack, first_hit_t);
    for channel in 0..3 {
        rgb[channel] *= filter[channel];
    }
    Some(rgb)
}

fn next_transmission_medium_stack(
    medium_stack: &[TransmissionMedium],
    hit: &Hit,
) -> Vec<TransmissionMedium> {
    if medium_stack
        .last()
        .is_some_and(|medium| medium_exits_hit(medium, hit))
    {
        medium_stack
            .get(..medium_stack.len().saturating_sub(1))
            .unwrap_or(&[])
            .to_vec()
    } else {
        let mut next = medium_stack.to_vec();
        next.push(TransmissionMedium::from_hit(hit));
        next
    }
}

fn medium_exits_hit(medium: &TransmissionMedium, hit: &Hit) -> bool {
    medium.entity_u32 == hit.entity_u32 && (medium.ior - hit.ior.max(1.0)).abs() < 0.05
}

fn rough_transmission_directions(
    hit: &Hit,
    direction: [f32; 3],
    sample_count: u32,
) -> Vec<[f32; 3]> {
    let roughness = hit.roughness.clamp(0.0, 1.0);
    if roughness <= 0.05 {
        return vec![normalize(direction)];
    }
    let direction = normalize(direction);
    let up = if direction[2].abs() < 0.9 {
        [0.0, 0.0, 1.0]
    } else {
        [0.0, 1.0, 0.0]
    };
    let tangent = normalize(cross(direction, up));
    let bitangent = normalize(cross(direction, tangent));
    let radius = roughness * roughness * 0.35;
    let sample_count = sample_count.clamp(1, 64);
    let mut directions = Vec::with_capacity(sample_count as usize);
    directions.push(direction);
    for sample_index in 1..sample_count {
        let index = sample_index as f32;
        let count = sample_count.max(2) as f32;
        let radial = (index / (count - 1.0)).sqrt() * radius;
        let angle = index * 2.399_963_1;
        let offset = [radial * angle.cos(), radial * angle.sin()];
        directions.push(normalize(add(
            direction,
            add(scale(tangent, offset[0]), scale(bitangent, offset[1])),
        )));
    }
    directions
}

fn transmission_background_refracts(hit: &Hit) -> bool {
    hit.alpha < 0.999
        && (hit.ior > 1.0001
            || hit.volume_thickness_m > 1.0e-6
            || hit.volume_attenuation_distance_m.is_finite()
            || hit.dispersion > 1.0e-6
            || hit
                .transmission_filter_rgb
                .iter()
                .any(|channel| (*channel - 1.0).abs() > 1.0e-6))
}

fn transmission_filter_for_hit_path(
    hits: &[Hit],
    index: usize,
    medium_stack: &[TransmissionMedium],
) -> [f32; 3] {
    let Some(hit) = hits.get(index) else {
        return [1.0, 1.0, 1.0];
    };
    if medium_stack
        .last()
        .is_some_and(|medium| medium_exits_hit(medium, hit))
    {
        return hit.transmission_filter_rgb;
    }
    let exit_hit = hits.get(index + 1..).and_then(|rest| {
        rest.iter()
            .find(|candidate| candidate.entity_u32 == hit.entity_u32 && candidate.t > hit.t)
    });
    let path_length_m = if let Some(exit) = exit_hit {
        (exit.t - hit.t).max(0.0)
    } else if hits.get(..index).is_some_and(|previous| {
        previous
            .iter()
            .any(|candidate| candidate.entity_u32 == hit.entity_u32)
    }) {
        return [1.0, 1.0, 1.0];
    } else {
        hit.volume_thickness_m
    };
    volume_attenuation_filter(hit, path_length_m)
}

fn transmission_medium_stack_filter(
    medium_stack: &[TransmissionMedium],
    path_length_m: f32,
) -> [f32; 3] {
    if path_length_m <= 1.0e-6 {
        return [1.0, 1.0, 1.0];
    }
    let mut filter = [1.0_f32, 1.0, 1.0];
    for medium in medium_stack {
        if !medium.volume_attenuation_distance_m.is_finite() {
            continue;
        }
        let distance = medium.volume_attenuation_distance_m.max(1.0e-6);
        let power = path_length_m / distance;
        for (channel, value) in filter.iter_mut().enumerate() {
            *value *= medium.volume_attenuation_color[channel]
                .clamp(0.0, 1.0)
                .powf(power);
        }
    }
    filter
}

fn dispersion_filter(hit: &Hit, filter: [f32; 3]) -> [f32; 3] {
    let strength = (hit.dispersion * (hit.ior - 1.0).max(0.0) * 0.35).clamp(0.0, 0.8);
    if strength <= 1.0e-6 {
        return filter;
    }
    [
        (filter[0] * (1.0 + strength)).clamp(0.0, 1.0),
        filter[1].clamp(0.0, 1.0),
        (filter[2] * (1.0 - strength)).clamp(0.0, 1.0),
    ]
}

fn volume_attenuation_filter(hit: &Hit, path_length_m: f32) -> [f32; 3] {
    if path_length_m <= 1.0e-6 || !hit.volume_attenuation_distance_m.is_finite() {
        return hit.transmission_filter_rgb;
    }
    let distance = hit.volume_attenuation_distance_m.max(1.0e-6);
    let power = path_length_m / distance;
    [
        hit.volume_attenuation_color[0].clamp(0.0, 1.0).powf(power),
        hit.volume_attenuation_color[1].clamp(0.0, 1.0).powf(power),
        hit.volume_attenuation_color[2].clamp(0.0, 1.0).powf(power),
    ]
}

fn environment_background_rgb(direction: [f32; 3], settings: &PreparedRenderSettings) -> [f32; 3] {
    if settings.settings.environment.is_none() {
        return srgb_u8_to_linear_radiance(settings.settings.background_rgb);
    }
    environment_radiance_rgb(direction, settings)
}

fn environment_radiance_rgb(direction: [f32; 3], settings: &PreparedRenderSettings) -> [f32; 3] {
    settings
        .settings
        .environment
        .as_ref()
        .map(|environment| {
            let radiance = settings
                .environment_map
                .as_deref()
                .map(|texture| {
                    sample_environment_texture(texture, direction, environment.map_rotation_deg)
                })
                .unwrap_or_else(|| procedural_environment_radiance_rgb(direction, environment));
            scale(radiance, environment.intensity.max(0.0))
        })
        .unwrap_or([0.0, 0.0, 0.0])
}

fn reflection_probe_influence_weight(probe: &ReflectionProbeSettings, point: [f32; 3]) -> f32 {
    let Some(position) = probe.position else {
        return 1.0;
    };
    let offset = sub(point, position);
    if let Some(radius) = probe.influence_radius_m {
        let radius = radius.max(1.0e-4);
        let distance = dot(offset, offset).sqrt();
        let normalized = (distance / radius).clamp(0.0, 1.0);
        let falloff_power = probe.falloff_power.unwrap_or(1.0).max(1.0e-4);
        return (1.0 - normalized).powf(falloff_power);
    }
    if let Some(size) = probe.box_size_m {
        return if point_is_inside_probe_box(offset, size) {
            1.0
        } else {
            0.0
        };
    }
    1.0
}

fn point_is_inside_probe_box(local_point: [f32; 3], box_size_m: [f32; 3]) -> bool {
    let half = [
        box_size_m[0].abs() * 0.5,
        box_size_m[1].abs() * 0.5,
        box_size_m[2].abs() * 0.5,
    ];
    local_point[0].abs() <= half[0]
        && local_point[1].abs() <= half[1]
        && local_point[2].abs() <= half[2]
}

fn reflection_probe_sample_direction(
    probe: &ReflectionProbeSettings,
    point: [f32; 3],
    direction: [f32; 3],
) -> [f32; 3] {
    let direction = normalize(direction);
    let (Some(position), Some(box_size_m)) = (probe.position, probe.box_size_m) else {
        return direction;
    };
    let local_origin = sub(point, position);
    if !point_is_inside_probe_box(local_origin, box_size_m) {
        return direction;
    }
    let half = [
        box_size_m[0].abs() * 0.5,
        box_size_m[1].abs() * 0.5,
        box_size_m[2].abs() * 0.5,
    ];
    let mut t_exit = f32::INFINITY;
    for axis in 0..3 {
        let d = direction[axis];
        if d.abs() <= 1.0e-6 {
            continue;
        }
        let plane = if d > 0.0 { half[axis] } else { -half[axis] };
        let t = (plane - local_origin[axis]) / d;
        if t > 1.0e-5 {
            t_exit = t_exit.min(t);
        }
    }
    if !t_exit.is_finite() {
        return direction;
    }
    normalize(add(local_origin, scale(direction, t_exit)))
}

fn blend_reflection_probe_diffuse_radiance(
    fallback: [f32; 3],
    normal: [f32; 3],
    point: [f32; 3],
    settings: &PreparedRenderSettings,
) -> [f32; 3] {
    if settings.reflection_probes.is_empty() {
        return fallback;
    }
    let mut weighted = [0.0_f32; 3];
    let mut total_weight = 0.0_f32;
    for probe in &settings.reflection_probes {
        let weight = reflection_probe_influence_weight(&probe.settings, point);
        if weight <= 0.0 {
            continue;
        }
        let radiance = scale(
            sample_diffuse_environment_texture(&probe.texture, normal, probe.settings.rotation_deg),
            probe.settings.intensity.max(0.0),
        );
        let rgb = rgb_f32_to_unit(radiance);
        let probe_rgb = [
            rgb[0] * probe.settings.ambient_intensity.max(0.0),
            rgb[1] * probe.settings.ambient_intensity.max(0.0),
            rgb[2] * probe.settings.ambient_intensity.max(0.0),
        ];
        for channel in 0..3 {
            weighted[channel] += probe_rgb[channel] * weight;
        }
        total_weight += weight;
    }
    if total_weight <= f32::EPSILON {
        return fallback;
    }
    let probe_rgb = scale(weighted, 1.0 / total_weight);
    lerp_vec3(fallback, probe_rgb, total_weight.clamp(0.0, 1.0))
}

fn blend_reflection_probe_rough_radiance(
    fallback: [f32; 3],
    direction: [f32; 3],
    normal: [f32; 3],
    point: [f32; 3],
    roughness: f32,
    settings: &PreparedRenderSettings,
) -> [f32; 3] {
    if settings.reflection_probes.is_empty() {
        return fallback;
    }
    let normal = normalize(normal);
    let mut weighted = [0.0_f32; 3];
    let mut total_weight = 0.0_f32;
    for probe in &settings.reflection_probes {
        let weight = reflection_probe_influence_weight(&probe.settings, point);
        if weight <= 0.0 {
            continue;
        }
        let axis = reflection_probe_sample_direction(&probe.settings, point, direction);
        let sample_dir = if dot(axis, normal) < 0.0 {
            normalize(lerp_vec3(axis, normal, 0.65))
        } else {
            axis
        };
        let probe_rgb = scale(
            sample_prefiltered_environment_texture(
                &probe.texture,
                sample_dir,
                probe.settings.rotation_deg,
                roughness,
            ),
            probe.settings.intensity.max(0.0),
        );
        for channel in 0..3 {
            weighted[channel] += probe_rgb[channel] * weight;
        }
        total_weight += weight;
    }
    if total_weight <= f32::EPSILON {
        return fallback;
    }
    let probe_rgb = scale(weighted, 1.0 / total_weight);
    lerp_vec3(fallback, probe_rgb, total_weight.clamp(0.0, 1.0))
}

fn rough_environment_radiance_rgb(
    direction: [f32; 3],
    normal: [f32; 3],
    point: [f32; 3],
    roughness: f32,
    settings: &PreparedRenderSettings,
) -> [f32; 3] {
    let roughness = roughness.clamp(0.04, 1.0);
    let fallback =
        rough_environment_radiance_rgb_without_probe(direction, normal, roughness, settings);
    blend_reflection_probe_rough_radiance(fallback, direction, normal, point, roughness, settings)
}

fn rough_environment_radiance_rgb_without_probe(
    direction: [f32; 3],
    normal: [f32; 3],
    roughness: f32,
    settings: &PreparedRenderSettings,
) -> [f32; 3] {
    let roughness = roughness.clamp(0.04, 1.0);
    if roughness <= 0.08 {
        return environment_radiance_rgb(direction, settings);
    }

    let axis = normalize(direction);
    let normal = normalize(normal);
    if let (Some(environment), Some(texture)) = (
        settings.settings.environment.as_ref(),
        settings.environment_map.as_deref(),
    ) {
        let sample_dir = if dot(axis, normal) < 0.0 {
            normalize(lerp_vec3(axis, normal, 0.65))
        } else {
            axis
        };
        return scale(
            sample_prefiltered_environment_texture(
                texture,
                sample_dir,
                environment.map_rotation_deg,
                roughness,
            ),
            environment.intensity.max(0.0),
        );
    }

    let (tangent, bitangent) = orthonormal_basis(axis);
    let spread = (roughness * roughness * 0.95).clamp(0.0, 0.95);
    let samples = [
        (0.0, 0.0, 1.0),
        (1.0, 0.0, 0.72),
        (-1.0, 0.0, 0.72),
        (0.0, 1.0, 0.72),
        (0.0, -1.0, 0.72),
        (0.7071, 0.7071, 0.45),
        (-0.7071, 0.7071, 0.45),
        (0.7071, -0.7071, 0.45),
        (-0.7071, -0.7071, 0.45),
    ];

    let mut rgb = [0.0_f32; 3];
    let mut total = 0.0_f32;
    for (x, y, weight) in samples {
        let candidate = normalize(add(
            axis,
            add(scale(tangent, x * spread), scale(bitangent, y * spread)),
        ));
        let sample_dir = if dot(candidate, normal) < 0.0 {
            normalize(lerp_vec3(candidate, normal, 0.65))
        } else {
            candidate
        };
        let sample = environment_radiance_rgb(sample_dir, settings);
        for channel in 0..3 {
            rgb[channel] += sample[channel] * weight;
        }
        total += weight;
    }

    if total <= f32::EPSILON {
        return environment_radiance_rgb(direction, settings);
    }
    scale(rgb, 1.0 / total)
}

fn orthonormal_basis(axis: [f32; 3]) -> ([f32; 3], [f32; 3]) {
    let axis = normalize(axis);
    let helper = if axis[2].abs() < 0.90 {
        [0.0, 0.0, 1.0]
    } else {
        [0.0, 1.0, 0.0]
    };
    let tangent = normalize(cross(helper, axis));
    let bitangent = normalize(cross(axis, tangent));
    (tangent, bitangent)
}

fn procedural_environment_radiance_rgb(
    direction: [f32; 3],
    environment: &EnvironmentSettings,
) -> [f32; 3] {
    let direction = normalize(direction);
    let z = direction[2].clamp(-1.0, 1.0);
    let (from, to, amount) = if z >= 0.0 {
        (
            environment.sky_horizon_rgb,
            environment.sky_top_rgb,
            z.sqrt(),
        )
    } else {
        (
            environment.sky_horizon_rgb,
            environment.ground_rgb,
            (-z).sqrt(),
        )
    };
    let from = srgb_u8_to_linear_radiance(from);
    let to = srgb_u8_to_linear_radiance(to);
    lerp_vec3(from, to, amount)
}

fn tone_map_rgb(color: [f32; 3], settings: &RenderSettings) -> [u8; 3] {
    let mut out = [0_u8; 3];
    let temperature_balance = settings
        .color_temperature_kelvin
        .map(color_temperature_white_balance_rgb)
        .unwrap_or([1.0, 1.0, 1.0]);
    for channel in 0..3 {
        let white_balance =
            settings.white_balance_rgb[channel].max(0.0) * temperature_balance[channel];
        let linear =
            color[channel].max(0.0) / 255.0 * settings.tone_exposure.max(0.0) * white_balance;
        let mapped = match settings.tone_mapping {
            ToneMapping::Linear => linear.clamp(0.0, 1.0),
            ToneMapping::Reinhard => linear / (1.0 + linear),
            ToneMapping::Aces => {
                let a = 2.51;
                let b = 0.03;
                let c = 2.43;
                let d = 0.59;
                let e = 0.14;
                ((linear * (a * linear + b)) / (linear * (c * linear + d) + e)).clamp(0.0, 1.0)
            }
        };
        out[channel] = (linear_unit_to_srgb_unit(mapped) * 255.0)
            .round()
            .clamp(0.0, 255.0) as u8;
    }
    out
}

fn color_temperature_white_balance_rgb(kelvin: f32) -> [f32; 3] {
    let rgb = color_temperature_rgb(kelvin);
    let max_channel = rgb[0].max(rgb[1]).max(rgb[2]).max(f32::EPSILON);
    [
        (max_channel / rgb[0].max(f32::EPSILON)).clamp(0.0, 8.0),
        (max_channel / rgb[1].max(f32::EPSILON)).clamp(0.0, 8.0),
        (max_channel / rgb[2].max(f32::EPSILON)).clamp(0.0, 8.0),
    ]
}

fn color_temperature_rgb(kelvin: f32) -> [f32; 3] {
    let temperature = (kelvin.clamp(1000.0, 40000.0) / 100.0).max(1.0);
    let red = if temperature <= 66.0 {
        255.0
    } else {
        (329.69873 * (temperature - 60.0).powf(-0.13320476)).clamp(0.0, 255.0)
    };
    let green = if temperature <= 66.0 {
        (99.4708 * temperature.ln() - 161.11957).clamp(0.0, 255.0)
    } else {
        (288.12216 * (temperature - 60.0).powf(-0.075514846)).clamp(0.0, 255.0)
    };
    let blue = if temperature >= 66.0 {
        255.0
    } else if temperature <= 19.0 {
        0.0
    } else {
        (138.51773 * (temperature - 10.0).ln() - 305.0448).clamp(0.0, 255.0)
    };
    [red / 255.0, green / 255.0, blue / 255.0]
}

fn fresnel_schlick_rgb(cos_theta: f32, f0: [f32; 3]) -> [f32; 3] {
    let amount = (1.0 - cos_theta.clamp(0.0, 1.0)).powi(5);
    [
        f0[0] + (1.0 - f0[0]) * amount,
        f0[1] + (1.0 - f0[1]) * amount,
        f0[2] + (1.0 - f0[2]) * amount,
    ]
}

fn iridescence_fresnel_tint_rgb(hit: &Hit, cos_theta: f32) -> [f32; 3] {
    let factor = hit.iridescence_factor.clamp(0.0, 1.0);
    if factor <= 1.0e-4 || hit.iridescence_thickness_nm <= 1.0e-4 {
        return [1.0, 1.0, 1.0];
    }
    let film_ior = hit.iridescence_ior.max(1.0);
    let optical_path_nm =
        2.0 * film_ior * hit.iridescence_thickness_nm.max(0.0) * cos_theta.clamp(0.0, 1.0);
    let wavelengths_nm = [650.0_f32, 510.0, 475.0];
    let mut tint = [1.0_f32; 3];
    for channel in 0..3 {
        let phase = 2.0 * PI * optical_path_nm / wavelengths_nm[channel];
        let interference = 0.5 + 0.5 * phase.cos();
        let channel_tint = 0.25 + 1.15 * interference;
        tint[channel] = mix_f32(1.0, channel_tint, factor).clamp(0.05, 1.6);
    }
    tint
}

fn ggx_distribution(ndoth: f32, roughness: f32) -> f32 {
    let alpha = (roughness * roughness).max(0.001);
    let alpha2 = alpha * alpha;
    let ndoth2 = ndoth.clamp(0.0, 1.0) * ndoth.clamp(0.0, 1.0);
    let denom = ndoth2 * (alpha2 - 1.0) + 1.0;
    alpha2 / (PI * denom * denom).max(1.0e-6)
}

fn smith_ggx_geometry(ndotv: f32, ndotl: f32, roughness: f32) -> f32 {
    let k = ((roughness + 1.0) * (roughness + 1.0)) / 8.0;
    schlick_ggx_geometry(ndotv, k) * schlick_ggx_geometry(ndotl, k)
}

fn schlick_ggx_geometry(ndot: f32, k: f32) -> f32 {
    let ndot = ndot.clamp(0.0, 1.0);
    ndot / (ndot * (1.0 - k) + k).max(1.0e-6)
}

fn mix_f32(left: f32, right: f32, amount: f32) -> f32 {
    left * (1.0 - amount) + right * amount
}

fn srgb_u8_to_linear_rgb(color: [u8; 3]) -> [f32; 3] {
    [
        srgb_unit_to_linear_unit(f32::from(color[0]) / 255.0),
        srgb_unit_to_linear_unit(f32::from(color[1]) / 255.0),
        srgb_unit_to_linear_unit(f32::from(color[2]) / 255.0),
    ]
}

fn srgb_u8_to_linear_radiance(color: [u8; 3]) -> [f32; 3] {
    let linear = srgb_u8_to_linear_rgb(color);
    [linear[0] * 255.0, linear[1] * 255.0, linear[2] * 255.0]
}

fn srgb_unit_to_linear_unit(value: f32) -> f32 {
    let value = value.clamp(0.0, 1.0);
    if value <= 0.04045 {
        value / 12.92
    } else {
        ((value + 0.055) / 1.055).powf(2.4)
    }
}

fn linear_unit_to_srgb_unit(value: f32) -> f32 {
    let value = value.clamp(0.0, 1.0);
    if value <= 0.0031308 {
        value * 12.92
    } else {
        1.055 * value.powf(1.0 / 2.4) - 0.055
    }
}

fn rgb_f32_to_unit(color: [f32; 3]) -> [f32; 3] {
    [
        (color[0] / 255.0).max(0.0),
        (color[1] / 255.0).max(0.0),
        (color[2] / 255.0).max(0.0),
    ]
}

fn fallback_lights() -> Vec<RenderLight> {
    vec![RenderLight {
        kind: RenderLightKind::Directional {
            direction: fallback_light_direction(),
            angular_radius_deg: 0.0,
        },
        color_rgb: [1.0, 1.0, 1.0],
        intensity: 1.0,
    }]
}

fn fallback_light_direction() -> [f32; 3] {
    normalize([-0.35, -0.55, 1.0])
}

#[cfg(test)]
fn shadow_visibility(
    hit: &Hit,
    light_dir: [f32; 3],
    max_t: Option<f32>,
    objects: &[RenderObject],
) -> f32 {
    let visibility = shadow_visibility_rgb(hit, light_dir, max_t, objects);
    (visibility[0] + visibility[1] + visibility[2]) / 3.0
}

#[allow(dead_code)]
fn shadow_visibility_rgb(
    hit: &Hit,
    light_dir: [f32; 3],
    max_t: Option<f32>,
    objects: &[RenderObject],
) -> [f32; 3] {
    let object_bvh = ObjectBvh::from_objects(objects);
    let intersector = SceneIntersector::new(objects, &object_bvh);
    shadow_visibility_rgb_with_intersector(hit, light_dir, max_t, &intersector)
}

fn shadow_visibility_rgb_with_intersector(
    hit: &Hit,
    light_dir: [f32; 3],
    max_t: Option<f32>,
    intersector: &SceneIntersector<'_>,
) -> [f32; 3] {
    let mut shadow_origin = add(hit.point, scale(hit.normal, 1.0e-3));
    let light_dir = normalize(light_dir);
    let mut traveled = 0.0_f32;
    let mut transmittance = [1.0_f32, 1.0, 1.0];

    for _ in 0..64 {
        let remaining_max_t = max_t.map(|max_t| max_t - traveled);
        if remaining_max_t.is_some_and(|remaining| remaining <= 1.0e-3) {
            break;
        }
        let shadow_ray = Ray {
            origin: shadow_origin,
            dir: light_dir,
        };
        let Some(shadow_hit) = closest_shadow_hit_with_intersector(
            &shadow_ray,
            hit.entity_u32,
            remaining_max_t,
            intersector,
        ) else {
            break;
        };
        let hit_transmittance = shadow_hit_transmittance_rgb(&shadow_hit);
        for channel in 0..3 {
            transmittance[channel] *= hit_transmittance[channel];
        }
        if transmittance.iter().all(|channel| *channel <= 1.0e-3) {
            return [0.0, 0.0, 0.0];
        }
        let step = shadow_hit.t + 1.0e-3;
        traveled += step;
        shadow_origin = add(shadow_origin, scale(light_dir, step));
    }

    [
        transmittance[0].clamp(0.0, 1.0),
        transmittance[1].clamp(0.0, 1.0),
        transmittance[2].clamp(0.0, 1.0),
    ]
}

#[allow(dead_code)]
fn closest_shadow_hit(
    shadow_ray: &Ray,
    source_entity_u32: u32,
    max_t: Option<f32>,
    objects: &[RenderObject],
) -> Option<Hit> {
    let object_bvh = ObjectBvh::from_objects(objects);
    let intersector = SceneIntersector::new(objects, &object_bvh);
    closest_shadow_hit_with_intersector(shadow_ray, source_entity_u32, max_t, &intersector)
}

fn closest_shadow_hit_with_intersector(
    shadow_ray: &Ray,
    source_entity_u32: u32,
    max_t: Option<f32>,
    intersector: &SceneIntersector<'_>,
) -> Option<Hit> {
    intersector
        .hits_with_mode(shadow_ray, TriangleIntersectionMode::VisibleSurface)
        .into_iter()
        .filter(|shadow_hit| {
            shadow_hit.entity_u32 != source_entity_u32
                && shadow_hit.t > 1.0e-3
                && max_t.is_none_or(|max_t| shadow_hit.t < max_t - 1.0e-3)
        })
        .min_by(|left, right| left.t.total_cmp(&right.t))
}

#[cfg(test)]
fn shadow_visibility_rgb_collecting_all_hits(
    hit: &Hit,
    light_dir: [f32; 3],
    max_t: Option<f32>,
    objects: &[RenderObject],
) -> [f32; 3] {
    let shadow_origin = add(hit.point, scale(hit.normal, 1.0e-3));
    let shadow_ray = Ray {
        origin: shadow_origin,
        dir: normalize(light_dir),
    };
    let mut shadow_hits = objects
        .iter()
        .flat_map(|object| intersect_object_hits(&shadow_ray, object))
        .filter(|shadow_hit| {
            shadow_hit.entity_u32 != hit.entity_u32
                && shadow_hit.t > 1.0e-3
                && max_t.is_none_or(|max_t| shadow_hit.t < max_t - 1.0e-3)
        })
        .collect::<Vec<_>>();
    shadow_hits.sort_by(|left, right| left.t.total_cmp(&right.t));

    let mut transmittance = [1.0_f32, 1.0, 1.0];
    for shadow_hit in shadow_hits {
        let hit_transmittance = shadow_hit_transmittance_rgb(&shadow_hit);
        for channel in 0..3 {
            transmittance[channel] *= hit_transmittance[channel];
        }
        if transmittance.iter().all(|channel| *channel <= 1.0e-3) {
            return [0.0, 0.0, 0.0];
        }
    }
    [
        transmittance[0].clamp(0.0, 1.0),
        transmittance[1].clamp(0.0, 1.0),
        transmittance[2].clamp(0.0, 1.0),
    ]
}

fn shadow_hit_transmittance_rgb(hit: &Hit) -> [f32; 3] {
    let alpha = hit.alpha.clamp(0.0, 1.0);
    let diffuse_transmission = hit.diffuse_transmission.clamp(0.0, 1.0);
    [
        ((1.0 - alpha)
            + alpha * diffuse_transmission * hit.diffuse_transmission_color[0].clamp(0.0, 1.0))
            * hit.transmission_filter_rgb[0].clamp(0.0, 1.0),
        ((1.0 - alpha)
            + alpha * diffuse_transmission * hit.diffuse_transmission_color[1].clamp(0.0, 1.0))
            * hit.transmission_filter_rgb[1].clamp(0.0, 1.0),
        ((1.0 - alpha)
            + alpha * diffuse_transmission * hit.diffuse_transmission_color[2].clamp(0.0, 1.0))
            * hit.transmission_filter_rgb[2].clamp(0.0, 1.0),
    ]
}

fn encode_normal_rgb(normal: Option<[f32; 3]>) -> [u8; 3] {
    let Some(normal) = normal else {
        return [0, 0, 0];
    };
    let normal = normalize(normal);
    [
        (((normal[0].clamp(-1.0, 1.0) * 0.5 + 0.5) * 255.0).round()) as u8,
        (((normal[1].clamp(-1.0, 1.0) * 0.5 + 0.5) * 255.0).round()) as u8,
        (((normal[2].clamp(-1.0, 1.0) * 0.5 + 0.5) * 255.0).round()) as u8,
    ]
}

fn encode_material_properties_rgb(hit: &Hit) -> [u8; 3] {
    [
        unit_f32_to_u8(hit.metallic),
        unit_f32_to_u8(hit.roughness),
        unit_f32_to_u8(hit.occlusion),
    ]
}

fn unit_f32_to_u8(value: f32) -> u8 {
    (value.clamp(0.0, 1.0) * 255.0).round() as u8
}

fn write_vec3_f32le(out: &mut [u8], value: [f32; 3]) {
    debug_assert!(out.len() >= 12);
    out[0..4].copy_from_slice(&value[0].to_le_bytes());
    out[4..8].copy_from_slice(&value[1].to_le_bytes());
    out[8..12].copy_from_slice(&value[2].to_le_bytes());
}

fn encode_png_rgb(width: u32, height: u32, rgb: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Cursor::new(Vec::new());
    let mut encoder = png::Encoder::new(&mut out, width, height);
    encoder.set_color(png::ColorType::Rgb);
    encoder.set_depth(png::BitDepth::Eight);
    let mut writer = encoder.write_header().map_err(|err| err.to_string())?;
    writer
        .write_image_data(rgb)
        .map_err(|err| err.to_string())?;
    drop(writer);
    Ok(out.into_inner())
}

#[cfg(test)]
fn encode_png_rgba(width: u32, height: u32, rgba: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Cursor::new(Vec::new());
    let mut encoder = png::Encoder::new(&mut out, width, height);
    encoder.set_color(png::ColorType::Rgba);
    encoder.set_depth(png::BitDepth::Eight);
    let mut writer = encoder.write_header().map_err(|err| err.to_string())?;
    writer
        .write_image_data(rgba)
        .map_err(|err| err.to_string())?;
    drop(writer);
    Ok(out.into_inner())
}

fn add(left: [f32; 3], right: [f32; 3]) -> [f32; 3] {
    [left[0] + right[0], left[1] + right[1], left[2] + right[2]]
}

fn scale(value: [f32; 3], factor: f32) -> [f32; 3] {
    [value[0] * factor, value[1] * factor, value[2] * factor]
}

fn reflect(incident: [f32; 3], normal: [f32; 3]) -> [f32; 3] {
    sub(incident, scale(normal, 2.0 * dot(incident, normal)))
}

fn refract(incident: [f32; 3], normal: [f32; 3], eta: f32) -> Option<[f32; 3]> {
    let incident = normalize(incident);
    let normal = normalize(normal);
    let cos_i = (-dot(normal, incident)).clamp(-1.0, 1.0);
    let sin_t2 = eta * eta * (1.0 - cos_i * cos_i);
    if sin_t2 > 1.0 {
        return None;
    }
    let cos_t = (1.0 - sin_t2).sqrt();
    Some(normalize(add(
        scale(incident, eta),
        scale(normal, eta * cos_i - cos_t),
    )))
}

fn lerp_vec3(left: [f32; 3], right: [f32; 3], factor: f32) -> [f32; 3] {
    [
        left[0] + (right[0] - left[0]) * factor,
        left[1] + (right[1] - left[1]) * factor,
        left[2] + (right[2] - left[2]) * factor,
    ]
}

fn sub(left: [f32; 3], right: [f32; 3]) -> [f32; 3] {
    [left[0] - right[0], left[1] - right[1], left[2] - right[2]]
}

fn dot(left: [f32; 3], right: [f32; 3]) -> f32 {
    left[0] * right[0] + left[1] * right[1] + left[2] * right[2]
}

fn cross(left: [f32; 3], right: [f32; 3]) -> [f32; 3] {
    [
        left[1] * right[2] - left[2] * right[1],
        left[2] * right[0] - left[0] * right[2],
        left[0] * right[1] - left[1] * right[0],
    ]
}

fn triangle_area(triangle: &Triangle) -> f32 {
    let cross = cross(sub(triangle.b, triangle.a), sub(triangle.c, triangle.a));
    0.5 * dot(cross, cross).sqrt()
}

fn triangle_barycentric_point(
    a: [f32; 3],
    b: [f32; 3],
    c: [f32; 3],
    weights: [f32; 3],
) -> [f32; 3] {
    [
        a[0] * weights[0] + b[0] * weights[1] + c[0] * weights[2],
        a[1] * weights[0] + b[1] * weights[1] + c[1] * weights[2],
        a[2] * weights[0] + b[2] * weights[1] + c[2] * weights[2],
    ]
}

fn normalize(value: [f32; 3]) -> [f32; 3] {
    let len = dot(value, value).sqrt();
    if len <= f32::EPSILON {
        return [1.0, 0.0, 0.0];
    }
    [value[0] / len, value[1] / len, value[2] / len]
}

fn normalize_quat(value: [f32; 4]) -> [f32; 4] {
    let len =
        (value[0] * value[0] + value[1] * value[1] + value[2] * value[2] + value[3] * value[3])
            .sqrt();
    if len <= f32::EPSILON {
        return [0.0, 0.0, 0.0, 1.0];
    }
    [
        value[0] / len,
        value[1] / len,
        value[2] / len,
        value[3] / len,
    ]
}

fn scale_quat(value: [f32; 4], factor: f32) -> [f32; 4] {
    [
        value[0] * factor,
        value[1] * factor,
        value[2] * factor,
        value[3] * factor,
    ]
}

fn slerp_quat(left: [f32; 4], right: [f32; 4], factor: f32) -> [f32; 4] {
    let mut right = right;
    let mut cos_theta =
        left[0] * right[0] + left[1] * right[1] + left[2] * right[2] + left[3] * right[3];
    if cos_theta < 0.0 {
        right = [-right[0], -right[1], -right[2], -right[3]];
        cos_theta = -cos_theta;
    }
    if cos_theta > 0.9995 {
        return normalize_quat([
            left[0] + (right[0] - left[0]) * factor,
            left[1] + (right[1] - left[1]) * factor,
            left[2] + (right[2] - left[2]) * factor,
            left[3] + (right[3] - left[3]) * factor,
        ]);
    }
    let theta = cos_theta.clamp(-1.0, 1.0).acos();
    let sin_theta = theta.sin();
    if sin_theta.abs() <= f32::EPSILON {
        return left;
    }
    let left_scale = ((1.0 - factor) * theta).sin() / sin_theta;
    let right_scale = (factor * theta).sin() / sin_theta;
    normalize_quat([
        left[0] * left_scale + right[0] * right_scale,
        left[1] * left_scale + right[1] * right_scale,
        left[2] * left_scale + right[2] * right_scale,
        left[3] * left_scale + right[3] * right_scale,
    ])
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

fn mat_mul_f32(left: [[f32; 3]; 3], right: [[f32; 3]; 3]) -> [[f32; 3]; 3] {
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

fn mat_transpose_vec_mul(matrix: [[f32; 3]; 3], vector: [f32; 3]) -> [f32; 3] {
    [
        matrix[0][0] * vector[0] + matrix[1][0] * vector[1] + matrix[2][0] * vector[2],
        matrix[0][1] * vector[0] + matrix[1][1] * vector[1] + matrix[2][1] * vector[2],
        matrix[0][2] * vector[0] + matrix[1][2] * vector[1] + matrix[2][2] * vector[2],
    ]
}

fn inverse_rotate(transform: Transform, vector: [f32; 3]) -> [f32; 3] {
    mat_transpose_vec_mul(rotation_matrix(transform), vector)
}

fn mat4_identity() -> [[f32; 4]; 4] {
    [
        [1.0, 0.0, 0.0, 0.0],
        [0.0, 1.0, 0.0, 0.0],
        [0.0, 0.0, 1.0, 0.0],
        [0.0, 0.0, 0.0, 1.0],
    ]
}

fn mat4_scale(scale: [f32; 3]) -> [[f32; 4]; 4] {
    [
        [scale[0], 0.0, 0.0, 0.0],
        [0.0, scale[1], 0.0, 0.0],
        [0.0, 0.0, scale[2], 0.0],
        [0.0, 0.0, 0.0, 1.0],
    ]
}

fn mat4_mul(left: [[f32; 4]; 4], right: [[f32; 4]; 4]) -> [[f32; 4]; 4] {
    let mut result = mat4_identity();
    for col in 0..4 {
        for row in 0..4 {
            result[col][row] = left[0][row] * right[col][0]
                + left[1][row] * right[col][1]
                + left[2][row] * right[col][2]
                + left[3][row] * right[col][3];
        }
    }
    result
}

fn mat4_inverse(matrix: [[f32; 4]; 4]) -> Option<[[f32; 4]; 4]> {
    let mut augmented = [[0.0_f32; 8]; 4];
    for row in 0..4 {
        for col in 0..4 {
            augmented[row][col] = matrix[col][row];
        }
        augmented[row][row + 4] = 1.0;
    }

    for pivot_col in 0..4 {
        let mut pivot_row = pivot_col;
        let mut pivot_abs = augmented[pivot_row][pivot_col].abs();
        for (row, values) in augmented.iter().enumerate().skip(pivot_col + 1) {
            let candidate_abs = values[pivot_col].abs();
            if candidate_abs > pivot_abs {
                pivot_row = row;
                pivot_abs = candidate_abs;
            }
        }
        if pivot_abs <= f32::EPSILON {
            return None;
        }
        if pivot_row != pivot_col {
            augmented.swap(pivot_row, pivot_col);
        }

        let pivot = augmented[pivot_col][pivot_col];
        for col in 0..8 {
            augmented[pivot_col][col] /= pivot;
        }
        for row in 0..4 {
            if row == pivot_col {
                continue;
            }
            let factor = augmented[row][pivot_col];
            for col in 0..8 {
                augmented[row][col] -= factor * augmented[pivot_col][col];
            }
        }
    }

    let mut inverse = mat4_identity();
    for row in 0..4 {
        for col in 0..4 {
            inverse[col][row] = augmented[row][col + 4];
        }
    }
    Some(inverse)
}

fn transform_point4(matrix: [[f32; 4]; 4], point: [f32; 3]) -> [f32; 3] {
    [
        matrix[0][0] * point[0] + matrix[1][0] * point[1] + matrix[2][0] * point[2] + matrix[3][0],
        matrix[0][1] * point[0] + matrix[1][1] * point[1] + matrix[2][1] * point[2] + matrix[3][1],
        matrix[0][2] * point[0] + matrix[1][2] * point[1] + matrix[2][2] * point[2] + matrix[3][2],
    ]
}

fn transform_direction4(matrix: [[f32; 4]; 4], direction: [f32; 3]) -> [f32; 3] {
    [
        matrix[0][0] * direction[0] + matrix[1][0] * direction[1] + matrix[2][0] * direction[2],
        matrix[0][1] * direction[0] + matrix[1][1] * direction[1] + matrix[2][1] * direction[2],
        matrix[0][2] * direction[0] + matrix[1][2] * direction[1] + matrix[2][2] * direction[2],
    ]
}

fn transform_normal4(matrix: [[f32; 4]; 4], normal: [f32; 3]) -> [f32; 3] {
    let a = matrix[0][0];
    let b = matrix[1][0];
    let c = matrix[2][0];
    let d = matrix[0][1];
    let e = matrix[1][1];
    let f = matrix[2][1];
    let g = matrix[0][2];
    let h = matrix[1][2];
    let i = matrix[2][2];

    let det = a * (e * i - f * h) - b * (d * i - f * g) + c * (d * h - e * g);
    if det.abs() <= f32::EPSILON {
        return transform_direction4(matrix, normal);
    }

    let inv_det = 1.0 / det;
    let inv = [
        [
            (e * i - f * h) * inv_det,
            (c * h - b * i) * inv_det,
            (b * f - c * e) * inv_det,
        ],
        [
            (f * g - d * i) * inv_det,
            (a * i - c * g) * inv_det,
            (c * d - a * f) * inv_det,
        ],
        [
            (d * h - e * g) * inv_det,
            (b * g - a * h) * inv_det,
            (a * e - b * d) * inv_det,
        ],
    ];

    [
        inv[0][0] * normal[0] + inv[1][0] * normal[1] + inv[2][0] * normal[2],
        inv[0][1] * normal[0] + inv[1][1] * normal[1] + inv[2][1] * normal[2],
        inv[0][2] * normal[0] + inv[1][2] * normal[1] + inv[2][2] * normal[2],
    ]
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Cursor;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use gltf::mesh::Mode;
    use robotdreams_core::{
        EnvironmentSettings, RenderSettings, RobotDreamsSnapshot,
        scene_graph::{
            CameraDistortion, CameraIntrinsics, CameraProjection, CameraSensorEffects, CameraSpec,
            EntityId, EntityMetadata, Geometry, LightKind, LightSpec, Material, ObservationRequest,
            ObservationView, ReflectionProbeSettings, SceneGraph, SceneNode, SceneNodeKind,
            SegmentationPolicy, ShutterMode, ShutterPolicy, ToneMapping, Transform,
        },
    };

    use super::{
        EnvironmentTexture, FrameBuffer, FrameKind, Hit, LightSample, MaterialAlphaMode,
        NativeRenderer, ObjectBvh, PreparedReflectionProbe, PreparedRenderSettings, PreparedScene,
        Ray, RenderGeometry, RenderLight, RenderLightKind, RenderObject, RenderSceneSample,
        SceneGraphSample, SceneIntersector, TextureFilter, TextureImage, TextureWrap,
        TransmissionMedium, Triangle, TriangleIntersectionMode, apply_depth_sensor_effects,
        apply_rgb_sensor_effects, build_environment_brdf_lut, build_environment_diffuse_irradiance,
        build_environment_roughness_mips, camera_ray_at_pixel, channel_dispersion_ior,
        color_temperature_white_balance_rgb, composite_debug_rgb_hits,
        decoded_environment_image_rgb, dielectric_f0_from_ior, distort_normalized_point, dot,
        emissive_triangle_lights, encode_png_rgb, encode_png_rgba, entity_u32_map,
        environment_background_rgb, environment_reflection_rgb, fallback_light_direction,
        fallback_lights, find_camera, interpolated_anisotropy_direction,
        interpolated_emission_sample, interpolated_material_sample, intersect_object_hits,
        intersect_triangle, intersect_triangle_with_mode, light_samples, lights_for_objects,
        load_gltf_mesh, load_gltf_mesh_at_time, mapped_normal, mat4_scale,
        next_transmission_medium_stack, normalize, point_light_attenuation,
        reflection_probe_influence_weight, reflection_probe_sample_direction, refract,
        refracted_background_rgb, refracted_scene_rgb, refracted_transmission_lobe_rgb,
        refraction_surface_normal, render_asset_lights, render_lights, render_object_bounds,
        render_objects, render_settings_for_scene, rough_environment_radiance_rgb,
        sample_color_texture_linear_rgb, sample_diffuse_environment_texture,
        sample_environment_brdf_lut, sample_environment_texture, sample_morph_weights_animation,
        sample_prefiltered_environment_texture, sample_texture, sample_vec3_animation, scale,
        scene_reflection_rgb, shade_hit, shade_hit_with_lights, shade_pbr_direct_rgb,
        shadow_visibility, shadow_visibility_rgb, shadow_visibility_rgb_collecting_all_hits,
        spot_light_angular_attenuation, srgb_u8_to_linear_radiance, srgb_u8_to_linear_rgb,
        texture_filter_from_gltf, tone_map_rgb, transform_normal4, triangle_index_triplets,
        undistort_normalized_point,
    };

    fn assert_rgb_close(actual: [f32; 3], expected: [f32; 3], epsilon: f32) {
        for channel in 0..3 {
            assert!(
                (actual[channel] - expected[channel]).abs() <= epsilon,
                "channel {channel}: actual {:?}, expected {:?}, epsilon {epsilon}",
                actual,
                expected
            );
        }
    }

    #[test]
    fn render_object_bounds_transform_local_geometry_bounds() {
        let bounds = render_object_bounds(
            &RenderGeometry::Box {
                size: [2.0, 4.0, 6.0],
            },
            Transform {
                translation: [10.0, -2.0, 1.5],
                rotation: [0.0, 0.0, std::f32::consts::FRAC_PI_2],
                rotation_matrix: None,
            },
        )
        .expect("box bounds");

        assert_rgb_close(bounds.0, [8.0, -3.0, -1.5], 1.0e-5);
        assert_rgb_close(bounds.1, [12.0, -1.0, 4.5], 1.0e-5);
    }

    #[test]
    fn scene_intersector_matches_linear_hits_for_bounded_and_unbounded_objects() {
        let bounded_geometry = Arc::new(RenderGeometry::Sphere { radius: 0.5 });
        let bounded_transform = Transform::translated([3.0, 0.0, 0.0]);
        let unbounded_geometry = Arc::new(RenderGeometry::Sphere { radius: 0.25 });
        let objects = vec![
            RenderObject {
                entity_u32: 1,
                bounds: render_object_bounds(bounded_geometry.as_ref(), bounded_transform),
                geometry: bounded_geometry,
                color_rgb: [255, 0, 0],
                transform: bounded_transform,
            },
            RenderObject {
                entity_u32: 2,
                bounds: None,
                geometry: unbounded_geometry,
                color_rgb: [0, 0, 255],
                transform: Transform::translated([4.0, 0.0, 0.0]),
            },
        ];
        let ray = Ray {
            origin: [0.0, 0.0, 0.0],
            dir: [1.0, 0.0, 0.0],
        };
        let mut linear_hits = objects
            .iter()
            .flat_map(|object| intersect_object_hits(&ray, object))
            .map(|hit| (hit.entity_u32, hit.t))
            .collect::<Vec<_>>();
        let object_bvh = ObjectBvh::from_objects(&objects);
        let intersector = SceneIntersector::new(&objects, &object_bvh);
        let mut indexed_hits = intersector
            .hits(&ray)
            .into_iter()
            .map(|hit| (hit.entity_u32, hit.t))
            .collect::<Vec<_>>();

        linear_hits.sort_by(|left, right| {
            left.0
                .cmp(&right.0)
                .then_with(|| left.1.total_cmp(&right.1))
        });
        indexed_hits.sort_by(|left, right| {
            left.0
                .cmp(&right.0)
                .then_with(|| left.1.total_cmp(&right.1))
        });
        assert_eq!(indexed_hits.len(), linear_hits.len());
        for ((actual_id, actual_t), (expected_id, expected_t)) in
            indexed_hits.iter().zip(linear_hits.iter())
        {
            assert_eq!(actual_id, expected_id);
            assert!((actual_t - expected_t).abs() <= 1.0e-5);
        }
    }

    fn matte_test_hit(normal: [f32; 3]) -> Hit {
        Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [200, 200, 200],
            color_linear_rgb: srgb_u8_to_linear_rgb([200, 200, 200]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 1.0,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: normal,
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal,
            view_dir: normal,
        }
    }

    fn medium_stack_summary(medium_stack: &[TransmissionMedium]) -> Vec<(u32, f32)> {
        medium_stack
            .iter()
            .map(|medium| (medium.entity_u32, medium.ior))
            .collect()
    }

    fn write_test_gltf(name: &str) -> (PathBuf, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "robotdreams-renderer-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let bin_path = dir.join("mesh.bin");
        let gltf_path = dir.join("mesh.gltf");

        let mut bin = Vec::new();
        for value in [0.0_f32, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0_u16, 1, 2] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        fs::write(&bin_path, bin).expect("write bin");
        fs::write(
            &gltf_path,
            r#"{
  "asset": { "version": "2.0" },
  "scene": 0,
  "scenes": [{ "nodes": [0] }],
  "nodes": [{ "mesh": 0, "translation": [1.0, 2.0, 3.0] }],
  "meshes": [{
    "primitives": [{
      "attributes": { "POSITION": 0, "NORMAL": 1 },
      "indices": 2,
      "material": 0
    }]
  }],
  "materials": [{
    "pbrMetallicRoughness": {
      "baseColorFactor": [0.1, 0.5, 0.9, 1.0]
    }
  }],
  "buffers": [{ "uri": "mesh.bin", "byteLength": 78 }],
  "bufferViews": [
    { "buffer": 0, "byteOffset": 0, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 36, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 72, "byteLength": 6, "target": 34963 }
  ],
  "accessors": [
    {
      "bufferView": 0,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 0.0],
      "max": [1.0, 1.0, 0.0]
    },
    {
      "bufferView": 1,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 1.0],
      "max": [0.0, 0.0, 1.0]
    },
    {
      "bufferView": 2,
      "componentType": 5123,
      "count": 3,
      "type": "SCALAR"
    }
  ]
}"#,
        )
        .expect("write gltf");

        (dir, gltf_path)
    }

    fn write_material_variants_test_gltf(name: &str) -> (PathBuf, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "robotdreams-renderer-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let bin_path = dir.join("mesh.bin");
        let gltf_path = dir.join("mesh.gltf");

        let mut bin = Vec::new();
        for value in [0.0_f32, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0_u16, 1, 2] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        fs::write(&bin_path, bin).expect("write bin");
        fs::write(
            &gltf_path,
            r#"{
  "asset": { "version": "2.0" },
  "extensionsUsed": ["KHR_materials_variants"],
  "extensions": {
    "KHR_materials_variants": {
      "variants": [{ "name": "blue" }]
    }
  },
  "scene": 0,
  "scenes": [{ "nodes": [0] }],
  "nodes": [{ "mesh": 0 }],
  "meshes": [{
    "primitives": [{
      "attributes": { "POSITION": 0, "NORMAL": 1 },
      "indices": 2,
      "material": 0,
      "extensions": {
        "KHR_materials_variants": {
          "mappings": [{
            "material": 1,
            "variants": [0]
          }]
        }
      }
    }]
  }],
  "materials": [
    {
      "pbrMetallicRoughness": {
        "baseColorFactor": [1.0, 0.0, 0.0, 1.0]
      }
    },
    {
      "pbrMetallicRoughness": {
        "baseColorFactor": [0.0, 0.0, 1.0, 1.0]
      }
    }
  ],
  "buffers": [{ "uri": "mesh.bin", "byteLength": 78 }],
  "bufferViews": [
    { "buffer": 0, "byteOffset": 0, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 36, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 72, "byteLength": 6, "target": 34963 }
  ],
  "accessors": [
    {
      "bufferView": 0,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 0.0],
      "max": [1.0, 1.0, 0.0]
    },
    {
      "bufferView": 1,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 1.0],
      "max": [0.0, 0.0, 1.0]
    },
    {
      "bufferView": 2,
      "componentType": 5123,
      "count": 3,
      "type": "SCALAR"
    }
  ]
}"#,
        )
        .expect("write gltf");

        (dir, gltf_path)
    }

    fn write_morph_target_test_gltf(
        name: &str,
        mesh_weight: f32,
        node_weight: Option<f32>,
    ) -> (PathBuf, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "robotdreams-renderer-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let bin_path = dir.join("mesh.bin");
        let gltf_path = dir.join("mesh.gltf");

        let mut bin = Vec::new();
        for value in [0.0_f32, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0_u16, 1, 2] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        fs::write(&bin_path, bin).expect("write bin");

        let node_weights = node_weight
            .map(|weight| format!(r#", "weights": [{weight}]"#))
            .unwrap_or_default();
        fs::write(
            &gltf_path,
            format!(
                r#"{{
  "asset": {{ "version": "2.0" }},
  "scene": 0,
  "scenes": [{{ "nodes": [0] }}],
  "nodes": [{{ "mesh": 0{node_weights} }}],
  "meshes": [{{
    "weights": [{mesh_weight}],
    "primitives": [{{
      "attributes": {{ "POSITION": 0, "NORMAL": 1 }},
      "targets": [{{ "POSITION": 2, "NORMAL": 3 }}],
      "indices": 4,
      "material": 0
    }}]
  }}],
  "materials": [{{
    "pbrMetallicRoughness": {{
      "baseColorFactor": [0.1, 0.5, 0.9, 1.0]
    }}
  }}],
  "buffers": [{{ "uri": "mesh.bin", "byteLength": 150 }}],
  "bufferViews": [
    {{ "buffer": 0, "byteOffset": 0, "byteLength": 36, "target": 34962 }},
    {{ "buffer": 0, "byteOffset": 36, "byteLength": 36, "target": 34962 }},
    {{ "buffer": 0, "byteOffset": 72, "byteLength": 36, "target": 34962 }},
    {{ "buffer": 0, "byteOffset": 108, "byteLength": 36, "target": 34962 }},
    {{ "buffer": 0, "byteOffset": 144, "byteLength": 6, "target": 34963 }}
  ],
  "accessors": [
    {{
      "bufferView": 0,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 0.0],
      "max": [1.0, 1.0, 0.0]
    }},
    {{
      "bufferView": 1,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 1.0],
      "max": [0.0, 0.0, 1.0]
    }},
    {{
      "bufferView": 2,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 1.0],
      "max": [0.0, 0.0, 1.0]
    }},
    {{
      "bufferView": 3,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 1.0, 0.0],
      "max": [0.0, 1.0, 0.0]
    }},
    {{
      "bufferView": 4,
      "componentType": 5123,
      "count": 3,
      "type": "SCALAR"
    }}
  ]
}}"#
            ),
        )
        .expect("write gltf");

        (dir, gltf_path)
    }

    fn write_morph_weight_animation_test_gltf(name: &str) -> (PathBuf, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "robotdreams-renderer-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let bin_path = dir.join("mesh.bin");
        let gltf_path = dir.join("mesh.gltf");

        let mut bin = Vec::new();
        for value in [0.0_f32, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0_u16, 1, 2] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        fs::write(&bin_path, bin).expect("write bin");

        fs::write(
            &gltf_path,
            r#"{
  "asset": { "version": "2.0" },
  "extensionsUsed": ["KHR_materials_emissive_strength"],
  "scene": 0,
  "scenes": [{ "nodes": [0] }],
  "nodes": [{ "mesh": 0 }],
  "meshes": [{
    "weights": [0.0],
    "primitives": [{
      "attributes": { "POSITION": 0, "NORMAL": 1 },
      "targets": [{ "POSITION": 2, "NORMAL": 3 }],
      "indices": 6,
      "material": 0
    }]
  }],
  "animations": [{
    "samplers": [{ "input": 4, "output": 5, "interpolation": "LINEAR" }],
    "channels": [{ "sampler": 0, "target": { "node": 0, "path": "weights" } }]
  }],
  "materials": [{
    "pbrMetallicRoughness": {
      "baseColorFactor": [0.1, 0.5, 0.9, 1.0]
    }
  }],
  "buffers": [{ "uri": "mesh.bin", "byteLength": 166 }],
  "bufferViews": [
    { "buffer": 0, "byteOffset": 0, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 36, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 72, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 108, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 144, "byteLength": 8 },
    { "buffer": 0, "byteOffset": 152, "byteLength": 8 },
    { "buffer": 0, "byteOffset": 160, "byteLength": 6, "target": 34963 }
  ],
  "accessors": [
    {
      "bufferView": 0,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 0.0],
      "max": [1.0, 1.0, 0.0]
    },
    {
      "bufferView": 1,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 1.0],
      "max": [0.0, 0.0, 1.0]
    },
    {
      "bufferView": 2,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 1.0],
      "max": [0.0, 0.0, 1.0]
    },
    {
      "bufferView": 3,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 1.0, 0.0],
      "max": [0.0, 1.0, 0.0]
    },
    {
      "bufferView": 4,
      "componentType": 5126,
      "count": 2,
      "type": "SCALAR",
      "min": [0.0],
      "max": [1.0]
    },
    {
      "bufferView": 5,
      "componentType": 5126,
      "count": 2,
      "type": "SCALAR"
    },
    {
      "bufferView": 6,
      "componentType": 5123,
      "count": 3,
      "type": "SCALAR"
    }
  ]
}"#,
        )
        .expect("write gltf");

        (dir, gltf_path)
    }

    fn write_skinned_test_gltf(name: &str) -> (PathBuf, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "robotdreams-renderer-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let bin_path = dir.join("mesh.bin");
        let gltf_path = dir.join("mesh.gltf");

        let mut bin = Vec::new();
        for value in [0.0_f32, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for _ in 0..3 {
            bin.extend_from_slice(&[0_u8, 0, 0, 0]);
        }
        for _ in 0..3 {
            for value in [1.0_f32, 0.0, 0.0, 0.0] {
                bin.extend_from_slice(&value.to_le_bytes());
            }
        }
        for col in 0..4 {
            for row in 0..4 {
                let value = if row == col { 1.0_f32 } else { 0.0 };
                bin.extend_from_slice(&value.to_le_bytes());
            }
        }
        for value in [0_u16, 1, 2] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        fs::write(&bin_path, bin).expect("write bin");
        fs::write(
            &gltf_path,
            r#"{
  "asset": { "version": "2.0" },
  "scene": 0,
  "scenes": [{ "nodes": [0, 1] }],
  "nodes": [
    { "mesh": 0, "skin": 0, "translation": [2.0, 0.0, 0.0] },
    { "translation": [2.0, 0.0, 1.0] }
  ],
  "skins": [{ "joints": [1], "inverseBindMatrices": 4 }],
  "meshes": [{
    "primitives": [{
      "attributes": {
        "POSITION": 0,
        "NORMAL": 1,
        "JOINTS_0": 2,
        "WEIGHTS_0": 3
      },
      "indices": 5,
      "material": 0
    }]
  }],
  "materials": [{
    "pbrMetallicRoughness": {
      "baseColorFactor": [0.1, 0.5, 0.9, 1.0]
    }
  }],
  "buffers": [{ "uri": "mesh.bin", "byteLength": 202 }],
  "bufferViews": [
    { "buffer": 0, "byteOffset": 0, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 36, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 72, "byteLength": 12, "target": 34962 },
    { "buffer": 0, "byteOffset": 84, "byteLength": 48, "target": 34962 },
    { "buffer": 0, "byteOffset": 132, "byteLength": 64 },
    { "buffer": 0, "byteOffset": 196, "byteLength": 6, "target": 34963 }
  ],
  "accessors": [
    {
      "bufferView": 0,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 0.0],
      "max": [1.0, 1.0, 0.0]
    },
    {
      "bufferView": 1,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 1.0],
      "max": [0.0, 0.0, 1.0]
    },
    {
      "bufferView": 2,
      "componentType": 5121,
      "count": 3,
      "type": "VEC4"
    },
    {
      "bufferView": 3,
      "componentType": 5126,
      "count": 3,
      "type": "VEC4"
    },
    {
      "bufferView": 4,
      "componentType": 5126,
      "count": 1,
      "type": "MAT4"
    },
    {
      "bufferView": 5,
      "componentType": 5123,
      "count": 3,
      "type": "SCALAR"
    }
  ]
}"#,
        )
        .expect("write gltf");

        (dir, gltf_path)
    }

    fn write_animated_trs_test_gltf(name: &str) -> (PathBuf, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "robotdreams-renderer-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let bin_path = dir.join("mesh.bin");
        let gltf_path = dir.join("mesh.gltf");

        let mut bin = Vec::new();
        for value in [0.0_f32, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 0.0, 0.0, 0.0, 0.0, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [1.0_f32, 1.0, 1.0, 2.0, 2.0, 2.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0_u16, 1, 2] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        fs::write(&bin_path, bin).expect("write bin");
        fs::write(
            &gltf_path,
            r#"{
  "asset": { "version": "2.0" },
  "scene": 0,
  "scenes": [{ "nodes": [0] }],
  "nodes": [{ "mesh": 0 }],
  "meshes": [{
    "primitives": [{
      "attributes": { "POSITION": 0, "NORMAL": 1 },
      "indices": 5,
      "material": 0
    }]
  }],
  "materials": [{
    "pbrMetallicRoughness": {
      "baseColorFactor": [0.1, 0.5, 0.9, 1.0]
    }
  }],
  "animations": [{
    "samplers": [
      { "input": 2, "output": 3, "interpolation": "LINEAR" },
      { "input": 2, "output": 4, "interpolation": "LINEAR" }
    ],
    "channels": [
      { "sampler": 0, "target": { "node": 0, "path": "translation" } },
      { "sampler": 1, "target": { "node": 0, "path": "scale" } }
    ]
  }],
  "buffers": [{ "uri": "mesh.bin", "byteLength": 134 }],
  "bufferViews": [
    { "buffer": 0, "byteOffset": 0, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 36, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 72, "byteLength": 8 },
    { "buffer": 0, "byteOffset": 80, "byteLength": 24 },
    { "buffer": 0, "byteOffset": 104, "byteLength": 24 },
    { "buffer": 0, "byteOffset": 128, "byteLength": 6, "target": 34963 }
  ],
  "accessors": [
    {
      "bufferView": 0,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 0.0],
      "max": [1.0, 1.0, 0.0]
    },
    {
      "bufferView": 1,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 1.0],
      "max": [0.0, 0.0, 1.0]
    },
    {
      "bufferView": 2,
      "componentType": 5126,
      "count": 2,
      "type": "SCALAR",
      "min": [0.0],
      "max": [1.0]
    },
    {
      "bufferView": 3,
      "componentType": 5126,
      "count": 2,
      "type": "VEC3"
    },
    {
      "bufferView": 4,
      "componentType": 5126,
      "count": 2,
      "type": "VEC3"
    },
    {
      "bufferView": 5,
      "componentType": 5123,
      "count": 3,
      "type": "SCALAR"
    }
  ]
}"#,
        )
        .expect("write gltf");

        (dir, gltf_path)
    }

    fn write_triangle_strip_test_gltf(name: &str) -> (PathBuf, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "robotdreams-renderer-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let bin_path = dir.join("mesh.bin");
        let gltf_path = dir.join("mesh.gltf");

        let mut bin = Vec::new();
        for value in [
            0.0_f32, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 1.0, 0.0,
        ] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [
            0.0_f32, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0,
        ] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0_u16, 1, 2, 3] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        fs::write(&bin_path, bin).expect("write bin");
        fs::write(
            &gltf_path,
            r#"{
  "asset": { "version": "2.0" },
  "scene": 0,
  "scenes": [{ "nodes": [0] }],
  "nodes": [{ "mesh": 0 }],
  "meshes": [{
    "primitives": [{
      "mode": 5,
      "attributes": { "POSITION": 0, "NORMAL": 1 },
      "indices": 2,
      "material": 0
    }]
  }],
  "materials": [{
    "pbrMetallicRoughness": {
      "baseColorFactor": [0.2, 0.4, 0.6, 1.0]
    }
  }],
  "buffers": [{ "uri": "mesh.bin", "byteLength": 104 }],
  "bufferViews": [
    { "buffer": 0, "byteOffset": 0, "byteLength": 48, "target": 34962 },
    { "buffer": 0, "byteOffset": 48, "byteLength": 48, "target": 34962 },
    { "buffer": 0, "byteOffset": 96, "byteLength": 8, "target": 34963 }
  ],
  "accessors": [
    {
      "bufferView": 0,
      "componentType": 5126,
      "count": 4,
      "type": "VEC3",
      "min": [0.0, 0.0, 0.0],
      "max": [1.0, 1.0, 0.0]
    },
    {
      "bufferView": 1,
      "componentType": 5126,
      "count": 4,
      "type": "VEC3",
      "min": [0.0, 0.0, 1.0],
      "max": [0.0, 0.0, 1.0]
    },
    {
      "bufferView": 2,
      "componentType": 5123,
      "count": 4,
      "type": "SCALAR"
    }
  ]
}"#,
        )
        .expect("write gltf");

        (dir, gltf_path)
    }

    fn write_textured_test_gltf(name: &str) -> (PathBuf, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "robotdreams-renderer-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let bin_path = dir.join("mesh.bin");
        let texture_path = dir.join("texture.png");
        let gltf_path = dir.join("mesh.gltf");

        let mut bin = Vec::new();
        for value in [0.0_f32, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.25_f32, 0.25, 0.25, 0.25, 0.25, 0.25] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0_u16, 1, 2] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        fs::write(&bin_path, bin).expect("write bin");
        fs::write(
            &texture_path,
            encode_png_rgb(
                2,
                2,
                &[
                    255, 0, 0, 0, 255, 0, //
                    0, 0, 255, 255, 255, 255,
                ],
            )
            .expect("encode texture"),
        )
        .expect("write texture");
        fs::write(
            &gltf_path,
            r#"{
  "asset": { "version": "2.0" },
  "scene": 0,
  "scenes": [{ "nodes": [0] }],
  "nodes": [{ "mesh": 0 }],
  "meshes": [{
    "primitives": [{
      "attributes": { "POSITION": 0, "NORMAL": 1, "TEXCOORD_0": 2 },
      "indices": 3,
      "material": 0
    }]
  }],
  "materials": [{
    "pbrMetallicRoughness": {
      "baseColorFactor": [1.0, 1.0, 1.0, 1.0],
      "baseColorTexture": { "index": 0 }
    }
  }],
  "samplers": [{ "magFilter": 9728, "minFilter": 9728 }],
  "textures": [{ "sampler": 0, "source": 0 }],
  "images": [{ "uri": "texture.png" }],
  "buffers": [{ "uri": "mesh.bin", "byteLength": 102 }],
  "bufferViews": [
    { "buffer": 0, "byteOffset": 0, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 36, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 72, "byteLength": 24, "target": 34962 },
    { "buffer": 0, "byteOffset": 96, "byteLength": 6, "target": 34963 }
  ],
  "accessors": [
    {
      "bufferView": 0,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 0.0],
      "max": [1.0, 1.0, 0.0]
    },
    {
      "bufferView": 1,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 1.0],
      "max": [0.0, 0.0, 1.0]
    },
    {
      "bufferView": 2,
      "componentType": 5126,
      "count": 3,
      "type": "VEC2"
    },
    {
      "bufferView": 3,
      "componentType": 5123,
      "count": 3,
      "type": "SCALAR"
    }
  ]
}"#,
        )
        .expect("write gltf");

        (dir, gltf_path)
    }

    fn write_texture_transform_test_gltf(name: &str) -> (PathBuf, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "robotdreams-renderer-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let bin_path = dir.join("mesh.bin");
        let texture_path = dir.join("texture.png");
        let gltf_path = dir.join("mesh.gltf");

        let mut bin = Vec::new();
        for value in [0.0_f32, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.25_f32, 0.25, 0.25, 0.25, 0.25, 0.25] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0_u16, 1, 2] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        fs::write(&bin_path, bin).expect("write bin");
        fs::write(
            &texture_path,
            encode_png_rgb(
                2,
                2,
                &[
                    255, 0, 0, 0, 255, 0, //
                    0, 0, 255, 255, 255, 255,
                ],
            )
            .expect("encode texture"),
        )
        .expect("write texture");
        fs::write(
            &gltf_path,
            r#"{
  "asset": { "version": "2.0" },
  "extensionsUsed": ["KHR_texture_transform"],
  "scene": 0,
  "scenes": [{ "nodes": [0] }],
  "nodes": [{ "mesh": 0 }],
  "meshes": [{
    "primitives": [{
      "attributes": { "POSITION": 0, "NORMAL": 1, "TEXCOORD_0": 2 },
      "indices": 3,
      "material": 0
    }]
  }],
  "materials": [{
    "pbrMetallicRoughness": {
      "baseColorFactor": [1.0, 1.0, 1.0, 1.0],
      "baseColorTexture": {
        "index": 0,
        "extensions": {
          "KHR_texture_transform": {
            "offset": [0.5, 0.0]
          }
        }
      }
    }
  }],
  "samplers": [{ "magFilter": 9728, "minFilter": 9728 }],
  "textures": [{ "sampler": 0, "source": 0 }],
  "images": [{ "uri": "texture.png" }],
  "buffers": [{ "uri": "mesh.bin", "byteLength": 102 }],
  "bufferViews": [
    { "buffer": 0, "byteOffset": 0, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 36, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 72, "byteLength": 24, "target": 34962 },
    { "buffer": 0, "byteOffset": 96, "byteLength": 6, "target": 34963 }
  ],
  "accessors": [
    {
      "bufferView": 0,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 0.0],
      "max": [1.0, 1.0, 0.0]
    },
    {
      "bufferView": 1,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 1.0],
      "max": [0.0, 0.0, 1.0]
    },
    {
      "bufferView": 2,
      "componentType": 5126,
      "count": 3,
      "type": "VEC2"
    },
    {
      "bufferView": 3,
      "componentType": 5123,
      "count": 3,
      "type": "SCALAR"
    }
  ]
}"#,
        )
        .expect("write gltf");

        (dir, gltf_path)
    }

    fn write_occlusion_test_gltf(name: &str) -> (PathBuf, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "robotdreams-renderer-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let bin_path = dir.join("mesh.bin");
        let texture_path = dir.join("occlusion.png");
        let gltf_path = dir.join("mesh.gltf");

        let mut bin = Vec::new();
        for value in [0.0_f32, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.25_f32, 0.25, 0.25, 0.25, 0.25, 0.25] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0_u16, 1, 2] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        fs::write(&bin_path, bin).expect("write bin");
        fs::write(
            &texture_path,
            encode_png_rgb(1, 1, &[64, 255, 255]).expect("encode occlusion texture"),
        )
        .expect("write occlusion texture");
        fs::write(
            &gltf_path,
            r#"{
  "asset": { "version": "2.0" },
  "scene": 0,
  "scenes": [{ "nodes": [0] }],
  "nodes": [{ "mesh": 0 }],
  "meshes": [{
    "primitives": [{
      "attributes": { "POSITION": 0, "NORMAL": 1, "TEXCOORD_0": 2 },
      "indices": 3,
      "material": 0
    }]
  }],
  "materials": [{
    "occlusionTexture": { "index": 0, "strength": 0.5 },
    "pbrMetallicRoughness": {
      "baseColorFactor": [1.0, 1.0, 1.0, 1.0]
    }
  }],
  "samplers": [{ "magFilter": 9728, "minFilter": 9728 }],
  "textures": [{ "sampler": 0, "source": 0 }],
  "images": [{ "uri": "occlusion.png" }],
  "buffers": [{ "uri": "mesh.bin", "byteLength": 102 }],
  "bufferViews": [
    { "buffer": 0, "byteOffset": 0, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 36, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 72, "byteLength": 24, "target": 34962 },
    { "buffer": 0, "byteOffset": 96, "byteLength": 6, "target": 34963 }
  ],
  "accessors": [
    {
      "bufferView": 0,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 0.0],
      "max": [1.0, 1.0, 0.0]
    },
    {
      "bufferView": 1,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 1.0],
      "max": [0.0, 0.0, 1.0]
    },
    {
      "bufferView": 2,
      "componentType": 5126,
      "count": 3,
      "type": "VEC2"
    },
    {
      "bufferView": 3,
      "componentType": 5123,
      "count": 3,
      "type": "SCALAR"
    }
  ]
}"#,
        )
        .expect("write gltf");

        (dir, gltf_path)
    }

    fn write_metallic_roughness_test_gltf(name: &str) -> (PathBuf, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "robotdreams-renderer-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let bin_path = dir.join("mesh.bin");
        let texture_path = dir.join("metallic-roughness.png");
        let gltf_path = dir.join("mesh.gltf");

        let mut bin = Vec::new();
        for value in [0.0_f32, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.25_f32, 0.25, 0.25, 0.25, 0.25, 0.25] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0_u16, 1, 2] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        fs::write(&bin_path, bin).expect("write bin");
        fs::write(
            &texture_path,
            encode_png_rgb(1, 1, &[0, 128, 64]).expect("encode metallic roughness texture"),
        )
        .expect("write metallic roughness texture");
        fs::write(
            &gltf_path,
            r#"{
  "asset": { "version": "2.0" },
  "scene": 0,
  "scenes": [{ "nodes": [0] }],
  "nodes": [{ "mesh": 0 }],
  "meshes": [{
    "primitives": [{
      "attributes": { "POSITION": 0, "NORMAL": 1, "TEXCOORD_0": 2 },
      "indices": 3,
      "material": 0
    }]
  }],
  "materials": [{
    "pbrMetallicRoughness": {
      "baseColorFactor": [0.8, 0.4, 0.2, 1.0],
      "metallicFactor": 0.5,
      "roughnessFactor": 0.5,
      "metallicRoughnessTexture": { "index": 0 }
    }
  }],
  "samplers": [{ "magFilter": 9728, "minFilter": 9728 }],
  "textures": [{ "sampler": 0, "source": 0 }],
  "images": [{ "uri": "metallic-roughness.png" }],
  "buffers": [{ "uri": "mesh.bin", "byteLength": 102 }],
  "bufferViews": [
    { "buffer": 0, "byteOffset": 0, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 36, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 72, "byteLength": 24, "target": 34962 },
    { "buffer": 0, "byteOffset": 96, "byteLength": 6, "target": 34963 }
  ],
  "accessors": [
    {
      "bufferView": 0,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 0.0],
      "max": [1.0, 1.0, 0.0]
    },
    {
      "bufferView": 1,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 1.0],
      "max": [0.0, 0.0, 1.0]
    },
    {
      "bufferView": 2,
      "componentType": 5126,
      "count": 3,
      "type": "VEC2"
    },
    {
      "bufferView": 3,
      "componentType": 5123,
      "count": 3,
      "type": "SCALAR"
    }
  ]
}"#,
        )
        .expect("write gltf");

        (dir, gltf_path)
    }

    fn write_specular_test_gltf(name: &str) -> (PathBuf, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "robotdreams-renderer-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let bin_path = dir.join("mesh.bin");
        let specular_path = dir.join("specular.png");
        let specular_color_path = dir.join("specular-color.png");
        let gltf_path = dir.join("mesh.gltf");

        let mut bin = Vec::new();
        for value in [0.0_f32, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.25_f32, 0.25, 0.25, 0.25, 0.25, 0.25] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0_u16, 1, 2] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        fs::write(&bin_path, bin).expect("write bin");
        fs::write(
            &specular_path,
            encode_png_rgba(1, 1, &[0, 0, 0, 128]).expect("encode specular texture"),
        )
        .expect("write specular texture");
        fs::write(
            &specular_color_path,
            encode_png_rgb(1, 1, &[128, 64, 255]).expect("encode specular color texture"),
        )
        .expect("write specular color texture");
        fs::write(
            &gltf_path,
            r#"{
  "asset": { "version": "2.0" },
  "extensionsUsed": ["KHR_materials_specular"],
  "scene": 0,
  "scenes": [{ "nodes": [0] }],
  "nodes": [{ "mesh": 0 }],
  "meshes": [{
    "primitives": [{
      "attributes": { "POSITION": 0, "NORMAL": 1, "TEXCOORD_0": 2 },
      "indices": 3,
      "material": 0
    }]
  }],
  "materials": [{
    "pbrMetallicRoughness": {
      "baseColorFactor": [0.8, 0.8, 0.8, 1.0],
      "metallicFactor": 0.0,
      "roughnessFactor": 0.2
    },
    "extensions": {
      "KHR_materials_specular": {
        "specularFactor": 0.5,
        "specularTexture": { "index": 0 },
        "specularColorFactor": [0.5, 1.0, 0.25],
        "specularColorTexture": { "index": 1 }
      }
    }
  }],
  "textures": [{ "source": 0 }, { "source": 1 }],
  "images": [{ "uri": "specular.png" }, { "uri": "specular-color.png" }],
  "buffers": [{ "uri": "mesh.bin", "byteLength": 102 }],
  "bufferViews": [
    { "buffer": 0, "byteOffset": 0, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 36, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 72, "byteLength": 24, "target": 34962 },
    { "buffer": 0, "byteOffset": 96, "byteLength": 6, "target": 34963 }
  ],
  "accessors": [
    {
      "bufferView": 0,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 0.0],
      "max": [1.0, 1.0, 0.0]
    },
    {
      "bufferView": 1,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 1.0],
      "max": [0.0, 0.0, 1.0]
    },
    {
      "bufferView": 2,
      "componentType": 5126,
      "count": 3,
      "type": "VEC2"
    },
    {
      "bufferView": 3,
      "componentType": 5123,
      "count": 3,
      "type": "SCALAR"
    }
  ]
}"#,
        )
        .expect("write gltf");

        (dir, gltf_path)
    }

    fn write_specular_glossiness_test_gltf(name: &str) -> (PathBuf, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "robotdreams-renderer-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let bin_path = dir.join("mesh.bin");
        let diffuse_path = dir.join("diffuse.png");
        let specular_glossiness_path = dir.join("specular-glossiness.png");
        let gltf_path = dir.join("mesh.gltf");

        let mut bin = Vec::new();
        for value in [0.0_f32, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.25_f32, 0.25, 0.25, 0.25, 0.25, 0.25] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0_u16, 1, 2] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        fs::write(&bin_path, bin).expect("write bin");
        fs::write(
            &diffuse_path,
            encode_png_rgba(1, 1, &[128, 64, 255, 200]).expect("encode diffuse texture"),
        )
        .expect("write diffuse texture");
        fs::write(
            &specular_glossiness_path,
            encode_png_rgba(1, 1, &[64, 128, 255, 128])
                .expect("encode specular glossiness texture"),
        )
        .expect("write specular glossiness texture");
        fs::write(
            &gltf_path,
            r#"{
  "asset": { "version": "2.0" },
  "extensionsUsed": ["KHR_materials_pbrSpecularGlossiness"],
  "scene": 0,
  "scenes": [{ "nodes": [0] }],
  "nodes": [{ "mesh": 0 }],
  "meshes": [{
    "primitives": [{
      "attributes": { "POSITION": 0, "NORMAL": 1, "TEXCOORD_0": 2 },
      "indices": 3,
      "material": 0
    }]
  }],
  "materials": [{
    "alphaMode": "BLEND",
    "extensions": {
      "KHR_materials_pbrSpecularGlossiness": {
        "diffuseFactor": [0.5, 0.25, 1.0, 0.8],
        "diffuseTexture": { "index": 0 },
        "specularFactor": [0.8, 0.4, 0.2],
        "glossinessFactor": 0.75,
        "specularGlossinessTexture": { "index": 1 }
      }
    }
  }],
  "textures": [{ "source": 0 }, { "source": 1 }],
  "images": [{ "uri": "diffuse.png" }, { "uri": "specular-glossiness.png" }],
  "buffers": [{ "uri": "mesh.bin", "byteLength": 102 }],
  "bufferViews": [
    { "buffer": 0, "byteOffset": 0, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 36, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 72, "byteLength": 24, "target": 34962 },
    { "buffer": 0, "byteOffset": 96, "byteLength": 6, "target": 34963 }
  ],
  "accessors": [
    {
      "bufferView": 0,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 0.0],
      "max": [1.0, 1.0, 0.0]
    },
    {
      "bufferView": 1,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 1.0],
      "max": [0.0, 0.0, 1.0]
    },
    {
      "bufferView": 2,
      "componentType": 5126,
      "count": 3,
      "type": "VEC2"
    },
    {
      "bufferView": 3,
      "componentType": 5123,
      "count": 3,
      "type": "SCALAR"
    }
  ]
}"#,
        )
        .expect("write gltf");

        (dir, gltf_path)
    }

    fn write_unlit_test_gltf(name: &str) -> (PathBuf, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "robotdreams-renderer-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let bin_path = dir.join("mesh.bin");
        let gltf_path = dir.join("mesh.gltf");

        let mut bin = Vec::new();
        for value in [0.0_f32, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0_u16, 1, 2] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        fs::write(&bin_path, bin).expect("write bin");
        fs::write(
            &gltf_path,
            r#"{
  "asset": { "version": "2.0" },
  "extensionsUsed": ["KHR_materials_unlit"],
  "scene": 0,
  "scenes": [{ "nodes": [0] }],
  "nodes": [{ "mesh": 0 }],
  "meshes": [{
    "primitives": [{
      "attributes": { "POSITION": 0, "NORMAL": 1 },
      "indices": 2,
      "material": 0
    }]
  }],
  "materials": [{
    "pbrMetallicRoughness": {
      "baseColorFactor": [0.25, 0.5, 0.75, 1.0]
    },
    "extensions": { "KHR_materials_unlit": {} }
  }],
  "buffers": [{ "uri": "mesh.bin", "byteLength": 78 }],
  "bufferViews": [
    { "buffer": 0, "byteOffset": 0, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 36, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 72, "byteLength": 6, "target": 34963 }
  ],
  "accessors": [
    {
      "bufferView": 0,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 0.0],
      "max": [1.0, 1.0, 0.0]
    },
    {
      "bufferView": 1,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 1.0],
      "max": [0.0, 0.0, 1.0]
    },
    {
      "bufferView": 2,
      "componentType": 5123,
      "count": 3,
      "type": "SCALAR"
    }
  ]
}"#,
        )
        .expect("write gltf");

        (dir, gltf_path)
    }

    fn write_ior_test_gltf(name: &str) -> (PathBuf, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "robotdreams-renderer-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let bin_path = dir.join("mesh.bin");
        let gltf_path = dir.join("mesh.gltf");

        let mut bin = Vec::new();
        for value in [0.0_f32, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0_u16, 1, 2] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        fs::write(&bin_path, bin).expect("write bin");
        fs::write(
            &gltf_path,
            r#"{
  "asset": { "version": "2.0" },
  "extensionsUsed": ["KHR_materials_ior"],
  "scene": 0,
  "scenes": [{ "nodes": [0] }],
  "nodes": [{ "mesh": 0 }],
  "meshes": [{
    "primitives": [{
      "attributes": { "POSITION": 0, "NORMAL": 1 },
      "indices": 2,
      "material": 0
    }]
  }],
  "materials": [{
    "pbrMetallicRoughness": {
      "baseColorFactor": [0.0, 0.0, 0.0, 1.0],
      "metallicFactor": 0.0,
      "roughnessFactor": 0.25
    },
    "extensions": { "KHR_materials_ior": { "ior": 2.0 } }
  }],
  "buffers": [{ "uri": "mesh.bin", "byteLength": 78 }],
  "bufferViews": [
    { "buffer": 0, "byteOffset": 0, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 36, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 72, "byteLength": 6, "target": 34963 }
  ],
  "accessors": [
    {
      "bufferView": 0,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 0.0],
      "max": [1.0, 1.0, 0.0]
    },
    {
      "bufferView": 1,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 1.0],
      "max": [0.0, 0.0, 1.0]
    },
    {
      "bufferView": 2,
      "componentType": 5123,
      "count": 3,
      "type": "SCALAR"
    }
  ]
}"#,
        )
        .expect("write gltf");

        (dir, gltf_path)
    }

    fn write_dispersion_test_gltf(name: &str) -> (PathBuf, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "robotdreams-renderer-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let bin_path = dir.join("mesh.bin");
        let gltf_path = dir.join("mesh.gltf");

        let mut bin = Vec::new();
        for value in [0.0_f32, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0_u16, 1, 2] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        fs::write(&bin_path, bin).expect("write bin");
        fs::write(
            &gltf_path,
            r#"{
  "asset": { "version": "2.0" },
  "extensionsUsed": [
    "KHR_materials_transmission",
    "KHR_materials_volume",
    "KHR_materials_ior",
    "KHR_materials_dispersion"
  ],
  "scene": 0,
  "scenes": [{ "nodes": [0] }],
  "nodes": [{ "mesh": 0 }],
  "meshes": [{
    "primitives": [{
      "attributes": { "POSITION": 0, "NORMAL": 1 },
      "indices": 2,
      "material": 0
    }]
  }],
  "materials": [{
    "pbrMetallicRoughness": {
      "baseColorFactor": [1.0, 1.0, 1.0, 1.0],
      "metallicFactor": 0.0,
      "roughnessFactor": 0.0
    },
    "extensions": {
      "KHR_materials_transmission": { "transmissionFactor": 1.0 },
      "KHR_materials_volume": {
        "thicknessFactor": 0.25,
        "attenuationDistance": 1.0
      },
      "KHR_materials_ior": { "ior": 1.75 },
      "KHR_materials_dispersion": { "dispersion": 1.2 }
    }
  }],
  "buffers": [{ "uri": "mesh.bin", "byteLength": 78 }],
  "bufferViews": [
    { "buffer": 0, "byteOffset": 0, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 36, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 72, "byteLength": 6, "target": 34963 }
  ],
  "accessors": [
    {
      "bufferView": 0,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 0.0],
      "max": [1.0, 1.0, 0.0]
    },
    {
      "bufferView": 1,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 1.0],
      "max": [0.0, 0.0, 1.0]
    },
    {
      "bufferView": 2,
      "componentType": 5123,
      "count": 3,
      "type": "SCALAR"
    }
  ]
}"#,
        )
        .expect("write gltf");

        (dir, gltf_path)
    }

    fn write_transmission_test_gltf(name: &str) -> (PathBuf, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "robotdreams-renderer-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let bin_path = dir.join("mesh.bin");
        let transmission_path = dir.join("transmission.png");
        let gltf_path = dir.join("mesh.gltf");

        let mut bin = Vec::new();
        for value in [0.0_f32, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.25_f32, 0.25, 0.25, 0.25, 0.25, 0.25] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0_u16, 1, 2] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        fs::write(&bin_path, bin).expect("write bin");
        fs::write(
            &transmission_path,
            encode_png_rgb(1, 1, &[128, 0, 0]).expect("encode transmission texture"),
        )
        .expect("write transmission texture");
        fs::write(
            &gltf_path,
            r#"{
  "asset": { "version": "2.0" },
  "extensionsUsed": ["KHR_materials_transmission"],
  "scene": 0,
  "scenes": [{ "nodes": [0] }],
  "nodes": [{ "mesh": 0 }],
  "meshes": [{
    "primitives": [{
      "attributes": { "POSITION": 0, "NORMAL": 1, "TEXCOORD_0": 2 },
      "indices": 3,
      "material": 0
    }]
  }],
  "materials": [{
    "pbrMetallicRoughness": {
      "baseColorFactor": [1.0, 1.0, 1.0, 1.0],
      "metallicFactor": 0.0,
      "roughnessFactor": 0.1
    },
    "extensions": {
      "KHR_materials_transmission": {
        "transmissionFactor": 0.5,
        "transmissionTexture": { "index": 0 }
      }
    }
  }],
  "textures": [{ "source": 0 }],
  "images": [{ "uri": "transmission.png" }],
  "buffers": [{ "uri": "mesh.bin", "byteLength": 102 }],
  "bufferViews": [
    { "buffer": 0, "byteOffset": 0, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 36, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 72, "byteLength": 24, "target": 34962 },
    { "buffer": 0, "byteOffset": 96, "byteLength": 6, "target": 34963 }
  ],
  "accessors": [
    {
      "bufferView": 0,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 0.0],
      "max": [1.0, 1.0, 0.0]
    },
    {
      "bufferView": 1,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 1.0],
      "max": [0.0, 0.0, 1.0]
    },
    {
      "bufferView": 2,
      "componentType": 5126,
      "count": 3,
      "type": "VEC2"
    },
    {
      "bufferView": 3,
      "componentType": 5123,
      "count": 3,
      "type": "SCALAR"
    }
  ]
}"#,
        )
        .expect("write gltf");

        (dir, gltf_path)
    }

    fn write_diffuse_transmission_test_gltf(name: &str) -> (PathBuf, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "robotdreams-renderer-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let bin_path = dir.join("mesh.bin");
        let factor_path = dir.join("diffuse-transmission.png");
        let color_path = dir.join("diffuse-transmission-color.png");
        let gltf_path = dir.join("mesh.gltf");

        let mut bin = Vec::new();
        for value in [0.0_f32, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.25_f32, 0.25, 0.25, 0.25, 0.25, 0.25] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0_u16, 1, 2] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        fs::write(&bin_path, bin).expect("write bin");
        fs::write(
            &factor_path,
            encode_png_rgba(1, 1, &[0, 0, 0, 128]).expect("encode diffuse transmission texture"),
        )
        .expect("write diffuse transmission texture");
        fs::write(
            &color_path,
            encode_png_rgb(1, 1, &[128, 255, 64])
                .expect("encode diffuse transmission color texture"),
        )
        .expect("write diffuse transmission color texture");
        fs::write(
            &gltf_path,
            r#"{
  "asset": { "version": "2.0" },
  "extensionsUsed": ["KHR_materials_diffuse_transmission"],
  "scene": 0,
  "scenes": [{ "nodes": [0] }],
  "nodes": [{ "mesh": 0 }],
  "meshes": [{
    "primitives": [{
      "attributes": { "POSITION": 0, "NORMAL": 1, "TEXCOORD_0": 2 },
      "indices": 3,
      "material": 0
    }]
  }],
  "materials": [{
    "pbrMetallicRoughness": {
      "baseColorFactor": [1.0, 1.0, 1.0, 1.0],
      "metallicFactor": 0.0,
      "roughnessFactor": 0.8
    },
    "extensions": {
      "KHR_materials_diffuse_transmission": {
        "diffuseTransmissionFactor": 0.5,
        "diffuseTransmissionTexture": { "index": 0 },
        "diffuseTransmissionColorFactor": [1.0, 0.5, 0.25],
        "diffuseTransmissionColorTexture": { "index": 1 }
      }
    }
  }],
  "textures": [{ "source": 0 }, { "source": 1 }],
  "images": [{ "uri": "diffuse-transmission.png" }, { "uri": "diffuse-transmission-color.png" }],
  "buffers": [{ "uri": "mesh.bin", "byteLength": 102 }],
  "bufferViews": [
    { "buffer": 0, "byteOffset": 0, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 36, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 72, "byteLength": 24, "target": 34962 },
    { "buffer": 0, "byteOffset": 96, "byteLength": 6, "target": 34963 }
  ],
  "accessors": [
    {
      "bufferView": 0,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 0.0],
      "max": [1.0, 1.0, 0.0]
    },
    {
      "bufferView": 1,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 1.0],
      "max": [0.0, 0.0, 1.0]
    },
    {
      "bufferView": 2,
      "componentType": 5126,
      "count": 3,
      "type": "VEC2"
    },
    {
      "bufferView": 3,
      "componentType": 5123,
      "count": 3,
      "type": "SCALAR"
    }
  ]
}"#,
        )
        .expect("write gltf");

        (dir, gltf_path)
    }

    fn write_volume_test_gltf(name: &str) -> (PathBuf, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "robotdreams-renderer-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let bin_path = dir.join("mesh.bin");
        let thickness_path = dir.join("thickness.png");
        let gltf_path = dir.join("mesh.gltf");

        let mut bin = Vec::new();
        for value in [0.0_f32, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.25_f32, 0.25, 0.25, 0.25, 0.25, 0.25] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0_u16, 1, 2] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        fs::write(&bin_path, bin).expect("write bin");
        fs::write(
            &thickness_path,
            encode_png_rgb(1, 1, &[0, 128, 0]).expect("encode thickness texture"),
        )
        .expect("write thickness texture");
        fs::write(
            &gltf_path,
            r#"{
  "asset": { "version": "2.0" },
  "extensionsUsed": ["KHR_materials_transmission", "KHR_materials_volume"],
  "scene": 0,
  "scenes": [{ "nodes": [0] }],
  "nodes": [{ "mesh": 0 }],
  "meshes": [{
    "primitives": [{
      "attributes": { "POSITION": 0, "NORMAL": 1, "TEXCOORD_0": 2 },
      "indices": 3,
      "material": 0
    }]
  }],
  "materials": [{
    "pbrMetallicRoughness": {
      "baseColorFactor": [1.0, 1.0, 1.0, 1.0],
      "metallicFactor": 0.0,
      "roughnessFactor": 0.1
    },
    "extensions": {
      "KHR_materials_transmission": {
        "transmissionFactor": 1.0
      },
      "KHR_materials_volume": {
        "thicknessFactor": 1.0,
        "thicknessTexture": { "index": 0 },
        "attenuationDistance": 0.5,
        "attenuationColor": [1.0, 0.25, 0.25]
      }
    }
  }],
  "textures": [{ "source": 0 }],
  "images": [{ "uri": "thickness.png" }],
  "buffers": [{ "uri": "mesh.bin", "byteLength": 102 }],
  "bufferViews": [
    { "buffer": 0, "byteOffset": 0, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 36, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 72, "byteLength": 24, "target": 34962 },
    { "buffer": 0, "byteOffset": 96, "byteLength": 6, "target": 34963 }
  ],
  "accessors": [
    {
      "bufferView": 0,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 0.0],
      "max": [1.0, 1.0, 0.0]
    },
    {
      "bufferView": 1,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 1.0],
      "max": [0.0, 0.0, 1.0]
    },
    {
      "bufferView": 2,
      "componentType": 5126,
      "count": 3,
      "type": "VEC2"
    },
    {
      "bufferView": 3,
      "componentType": 5123,
      "count": 3,
      "type": "SCALAR"
    }
  ]
}"#,
        )
        .expect("write gltf");

        (dir, gltf_path)
    }

    fn write_clearcoat_test_gltf(name: &str) -> (PathBuf, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "robotdreams-renderer-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let bin_path = dir.join("mesh.bin");
        let clearcoat_path = dir.join("clearcoat.png");
        let roughness_path = dir.join("clearcoat-roughness.png");
        let normal_path = dir.join("clearcoat-normal.png");
        let gltf_path = dir.join("mesh.gltf");

        let mut bin = Vec::new();
        for value in [0.0_f32, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.25_f32, 0.25, 0.25, 0.25, 0.25, 0.25] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [
            1.0_f32, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 1.0,
        ] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0_u16, 1, 2] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        fs::write(&bin_path, bin).expect("write bin");
        fs::write(
            &clearcoat_path,
            encode_png_rgb(1, 1, &[128, 0, 0]).expect("encode clearcoat texture"),
        )
        .expect("write clearcoat texture");
        fs::write(
            &roughness_path,
            encode_png_rgb(1, 1, &[0, 64, 0]).expect("encode clearcoat roughness texture"),
        )
        .expect("write clearcoat roughness texture");
        fs::write(
            &normal_path,
            encode_png_rgb(1, 1, &[255, 128, 255]).expect("encode clearcoat normal texture"),
        )
        .expect("write clearcoat normal texture");
        fs::write(
            &gltf_path,
            r#"{
  "asset": { "version": "2.0" },
  "extensionsUsed": ["KHR_materials_clearcoat"],
  "scene": 0,
  "scenes": [{ "nodes": [0] }],
  "nodes": [{ "mesh": 0 }],
  "meshes": [{
    "primitives": [{
      "attributes": { "POSITION": 0, "NORMAL": 1, "TEXCOORD_0": 2, "TANGENT": 3 },
      "indices": 4,
      "material": 0
    }]
  }],
  "materials": [{
    "pbrMetallicRoughness": {
      "baseColorFactor": [0.8, 0.8, 0.8, 1.0],
      "metallicFactor": 0.0,
      "roughnessFactor": 0.4
    },
    "extensions": {
      "KHR_materials_clearcoat": {
        "clearcoatFactor": 0.5,
        "clearcoatTexture": { "index": 0 },
        "clearcoatRoughnessFactor": 0.5,
        "clearcoatRoughnessTexture": { "index": 1 },
        "clearcoatNormalTexture": { "index": 2, "scale": 0.5 }
      }
    }
  }],
  "textures": [{ "source": 0 }, { "source": 1 }, { "source": 2 }],
  "images": [
    { "uri": "clearcoat.png" },
    { "uri": "clearcoat-roughness.png" },
    { "uri": "clearcoat-normal.png" }
  ],
  "buffers": [{ "uri": "mesh.bin", "byteLength": 150 }],
  "bufferViews": [
    { "buffer": 0, "byteOffset": 0, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 36, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 72, "byteLength": 24, "target": 34962 },
    { "buffer": 0, "byteOffset": 96, "byteLength": 48, "target": 34962 },
    { "buffer": 0, "byteOffset": 144, "byteLength": 6, "target": 34963 }
  ],
  "accessors": [
    {
      "bufferView": 0,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 0.0],
      "max": [1.0, 1.0, 0.0]
    },
    {
      "bufferView": 1,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 1.0],
      "max": [0.0, 0.0, 1.0]
    },
    {
      "bufferView": 2,
      "componentType": 5126,
      "count": 3,
      "type": "VEC2"
    },
    {
      "bufferView": 3,
      "componentType": 5126,
      "count": 3,
      "type": "VEC4"
    },
    {
      "bufferView": 4,
      "componentType": 5123,
      "count": 3,
      "type": "SCALAR"
    }
  ]
}"#,
        )
        .expect("write gltf");

        (dir, gltf_path)
    }

    fn write_sheen_test_gltf(name: &str) -> (PathBuf, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "robotdreams-renderer-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let bin_path = dir.join("mesh.bin");
        let sheen_color_path = dir.join("sheen-color.png");
        let sheen_roughness_path = dir.join("sheen-roughness.png");
        let gltf_path = dir.join("mesh.gltf");

        let mut bin = Vec::new();
        for value in [0.0_f32, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.25_f32, 0.25, 0.25, 0.25, 0.25, 0.25] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0_u16, 1, 2] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        fs::write(&bin_path, bin).expect("write bin");
        fs::write(
            &sheen_color_path,
            encode_png_rgb(1, 1, &[128, 64, 255]).expect("encode sheen color texture"),
        )
        .expect("write sheen color texture");
        fs::write(
            &sheen_roughness_path,
            encode_png_rgba(1, 1, &[0, 0, 0, 128]).expect("encode sheen roughness texture"),
        )
        .expect("write sheen roughness texture");
        fs::write(
            &gltf_path,
            r#"{
  "asset": { "version": "2.0" },
  "extensionsUsed": ["KHR_materials_sheen"],
  "scene": 0,
  "scenes": [{ "nodes": [0] }],
  "nodes": [{ "mesh": 0 }],
  "meshes": [{
    "primitives": [{
      "attributes": { "POSITION": 0, "NORMAL": 1, "TEXCOORD_0": 2 },
      "indices": 3,
      "material": 0
    }]
  }],
  "materials": [{
    "pbrMetallicRoughness": {
      "baseColorFactor": [0.2, 0.2, 0.2, 1.0],
      "metallicFactor": 0.0,
      "roughnessFactor": 0.8
    },
    "extensions": {
      "KHR_materials_sheen": {
        "sheenColorFactor": [0.5, 1.0, 0.25],
        "sheenColorTexture": { "index": 0 },
        "sheenRoughnessFactor": 0.5,
        "sheenRoughnessTexture": { "index": 1 }
      }
    }
  }],
  "textures": [{ "source": 0 }, { "source": 1 }],
  "images": [{ "uri": "sheen-color.png" }, { "uri": "sheen-roughness.png" }],
  "buffers": [{ "uri": "mesh.bin", "byteLength": 102 }],
  "bufferViews": [
    { "buffer": 0, "byteOffset": 0, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 36, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 72, "byteLength": 24, "target": 34962 },
    { "buffer": 0, "byteOffset": 96, "byteLength": 6, "target": 34963 }
  ],
  "accessors": [
    {
      "bufferView": 0,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 0.0],
      "max": [1.0, 1.0, 0.0]
    },
    {
      "bufferView": 1,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 1.0],
      "max": [0.0, 0.0, 1.0]
    },
    {
      "bufferView": 2,
      "componentType": 5126,
      "count": 3,
      "type": "VEC2"
    },
    {
      "bufferView": 3,
      "componentType": 5123,
      "count": 3,
      "type": "SCALAR"
    }
  ]
}"#,
        )
        .expect("write gltf");

        (dir, gltf_path)
    }

    fn write_anisotropy_test_gltf(name: &str) -> (PathBuf, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "robotdreams-renderer-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let bin_path = dir.join("mesh.bin");
        let anisotropy_path = dir.join("anisotropy.png");
        let gltf_path = dir.join("mesh.gltf");

        let mut bin = Vec::new();
        for value in [0.0_f32, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.25_f32, 0.25, 0.25, 0.25, 0.25, 0.25] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [
            1.0_f32, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 1.0,
        ] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0_u16, 1, 2] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        fs::write(&bin_path, bin).expect("write bin");
        fs::write(
            &anisotropy_path,
            encode_png_rgb(1, 1, &[255, 128, 128]).expect("encode anisotropy texture"),
        )
        .expect("write anisotropy texture");
        fs::write(
            &gltf_path,
            r#"{
  "asset": { "version": "2.0" },
  "extensionsUsed": ["KHR_materials_anisotropy"],
  "scene": 0,
  "scenes": [{ "nodes": [0] }],
  "nodes": [{ "mesh": 0 }],
  "meshes": [{
    "primitives": [{
      "attributes": { "POSITION": 0, "NORMAL": 1, "TEXCOORD_0": 2, "TANGENT": 3 },
      "indices": 4,
      "material": 0
    }]
  }],
  "materials": [{
    "pbrMetallicRoughness": {
      "baseColorFactor": [1.0, 1.0, 1.0, 1.0],
      "metallicFactor": 1.0,
      "roughnessFactor": 0.25
    },
    "extensions": {
      "KHR_materials_anisotropy": {
        "anisotropyStrength": 0.5,
        "anisotropyRotation": 0.25,
        "anisotropyTexture": { "index": 0 }
      }
    }
  }],
  "textures": [{ "source": 0 }],
  "images": [{ "uri": "anisotropy.png" }],
  "buffers": [{ "uri": "mesh.bin", "byteLength": 150 }],
  "bufferViews": [
    { "buffer": 0, "byteOffset": 0, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 36, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 72, "byteLength": 24, "target": 34962 },
    { "buffer": 0, "byteOffset": 96, "byteLength": 48, "target": 34962 },
    { "buffer": 0, "byteOffset": 144, "byteLength": 6, "target": 34963 }
  ],
  "accessors": [
    {
      "bufferView": 0,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 0.0],
      "max": [1.0, 1.0, 0.0]
    },
    {
      "bufferView": 1,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 1.0],
      "max": [0.0, 0.0, 1.0]
    },
    {
      "bufferView": 2,
      "componentType": 5126,
      "count": 3,
      "type": "VEC2"
    },
    {
      "bufferView": 3,
      "componentType": 5126,
      "count": 3,
      "type": "VEC4"
    },
    {
      "bufferView": 4,
      "componentType": 5123,
      "count": 3,
      "type": "SCALAR"
    }
  ]
}"#,
        )
        .expect("write gltf");

        (dir, gltf_path)
    }

    fn write_iridescence_test_gltf(name: &str) -> (PathBuf, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "robotdreams-renderer-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let bin_path = dir.join("mesh.bin");
        let iridescence_path = dir.join("iridescence.png");
        let thickness_path = dir.join("iridescence-thickness.png");
        let gltf_path = dir.join("mesh.gltf");

        let mut bin = Vec::new();
        for value in [0.0_f32, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.25_f32, 0.25, 0.25, 0.25, 0.25, 0.25] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0_u16, 1, 2] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        fs::write(&bin_path, bin).expect("write bin");
        fs::write(
            &iridescence_path,
            encode_png_rgb(1, 1, &[128, 0, 0]).expect("encode iridescence texture"),
        )
        .expect("write iridescence texture");
        fs::write(
            &thickness_path,
            encode_png_rgb(1, 1, &[0, 128, 0]).expect("encode iridescence thickness texture"),
        )
        .expect("write iridescence thickness texture");
        fs::write(
            &gltf_path,
            r#"{
  "asset": { "version": "2.0" },
  "extensionsUsed": ["KHR_materials_iridescence"],
  "scene": 0,
  "scenes": [{ "nodes": [0] }],
  "nodes": [{ "mesh": 0 }],
  "meshes": [{
    "primitives": [{
      "attributes": { "POSITION": 0, "NORMAL": 1, "TEXCOORD_0": 2 },
      "indices": 3,
      "material": 0
    }]
  }],
  "materials": [{
    "pbrMetallicRoughness": {
      "baseColorFactor": [1.0, 1.0, 1.0, 1.0],
      "metallicFactor": 0.0,
      "roughnessFactor": 0.2
    },
    "extensions": {
      "KHR_materials_iridescence": {
        "iridescenceFactor": 0.5,
        "iridescenceTexture": { "index": 0 },
        "iridescenceIor": 1.45,
        "iridescenceThicknessMinimum": 120.0,
        "iridescenceThicknessMaximum": 520.0,
        "iridescenceThicknessTexture": { "index": 1 }
      }
    }
  }],
  "textures": [{ "source": 0 }, { "source": 1 }],
  "images": [{ "uri": "iridescence.png" }, { "uri": "iridescence-thickness.png" }],
  "buffers": [{ "uri": "mesh.bin", "byteLength": 102 }],
  "bufferViews": [
    { "buffer": 0, "byteOffset": 0, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 36, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 72, "byteLength": 24, "target": 34962 },
    { "buffer": 0, "byteOffset": 96, "byteLength": 6, "target": 34963 }
  ],
  "accessors": [
    {
      "bufferView": 0,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 0.0],
      "max": [1.0, 1.0, 0.0]
    },
    {
      "bufferView": 1,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 1.0],
      "max": [0.0, 0.0, 1.0]
    },
    {
      "bufferView": 2,
      "componentType": 5126,
      "count": 3,
      "type": "VEC2"
    },
    {
      "bufferView": 3,
      "componentType": 5123,
      "count": 3,
      "type": "SCALAR"
    }
  ]
}"#,
        )
        .expect("write gltf");

        (dir, gltf_path)
    }

    fn write_punctual_light_test_gltf(name: &str) -> (PathBuf, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "robotdreams-renderer-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let bin_path = dir.join("mesh.bin");
        let gltf_path = dir.join("mesh.gltf");

        let mut bin = Vec::new();
        for value in [2.0_f32, -0.5, -0.5, 2.0, -0.5, 0.5, 2.0, 0.5, -0.5] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [-1.0_f32, 0.0, 0.0, -1.0, 0.0, 0.0, -1.0, 0.0, 0.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0_u16, 1, 2] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        fs::write(&bin_path, bin).expect("write bin");
        fs::write(
            &gltf_path,
            r#"{
  "asset": { "version": "2.0" },
  "extensionsUsed": ["KHR_lights_punctual"],
  "extensions": {
    "KHR_lights_punctual": {
      "lights": [{
        "type": "point",
        "color": [0.001, 1.0, 0.5],
        "intensity": 20.0,
        "range": 10.0
      }]
    }
  },
  "scene": 0,
  "scenes": [{ "nodes": [0, 1] }],
  "nodes": [
    { "mesh": 0 },
    { "translation": [0.0, 0.0, 0.0], "extensions": { "KHR_lights_punctual": { "light": 0 } } }
  ],
  "meshes": [{
    "primitives": [{
      "attributes": { "POSITION": 0, "NORMAL": 1 },
      "indices": 2,
      "material": 0
    }]
  }],
  "materials": [{
    "pbrMetallicRoughness": {
      "baseColorFactor": [1.0, 1.0, 1.0, 1.0]
    }
  }],
  "buffers": [{ "uri": "mesh.bin", "byteLength": 78 }],
  "bufferViews": [
    { "buffer": 0, "byteOffset": 0, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 36, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 72, "byteLength": 6, "target": 34963 }
  ],
  "accessors": [
    {
      "bufferView": 0,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [2.0, -0.5, -0.5],
      "max": [2.0, 0.5, 0.5]
    },
    {
      "bufferView": 1,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [-1.0, 0.0, 0.0],
      "max": [-1.0, 0.0, 0.0]
    },
    {
      "bufferView": 2,
      "componentType": 5123,
      "count": 3,
      "type": "SCALAR"
    }
  ]
}"#,
        )
        .expect("write gltf");

        (dir, gltf_path)
    }

    fn write_texture_index_sampler_test_gltf(name: &str) -> (PathBuf, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "robotdreams-renderer-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let bin_path = dir.join("mesh.bin");
        let selected_texture_path = dir.join("selected.png");
        let unused_texture_path = dir.join("unused.png");
        let gltf_path = dir.join("mesh.gltf");

        let mut bin = Vec::new();
        for value in [0.0_f32, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [1.25_f32, 0.25, 1.25, 0.25, 1.25, 0.25] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0_u16, 1, 2] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        fs::write(&bin_path, bin).expect("write bin");
        fs::write(
            &selected_texture_path,
            encode_png_rgb(2, 1, &[255, 0, 0, 0, 255, 0]).expect("encode selected texture"),
        )
        .expect("write selected texture");
        fs::write(
            &unused_texture_path,
            encode_png_rgb(1, 1, &[0, 0, 255]).expect("encode unused texture"),
        )
        .expect("write unused texture");
        fs::write(
            &gltf_path,
            r#"{
  "asset": { "version": "2.0" },
  "scene": 0,
  "scenes": [{ "nodes": [0] }],
  "nodes": [{ "mesh": 0 }],
  "meshes": [{
    "primitives": [{
      "attributes": { "POSITION": 0, "NORMAL": 1, "TEXCOORD_0": 2 },
      "indices": 3,
      "material": 0
    }]
  }],
  "materials": [{
    "pbrMetallicRoughness": {
      "baseColorFactor": [1.0, 1.0, 1.0, 1.0],
      "baseColorTexture": { "index": 1 }
    }
  }],
  "samplers": [{ "magFilter": 9729, "minFilter": 9729, "wrapS": 33071, "wrapT": 33071 }],
  "textures": [
    { "source": 1 },
    { "sampler": 0, "source": 0 }
  ],
  "images": [
    { "uri": "selected.png" },
    { "uri": "unused.png" }
  ],
  "buffers": [{ "uri": "mesh.bin", "byteLength": 102 }],
  "bufferViews": [
    { "buffer": 0, "byteOffset": 0, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 36, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 72, "byteLength": 24, "target": 34962 },
    { "buffer": 0, "byteOffset": 96, "byteLength": 6, "target": 34963 }
  ],
  "accessors": [
    {
      "bufferView": 0,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 0.0],
      "max": [1.0, 1.0, 0.0]
    },
    {
      "bufferView": 1,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 1.0],
      "max": [0.0, 0.0, 1.0]
    },
    {
      "bufferView": 2,
      "componentType": 5126,
      "count": 3,
      "type": "VEC2"
    },
    {
      "bufferView": 3,
      "componentType": 5123,
      "count": 3,
      "type": "SCALAR"
    }
  ]
}"#,
        )
        .expect("write gltf");

        (dir, gltf_path)
    }

    fn write_texcoord1_texture_test_gltf(name: &str) -> (PathBuf, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "robotdreams-renderer-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let bin_path = dir.join("mesh.bin");
        let texture_path = dir.join("texture.png");
        let gltf_path = dir.join("mesh.gltf");

        let mut bin = Vec::new();
        for value in [0.0_f32, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 0.0, 0.0, 0.0, 0.0, 0.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.75_f32, 0.0, 0.75, 0.0, 0.75, 0.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0_u16, 1, 2] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        fs::write(&bin_path, bin).expect("write bin");
        fs::write(
            &texture_path,
            encode_png_rgb(2, 1, &[255, 0, 0, 0, 255, 0]).expect("encode uv1 texture"),
        )
        .expect("write texture");
        fs::write(
            &gltf_path,
            r#"{
  "asset": { "version": "2.0" },
  "scene": 0,
  "scenes": [{ "nodes": [0] }],
  "nodes": [{ "mesh": 0 }],
  "meshes": [{
    "primitives": [{
      "attributes": { "POSITION": 0, "NORMAL": 1, "TEXCOORD_0": 2, "TEXCOORD_1": 3 },
      "indices": 4,
      "material": 0
    }]
  }],
  "materials": [{
    "pbrMetallicRoughness": {
      "baseColorFactor": [1.0, 1.0, 1.0, 1.0],
      "baseColorTexture": { "index": 0, "texCoord": 1 }
    }
  }],
  "samplers": [{ "magFilter": 9728, "minFilter": 9728 }],
  "textures": [{ "sampler": 0, "source": 0 }],
  "images": [{ "uri": "texture.png" }],
  "buffers": [{ "uri": "mesh.bin", "byteLength": 126 }],
  "bufferViews": [
    { "buffer": 0, "byteOffset": 0, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 36, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 72, "byteLength": 24, "target": 34962 },
    { "buffer": 0, "byteOffset": 96, "byteLength": 24, "target": 34962 },
    { "buffer": 0, "byteOffset": 120, "byteLength": 6, "target": 34963 }
  ],
  "accessors": [
    {
      "bufferView": 0,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 0.0],
      "max": [1.0, 1.0, 0.0]
    },
    {
      "bufferView": 1,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 1.0],
      "max": [0.0, 0.0, 1.0]
    },
    {
      "bufferView": 2,
      "componentType": 5126,
      "count": 3,
      "type": "VEC2"
    },
    {
      "bufferView": 3,
      "componentType": 5126,
      "count": 3,
      "type": "VEC2"
    },
    {
      "bufferView": 4,
      "componentType": 5123,
      "count": 3,
      "type": "SCALAR"
    }
  ]
}"#,
        )
        .expect("write gltf");

        (dir, gltf_path)
    }

    fn write_vertex_color_test_gltf(name: &str) -> (PathBuf, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "robotdreams-renderer-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let bin_path = dir.join("mesh.bin");
        let gltf_path = dir.join("mesh.gltf");

        let mut bin = Vec::new();
        for value in [0.0_f32, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [
            1.0_f32, 0.0, 0.0, 0.5, 0.0, 1.0, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0,
        ] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0_u16, 1, 2] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        fs::write(&bin_path, bin).expect("write bin");
        fs::write(
            &gltf_path,
            r#"{
  "asset": { "version": "2.0" },
  "scene": 0,
  "scenes": [{ "nodes": [0] }],
  "nodes": [{ "mesh": 0 }],
  "meshes": [{
    "primitives": [{
      "attributes": { "POSITION": 0, "NORMAL": 1, "COLOR_0": 2 },
      "indices": 3,
      "material": 0
    }]
  }],
  "materials": [{
    "pbrMetallicRoughness": {
      "baseColorFactor": [1.0, 1.0, 1.0, 1.0]
    }
  }],
  "buffers": [{ "uri": "mesh.bin", "byteLength": 126 }],
  "bufferViews": [
    { "buffer": 0, "byteOffset": 0, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 36, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 72, "byteLength": 48, "target": 34962 },
    { "buffer": 0, "byteOffset": 120, "byteLength": 6, "target": 34963 }
  ],
  "accessors": [
    {
      "bufferView": 0,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 0.0],
      "max": [1.0, 1.0, 0.0]
    },
    {
      "bufferView": 1,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 1.0],
      "max": [0.0, 0.0, 1.0]
    },
    {
      "bufferView": 2,
      "componentType": 5126,
      "count": 3,
      "type": "VEC4"
    },
    {
      "bufferView": 3,
      "componentType": 5123,
      "count": 3,
      "type": "SCALAR"
    }
  ]
}"#,
        )
        .expect("write gltf");

        (dir, gltf_path)
    }

    fn write_emissive_test_gltf(name: &str) -> (PathBuf, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "robotdreams-renderer-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let bin_path = dir.join("mesh.bin");
        let gltf_path = dir.join("mesh.gltf");

        let mut bin = Vec::new();
        for value in [0.0_f32, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0_u16, 1, 2] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        fs::write(&bin_path, bin).expect("write bin");
        fs::write(
            &gltf_path,
            r#"{
  "asset": { "version": "2.0" },
  "scene": 0,
  "scenes": [{ "nodes": [0] }],
  "nodes": [{ "mesh": 0 }],
  "meshes": [{
    "primitives": [{
      "attributes": { "POSITION": 0, "NORMAL": 1 },
      "indices": 2,
      "material": 0
    }]
  }],
  "materials": [{
    "emissiveFactor": [0.75, 0.25, 0.0],
    "extensions": {
      "KHR_materials_emissive_strength": { "emissiveStrength": 4.0 }
    },
    "pbrMetallicRoughness": {
      "baseColorFactor": [0.1, 0.1, 0.1, 1.0]
    }
  }],
  "buffers": [{ "uri": "mesh.bin", "byteLength": 78 }],
  "bufferViews": [
    { "buffer": 0, "byteOffset": 0, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 36, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 72, "byteLength": 6, "target": 34963 }
  ],
  "accessors": [
    {
      "bufferView": 0,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 0.0],
      "max": [1.0, 1.0, 0.0]
    },
    {
      "bufferView": 1,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 1.0],
      "max": [0.0, 0.0, 1.0]
    },
    {
      "bufferView": 2,
      "componentType": 5123,
      "count": 3,
      "type": "SCALAR"
    }
  ]
}"#,
        )
        .expect("write gltf");

        (dir, gltf_path)
    }

    fn write_normal_mapped_test_gltf(name: &str) -> (PathBuf, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "robotdreams-renderer-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let bin_path = dir.join("mesh.bin");
        let normal_path = dir.join("normal.png");
        let gltf_path = dir.join("mesh.gltf");

        let mut bin = Vec::new();
        for value in [0.0_f32, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [
            1.0_f32, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 1.0,
        ] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.25_f32, 0.25, 0.25, 0.25, 0.25, 0.25] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0_u16, 1, 2] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        fs::write(&bin_path, bin).expect("write bin");
        fs::write(
            &normal_path,
            encode_png_rgb(1, 1, &[255, 128, 128]).expect("encode normal texture"),
        )
        .expect("write normal texture");
        fs::write(
            &gltf_path,
            r#"{
  "asset": { "version": "2.0" },
  "scene": 0,
  "scenes": [{ "nodes": [0] }],
  "nodes": [{ "mesh": 0 }],
  "meshes": [{
    "primitives": [{
      "attributes": { "POSITION": 0, "NORMAL": 1, "TANGENT": 2, "TEXCOORD_0": 3 },
      "indices": 4,
      "material": 0
    }]
  }],
  "materials": [{
    "normalTexture": { "index": 0, "scale": 1.0 },
    "pbrMetallicRoughness": {
      "baseColorFactor": [1.0, 1.0, 1.0, 1.0]
    }
  }],
  "textures": [{ "source": 0 }],
  "images": [{ "uri": "normal.png" }],
  "buffers": [{ "uri": "mesh.bin", "byteLength": 150 }],
  "bufferViews": [
    { "buffer": 0, "byteOffset": 0, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 36, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 72, "byteLength": 48, "target": 34962 },
    { "buffer": 0, "byteOffset": 120, "byteLength": 24, "target": 34962 },
    { "buffer": 0, "byteOffset": 144, "byteLength": 6, "target": 34963 }
  ],
  "accessors": [
    {
      "bufferView": 0,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 0.0],
      "max": [1.0, 1.0, 0.0]
    },
    {
      "bufferView": 1,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 1.0],
      "max": [0.0, 0.0, 1.0]
    },
    {
      "bufferView": 2,
      "componentType": 5126,
      "count": 3,
      "type": "VEC4"
    },
    {
      "bufferView": 3,
      "componentType": 5126,
      "count": 3,
      "type": "VEC2"
    },
    {
      "bufferView": 4,
      "componentType": 5123,
      "count": 3,
      "type": "SCALAR"
    }
  ]
}"#,
        )
        .expect("write gltf");

        (dir, gltf_path)
    }

    fn write_normal_mapped_without_tangent_test_gltf(name: &str) -> (PathBuf, PathBuf) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "robotdreams-renderer-{name}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        let bin_path = dir.join("mesh.bin");
        let normal_path = dir.join("normal.png");
        let gltf_path = dir.join("mesh.gltf");

        let mut bin = Vec::new();
        for value in [0.0_f32, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0.0_f32, 0.0, 1.0, 0.0, 0.0, 1.0] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        for value in [0_u16, 1, 2] {
            bin.extend_from_slice(&value.to_le_bytes());
        }
        fs::write(&bin_path, bin).expect("write bin");
        fs::write(
            &normal_path,
            encode_png_rgb(1, 1, &[255, 128, 128]).expect("encode normal texture"),
        )
        .expect("write normal texture");
        fs::write(
            &gltf_path,
            r#"{
  "asset": { "version": "2.0" },
  "scene": 0,
  "scenes": [{ "nodes": [0] }],
  "nodes": [{ "mesh": 0 }],
  "meshes": [{
    "primitives": [{
      "attributes": { "POSITION": 0, "NORMAL": 1, "TEXCOORD_0": 2 },
      "indices": 3,
      "material": 0
    }]
  }],
  "materials": [{
    "normalTexture": { "index": 0, "scale": 1.0 },
    "pbrMetallicRoughness": {
      "baseColorFactor": [1.0, 1.0, 1.0, 1.0]
    }
  }],
  "textures": [{ "source": 0 }],
  "images": [{ "uri": "normal.png" }],
  "buffers": [{ "uri": "mesh.bin", "byteLength": 102 }],
  "bufferViews": [
    { "buffer": 0, "byteOffset": 0, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 36, "byteLength": 36, "target": 34962 },
    { "buffer": 0, "byteOffset": 72, "byteLength": 24, "target": 34962 },
    { "buffer": 0, "byteOffset": 96, "byteLength": 6, "target": 34963 }
  ],
  "accessors": [
    {
      "bufferView": 0,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 0.0],
      "max": [1.0, 1.0, 0.0]
    },
    {
      "bufferView": 1,
      "componentType": 5126,
      "count": 3,
      "type": "VEC3",
      "min": [0.0, 0.0, 1.0],
      "max": [0.0, 0.0, 1.0]
    },
    {
      "bufferView": 2,
      "componentType": 5126,
      "count": 3,
      "type": "VEC2"
    },
    {
      "bufferView": 3,
      "componentType": 5123,
      "count": 3,
      "type": "SCALAR"
    }
  ]
}"#,
        )
        .expect("write gltf");

        (dir, gltf_path)
    }

    fn box_scene() -> SceneGraph {
        colored_box_scene([255, 0, 0])
    }

    fn colored_box_scene(color_rgb: [u8; 3]) -> SceneGraph {
        let mut scene = SceneGraph::empty();
        scene.add_entity(EntityMetadata {
            id: EntityId("box".to_string()),
            name: "Box".to_string(),
            kind: "object".to_string(),
            robot_id: None,
            link_name: None,
        });
        scene.add_entity(EntityMetadata {
            id: EntityId("camera".to_string()),
            name: "Camera".to_string(),
            kind: "camera".to_string(),
            robot_id: None,
            link_name: None,
        });
        scene.root.children.push(SceneNode::mesh(
            "box",
            "Box",
            Geometry::Box {
                size: [1.0, 1.0, 1.0],
            },
            Material { color_rgb },
            Transform::translated([2.0, 0.0, 0.0]),
        ));
        scene.root.children.push(SceneNode::camera(
            "camera",
            "Camera",
            CameraSpec {
                id: "camera".to_string(),
                name: "Camera".to_string(),
                transform: Transform::default(),
                fov_deg: 60.0,
                projection: CameraProjection::Perspective,
                resolution: [5, 5],
                intrinsics: None,
                distortion: None,
                depth_range_m: None,
                sensor_effects: None,
            },
        ));
        scene
    }

    fn translated_box_scene(entity_id: &str, translation: [f32; 3]) -> SceneGraph {
        let mut scene = SceneGraph::empty();
        scene.add_entity(EntityMetadata {
            id: EntityId(entity_id.to_string()),
            name: entity_id.to_string(),
            kind: "object".to_string(),
            robot_id: None,
            link_name: None,
        });
        scene.add_entity(EntityMetadata {
            id: EntityId("camera".to_string()),
            name: "Camera".to_string(),
            kind: "camera".to_string(),
            robot_id: None,
            link_name: None,
        });
        scene.root.children.push(SceneNode::mesh(
            entity_id,
            entity_id,
            Geometry::Box {
                size: [1.0, 1.0, 1.0],
            },
            Material {
                color_rgb: [255, 255, 255],
            },
            Transform::translated(translation),
        ));
        scene.root.children.push(SceneNode::camera(
            "camera",
            "Camera",
            CameraSpec {
                id: "camera".to_string(),
                name: "Camera".to_string(),
                transform: Transform::default(),
                fov_deg: 60.0,
                projection: CameraProjection::Perspective,
                resolution: [5, 5],
                intrinsics: None,
                distortion: None,
                depth_range_m: None,
                sensor_effects: None,
            },
        ));
        scene
    }

    fn two_label_scene(visible_entity_id: &str) -> SceneGraph {
        let mut scene = SceneGraph::empty();
        for entity_id in ["a", "b"] {
            scene.add_entity(EntityMetadata {
                id: EntityId(entity_id.to_string()),
                name: entity_id.to_string(),
                kind: "object".to_string(),
                robot_id: None,
                link_name: None,
            });
        }
        scene.add_entity(EntityMetadata {
            id: EntityId("camera".to_string()),
            name: "Camera".to_string(),
            kind: "camera".to_string(),
            robot_id: None,
            link_name: None,
        });
        for entity_id in ["a", "b"] {
            let y = if entity_id == visible_entity_id {
                0.0
            } else {
                50.0
            };
            scene.root.children.push(SceneNode::mesh(
                entity_id,
                entity_id,
                Geometry::Box {
                    size: [1.0, 10.0, 10.0],
                },
                Material {
                    color_rgb: [255, 255, 255],
                },
                Transform::translated([2.0, y, 0.0]),
            ));
        }
        scene.root.children.push(SceneNode::camera(
            "camera",
            "Camera",
            CameraSpec {
                id: "camera".to_string(),
                name: "Camera".to_string(),
                transform: Transform::default(),
                fov_deg: 60.0,
                projection: CameraProjection::Perspective,
                resolution: [5, 2],
                intrinsics: None,
                distortion: None,
                depth_range_m: None,
                sensor_effects: None,
            },
        ));
        scene
    }

    fn frame_center_f32(frame: &FrameBuffer, resolution: [u32; 2]) -> f32 {
        let center_offset =
            (((resolution[1] / 2) * resolution[0] + resolution[0] / 2) * 4) as usize;
        f32::from_le_bytes(
            frame.bytes[center_offset..center_offset + 4]
                .try_into()
                .unwrap(),
        )
    }

    fn frame_u32_at(frame: &FrameBuffer, resolution: [u32; 2], x: u32, y: u32) -> u32 {
        let offset = ((y * resolution[0] + x) * 4) as usize;
        u32::from_le_bytes(frame.bytes[offset..offset + 4].try_into().unwrap())
    }

    fn frame_vec3_f32_at(frame: &FrameBuffer, resolution: [u32; 2], x: u32, y: u32) -> [f32; 3] {
        let offset = ((y * resolution[0] + x) * 12) as usize;
        [
            f32::from_le_bytes(frame.bytes[offset..offset + 4].try_into().unwrap()),
            f32::from_le_bytes(frame.bytes[offset + 4..offset + 8].try_into().unwrap()),
            f32::from_le_bytes(frame.bytes[offset + 8..offset + 12].try_into().unwrap()),
        ]
    }

    fn decode_png_rgb(bytes: &[u8]) -> Vec<u8> {
        let decoder = png::Decoder::new(Cursor::new(bytes));
        let mut reader = decoder.read_info().expect("png info");
        let mut rgb = vec![0; reader.output_buffer_size()];
        let info = reader.next_frame(&mut rgb).expect("png frame");
        rgb.truncate(info.buffer_size());
        rgb
    }

    fn update_camera(scene: &mut SceneGraph, f: impl FnOnce(&mut CameraSpec)) {
        let camera = scene
            .root
            .children
            .iter_mut()
            .find_map(|node| match &mut node.kind {
                SceneNodeKind::Camera(camera) => Some(camera),
                _ => None,
            })
            .expect("test scene camera");
        f(camera);
    }

    fn segmentation_center(scene: &SceneGraph) -> u32 {
        let output = NativeRenderer::new()
            .render(
                scene,
                None,
                &ObservationRequest {
                    camera_id: Some("camera".to_string()),
                    views: vec![ObservationView::Segmentation],
                    resolution: [5, 5],
                    segmentation_policy: None,
                    shutter_policy: None,
                    render_settings: None,
                },
            )
            .expect("render segmentation");
        let frame = output
            .frames
            .iter()
            .find(|frame| frame.kind == FrameKind::Segmentation)
            .expect("segmentation frame");
        let center_offset = ((2 * 5 + 2) * 4) as usize;
        u32::from_le_bytes(
            frame.bytes[center_offset..center_offset + 4]
                .try_into()
                .unwrap(),
        )
    }

    #[test]
    fn segmentation_hits_center_box() {
        let output = NativeRenderer::new()
            .render(
                &box_scene(),
                None,
                &ObservationRequest {
                    camera_id: Some("camera".to_string()),
                    views: vec![ObservationView::Segmentation],
                    resolution: [5, 5],
                    segmentation_policy: None,
                    shutter_policy: None,
                    render_settings: None,
                },
            )
            .expect("render");
        let frame = output
            .frames
            .iter()
            .find(|frame| frame.kind == FrameKind::Segmentation)
            .expect("segmentation frame");
        let center_offset = ((2 * 5 + 2) * 4) as usize;
        let center = u32::from_le_bytes(
            frame.bytes[center_offset..center_offset + 4]
                .try_into()
                .unwrap(),
        );
        assert!(center > 0);
    }

    #[test]
    fn normal_view_encodes_world_space_surface_normals() {
        let output = NativeRenderer::new()
            .render(
                &box_scene(),
                None,
                &ObservationRequest {
                    camera_id: Some("camera".to_string()),
                    views: vec![ObservationView::Normal],
                    resolution: [5, 5],
                    segmentation_policy: None,
                    shutter_policy: None,
                    render_settings: None,
                },
            )
            .expect("render normal");
        let frame = output
            .frames
            .iter()
            .find(|frame| frame.kind == FrameKind::Normal)
            .expect("normal frame");
        assert!(frame.bytes.starts_with(b"\x89PNG\r\n\x1a\n"));
        let rgb = decode_png_rgb(&frame.bytes);
        let center_offset = (2 * 5 + 2) * 3;
        assert_eq!(&rgb[center_offset..center_offset + 3], &[0, 128, 128]);

        let corner_offset = 3 * 3;
        assert_eq!(&rgb[corner_offset..corner_offset + 3], &[0, 0, 0]);
    }

    #[test]
    fn albedo_view_encodes_unlit_visible_material_color() {
        let output = NativeRenderer::new()
            .render(
                &colored_box_scene([25, 150, 230]),
                None,
                &ObservationRequest {
                    camera_id: Some("camera".to_string()),
                    views: vec![ObservationView::Albedo],
                    resolution: [5, 5],
                    segmentation_policy: None,
                    shutter_policy: None,
                    render_settings: Some(RenderSettings {
                        background_rgb: [255, 255, 255],
                        ambient_intensity: 0.0,
                        ..RenderSettings::default()
                    }),
                },
            )
            .expect("render albedo");
        let frame = output
            .frames
            .iter()
            .find(|frame| frame.kind == FrameKind::Albedo)
            .expect("albedo frame");
        assert!(frame.bytes.starts_with(b"\x89PNG\r\n\x1a\n"));
        let rgb = decode_png_rgb(&frame.bytes);
        let center_offset = (2 * 5 + 2) * 3;
        assert_eq!(&rgb[center_offset..center_offset + 3], &[25, 150, 230]);

        let corner_offset = 3 * 3;
        assert_eq!(&rgb[corner_offset..corner_offset + 3], &[0, 0, 0]);
    }

    #[test]
    fn material_properties_view_encodes_pbr_channels() {
        let output = NativeRenderer::new()
            .render(
                &colored_box_scene([25, 150, 230]),
                None,
                &ObservationRequest {
                    camera_id: Some("camera".to_string()),
                    views: vec![ObservationView::MaterialProperties],
                    resolution: [5, 5],
                    segmentation_policy: None,
                    shutter_policy: None,
                    render_settings: None,
                },
            )
            .expect("render material properties");
        let frame = output
            .frames
            .iter()
            .find(|frame| frame.kind == FrameKind::MaterialProperties)
            .expect("material properties frame");
        assert!(frame.bytes.starts_with(b"\x89PNG\r\n\x1a\n"));
        let rgb = decode_png_rgb(&frame.bytes);
        let center_offset = (2 * 5 + 2) * 3;
        assert_eq!(
            &rgb[center_offset..center_offset + 3],
            &[0, 255, 255],
            "default primitive material should encode metallic 0, roughness 1, occlusion 1"
        );

        let corner_offset = 3 * 3;
        assert_eq!(&rgb[corner_offset..corner_offset + 3], &[0, 0, 0]);
    }

    #[test]
    fn world_position_view_encodes_world_hit_point() {
        let output = NativeRenderer::new()
            .render(
                &box_scene(),
                None,
                &ObservationRequest {
                    camera_id: Some("camera".to_string()),
                    views: vec![ObservationView::WorldPosition],
                    resolution: [5, 5],
                    segmentation_policy: None,
                    shutter_policy: None,
                    render_settings: None,
                },
            )
            .expect("render world position");
        let frame = output
            .frames
            .iter()
            .find(|frame| frame.kind == FrameKind::WorldPosition)
            .expect("world position frame");
        assert_eq!(frame.bytes.len(), 5 * 5 * 12);

        let center = frame_vec3_f32_at(frame, [5, 5], 2, 2);
        assert!(
            (center[0] - 1.5).abs() < 1.0e-5,
            "center x should hit the front face of the box, got {center:?}"
        );
        assert!(
            center[1].abs() <= 0.5 && center[2].abs() <= 0.5,
            "center should land inside the visible box face, got {center:?}"
        );

        let miss = frame_vec3_f32_at(frame, [5, 5], 3, 0);
        assert!(miss.iter().all(|value| value.is_nan()));
    }

    #[test]
    fn render_scene_samples_averages_debug_rgb_from_multiple_robotdreams_states() {
        let request = ObservationRequest {
            camera_id: Some("camera".to_string()),
            views: vec![ObservationView::DebugRgb],
            resolution: [5, 5],
            segmentation_policy: None,
            shutter_policy: Some(ShutterPolicy {
                exposure_sec: 0.2,
                samples: 2,
                ..ShutterPolicy::default()
            }),
            render_settings: None,
        };
        let output = NativeRenderer::new()
            .render_scene_samples(
                &[
                    SceneGraphSample {
                        scene: colored_box_scene([255, 0, 0]),
                        state: Some(RobotDreamsSnapshot {
                            clock_sec: 0.0,
                            ..RobotDreamsSnapshot::default()
                        }),
                    },
                    SceneGraphSample {
                        scene: colored_box_scene([0, 0, 255]),
                        state: Some(RobotDreamsSnapshot {
                            clock_sec: 0.1,
                            ..RobotDreamsSnapshot::default()
                        }),
                    },
                ],
                &request,
            )
            .expect("render scene samples");
        let frame = output
            .frames
            .iter()
            .find(|frame| frame.kind == FrameKind::DebugRgb)
            .expect("debug rgb frame");
        let rgb = decode_png_rgb(&frame.bytes);
        let center_offset = (2 * 5 + 2) * 3;
        let center = &rgb[center_offset..center_offset + 3];

        assert_eq!(output.metadata.timestamp_sec, 0.1);
        assert!(center[0] > 10, "previous red sample should contribute");
        assert!(center[2] > 10, "current blue sample should contribute");
        assert!(
            center[0] < 250 && center[2] < 250,
            "multi-state exposure should not equal either single state"
        );
    }

    #[test]
    fn debug_rgb_samples_per_pixel_antialiases_subpixel_edges() {
        let camera = CameraSpec {
            id: "camera".to_string(),
            name: "Camera".to_string(),
            transform: Transform::default(),
            fov_deg: 60.0,
            projection: CameraProjection::Perspective,
            resolution: [4, 4],
            intrinsics: None,
            distortion: None,
            depth_range_m: None,
            sensor_effects: None,
        };
        let target_ray = camera_ray_at_pixel(&camera, [4, 4], 1.25, 1.25);
        let object = RenderObject {
            entity_u32: 7,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Sphere { radius: 0.05 }),
            color_rgb: [255, 0, 0],
            transform: Transform::translated(scale(target_ray.dir, 3.0)),
        };
        let renderer = NativeRenderer::new();
        let single_sample = PreparedRenderSettings::from_settings(RenderSettings {
            ambient_intensity: 1.0,
            debug_rgb_samples_per_pixel: 1,
            ..RenderSettings::default()
        });
        let multi_sample = PreparedRenderSettings::from_settings(RenderSettings {
            ambient_intensity: 1.0,
            debug_rgb_samples_per_pixel: 4,
            ..RenderSettings::default()
        });

        let center = renderer.trace_debug_rgb(
            &camera,
            [4, 4],
            1,
            1,
            std::slice::from_ref(&object),
            &[],
            &single_sample,
        );
        let antialiased =
            renderer.trace_debug_rgb_sampled(&camera, [4, 4], 1, 1, &[object], &[], &multi_sample);

        assert_eq!(center, RenderSettings::default().background_rgb);
        assert!(
            antialiased[0] > center[0],
            "subpixel sampling should include the red sphere"
        );
        assert!(
            antialiased[0] < 100,
            "subpixel hit should be averaged with background, not replace the whole pixel"
        );
        assert!(antialiased[1] < center[1]);
        assert!(antialiased[2] < center[2]);
        assert!(antialiased[0] > antialiased[1] * 4);
    }

    #[test]
    fn orthographic_camera_rays_shift_origin_without_changing_direction() {
        let camera = CameraSpec {
            id: "camera".to_string(),
            name: "Camera".to_string(),
            transform: Transform::default(),
            fov_deg: 60.0,
            projection: CameraProjection::Orthographic { size_m: 2.0 },
            resolution: [4, 2],
            intrinsics: None,
            distortion: None,
            depth_range_m: None,
            sensor_effects: None,
        };

        let left = camera_ray_at_pixel(&camera, [4, 2], 0.5, 0.5);
        let right = camera_ray_at_pixel(&camera, [4, 2], 3.5, 0.5);

        assert_rgb_close(left.dir, [1.0, 0.0, 0.0], 1.0e-6);
        assert_rgb_close(right.dir, left.dir, 1.0e-6);
        assert!(
            left.origin[1] > right.origin[1] + 2.0,
            "orthographic camera should move ray origins across the image plane, left {:?} right {:?}",
            left.origin,
            right.origin
        );
        assert!((left.origin[0] - right.origin[0]).abs() < 1.0e-6);
    }

    #[test]
    fn render_scene_samples_averages_depth_from_multiple_robotdreams_states() {
        let request = ObservationRequest {
            camera_id: Some("camera".to_string()),
            views: vec![ObservationView::Depth],
            resolution: [5, 5],
            segmentation_policy: None,
            shutter_policy: Some(ShutterPolicy {
                exposure_sec: 0.2,
                samples: 2,
                ..ShutterPolicy::default()
            }),
            render_settings: None,
        };
        let output = NativeRenderer::new()
            .render_scene_samples(
                &[
                    SceneGraphSample {
                        scene: translated_box_scene("box", [2.0, 0.0, 0.0]),
                        state: Some(RobotDreamsSnapshot {
                            clock_sec: 0.0,
                            ..RobotDreamsSnapshot::default()
                        }),
                    },
                    SceneGraphSample {
                        scene: translated_box_scene("box", [4.0, 0.0, 0.0]),
                        state: Some(RobotDreamsSnapshot {
                            clock_sec: 0.1,
                            ..RobotDreamsSnapshot::default()
                        }),
                    },
                ],
                &request,
            )
            .expect("render depth scene samples");
        let frame = output
            .frames
            .iter()
            .find(|frame| frame.kind == FrameKind::Depth)
            .expect("depth frame");
        let center = frame_center_f32(frame, [5, 5]);

        assert!(
            (center - 2.5).abs() < 0.05,
            "depth exposure should average near/far samples, got {center}"
        );
    }

    #[test]
    fn temporal_scene_samples_interpolate_between_robotdreams_states() {
        let request = ObservationRequest {
            camera_id: Some("camera".to_string()),
            views: vec![ObservationView::Depth],
            resolution: [5, 5],
            segmentation_policy: None,
            shutter_policy: Some(ShutterPolicy {
                exposure_sec: 0.5,
                samples: 3,
                ..ShutterPolicy::default()
            }),
            render_settings: None,
        };
        let renderer = NativeRenderer::new();
        let samples = renderer
            .temporal_samples_for_scene_samples(
                &[
                    SceneGraphSample {
                        scene: translated_box_scene("box", [2.0, 0.0, 0.0]),
                        state: Some(RobotDreamsSnapshot {
                            clock_sec: 0.0,
                            ..RobotDreamsSnapshot::default()
                        }),
                    },
                    SceneGraphSample {
                        scene: translated_box_scene("box", [4.0, 0.0, 0.0]),
                        state: Some(RobotDreamsSnapshot {
                            clock_sec: 0.5,
                            ..RobotDreamsSnapshot::default()
                        }),
                    },
                ],
                &request,
                None,
            )
            .expect("temporal samples");

        assert_eq!(samples.len(), 3);
        assert!((samples[0].timestamp_sec - 0.25).abs() < 1.0e-6);
        assert!(
            (samples[0].objects[0].transform.translation[0] - 3.0).abs() < 1.0e-6,
            "mid-shutter sample should interpolate object pose"
        );
    }

    #[test]
    fn rolling_shutter_segmentation_uses_row_specific_sample_times() {
        let request = ObservationRequest {
            camera_id: Some("camera".to_string()),
            views: vec![ObservationView::Segmentation],
            resolution: [5, 2],
            segmentation_policy: None,
            shutter_policy: Some(ShutterPolicy {
                exposure_sec: 0.0,
                samples: 1,
                mode: ShutterMode::RollingTopToBottom,
                readout_sec: 1.0,
            }),
            render_settings: None,
        };
        let output = NativeRenderer::new()
            .render_scene_samples(
                &[
                    SceneGraphSample {
                        scene: two_label_scene("a"),
                        state: Some(RobotDreamsSnapshot {
                            clock_sec: 0.0,
                            ..RobotDreamsSnapshot::default()
                        }),
                    },
                    SceneGraphSample {
                        scene: two_label_scene("b"),
                        state: Some(RobotDreamsSnapshot {
                            clock_sec: 1.0,
                            ..RobotDreamsSnapshot::default()
                        }),
                    },
                ],
                &request,
            )
            .expect("render segmentation scene samples");
        let frame = output
            .frames
            .iter()
            .find(|frame| frame.kind == FrameKind::Segmentation)
            .expect("segmentation frame");

        assert_eq!(frame_u32_at(frame, [5, 2], 2, 0), 1);
        assert_eq!(frame_u32_at(frame, [5, 2], 2, 1), 2);
    }

    #[test]
    fn metadata_reports_scaled_camera_calibration() {
        let mut scene = box_scene();
        update_camera(&mut scene, |camera| {
            camera.resolution = [10, 10];
            camera.intrinsics = Some(CameraIntrinsics {
                fx: 20.0,
                fy: 30.0,
                cx: 4.0,
                cy: 5.0,
                skew: 0.5,
            });
            camera.distortion = Some(CameraDistortion {
                k1: 0.1,
                k2: 0.2,
                p1: 0.3,
                p2: 0.4,
                k3: 0.5,
            });
            camera.depth_range_m = Some([0.2, 4.0]);
            camera.sensor_effects = Some(CameraSensorEffects {
                exposure: 1.25,
                gamma: 2.2,
                rgb_noise_stddev: 3.0,
                depth_noise_stddev_m: 0.01,
                depth_quantization_m: 0.005,
                noise_seed: 42,
            });
        });

        let output = NativeRenderer::new()
            .render(
                &scene,
                None,
                &ObservationRequest {
                    camera_id: Some("camera".to_string()),
                    views: vec![ObservationView::DebugRgb],
                    resolution: [20, 5],
                    segmentation_policy: None,
                    shutter_policy: Some(ShutterPolicy {
                        exposure_sec: 0.01,
                        samples: 2,
                        ..ShutterPolicy::default()
                    }),
                    render_settings: None,
                },
            )
            .expect("render calibrated frame");
        let intrinsics = output
            .metadata
            .camera_intrinsics
            .expect("camera intrinsics metadata");

        assert_eq!(intrinsics.fx, 40.0);
        assert_eq!(intrinsics.fy, 15.0);
        assert_eq!(intrinsics.cx, 8.0);
        assert_eq!(intrinsics.cy, 2.5);
        assert_eq!(intrinsics.skew, 1.0);
        assert_eq!(
            output.metadata.camera_projection,
            Some(CameraProjection::Perspective)
        );
        assert_eq!(output.metadata.camera_distortion.unwrap().k3, 0.5);
        assert_eq!(output.metadata.depth_range_m, Some([0.2, 4.0]));
        assert_eq!(output.metadata.sensor_effects.unwrap().noise_seed, 42);
        assert_eq!(output.metadata.shutter_policy.unwrap().samples, 2);
        assert_eq!(
            output.metadata.camera_extrinsics_matrix.unwrap()[3],
            [0.0, 0.0, 0.0, 1.0]
        );
    }

    #[test]
    fn metadata_reports_orthographic_camera_projection() {
        let mut scene = box_scene();
        update_camera(&mut scene, |camera| {
            camera.projection = CameraProjection::Orthographic { size_m: 1.25 };
        });

        let output = NativeRenderer::new()
            .render(
                &scene,
                None,
                &ObservationRequest {
                    camera_id: Some("camera".to_string()),
                    views: vec![ObservationView::DebugRgb],
                    resolution: [5, 5],
                    segmentation_policy: None,
                    shutter_policy: None,
                    render_settings: None,
                },
            )
            .expect("render orthographic frame");

        assert_eq!(
            output.metadata.camera_projection,
            Some(CameraProjection::Orthographic { size_m: 1.25 })
        );
    }

    #[test]
    fn rgb_sensor_effects_apply_exposure_gamma_and_deterministic_noise() {
        let exposure_only = CameraSensorEffects {
            exposure: 2.0,
            gamma: 1.0,
            rgb_noise_stddev: 0.0,
            depth_noise_stddev_m: 0.0,
            depth_quantization_m: 0.0,
            noise_seed: 7,
        };
        assert_eq!(
            apply_rgb_sensor_effects([60, 80, 120], Some(exposure_only), 1, 2),
            [120, 160, 240]
        );

        let noisy = CameraSensorEffects {
            exposure: 1.0,
            gamma: 1.0,
            rgb_noise_stddev: 8.0,
            depth_noise_stddev_m: 0.0,
            depth_quantization_m: 0.0,
            noise_seed: 7,
        };
        let first = apply_rgb_sensor_effects([60, 80, 120], Some(noisy), 1, 2);
        let second = apply_rgb_sensor_effects([60, 80, 120], Some(noisy), 1, 2);

        assert_eq!(first, second);
        assert_ne!(first, [60, 80, 120]);
    }

    #[test]
    fn depth_sensor_effects_apply_deterministic_noise_only_to_finite_depth() {
        let effects = CameraSensorEffects {
            exposure: 1.0,
            gamma: 1.0,
            rgb_noise_stddev: 0.0,
            depth_noise_stddev_m: 0.05,
            depth_quantization_m: 0.0,
            noise_seed: 11,
        };
        let first = apply_depth_sensor_effects(2.0, Some(effects), 3, 4);
        let second = apply_depth_sensor_effects(2.0, Some(effects), 3, 4);

        assert_eq!(first, second);
        assert_ne!(first, 2.0);
        assert!(apply_depth_sensor_effects(f32::INFINITY, Some(effects), 3, 4).is_infinite());
    }

    #[test]
    fn depth_sensor_effects_quantize_finite_depth() {
        let effects = CameraSensorEffects {
            exposure: 1.0,
            gamma: 1.0,
            rgb_noise_stddev: 0.0,
            depth_noise_stddev_m: 0.0,
            depth_quantization_m: 0.05,
            noise_seed: 11,
        };

        let depth = apply_depth_sensor_effects(2.03, Some(effects), 3, 4);
        assert!((depth - 2.05).abs() < 1.0e-6, "depth was {depth}");
        assert!(apply_depth_sensor_effects(f32::INFINITY, Some(effects), 3, 4).is_infinite());
    }

    #[test]
    fn calibrated_intrinsics_affect_segmentation_rays() {
        let mut scene = box_scene();
        assert!(segmentation_center(&scene) > 0);

        update_camera(&mut scene, |camera| {
            camera.intrinsics = Some(CameraIntrinsics {
                fx: 2.0,
                fy: 2.0,
                cx: -20.0,
                cy: 2.0,
                skew: 0.0,
            });
        });

        assert_eq!(segmentation_center(&scene), 0);
    }

    #[test]
    fn distortion_inverse_recovers_normalized_sensor_point() {
        let distortion = CameraDistortion {
            k1: 0.12,
            k2: -0.03,
            p1: 0.01,
            p2: -0.02,
            k3: 0.004,
        };
        let original = [0.35, -0.22];
        let distorted = distort_normalized_point(original, distortion);
        let recovered = undistort_normalized_point(distorted, distortion);

        assert!((recovered[0] - original[0]).abs() < 1.0e-4);
        assert!((recovered[1] - original[1]).abs() < 1.0e-4);
    }

    #[test]
    fn lens_distortion_affects_segmentation_rays() {
        let mut scene = box_scene();
        update_camera(&mut scene, |camera| {
            camera.intrinsics = Some(CameraIntrinsics {
                fx: 1.0,
                fy: 1.0,
                cx: 2.0,
                cy: 2.0,
                skew: 0.0,
            });
        });
        assert_eq!(segmentation_center(&scene), 0);

        update_camera(&mut scene, |camera| {
            camera.distortion = Some(CameraDistortion {
                k1: 5.0,
                k2: 0.0,
                p1: 0.0,
                p2: 0.0,
                k3: 0.0,
            });
        });

        assert!(
            segmentation_center(&scene) > 0,
            "distortion should undistort the output pixel before ray casting"
        );
    }

    #[test]
    fn camera_depth_range_clips_segmentation_hits() {
        let mut scene = box_scene();
        assert!(segmentation_center(&scene) > 0);

        update_camera(&mut scene, |camera| {
            camera.depth_range_m = Some([0.0, 1.0]);
        });

        assert_eq!(segmentation_center(&scene), 0);
    }

    #[test]
    fn scene_directional_light_brightens_debug_rgb_render() {
        let mut scene = SceneGraph::empty();
        scene.add_entity(EntityMetadata {
            id: EntityId("box".to_string()),
            name: "Box".to_string(),
            kind: "object".to_string(),
            robot_id: None,
            link_name: None,
        });
        scene.add_entity(EntityMetadata {
            id: EntityId("camera".to_string()),
            name: "Camera".to_string(),
            kind: "camera".to_string(),
            robot_id: None,
            link_name: None,
        });
        scene.root.children.push(SceneNode::mesh(
            "box",
            "Box",
            Geometry::Box {
                size: [1.0, 1.0, 1.0],
            },
            Material {
                color_rgb: [220, 220, 220],
            },
            Transform::translated([2.0, 0.0, 0.0]),
        ));
        scene.root.children.push(SceneNode::camera(
            "camera",
            "Camera",
            CameraSpec {
                id: "camera".to_string(),
                name: "Camera".to_string(),
                transform: Transform::default(),
                fov_deg: 60.0,
                projection: CameraProjection::Perspective,
                resolution: [5, 5],
                intrinsics: None,
                distortion: None,
                depth_range_m: None,
                sensor_effects: None,
            },
        ));
        scene.root.children.push(SceneNode::light(
            "sun",
            "Sun",
            LightSpec {
                id: "sun".to_string(),
                name: "Sun".to_string(),
                transform: Transform::default(),
                kind: LightKind::Directional {
                    direction: [1.0, 0.0, 0.0],
                    angular_radius_deg: 0.0,
                },
                color_rgb: [255, 255, 255],
                intensity: 1.0,
            },
        ));

        let renderer = NativeRenderer::new();
        let camera = find_camera(&scene.root, "camera").expect("camera");
        let entity_ids = entity_u32_map(&scene);
        let objects = render_objects(
            &scene.root,
            Transform::default(),
            &entity_ids,
            &renderer.mesh_cache,
            0.0,
            None,
        )
        .expect("objects");
        let lights = render_lights(&scene.root, Transform::default());
        let settings = PreparedRenderSettings::from_settings(RenderSettings::default());

        assert_eq!(lights.len(), 1);
        let ambient = renderer.trace_debug_rgb(&camera, [5, 5], 2, 2, &objects, &[], &settings);
        let lit = renderer.trace_debug_rgb(&camera, [5, 5], 2, 2, &objects, &lights, &settings);

        assert!(
            lit[0] > ambient[0],
            "scene light should brighten center hit"
        );
    }

    #[test]
    fn gltf_loader_preserves_node_transform_and_material_color() {
        let (dir, gltf_path) = write_test_gltf("gltf-test");

        let triangles = load_gltf_mesh(&gltf_path, [1.0, 1.0, 1.0])
            .expect("load gltf")
            .triangles;
        fs::remove_dir_all(&dir).ok();

        assert_eq!(triangles.len(), 1);
        assert_eq!(triangles[0].a, [1.0, 2.0, 3.0]);
        assert_eq!(triangles[0].b, [2.0, 2.0, 3.0]);
        assert_eq!(triangles[0].c, [1.0, 3.0, 3.0]);
        assert_eq!(triangles[0].color_rgba, [26, 128, 230, 255]);
        assert_rgb_close(triangles[0].color_linear_rgb, [0.1, 0.5, 0.9], 1.0e-6);
        assert_eq!(triangles[0].normal_a, [0.0, 0.0, 1.0]);
        assert_eq!(triangles[0].normal_b, [0.0, 0.0, 1.0]);
        assert_eq!(triangles[0].normal_c, [0.0, 0.0, 1.0]);

        let sample = interpolated_material_sample(&triangles[0], &[], 0.2, 0.2);
        assert_rgb_close(sample.color_linear_rgb, [0.1, 0.5, 0.9], 1.0e-6);
    }

    #[test]
    fn gltf_loader_applies_selected_khr_materials_variant() {
        let (dir, gltf_path) = write_material_variants_test_gltf("material-variant-test");

        let default_triangles = load_gltf_mesh(&gltf_path, [1.0, 1.0, 1.0])
            .expect("load default variant gltf")
            .triangles;
        let variant_triangles =
            load_gltf_mesh_at_time(&gltf_path, [1.0, 1.0, 1.0], 0.0, Some("blue"))
                .expect("load selected variant gltf")
                .triangles;
        fs::remove_dir_all(&dir).ok();

        assert_eq!(default_triangles.len(), 1);
        assert_eq!(variant_triangles.len(), 1);
        assert_eq!(default_triangles[0].color_rgba, [255, 0, 0, 255]);
        assert_eq!(variant_triangles[0].color_rgba, [0, 0, 255, 255]);
    }

    #[test]
    fn gltf_loader_applies_mesh_default_morph_weights() {
        let (dir, gltf_path) = write_morph_target_test_gltf("morph-mesh-weight-test", 0.5, None);

        let triangles = load_gltf_mesh(&gltf_path, [1.0, 1.0, 1.0])
            .expect("load morph target gltf")
            .triangles;
        fs::remove_dir_all(&dir).ok();

        assert_eq!(triangles.len(), 1);
        assert_eq!(triangles[0].a, [0.0, 0.0, 0.5]);
        assert_eq!(triangles[0].b, [1.0, 0.0, 0.5]);
        assert_eq!(triangles[0].c, [0.0, 1.0, 0.5]);
    }

    #[test]
    fn gltf_loader_node_morph_weights_override_mesh_weights() {
        let (dir, gltf_path) =
            write_morph_target_test_gltf("morph-node-weight-test", 0.0, Some(1.0));

        let triangles = load_gltf_mesh(&gltf_path, [1.0, 1.0, 1.0])
            .expect("load node morph target gltf")
            .triangles;
        fs::remove_dir_all(&dir).ok();

        assert_eq!(triangles.len(), 1);
        assert_eq!(triangles[0].a, [0.0, 0.0, 1.0]);
        assert_eq!(triangles[0].b, [1.0, 0.0, 1.0]);
        assert_eq!(triangles[0].c, [0.0, 1.0, 1.0]);

        let expected_normal = normalize([0.0, 1.0, 1.0]);
        assert!((triangles[0].normal_a[0] - expected_normal[0]).abs() < 1.0e-6);
        assert!((triangles[0].normal_a[1] - expected_normal[1]).abs() < 1.0e-6);
        assert!((triangles[0].normal_a[2] - expected_normal[2]).abs() < 1.0e-6);
    }

    #[test]
    fn gltf_loader_samples_morph_weight_animations() {
        let (dir, gltf_path) =
            write_morph_weight_animation_test_gltf("morph-weight-animation-test");

        let triangles = load_gltf_mesh_at_time(&gltf_path, [1.0, 1.0, 1.0], 0.5, None)
            .expect("load morph weight animation gltf")
            .triangles;
        fs::remove_dir_all(&dir).ok();

        assert_eq!(triangles.len(), 1);
        assert_eq!(triangles[0].a, [0.0, 0.0, 0.5]);
        assert_eq!(triangles[0].b, [1.0, 0.0, 0.5]);
        assert_eq!(triangles[0].c, [0.0, 1.0, 0.5]);
    }

    #[test]
    fn cubic_spline_vec3_animation_uses_authored_tangents() {
        let sampled = sample_vec3_animation(
            &[0.0, 1.0],
            &[
                [0.0, 0.0, 0.0],
                [0.0, 0.0, 0.0],
                [4.0, 0.0, 0.0],
                [0.0, 0.0, 0.0],
                [1.0, 0.0, 0.0],
                [0.0, 0.0, 0.0],
            ],
            gltf::animation::Interpolation::CubicSpline,
            0.5,
        )
        .expect("sample cubic vec3");

        assert!((sampled[0] - 1.0).abs() < 1.0e-6);
        assert_eq!(sampled[1], 0.0);
        assert_eq!(sampled[2], 0.0);
    }

    #[test]
    fn cubic_spline_morph_weight_animation_uses_authored_tangents() {
        let sampled = sample_morph_weights_animation(
            &[0.0, 1.0],
            &[0.0, 0.0, 4.0, 0.0, 1.0, 0.0],
            gltf::animation::Interpolation::CubicSpline,
            0.5,
        )
        .expect("sample cubic morph weights");

        assert_eq!(sampled.len(), 1);
        assert!((sampled[0] - 1.0).abs() < 1.0e-6);
    }

    #[test]
    fn gltf_loader_applies_skin_joint_weights_in_mesh_space() {
        let (dir, gltf_path) = write_skinned_test_gltf("skinned-mesh-test");

        let triangles = load_gltf_mesh(&gltf_path, [1.0, 1.0, 1.0])
            .expect("load skinned gltf")
            .triangles;
        fs::remove_dir_all(&dir).ok();

        assert_eq!(triangles.len(), 1);
        assert_eq!(triangles[0].a, [2.0, 0.0, 1.0]);
        assert_eq!(triangles[0].b, [3.0, 0.0, 1.0]);
        assert_eq!(triangles[0].c, [2.0, 1.0, 1.0]);
        assert_eq!(triangles[0].normal_a, [0.0, 0.0, 1.0]);
    }

    #[test]
    fn gltf_loader_samples_linear_translation_and_scale_animations() {
        let (dir, gltf_path) = write_animated_trs_test_gltf("animated-trs-test");

        let triangles = load_gltf_mesh_at_time(&gltf_path, [1.0, 1.0, 1.0], 0.5, None)
            .expect("load animated gltf")
            .triangles;
        fs::remove_dir_all(&dir).ok();

        assert_eq!(triangles.len(), 1);
        assert_eq!(triangles[0].a, [0.0, 0.0, 0.5]);
        assert_eq!(triangles[0].b, [1.5, 0.0, 0.5]);
        assert_eq!(triangles[0].c, [0.0, 1.5, 0.5]);
    }

    #[test]
    fn shutter_samples_evaluate_animated_gltf_at_exposure_times() {
        let (dir, gltf_path) = write_animated_trs_test_gltf("animated-shutter-test");
        let mut scene = SceneGraph::empty();
        scene.add_entity(EntityMetadata {
            id: EntityId("mesh".to_string()),
            name: "Mesh".to_string(),
            kind: "object".to_string(),
            robot_id: None,
            link_name: None,
        });
        scene.root.children.push(SceneNode::mesh(
            "mesh",
            "Mesh",
            Geometry::MeshAsset {
                asset: gltf_path.display().to_string(),
                scale: [1.0, 1.0, 1.0],
                bounds: None,
            },
            Material::default(),
            Transform::default(),
        ));

        let renderer = NativeRenderer::new();
        let entity_ids = entity_u32_map(&scene);
        let scene_lights = render_lights(&scene.root, Transform::default());
        let center_objects = render_objects(
            &scene.root,
            Transform::default(),
            &entity_ids,
            &renderer.mesh_cache,
            0.5,
            None,
        )
        .expect("center objects");
        let center_lights = lights_for_objects(&scene_lights, &center_objects);
        let center_object_bvh = Arc::new(ObjectBvh::from_objects(&center_objects));
        let center = PreparedScene {
            camera: None,
            object_bvh: center_object_bvh,
            objects: center_objects,
            lights: center_lights,
            scene_lights,
            state: None,
            entities: scene.entities.clone(),
        };
        let samples = renderer
            .shutter_samples_for_scene(
                &scene,
                &entity_ids,
                &center,
                0.5,
                [5, 5],
                Some(ShutterPolicy {
                    exposure_sec: 1.0,
                    samples: 2,
                    ..ShutterPolicy::default()
                }),
                None,
            )
            .expect("shutter samples");
        fs::remove_dir_all(&dir).ok();

        let triangle_a = |sample: &RenderSceneSample| match sample.objects[0].geometry.as_ref() {
            RenderGeometry::Triangles { triangles, .. } => triangles[0].a,
            _ => panic!("expected triangle geometry"),
        };

        assert_eq!(samples.len(), 2);
        assert_eq!(triangle_a(&samples[0]), [0.0, 0.0, 0.0]);
        assert_eq!(triangle_a(&samples[1]), [0.0, 0.0, 1.0]);
    }

    #[test]
    fn normal_transform_uses_inverse_transpose_for_nonuniform_scale() {
        let normal = normalize(transform_normal4(
            mat4_scale([2.0, 1.0, 1.0]),
            normalize([1.0, 1.0, 0.0]),
        ));
        let expected = normalize([0.5, 1.0, 0.0]);

        assert!((normal[0] - expected[0]).abs() < 1.0e-6);
        assert!((normal[1] - expected[1]).abs() < 1.0e-6);
        assert!((normal[2] - expected[2]).abs() < 1.0e-6);
    }

    #[test]
    fn gltf_loader_expands_triangle_strip_primitives() {
        let (dir, gltf_path) = write_triangle_strip_test_gltf("triangle-strip-test");

        let triangles = load_gltf_mesh(&gltf_path, [1.0, 1.0, 1.0])
            .expect("load triangle strip gltf")
            .triangles;
        fs::remove_dir_all(&dir).ok();

        assert_eq!(triangles.len(), 2);
        assert_eq!(triangles[0].a, [0.0, 0.0, 0.0]);
        assert_eq!(triangles[0].b, [1.0, 0.0, 0.0]);
        assert_eq!(triangles[0].c, [0.0, 1.0, 0.0]);
        assert_eq!(triangles[1].a, [0.0, 1.0, 0.0]);
        assert_eq!(triangles[1].b, [1.0, 0.0, 0.0]);
        assert_eq!(triangles[1].c, [1.0, 1.0, 0.0]);
    }

    #[test]
    fn triangle_fan_indices_expand_to_triangle_list() {
        assert_eq!(
            triangle_index_triplets(Mode::TriangleFan, &[0, 1, 2, 3]),
            vec![[0, 1, 2], [0, 2, 3]]
        );
    }

    fn test_triangle(alpha_cutoff: Option<f32>, double_sided: bool) -> Triangle {
        Triangle {
            a: [0.0, 0.0, 0.0],
            b: [1.0, 0.0, 0.0],
            c: [0.0, 1.0, 0.0],
            normal_a: [0.0, 0.0, 1.0],
            normal_b: [0.0, 0.0, 1.0],
            normal_c: [0.0, 0.0, 1.0],
            color_rgba: [255, 255, 255, 255],
            color_linear_rgb: [1.0, 1.0, 1.0],
            color_alpha: 1.0,
            vertex_color_a: None,
            vertex_color_b: None,
            vertex_color_c: None,
            vertex_color_linear_a: None,
            vertex_color_linear_b: None,
            vertex_color_linear_c: None,
            texcoord_a: Some([0.25, 0.25]),
            texcoord_b: Some([0.25, 0.25]),
            texcoord_c: Some([0.25, 0.25]),
            texcoord1_a: None,
            texcoord1_b: None,
            texcoord1_c: None,
            tangent_a: None,
            tangent_b: None,
            tangent_c: None,
            texture_index: Some(0),
            texture_texcoord: 0,
            texture_transform: super::TextureTransform2D::default(),
            metallic_factor: 0.0,
            roughness_factor: 1.0,
            metallic_roughness_texture_index: None,
            metallic_roughness_texture_texcoord: 0,
            metallic_roughness_texture_transform: super::TextureTransform2D::default(),
            specular_glossiness_texture_index: None,
            specular_glossiness_texture_texcoord: 0,
            specular_glossiness_texture_transform: super::TextureTransform2D::default(),
            ior: 1.5,
            transmission_factor: 0.0,
            transmission_texture_index: None,
            transmission_texture_texcoord: 0,
            transmission_texture_transform: super::TextureTransform2D::default(),
            diffuse_transmission_factor: 0.0,
            diffuse_transmission_texture_index: None,
            diffuse_transmission_texture_texcoord: 0,
            diffuse_transmission_texture_transform: super::TextureTransform2D::default(),
            diffuse_transmission_color_factor: [1.0, 1.0, 1.0],
            diffuse_transmission_color_texture_index: None,
            diffuse_transmission_color_texture_texcoord: 0,
            diffuse_transmission_color_texture_transform: super::TextureTransform2D::default(),
            dispersion: 0.0,
            volume_thickness_factor: 0.0,
            volume_thickness_texture_index: None,
            volume_thickness_texture_texcoord: 0,
            volume_thickness_texture_transform: super::TextureTransform2D::default(),
            volume_attenuation_distance: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            clearcoat_factor: 0.0,
            clearcoat_texture_index: None,
            clearcoat_texture_texcoord: 0,
            clearcoat_texture_transform: super::TextureTransform2D::default(),
            clearcoat_roughness_factor: 1.0,
            clearcoat_roughness_texture_index: None,
            clearcoat_roughness_texture_texcoord: 0,
            clearcoat_roughness_texture_transform: super::TextureTransform2D::default(),
            clearcoat_normal_texture_index: None,
            clearcoat_normal_texture_texcoord: 0,
            clearcoat_normal_texture_transform: super::TextureTransform2D::default(),
            clearcoat_normal_scale: 1.0,
            sheen_color_factor: [0.0, 0.0, 0.0],
            sheen_color_texture_index: None,
            sheen_color_texture_texcoord: 0,
            sheen_color_texture_transform: super::TextureTransform2D::default(),
            sheen_roughness_factor: 1.0,
            sheen_roughness_texture_index: None,
            sheen_roughness_texture_texcoord: 0,
            sheen_roughness_texture_transform: super::TextureTransform2D::default(),
            anisotropy_strength: 0.0,
            anisotropy_rotation: 0.0,
            anisotropy_texture_index: None,
            anisotropy_texture_texcoord: 0,
            anisotropy_texture_transform: super::TextureTransform2D::default(),
            iridescence_factor: 0.0,
            iridescence_texture_index: None,
            iridescence_texture_texcoord: 0,
            iridescence_texture_transform: super::TextureTransform2D::default(),
            iridescence_ior: 1.3,
            iridescence_thickness_minimum_nm: 100.0,
            iridescence_thickness_maximum_nm: 400.0,
            iridescence_thickness_texture_index: None,
            iridescence_thickness_texture_texcoord: 0,
            iridescence_thickness_texture_transform: super::TextureTransform2D::default(),
            specular_factor: 1.0,
            specular_texture_index: None,
            specular_texture_texcoord: 0,
            specular_texture_transform: super::TextureTransform2D::default(),
            specular_color_factor: [1.0, 1.0, 1.0],
            specular_color_texture_index: None,
            specular_color_texture_texcoord: 0,
            specular_color_texture_transform: super::TextureTransform2D::default(),
            normal_texture_index: None,
            normal_texture_texcoord: 0,
            normal_texture_transform: super::TextureTransform2D::default(),
            normal_scale: 1.0,
            emissive_rgb: [0.0, 0.0, 0.0],
            emissive_texture_index: None,
            emissive_texture_texcoord: 0,
            emissive_texture_transform: super::TextureTransform2D::default(),
            occlusion_texture_index: None,
            occlusion_texture_texcoord: 0,
            occlusion_texture_transform: super::TextureTransform2D::default(),
            occlusion_strength: 1.0,
            unlit: false,
            alpha_cutoff,
            alpha_mode: if alpha_cutoff.is_some() {
                MaterialAlphaMode::Mask
            } else {
                MaterialAlphaMode::Opaque
            },
            double_sided,
        }
    }

    fn ray_facing_triangle(x: f32, color_rgba: [u8; 4]) -> Triangle {
        Triangle {
            a: [x, -0.5, -0.5],
            b: [x, -0.5, 0.5],
            c: [x, 0.5, -0.5],
            normal_a: [-1.0, 0.0, 0.0],
            normal_b: [-1.0, 0.0, 0.0],
            normal_c: [-1.0, 0.0, 0.0],
            color_rgba,
            color_linear_rgb: srgb_u8_to_linear_rgb([color_rgba[0], color_rgba[1], color_rgba[2]]),
            color_alpha: f32::from(color_rgba[3]) / 255.0,
            vertex_color_a: None,
            vertex_color_b: None,
            vertex_color_c: None,
            vertex_color_linear_a: None,
            vertex_color_linear_b: None,
            vertex_color_linear_c: None,
            texcoord_a: None,
            texcoord_b: None,
            texcoord_c: None,
            texcoord1_a: None,
            texcoord1_b: None,
            texcoord1_c: None,
            tangent_a: None,
            tangent_b: None,
            tangent_c: None,
            texture_index: None,
            texture_texcoord: 0,
            texture_transform: super::TextureTransform2D::default(),
            metallic_factor: 0.0,
            roughness_factor: 1.0,
            metallic_roughness_texture_index: None,
            metallic_roughness_texture_texcoord: 0,
            metallic_roughness_texture_transform: super::TextureTransform2D::default(),
            specular_glossiness_texture_index: None,
            specular_glossiness_texture_texcoord: 0,
            specular_glossiness_texture_transform: super::TextureTransform2D::default(),
            ior: 1.5,
            transmission_factor: 0.0,
            transmission_texture_index: None,
            transmission_texture_texcoord: 0,
            transmission_texture_transform: super::TextureTransform2D::default(),
            diffuse_transmission_factor: 0.0,
            diffuse_transmission_texture_index: None,
            diffuse_transmission_texture_texcoord: 0,
            diffuse_transmission_texture_transform: super::TextureTransform2D::default(),
            diffuse_transmission_color_factor: [1.0, 1.0, 1.0],
            diffuse_transmission_color_texture_index: None,
            diffuse_transmission_color_texture_texcoord: 0,
            diffuse_transmission_color_texture_transform: super::TextureTransform2D::default(),
            dispersion: 0.0,
            volume_thickness_factor: 0.0,
            volume_thickness_texture_index: None,
            volume_thickness_texture_texcoord: 0,
            volume_thickness_texture_transform: super::TextureTransform2D::default(),
            volume_attenuation_distance: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            clearcoat_factor: 0.0,
            clearcoat_texture_index: None,
            clearcoat_texture_texcoord: 0,
            clearcoat_texture_transform: super::TextureTransform2D::default(),
            clearcoat_roughness_factor: 1.0,
            clearcoat_roughness_texture_index: None,
            clearcoat_roughness_texture_texcoord: 0,
            clearcoat_roughness_texture_transform: super::TextureTransform2D::default(),
            clearcoat_normal_texture_index: None,
            clearcoat_normal_texture_texcoord: 0,
            clearcoat_normal_texture_transform: super::TextureTransform2D::default(),
            clearcoat_normal_scale: 1.0,
            sheen_color_factor: [0.0, 0.0, 0.0],
            sheen_color_texture_index: None,
            sheen_color_texture_texcoord: 0,
            sheen_color_texture_transform: super::TextureTransform2D::default(),
            sheen_roughness_factor: 1.0,
            sheen_roughness_texture_index: None,
            sheen_roughness_texture_texcoord: 0,
            sheen_roughness_texture_transform: super::TextureTransform2D::default(),
            anisotropy_strength: 0.0,
            anisotropy_rotation: 0.0,
            anisotropy_texture_index: None,
            anisotropy_texture_texcoord: 0,
            anisotropy_texture_transform: super::TextureTransform2D::default(),
            iridescence_factor: 0.0,
            iridescence_texture_index: None,
            iridescence_texture_texcoord: 0,
            iridescence_texture_transform: super::TextureTransform2D::default(),
            iridescence_ior: 1.3,
            iridescence_thickness_minimum_nm: 100.0,
            iridescence_thickness_maximum_nm: 400.0,
            iridescence_thickness_texture_index: None,
            iridescence_thickness_texture_texcoord: 0,
            iridescence_thickness_texture_transform: super::TextureTransform2D::default(),
            specular_factor: 1.0,
            specular_texture_index: None,
            specular_texture_texcoord: 0,
            specular_texture_transform: super::TextureTransform2D::default(),
            specular_color_factor: [1.0, 1.0, 1.0],
            specular_color_texture_index: None,
            specular_color_texture_texcoord: 0,
            specular_color_texture_transform: super::TextureTransform2D::default(),
            normal_texture_index: None,
            normal_texture_texcoord: 0,
            normal_texture_transform: super::TextureTransform2D::default(),
            normal_scale: 1.0,
            emissive_rgb: [0.0, 0.0, 0.0],
            emissive_texture_index: None,
            emissive_texture_texcoord: 0,
            emissive_texture_transform: super::TextureTransform2D::default(),
            occlusion_texture_index: None,
            occlusion_texture_texcoord: 0,
            occlusion_texture_transform: super::TextureTransform2D::default(),
            occlusion_strength: 1.0,
            unlit: false,
            alpha_cutoff: Some(1.0 / 255.0),
            alpha_mode: MaterialAlphaMode::Blend,
            double_sided: true,
        }
    }

    fn ray_facing_rect(
        x: f32,
        y_min: f32,
        y_max: f32,
        z_min: f32,
        z_max: f32,
        color_rgba: [u8; 4],
    ) -> Vec<Triangle> {
        let mut first = ray_facing_triangle(x, color_rgba);
        first.a = [x, y_min, z_min];
        first.b = [x, y_min, z_max];
        first.c = [x, y_max, z_min];
        let mut second = ray_facing_triangle(x, color_rgba);
        second.a = [x, y_max, z_min];
        second.b = [x, y_min, z_max];
        second.c = [x, y_max, z_max];
        vec![first, second]
    }

    fn z_facing_rect(
        z: f32,
        x_min: f32,
        x_max: f32,
        y_min: f32,
        y_max: f32,
        color_rgba: [u8; 4],
    ) -> Vec<Triangle> {
        let mut first = ray_facing_triangle(x_min, color_rgba);
        first.a = [x_min, y_min, z];
        first.b = [x_max, y_min, z];
        first.c = [x_min, y_max, z];
        first.normal_a = [0.0, 0.0, -1.0];
        first.normal_b = [0.0, 0.0, -1.0];
        first.normal_c = [0.0, 0.0, -1.0];

        let mut second = ray_facing_triangle(x_min, color_rgba);
        second.a = [x_min, y_max, z];
        second.b = [x_max, y_min, z];
        second.c = [x_max, y_max, z];
        second.normal_a = [0.0, 0.0, -1.0];
        second.normal_b = [0.0, 0.0, -1.0];
        second.normal_c = [0.0, 0.0, -1.0];
        vec![first, second]
    }

    #[test]
    fn masked_alpha_below_cutoff_does_not_hit_triangle() {
        let triangle = test_triangle(Some(0.5), false);
        let texture = TextureImage {
            width: 1,
            height: 1,
            rgba: vec![255, 0, 0, 0],
            wrap_s: TextureWrap::Repeat,
            wrap_t: TextureWrap::Repeat,
            filter: TextureFilter::Nearest,
        };
        let ray = Ray {
            origin: [0.25, 0.25, 1.0],
            dir: [0.0, 0.0, -1.0],
        };

        assert!(intersect_triangle(&ray, &triangle, &[texture]).is_none());
    }

    #[test]
    fn opaque_alpha_mode_ignores_base_and_texture_alpha() {
        let mut triangle = test_triangle(None, false);
        triangle.color_rgba = [255, 255, 255, 64];
        triangle.color_alpha = 64.0 / 255.0;
        triangle.alpha_mode = MaterialAlphaMode::Opaque;
        let texture = TextureImage {
            width: 1,
            height: 1,
            rgba: vec![255, 255, 255, 96],
            wrap_s: TextureWrap::Repeat,
            wrap_t: TextureWrap::Repeat,
            filter: TextureFilter::Nearest,
        };

        let sample = interpolated_material_sample(&triangle, &[texture], 0.0, 0.0);

        assert_eq!(sample.alpha, 1.0);
    }

    #[test]
    fn mask_alpha_mode_is_opaque_after_cutoff_passes() {
        let mut triangle = test_triangle(Some(0.5), false);
        triangle.color_rgba = [255, 255, 255, 192];
        triangle.color_alpha = 192.0 / 255.0;
        triangle.alpha_mode = MaterialAlphaMode::Mask;

        let sample = interpolated_material_sample(&triangle, &[], 0.0, 0.0);

        assert_eq!(sample.alpha, 1.0);
    }

    #[test]
    fn blend_alpha_mode_preserves_fractional_base_and_texture_alpha() {
        let mut triangle = test_triangle(None, false);
        triangle.color_rgba = [255, 255, 255, 128];
        triangle.color_alpha = 128.0 / 255.0;
        triangle.alpha_mode = MaterialAlphaMode::Blend;
        let texture = TextureImage {
            width: 1,
            height: 1,
            rgba: vec![255, 255, 255, 128],
            wrap_s: TextureWrap::Repeat,
            wrap_t: TextureWrap::Repeat,
            filter: TextureFilter::Nearest,
        };

        let sample = interpolated_material_sample(&triangle, &[texture], 0.0, 0.0);

        assert!((sample.alpha - (128.0 / 255.0) * (128.0 / 255.0)).abs() < 1.0e-6);
    }

    #[test]
    fn blend_alpha_mode_uses_authored_float_alpha_without_u8_quantization() {
        let mut triangle = test_triangle(None, false);
        triangle.color_rgba = [255, 255, 255, 26];
        triangle.color_alpha = 0.1;
        triangle.alpha_mode = MaterialAlphaMode::Blend;

        let sample = interpolated_material_sample(&triangle, &[], 0.0, 0.0);

        assert!((sample.alpha - 0.1).abs() < 1.0e-6);
        assert!(
            (sample.alpha - (26.0 / 255.0)).abs() > 1.0e-3,
            "blend alpha must not be reconstructed from rounded display bytes"
        );
    }

    #[test]
    fn single_sided_triangle_culls_backface_but_double_sided_hits() {
        let ray = Ray {
            origin: [0.25, 0.25, -1.0],
            dir: [0.0, 0.0, 1.0],
        };

        assert!(intersect_triangle(&ray, &test_triangle(None, false), &[]).is_none());
        assert!(intersect_triangle(&ray, &test_triangle(None, true), &[]).is_some());
    }

    #[test]
    fn refractive_boundary_mode_hits_single_sided_transmissive_backface() {
        let ray = Ray {
            origin: [0.25, 0.25, -1.0],
            dir: [0.0, 0.0, 1.0],
        };
        let mut triangle = test_triangle(None, false);
        triangle.transmission_factor = 1.0;
        triangle.color_rgba[3] = 0;
        triangle.color_alpha = 0.0;

        assert!(intersect_triangle(&ray, &triangle, &[]).is_none());
        assert!(
            intersect_triangle_with_mode(
                &ray,
                &triangle,
                &[],
                TriangleIntersectionMode::RefractiveBoundary
            )
            .is_some()
        );
    }

    #[test]
    fn gltf_loader_preserves_base_color_texture_and_uvs() {
        let (dir, gltf_path) = write_textured_test_gltf("texture-test");

        let mesh = load_gltf_mesh(&gltf_path, [1.0, 1.0, 1.0]).expect("load textured gltf");
        fs::remove_dir_all(&dir).ok();

        assert_eq!(mesh.triangles.len(), 1);
        assert_eq!(mesh.textures.len(), 1);
        assert_eq!(mesh.triangles[0].texture_index, Some(0));
        assert_eq!(
            interpolated_material_sample(&mesh.triangles[0], &mesh.textures, 0.2, 0.2).color_rgb,
            [255, 0, 0]
        );
    }

    #[test]
    fn gltf_loader_applies_khr_texture_transform_to_base_color_texture() {
        let (dir, gltf_path) = write_texture_transform_test_gltf("texture-transform-test");

        let mesh =
            load_gltf_mesh(&gltf_path, [1.0, 1.0, 1.0]).expect("load texture transform gltf");
        fs::remove_dir_all(&dir).ok();

        assert_eq!(mesh.triangles.len(), 1);
        assert_eq!(mesh.triangles[0].texture_transform.offset, [0.5, 0.0]);
        assert_eq!(
            interpolated_material_sample(&mesh.triangles[0], &mesh.textures, 0.2, 0.2).color_rgb,
            [0, 255, 0],
            "KHR_texture_transform offset should move the UV from red to green texel"
        );
    }

    #[test]
    fn gltf_loader_preserves_occlusion_texture_for_ambient_shading() {
        let (dir, gltf_path) = write_occlusion_test_gltf("occlusion-test");

        let mesh = load_gltf_mesh(&gltf_path, [1.0, 1.0, 1.0]).expect("load occlusion gltf");
        fs::remove_dir_all(&dir).ok();

        assert_eq!(mesh.triangles.len(), 1);
        assert_eq!(mesh.textures.len(), 1);
        assert_eq!(mesh.triangles[0].occlusion_texture_index, Some(0));
        assert_eq!(mesh.triangles[0].occlusion_texture_texcoord, 0);
        assert!((mesh.triangles[0].occlusion_strength - 0.5).abs() < 1.0e-6);

        let sample = interpolated_material_sample(&mesh.triangles[0], &mesh.textures, 0.2, 0.2);
        let expected_occlusion = 1.0 - 0.5 * (1.0 - 64.0 / 255.0);
        assert!((sample.occlusion - expected_occlusion).abs() < 1.0e-6);
    }

    #[test]
    fn occlusion_darkens_ambient_debug_rgb_shading() {
        let unoccluded = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [200, 200, 200],
            color_linear_rgb: srgb_u8_to_linear_rgb([200, 200, 200]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 1.0,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [0.0, 0.0, -1.0],
            view_dir: [0.0, 0.0, 1.0],
        };
        let occluded = Hit {
            occlusion: 0.25,
            ..unoccluded
        };

        assert!(shade_hit(&occluded, &[])[0] < shade_hit(&unoccluded, &[])[0]);
    }

    #[test]
    fn material_occlusion_does_not_darken_direct_lighting() {
        let light = LightSample {
            direction: [0.0, 0.0, 1.0],
            color_rgb: [1.0, 0.8, 0.6],
            intensity: 1.0,
            max_t: None,
        };
        let unoccluded = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [200, 160, 120],
            color_linear_rgb: srgb_u8_to_linear_rgb([200, 160, 120]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.15,
            roughness: 0.35,
            ior: 1.5,
            clearcoat_factor: 0.6,
            clearcoat_roughness: 0.2,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.4, 0.2, 0.1],
            sheen_roughness: 0.45,
            anisotropy_strength: 0.25,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.4,
            iridescence_thickness_nm: 420.0,
            iridescence_ior: 1.3,
            specular_factor: 0.8,
            specular_color: [1.0, 0.9, 0.8],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [0.0, 0.0, 1.0],
            view_dir: [0.0, 0.0, 1.0],
        };
        let occluded = Hit {
            occlusion: 0.1,
            ..unoccluded
        };

        let unoccluded_rgb = shade_pbr_direct_rgb(&unoccluded, &light, [1.0, 0.75, 0.5]);
        let occluded_rgb = shade_pbr_direct_rgb(&occluded, &light, [1.0, 0.75, 0.5]);

        for channel in 0..3 {
            assert!(
                (unoccluded_rgb[channel] - occluded_rgb[channel]).abs() < 1.0e-5,
                "direct-light channel {channel} should ignore material occlusion"
            );
        }
    }

    #[test]
    fn material_occlusion_does_not_darken_direct_diffuse_transmission() {
        let light = LightSample {
            direction: [0.0, 0.0, -1.0],
            color_rgb: [1.0, 0.8, 0.6],
            intensity: 1.0,
            max_t: None,
        };
        let unoccluded = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [200, 160, 120],
            color_linear_rgb: srgb_u8_to_linear_rgb([200, 160, 120]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.75,
            diffuse_transmission_color: [0.9, 0.8, 0.7],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 0.5,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [0.0, 0.0, 1.0],
            view_dir: [0.0, 0.0, 1.0],
        };
        let occluded = Hit {
            occlusion: 0.1,
            ..unoccluded
        };

        let unoccluded_rgb = shade_pbr_direct_rgb(&unoccluded, &light, [0.5, 0.75, 1.0]);
        let occluded_rgb = shade_pbr_direct_rgb(&occluded, &light, [0.5, 0.75, 1.0]);

        for channel in 0..3 {
            assert!(
                (unoccluded_rgb[channel] - occluded_rgb[channel]).abs() < 1.0e-5,
                "direct diffuse-transmission channel {channel} should ignore material occlusion"
            );
        }
    }

    #[test]
    fn geometry_ambient_occlusion_darkens_nearby_blocked_ambient() {
        let hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [200, 200, 200],
            color_linear_rgb: srgb_u8_to_linear_rgb([200, 200, 200]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 1.0,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [1.0, 0.0, 0.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [0.0, 1.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [1.0, 0.0, 0.0],
            view_dir: [1.0, 0.0, 0.0],
        };
        let blocker = RenderObject {
            entity_u32: 2,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Sphere { radius: 0.35 }),
            color_rgb: [255, 255, 255],
            transform: Transform::translated([0.45, 0.0, 0.0]),
        };
        let unblocked = PreparedRenderSettings::from_settings(RenderSettings {
            ambient_intensity: 1.0,
            ambient_occlusion_samples: 16,
            ambient_occlusion_radius_m: 0.8,
            ambient_occlusion_intensity: 1.0,
            ..RenderSettings::default()
        });
        let disabled = PreparedRenderSettings::from_settings(RenderSettings {
            ambient_intensity: 1.0,
            ambient_occlusion_samples: 0,
            ..RenderSettings::default()
        });

        let bright = shade_hit_with_lights(&hit, &[blocker.clone()], &[], &disabled);
        let occluded = shade_hit_with_lights(&hit, &[blocker], &[], &unblocked);

        assert!(
            occluded[0] < bright[0] * 0.85,
            "nearby geometry should reduce ambient contribution"
        );
        assert_eq!(occluded[0], occluded[1]);
        assert_eq!(occluded[1], occluded[2]);
    }

    #[test]
    fn indirect_diffuse_bounces_nearby_surface_color_into_ambient() {
        let hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [200, 200, 200],
            color_linear_rgb: srgb_u8_to_linear_rgb([200, 200, 200]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 1.0,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [1.0, 0.0, 0.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [0.0, 1.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [1.0, 0.0, 0.0],
            view_dir: [1.0, 0.0, 0.0],
        };
        let red_surface = RenderObject {
            entity_u32: 2,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Sphere { radius: 0.35 }),
            color_rgb: [255, 0, 0],
            transform: Transform::translated([0.45, 0.0, 0.0]),
        };
        let disabled = PreparedRenderSettings::from_settings(RenderSettings {
            ambient_intensity: 1.0,
            indirect_diffuse_samples: 0,
            ..RenderSettings::default()
        });
        let bounced_settings = PreparedRenderSettings::from_settings(RenderSettings {
            ambient_intensity: 1.0,
            indirect_diffuse_samples: 16,
            indirect_diffuse_radius_m: 0.8,
            indirect_diffuse_intensity: 3.0,
            ..RenderSettings::default()
        });

        let plain = shade_hit_with_lights(&hit, std::slice::from_ref(&red_surface), &[], &disabled);
        let bounced = shade_hit_with_lights(&hit, &[red_surface], &[], &bounced_settings);
        let red_increase = bounced[0] as i16 - plain[0] as i16;
        let green_increase = bounced[1] as i16 - plain[1] as i16;
        let blue_increase = bounced[2] as i16 - plain[2] as i16;

        assert!(
            red_increase > 10,
            "red surface should add visible red bounce"
        );
        assert!(
            red_increase > green_increase * 3 && red_increase > blue_increase * 3,
            "red bounce should dominate green and blue increases"
        );
    }

    #[test]
    fn indirect_diffuse_does_not_treat_unlit_albedo_as_light() {
        let hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [200, 200, 200],
            color_linear_rgb: srgb_u8_to_linear_rgb([200, 200, 200]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 1.0,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [1.0, 0.0, 0.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [0.0, 1.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [1.0, 0.0, 0.0],
            view_dir: [1.0, 0.0, 0.0],
        };
        let unlit_red_surface = RenderObject {
            entity_u32: 2,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Sphere { radius: 0.35 }),
            color_rgb: [255, 0, 0],
            transform: Transform::translated([0.45, 0.0, 0.0]),
        };
        let settings = PreparedRenderSettings::from_settings(RenderSettings {
            ambient_rgb: [0, 0, 0],
            ambient_intensity: 1.0,
            indirect_diffuse_samples: 16,
            indirect_diffuse_radius_m: 0.8,
            indirect_diffuse_intensity: 4.0,
            indirect_diffuse_bounces: 2,
            ..RenderSettings::default()
        });

        let shaded = shade_hit_with_lights(&hit, &[unlit_red_surface], &[], &settings);

        assert_eq!(shaded, [0.0, 0.0, 0.0]);
    }

    #[test]
    fn indirect_diffuse_secondary_bounce_carries_hidden_surface_color() {
        let hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [200, 200, 200],
            color_linear_rgb: srgb_u8_to_linear_rgb([200, 200, 200]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 1.0,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [1.0, 0.0, 0.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [0.0, 1.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [1.0, 0.0, 0.0],
            view_dir: [1.0, 0.0, 0.0],
        };
        let white_relay = RenderObject {
            entity_u32: 2,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Sphere { radius: 0.18 }),
            color_rgb: [255, 255, 255],
            transform: Transform::translated([0.45, 0.0, 0.0]),
        };
        let hidden_red = RenderObject {
            entity_u32: 3,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Sphere { radius: 0.28 }),
            color_rgb: [255, 0, 0],
            transform: Transform::translated([-0.35, 0.0, 0.0]),
        };
        let hidden_light = RenderLight {
            kind: RenderLightKind::Point {
                position: [0.05, 0.65, 0.0],
                range_m: None,
            },
            color_rgb: [1.0, 1.0, 1.0],
            intensity: 20.0,
        };
        let one_bounce_settings = PreparedRenderSettings::from_settings(RenderSettings {
            ambient_rgb: [0, 0, 0],
            ambient_intensity: 1.0,
            indirect_diffuse_samples: 32,
            indirect_diffuse_radius_m: 1.2,
            indirect_diffuse_intensity: 4.0,
            indirect_diffuse_bounces: 1,
            ..RenderSettings::default()
        });
        let two_bounce_settings = PreparedRenderSettings::from_settings(RenderSettings {
            indirect_diffuse_bounces: 2,
            ..one_bounce_settings.settings.clone()
        });
        let objects = [white_relay, hidden_red];

        let one_bounce =
            shade_hit_with_lights(&hit, &objects, &[hidden_light], &one_bounce_settings);
        let two_bounce =
            shade_hit_with_lights(&hit, &objects, &[hidden_light], &two_bounce_settings);
        let red_delta = two_bounce[0] - one_bounce[0];
        let green_delta = two_bounce[1] - one_bounce[1];
        let blue_delta = two_bounce[2] - one_bounce[2];

        assert!(red_delta > 8.0);
        assert!(
            red_delta > green_delta * 3.0 && red_delta > blue_delta * 3.0,
            "secondary red surface should add mostly red indirect energy"
        );
    }

    #[test]
    fn gltf_loader_preserves_metallic_roughness_factors_and_texture() {
        let (dir, gltf_path) = write_metallic_roughness_test_gltf("metallic-roughness-test");

        let mesh =
            load_gltf_mesh(&gltf_path, [1.0, 1.0, 1.0]).expect("load metallic roughness gltf");
        fs::remove_dir_all(&dir).ok();

        assert_eq!(mesh.triangles.len(), 1);
        assert_eq!(mesh.textures.len(), 1);
        assert_eq!(mesh.triangles[0].metallic_roughness_texture_index, Some(0));
        assert_eq!(mesh.triangles[0].metallic_roughness_texture_texcoord, 0);
        assert!((mesh.triangles[0].metallic_factor - 0.5).abs() < 1.0e-6);
        assert!((mesh.triangles[0].roughness_factor - 0.5).abs() < 1.0e-6);

        let sample = interpolated_material_sample(&mesh.triangles[0], &mesh.textures, 0.2, 0.2);
        assert!((sample.metallic - (0.5 * 64.0 / 255.0)).abs() < 1.0e-6);
        assert!((sample.roughness - (0.5 * 128.0 / 255.0)).abs() < 1.0e-6);
    }

    #[test]
    fn gltf_loader_preserves_khr_materials_specular_factors_and_textures() {
        let (dir, gltf_path) = write_specular_test_gltf("specular-test");

        let mesh = load_gltf_mesh(&gltf_path, [1.0, 1.0, 1.0]).expect("load specular gltf");
        fs::remove_dir_all(&dir).ok();

        assert_eq!(mesh.triangles.len(), 1);
        assert_eq!(mesh.textures.len(), 2);
        assert!((mesh.triangles[0].specular_factor - 0.5).abs() < 1.0e-6);
        assert_eq!(mesh.triangles[0].specular_texture_index, Some(0));
        assert_eq!(mesh.triangles[0].specular_texture_texcoord, 0);
        assert_eq!(mesh.triangles[0].specular_color_texture_index, Some(1));
        assert_eq!(mesh.triangles[0].specular_color_texture_texcoord, 0);
        assert_eq!(mesh.triangles[0].specular_color_factor, [0.5, 1.0, 0.25]);

        let sample = interpolated_material_sample(&mesh.triangles[0], &mesh.textures, 0.2, 0.2);
        assert!((sample.specular_factor - (0.5 * 128.0 / 255.0)).abs() < 1.0e-6);
        let specular_color_texture = srgb_u8_to_linear_rgb([128, 64, 255]);
        assert!((sample.specular_color[0] - (0.5 * specular_color_texture[0])).abs() < 1.0e-6);
        assert!((sample.specular_color[1] - specular_color_texture[1]).abs() < 1.0e-6);
        assert!((sample.specular_color[2] - 0.25).abs() < 1.0e-6);
    }

    #[test]
    fn gltf_loader_applies_khr_materials_pbr_specular_glossiness() {
        let (dir, gltf_path) = write_specular_glossiness_test_gltf("specular-glossiness-test");

        let mesh =
            load_gltf_mesh(&gltf_path, [1.0, 1.0, 1.0]).expect("load specular glossiness gltf");
        fs::remove_dir_all(&dir).ok();

        assert_eq!(mesh.triangles.len(), 1);
        assert_eq!(mesh.textures.len(), 2);
        assert_eq!(mesh.triangles[0].color_rgba, [128, 64, 255, 204]);
        assert_eq!(mesh.triangles[0].texture_index, Some(0));
        assert_eq!(mesh.triangles[0].alpha_mode, MaterialAlphaMode::Blend);
        assert!((mesh.triangles[0].metallic_factor - 0.0).abs() < 1.0e-6);
        assert!((mesh.triangles[0].roughness_factor - 0.25).abs() < 1.0e-6);
        assert_eq!(mesh.triangles[0].metallic_roughness_texture_index, None);
        assert_eq!(mesh.triangles[0].specular_glossiness_texture_index, Some(1));
        assert_eq!(mesh.triangles[0].specular_color_texture_index, Some(1));
        assert!((mesh.triangles[0].specular_factor - 1.0).abs() < 1.0e-6);
        assert_eq!(mesh.triangles[0].specular_color_factor, [1.0, 0.5, 0.25]);
        assert!((dielectric_f0_from_ior(mesh.triangles[0].ior) - 0.8).abs() < 1.0e-4);

        let sample = interpolated_material_sample(&mesh.triangles[0], &mesh.textures, 0.2, 0.2);
        assert_eq!(sample.color_rgb, [64, 16, 255]);
        assert!((sample.alpha - ((204.0 / 255.0) * (200.0 / 255.0))).abs() < 1.0e-6);
        let expected_roughness = 1.0 - 0.75 * (128.0 / 255.0);
        assert!((sample.roughness - expected_roughness).abs() < 1.0e-6);
        let specular_glossiness_texture = srgb_u8_to_linear_rgb([64, 128, 255]);
        assert!((sample.specular_color[0] - specular_glossiness_texture[0]).abs() < 1.0e-6);
        assert!((sample.specular_color[1] - (0.5 * specular_glossiness_texture[1])).abs() < 1.0e-6);
        assert!((sample.specular_color[2] - 0.25).abs() < 1.0e-6);
    }

    #[test]
    fn gltf_loader_preserves_khr_materials_unlit_flag() {
        let (dir, gltf_path) = write_unlit_test_gltf("unlit-test");

        let mesh = load_gltf_mesh(&gltf_path, [1.0, 1.0, 1.0]).expect("load unlit gltf");
        fs::remove_dir_all(&dir).ok();

        assert_eq!(mesh.triangles.len(), 1);
        assert!(mesh.triangles[0].unlit);
        let sample = interpolated_material_sample(&mesh.triangles[0], &mesh.textures, 0.2, 0.2);
        assert_eq!(sample.color_rgb, [64, 128, 191]);
    }

    #[test]
    fn gltf_loader_preserves_khr_materials_ior() {
        let (dir, gltf_path) = write_ior_test_gltf("ior-test");

        let mesh = load_gltf_mesh(&gltf_path, [1.0, 1.0, 1.0]).expect("load ior gltf");
        fs::remove_dir_all(&dir).ok();

        assert_eq!(mesh.triangles.len(), 1);
        assert!((mesh.triangles[0].ior - 2.0).abs() < 1.0e-6);
        let sample = interpolated_material_sample(&mesh.triangles[0], &mesh.textures, 0.2, 0.2);
        assert!((sample.ior - 2.0).abs() < 1.0e-6);
    }

    #[test]
    fn gltf_loader_applies_khr_materials_dispersion() {
        let (dir, gltf_path) = write_dispersion_test_gltf("dispersion-test");

        let mesh = load_gltf_mesh(&gltf_path, [1.0, 1.0, 1.0]).expect("load dispersion gltf");
        fs::remove_dir_all(&dir).ok();

        assert_eq!(mesh.triangles.len(), 1);
        assert!((mesh.triangles[0].ior - 1.75).abs() < 1.0e-6);
        assert!((mesh.triangles[0].dispersion - 1.2).abs() < 1.0e-6);
        let sample = interpolated_material_sample(&mesh.triangles[0], &mesh.textures, 0.2, 0.2);
        assert!((sample.dispersion - 1.2).abs() < 1.0e-6);
    }

    #[test]
    fn gltf_loader_applies_khr_materials_transmission_to_debug_alpha() {
        let (dir, gltf_path) = write_transmission_test_gltf("transmission-test");

        let mesh = load_gltf_mesh(&gltf_path, [1.0, 1.0, 1.0]).expect("load transmission gltf");
        fs::remove_dir_all(&dir).ok();

        assert_eq!(mesh.triangles.len(), 1);
        assert_eq!(mesh.triangles[0].transmission_texture_index, Some(0));
        assert!((mesh.triangles[0].transmission_factor - 0.5).abs() < 1.0e-6);
        let sample = interpolated_material_sample(&mesh.triangles[0], &mesh.textures, 0.2, 0.2);
        let expected_alpha = 1.0 - 0.5 * (128.0 / 255.0);
        assert!((sample.alpha - expected_alpha).abs() < 1.0e-6);
    }

    #[test]
    fn gltf_loader_applies_khr_materials_diffuse_transmission_to_material_sample() {
        let (dir, gltf_path) = write_diffuse_transmission_test_gltf("diffuse-transmission-test");

        let mesh =
            load_gltf_mesh(&gltf_path, [1.0, 1.0, 1.0]).expect("load diffuse transmission gltf");
        fs::remove_dir_all(&dir).ok();

        assert_eq!(mesh.triangles.len(), 1);
        assert_eq!(
            mesh.triangles[0].diffuse_transmission_texture_index,
            Some(0)
        );
        assert_eq!(
            mesh.triangles[0].diffuse_transmission_color_texture_index,
            Some(1)
        );
        assert!((mesh.triangles[0].diffuse_transmission_factor - 0.5).abs() < 1.0e-6);
        assert_eq!(
            mesh.triangles[0].diffuse_transmission_color_factor,
            [1.0, 0.5, 0.25]
        );

        let sample = interpolated_material_sample(&mesh.triangles[0], &mesh.textures, 0.2, 0.2);
        let expected_transmission = 0.5 * (128.0 / 255.0);
        let color_texture = srgb_u8_to_linear_rgb([128, 255, 64]);
        assert!((sample.diffuse_transmission - expected_transmission).abs() < 1.0e-6);
        assert!((sample.diffuse_transmission_color[0] - color_texture[0]).abs() < 1.0e-6);
        assert!((sample.diffuse_transmission_color[1] - color_texture[1] * 0.5).abs() < 1.0e-6);
        assert!((sample.diffuse_transmission_color[2] - color_texture[2] * 0.25).abs() < 1.0e-6);
    }

    #[test]
    fn gltf_loader_applies_khr_materials_volume_to_transmission_filter() {
        let (dir, gltf_path) = write_volume_test_gltf("volume-test");

        let mesh = load_gltf_mesh(&gltf_path, [1.0, 1.0, 1.0]).expect("load volume gltf");
        fs::remove_dir_all(&dir).ok();

        assert_eq!(mesh.triangles.len(), 1);
        assert_eq!(mesh.triangles[0].volume_thickness_texture_index, Some(0));
        assert!((mesh.triangles[0].volume_thickness_factor - 1.0).abs() < 1.0e-6);
        assert!((mesh.triangles[0].volume_attenuation_distance - 0.5).abs() < 1.0e-6);
        assert_eq!(
            mesh.triangles[0].volume_attenuation_color,
            [1.0, 0.25, 0.25]
        );
        let sample = interpolated_material_sample(&mesh.triangles[0], &mesh.textures, 0.2, 0.2);
        assert!(
            sample.alpha <= 1.0e-6,
            "full transmission should contribute no front alpha"
        );
        assert!(
            sample.transmission_filter_rgb[0] > sample.transmission_filter_rgb[1] * 3.0,
            "volume attenuation should tint transmission toward red"
        );
        assert!(
            sample.transmission_filter_rgb[1] < 0.30,
            "green channel should be attenuated by thickness texture and distance"
        );
    }

    #[test]
    fn gltf_loader_applies_khr_materials_clearcoat_factors_and_textures() {
        let (dir, gltf_path) = write_clearcoat_test_gltf("clearcoat-test");

        let mesh = load_gltf_mesh(&gltf_path, [1.0, 1.0, 1.0]).expect("load clearcoat gltf");
        fs::remove_dir_all(&dir).ok();

        assert_eq!(mesh.triangles.len(), 1);
        assert!((mesh.triangles[0].clearcoat_factor - 0.5).abs() < 1.0e-6);
        assert_eq!(mesh.triangles[0].clearcoat_texture_index, Some(0));
        assert_eq!(mesh.triangles[0].clearcoat_texture_texcoord, 0);
        assert!((mesh.triangles[0].clearcoat_roughness_factor - 0.5).abs() < 1.0e-6);
        assert_eq!(mesh.triangles[0].clearcoat_roughness_texture_index, Some(1));
        assert_eq!(mesh.triangles[0].clearcoat_roughness_texture_texcoord, 0);
        assert_eq!(mesh.triangles[0].clearcoat_normal_texture_index, Some(2));
        assert_eq!(mesh.triangles[0].clearcoat_normal_texture_texcoord, 0);
        assert!((mesh.triangles[0].clearcoat_normal_scale - 0.5).abs() < 1.0e-6);

        let sample = interpolated_material_sample(&mesh.triangles[0], &mesh.textures, 0.2, 0.2);
        assert!((sample.clearcoat_factor - (0.5 * 128.0 / 255.0)).abs() < 1.0e-6);
        assert!((sample.clearcoat_roughness - (0.5 * 64.0 / 255.0)).abs() < 1.0e-6);

        let hit = intersect_triangle(
            &Ray {
                origin: [0.25, 0.25, 1.0],
                dir: [0.0, 0.0, -1.0],
            },
            &mesh.triangles[0],
            &mesh.textures,
        )
        .expect("ray should hit clearcoat test triangle");
        assert!(
            hit.clearcoat_normal[0] > 0.40 && hit.clearcoat_normal[2] > 0.85,
            "clearcoat normal texture should tilt only the clearcoat normal"
        );
    }

    #[test]
    fn gltf_loader_applies_khr_materials_sheen_factors_and_textures() {
        let (dir, gltf_path) = write_sheen_test_gltf("sheen-test");

        let mesh = load_gltf_mesh(&gltf_path, [1.0, 1.0, 1.0]).expect("load sheen gltf");
        fs::remove_dir_all(&dir).ok();

        assert_eq!(mesh.triangles.len(), 1);
        assert_eq!(mesh.triangles[0].sheen_color_factor, [0.5, 1.0, 0.25]);
        assert_eq!(mesh.triangles[0].sheen_color_texture_index, Some(0));
        assert_eq!(mesh.triangles[0].sheen_color_texture_texcoord, 0);
        assert!((mesh.triangles[0].sheen_roughness_factor - 0.5).abs() < 1.0e-6);
        assert_eq!(mesh.triangles[0].sheen_roughness_texture_index, Some(1));
        assert_eq!(mesh.triangles[0].sheen_roughness_texture_texcoord, 0);

        let sample = interpolated_material_sample(&mesh.triangles[0], &mesh.textures, 0.2, 0.2);
        let sheen_color_texture = srgb_u8_to_linear_rgb([128, 64, 255]);
        assert!((sample.sheen_color[0] - (0.5 * sheen_color_texture[0])).abs() < 1.0e-6);
        assert!((sample.sheen_color[1] - sheen_color_texture[1]).abs() < 1.0e-6);
        assert!((sample.sheen_color[2] - 0.25).abs() < 1.0e-6);
        assert!((sample.sheen_roughness - (0.5 * 128.0 / 255.0)).abs() < 1.0e-6);
    }

    #[test]
    fn gltf_loader_applies_khr_materials_anisotropy_factor_texture_and_direction() {
        let (dir, gltf_path) = write_anisotropy_test_gltf("anisotropy-test");

        let mesh = load_gltf_mesh(&gltf_path, [1.0, 1.0, 1.0]).expect("load anisotropy gltf");
        fs::remove_dir_all(&dir).ok();

        assert_eq!(mesh.triangles.len(), 1);
        assert!((mesh.triangles[0].anisotropy_strength - 0.5).abs() < 1.0e-6);
        assert!((mesh.triangles[0].anisotropy_rotation - 0.25).abs() < 1.0e-6);
        assert_eq!(mesh.triangles[0].anisotropy_texture_index, Some(0));
        assert_eq!(mesh.triangles[0].anisotropy_texture_texcoord, 0);

        let sample = interpolated_material_sample(&mesh.triangles[0], &mesh.textures, 0.2, 0.2);
        assert!((sample.anisotropy_strength - (0.5 * 128.0 / 255.0)).abs() < 1.0e-6);

        let direction = interpolated_anisotropy_direction(
            &mesh.triangles[0],
            &mesh.textures,
            0.2,
            0.2,
            [0.0, 0.0, 1.0],
        );
        assert!(
            direction[0] > 0.95,
            "anisotropy direction should follow tangent X"
        );
        assert!(
            direction[1].abs() < 0.30,
            "anisotropy direction should include only small authored rotation"
        );
    }

    #[test]
    fn gltf_loader_applies_khr_materials_iridescence_factor_and_thickness() {
        let (dir, gltf_path) = write_iridescence_test_gltf("iridescence-test");

        let mesh = load_gltf_mesh(&gltf_path, [1.0, 1.0, 1.0]).expect("load iridescence gltf");
        fs::remove_dir_all(&dir).ok();

        assert_eq!(mesh.triangles.len(), 1);
        assert!((mesh.triangles[0].iridescence_factor - 0.5).abs() < 1.0e-6);
        assert_eq!(mesh.triangles[0].iridescence_texture_index, Some(0));
        assert_eq!(mesh.triangles[0].iridescence_texture_texcoord, 0);
        assert!((mesh.triangles[0].iridescence_ior - 1.45).abs() < 1.0e-6);
        assert!((mesh.triangles[0].iridescence_thickness_minimum_nm - 120.0).abs() < 1.0e-6);
        assert!((mesh.triangles[0].iridescence_thickness_maximum_nm - 520.0).abs() < 1.0e-6);
        assert_eq!(
            mesh.triangles[0].iridescence_thickness_texture_index,
            Some(1)
        );
        assert_eq!(mesh.triangles[0].iridescence_thickness_texture_texcoord, 0);

        let sample = interpolated_material_sample(&mesh.triangles[0], &mesh.textures, 0.2, 0.2);
        assert!((sample.iridescence_factor - (0.5 * 128.0 / 255.0)).abs() < 1.0e-6);
        let expected_thickness = 120.0 + (520.0 - 120.0) * (128.0 / 255.0);
        assert!((sample.iridescence_thickness_nm - expected_thickness).abs() < 1.0e-5);
        assert!((sample.iridescence_ior - 1.45).abs() < 1.0e-6);
    }

    #[test]
    fn debug_rgb_composites_volume_filter_into_background() {
        let hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [255, 255, 255],
            color_linear_rgb: srgb_u8_to_linear_rgb([255, 255, 255]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 0.0,
            transmission_filter_rgb: [1.0, 0.25, 0.25],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 0.5,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [0.0, 0.0, 1.0],
            view_dir: [0.0, 0.0, 1.0],
        };
        let settings = PreparedRenderSettings::from_settings(RenderSettings::default());

        assert_eq!(
            composite_debug_rgb_hits(
                &[hit],
                &[],
                &[],
                &settings,
                srgb_u8_to_linear_radiance([100, 100, 100]),
            ),
            [100, 50, 50]
        );
    }

    #[test]
    fn diffuse_transmission_tints_background_through_opaque_surface() {
        let hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [0, 0, 0],
            color_linear_rgb: [0.0, 0.0, 0.0],
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.5,
            diffuse_transmission_color: [0.25, 1.0, 0.25],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 0.5,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: true,
            point: [0.0, 0.0, 0.0],
            normal: [0.0, 0.0, 1.0],
            view_dir: [0.0, 0.0, 1.0],
        };
        let settings = PreparedRenderSettings::from_settings(RenderSettings::default());

        let rgb = composite_debug_rgb_hits(
            &[hit],
            &[],
            &[],
            &settings,
            srgb_u8_to_linear_radiance([100, 100, 100]),
        );

        assert!(
            rgb[1] > rgb[0] + 20,
            "green diffuse transmission color should pass more background light"
        );
        assert_eq!(rgb[0], rgb[2]);
    }

    #[test]
    fn dispersion_chromatically_separates_transmitted_background() {
        let hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [255, 255, 255],
            color_linear_rgb: srgb_u8_to_linear_rgb([255, 255, 255]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 0.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 1.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 0.5,
            ior: 2.0,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [0.0, 0.0, 1.0],
            view_dir: [0.0, 0.0, 1.0],
        };
        let settings = PreparedRenderSettings::from_settings(RenderSettings::default());

        let rgb = composite_debug_rgb_hits(
            &[hit],
            &[],
            &[],
            &settings,
            srgb_u8_to_linear_radiance([100, 100, 100]),
        );

        assert!(
            rgb[0] > rgb[2] + 10,
            "dispersion should keep red transmission stronger than blue"
        );
        assert!(
            rgb[1] > rgb[2] + 10,
            "dispersion should reduce blue transmission from neutral background"
        );
    }

    #[test]
    fn dispersion_channel_ior_spreads_red_green_blue() {
        let red = channel_dispersion_ior(2.0, 1.0, 0);
        let green = channel_dispersion_ior(2.0, 1.0, 1);
        let blue = channel_dispersion_ior(2.0, 1.0, 2);

        assert!(
            red < green && green < blue,
            "dispersion should give each RGB channel a distinct IOR"
        );
        assert_eq!(channel_dispersion_ior(2.0, 0.0, 0), 2.0);
        assert_eq!(channel_dispersion_ior(2.0, 0.0, 2), 2.0);
    }

    #[test]
    fn dispersion_refracts_rgb_channels_into_different_scene_directions() {
        let incident = normalize([-1.0, 0.0, 0.0]);
        let surface_normal = normalize([0.5, 0.0, 0.866]);
        let hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [255, 255, 255],
            color_linear_rgb: srgb_u8_to_linear_rgb([255, 255, 255]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 0.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 3.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 0.0,
            ior: 3.0,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: surface_normal,
            view_dir: scale(incident, -1.0),
        };
        let target_x = -1.0;
        let channel_target_z = |channel| {
            let ior = channel_dispersion_ior(hit.ior, hit.dispersion, channel);
            let direction =
                refract(incident, surface_normal, 1.0 / ior).expect("refracted channel ray");
            let distance = target_x / direction[0];
            direction[2] * distance
        };
        let red_z = channel_target_z(0);
        let blue_z = channel_target_z(2);
        assert!(
            (red_z - blue_z).abs() > 0.05,
            "test geometry needs separated channel targets"
        );

        let mut red_triangles = ray_facing_rect(
            target_x,
            -0.2,
            0.2,
            red_z - 0.025,
            red_z + 0.025,
            [255, 0, 0, 255],
        );
        let mut blue_triangles = ray_facing_rect(
            target_x,
            -0.2,
            0.2,
            blue_z - 0.025,
            blue_z + 0.025,
            [0, 0, 255, 255],
        );
        for triangle in red_triangles.iter_mut().chain(blue_triangles.iter_mut()) {
            triangle.unlit = true;
        }
        let red_target = RenderObject {
            entity_u32: 2,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Triangles {
                bounds_min: [target_x - 0.001, -0.2, red_z - 0.025],
                bounds_max: [target_x + 0.001, 0.2, red_z + 0.025],
                triangles: red_triangles,
                textures: Vec::new(),
                lights: Vec::new(),
                bvh: Vec::new(),
            }),
            color_rgb: [255, 255, 255],
            transform: Transform::default(),
        };
        let blue_target = RenderObject {
            entity_u32: 3,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Triangles {
                bounds_min: [target_x - 0.001, -0.2, blue_z - 0.025],
                bounds_max: [target_x + 0.001, 0.2, blue_z + 0.025],
                triangles: blue_triangles,
                textures: Vec::new(),
                lights: Vec::new(),
                bvh: Vec::new(),
            }),
            color_rgb: [255, 255, 255],
            transform: Transform::default(),
        };
        let settings = PreparedRenderSettings::from_settings(RenderSettings {
            background_rgb: [0, 0, 0],
            rough_transmission_samples: 1,
            ..RenderSettings::default()
        });
        let next_medium_stack = next_transmission_medium_stack(&[], &hit);

        let (rgb, total_internal_reflection) = refracted_transmission_lobe_rgb(
            &hit,
            incident,
            surface_normal,
            &[red_target, blue_target],
            &[],
            &settings,
            [0.0, 0.0, 0.0],
            1,
            &[],
            &next_medium_stack,
        );

        assert_eq!(total_internal_reflection, [false, false, false]);
        assert!(
            rgb[0] > 200.0 && rgb[1] < 1.0 && rgb[2] > 200.0,
            "red and blue channels should sample their own refracted target rays, got {rgb:?}"
        );
    }

    #[test]
    fn refract_bends_incident_ray_toward_surface_normal() {
        let incident = normalize([-1.0, 0.0, 0.0]);
        let normal = normalize([0.5, 0.0, 0.866]);

        let direction = refract(incident, normal, 1.0 / 1.5).expect("refracted ray");

        assert!((dot(direction, direction) - 1.0).abs() < 1.0e-5);
        assert!(
            direction[2] < -0.20,
            "tilted surface normal should bend the ray downward"
        );
    }

    #[test]
    fn refraction_surface_normal_flips_for_exiting_closed_mesh_surface() {
        let incident = normalize([1.0, 0.0, 0.0]);
        let outward_normal = normalize([0.8, 0.0, 0.6]);
        let interface_normal = refraction_surface_normal(incident, outward_normal);

        assert!(
            dot(interface_normal, incident) < 0.0,
            "refraction should use the incident-side normal when leaving a closed mesh"
        );
        let direction = refract(incident, interface_normal, 1.5).expect("valid exit refraction");

        assert!(
            direction[0] > 0.0,
            "exiting material should continue through the surface, got {direction:?}"
        );
    }

    #[test]
    fn transmission_medium_stack_returns_to_outer_ior_after_nested_exit() {
        let glass_hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [255, 255, 255],
            color_linear_rgb: srgb_u8_to_linear_rgb([255, 255, 255]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 0.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 0.0,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [1.0, 0.0, 0.0],
            view_dir: [1.0, 0.0, 0.0],
        };
        let liquid_hit = Hit {
            entity_u32: 2,
            ior: 1.3,
            ..glass_hit
        };

        let glass = next_transmission_medium_stack(&[], &glass_hit);
        assert_eq!(medium_stack_summary(&glass), vec![(1, 1.5)]);

        let liquid_inside_glass = next_transmission_medium_stack(&glass, &liquid_hit);
        assert_eq!(
            medium_stack_summary(&liquid_inside_glass),
            vec![(1, 1.5), (2, 1.3)]
        );

        let back_to_glass = next_transmission_medium_stack(&liquid_inside_glass, &liquid_hit);
        assert_eq!(medium_stack_summary(&back_to_glass), vec![(1, 1.5)]);

        let back_to_air = next_transmission_medium_stack(&back_to_glass, &glass_hit);
        assert!(back_to_air.is_empty());
    }

    #[test]
    fn transmissive_exit_uses_material_to_air_ior() {
        let hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [255, 255, 255],
            color_linear_rgb: srgb_u8_to_linear_rgb([255, 255, 255]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 0.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 0.0,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: normalize([0.8, 0.0, 0.6]),
            view_dir: [1.0, 0.0, 0.0],
        };
        let settings = PreparedRenderSettings::from_settings(RenderSettings {
            environment: Some(EnvironmentSettings {
                sky_top_rgb: [255, 0, 0],
                sky_horizon_rgb: [0, 255, 0],
                ground_rgb: [0, 0, 255],
                map: None,
                map_rotation_deg: 0.0,
                intensity: 1.0,
                ambient_intensity: 0.0,
            }),
            ..RenderSettings::default()
        });
        let fallback = srgb_u8_to_linear_radiance([0, 0, 0]);

        let entry_rgb = refracted_background_rgb(&[hit], &[], &[], &settings, fallback, 1, &[]);
        let medium_stack = [TransmissionMedium::from_hit(&hit)];
        let exit_rgb =
            refracted_background_rgb(&[hit], &[], &[], &settings, fallback, 1, &medium_stack);

        assert!(
            entry_rgb[2] > entry_rgb[0] + 0.1,
            "air-to-material refraction should bend toward the ground side"
        );
        assert!(
            exit_rgb[0] > exit_rgb[2] + 0.1,
            "material-to-air refraction should bend away toward the sky side"
        );
    }

    #[test]
    fn total_internal_reflection_samples_reflected_environment() {
        let hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [255, 255, 255],
            color_linear_rgb: srgb_u8_to_linear_rgb([255, 255, 255]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 0.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 0.0,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: normalize([0.5, 0.0, 0.866]),
            view_dir: [1.0, 0.0, 0.0],
        };
        let settings = PreparedRenderSettings::from_settings(RenderSettings {
            environment: Some(EnvironmentSettings {
                sky_top_rgb: [255, 0, 0],
                sky_horizon_rgb: [0, 255, 0],
                ground_rgb: [0, 0, 255],
                map: None,
                map_rotation_deg: 0.0,
                intensity: 1.0,
                ambient_intensity: 0.0,
            }),
            ..RenderSettings::default()
        });

        let rgb = refracted_background_rgb(
            &[hit],
            &[],
            &[],
            &settings,
            srgb_u8_to_linear_radiance([0, 0, 0]),
            1,
            &[TransmissionMedium::from_hit(&hit)],
        );

        assert!(
            rgb[0] > rgb[2] + 0.1,
            "total internal reflection should sample the reflected sky side instead of black fallback"
        );
    }

    #[test]
    fn rough_total_internal_reflection_samples_off_axis_geometry() {
        let sharp_hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [255, 255, 255],
            color_linear_rgb: srgb_u8_to_linear_rgb([255, 255, 255]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 0.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 0.0,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: normalize([0.5, 0.0, 0.866]),
            view_dir: [1.0, 0.0, 0.0],
        };
        let rough_hit = Hit {
            roughness: 1.0,
            ..sharp_hit
        };
        let mut center_triangles = ray_facing_rect(-1.0, -0.2, 0.2, 1.55, 1.95, [255, 0, 0, 255]);
        for triangle in &mut center_triangles {
            triangle.unlit = true;
        }
        let center_target = RenderObject {
            entity_u32: 2,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Triangles {
                bounds_min: [-1.001, -0.2, 1.55],
                bounds_max: [-0.999, 0.2, 1.95],
                triangles: center_triangles,
                textures: Vec::new(),
                lights: Vec::new(),
                bvh: Vec::new(),
            }),
            color_rgb: [255, 255, 255],
            transform: Transform::default(),
        };
        let mut off_axis_triangles = ray_facing_rect(-1.0, 0.25, 1.2, 0.9, 2.6, [0, 0, 255, 255]);
        for triangle in &mut off_axis_triangles {
            triangle.unlit = true;
        }
        let off_axis_target = RenderObject {
            entity_u32: 3,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Triangles {
                bounds_min: [-1.001, 0.25, 0.9],
                bounds_max: [-0.999, 1.2, 2.6],
                triangles: off_axis_triangles,
                textures: Vec::new(),
                lights: Vec::new(),
                bvh: Vec::new(),
            }),
            color_rgb: [255, 255, 255],
            transform: Transform::default(),
        };
        let one_sample_settings = PreparedRenderSettings::from_settings(RenderSettings {
            background_rgb: [0, 0, 0],
            rough_transmission_samples: 1,
            ..RenderSettings::default()
        });
        let multi_sample_settings = PreparedRenderSettings::from_settings(RenderSettings {
            background_rgb: [0, 0, 0],
            rough_transmission_samples: 32,
            ..RenderSettings::default()
        });
        let medium_stack = [TransmissionMedium::from_hit(&sharp_hit)];

        let sharp_rgb = refracted_background_rgb(
            &[sharp_hit],
            &[center_target.clone(), off_axis_target.clone()],
            &[],
            &one_sample_settings,
            srgb_u8_to_linear_radiance([0, 0, 0]),
            1,
            &medium_stack,
        );
        let rough_one_sample_rgb = refracted_background_rgb(
            &[rough_hit],
            &[center_target.clone(), off_axis_target.clone()],
            &[],
            &one_sample_settings,
            srgb_u8_to_linear_radiance([0, 0, 0]),
            1,
            &medium_stack,
        );
        let rough_multi_sample_rgb = refracted_background_rgb(
            &[rough_hit],
            &[center_target, off_axis_target],
            &[],
            &multi_sample_settings,
            srgb_u8_to_linear_radiance([0, 0, 0]),
            1,
            &medium_stack,
        );

        assert!(
            sharp_rgb[0] > 100.0 && sharp_rgb[2] < 20.0,
            "sharp TIR should see only the centered red target, got {sharp_rgb:?}"
        );
        assert!(
            rough_one_sample_rgb[2] < 20.0,
            "one reflected rough sample should miss off-axis blue geometry, got {rough_one_sample_rgb:?}"
        );
        assert!(
            rough_multi_sample_rgb[2] > rough_one_sample_rgb[2] + 20.0,
            "rough reflected TIR should gather off-axis blue geometry, got one {rough_one_sample_rgb:?} multi {rough_multi_sample_rgb:?}"
        );
        assert!(
            rough_multi_sample_rgb[0] < sharp_rgb[0],
            "rough reflected TIR should spread energy away from the centered target"
        );
    }

    #[test]
    fn transmissive_background_uses_refracted_environment_direction() {
        let hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [255, 255, 255],
            color_linear_rgb: srgb_u8_to_linear_rgb([255, 255, 255]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 0.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 0.0,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: normalize([0.5, 0.0, 0.866]),
            view_dir: [1.0, 0.0, 0.0],
        };
        let settings = PreparedRenderSettings::from_settings(RenderSettings {
            environment: Some(EnvironmentSettings {
                sky_top_rgb: [255, 0, 0],
                sky_horizon_rgb: [0, 255, 0],
                ground_rgb: [0, 0, 255],
                map: None,
                map_rotation_deg: 0.0,
                intensity: 1.0,
                ambient_intensity: 0.0,
            }),
            ..RenderSettings::default()
        });
        let straight_background = environment_background_rgb([-1.0, 0.0, 0.0], &settings);

        let rgb = composite_debug_rgb_hits(&[hit], &[], &[], &settings, straight_background);

        assert!(
            rgb[2] > rgb[1] + 20,
            "refracted background should sample the ground side of the environment"
        );
        assert!(
            rgb[2] > rgb[0] + 20,
            "refracted background should differ from the straight horizon sample"
        );
    }

    #[test]
    fn transmissive_background_includes_fresnel_reflection_at_grazing_angle() {
        let base_hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [255, 255, 255],
            color_linear_rgb: srgb_u8_to_linear_rgb([255, 255, 255]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 0.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 0.0,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: normalize([0.5, 0.0, 0.866]),
            view_dir: [1.0, 0.0, 0.0],
        };
        let grazing_hit = Hit {
            normal: normalize([0.1, 0.0, 0.995]),
            ior: 2.4,
            ..base_hit
        };
        let settings = PreparedRenderSettings::from_settings(RenderSettings {
            environment: Some(EnvironmentSettings {
                sky_top_rgb: [255, 0, 0],
                sky_horizon_rgb: [0, 0, 0],
                ground_rgb: [0, 0, 255],
                map: None,
                map_rotation_deg: 0.0,
                intensity: 1.0,
                ambient_intensity: 0.0,
            }),
            ..RenderSettings::default()
        });
        let fallback = srgb_u8_to_linear_radiance([0, 0, 0]);

        let base_rgb = refracted_background_rgb(&[base_hit], &[], &[], &settings, fallback, 1, &[]);
        let grazing_rgb =
            refracted_background_rgb(&[grazing_hit], &[], &[], &settings, fallback, 1, &[]);

        assert!(
            grazing_rgb[0] > base_rgb[0] + 0.10,
            "grazing transmission should gain reflected sky energy, base {base_rgb:?} grazing {grazing_rgb:?}"
        );
        assert!(
            grazing_rgb[0] > grazing_rgb[2] * 0.5,
            "Fresnel reflection should visibly mix red sky into the transmitted background, got {grazing_rgb:?}"
        );
    }

    #[test]
    fn transmissive_surface_keeps_direct_specular_highlight() {
        let hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [255, 255, 255],
            color_linear_rgb: srgb_u8_to_linear_rgb([255, 255, 255]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 0.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 0.08,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [1.0, 0.0, 0.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [1.0, 0.0, 0.0],
            view_dir: [1.0, 0.0, 0.0],
        };
        let light = RenderLight {
            kind: RenderLightKind::Directional {
                direction: [1.0, 0.0, 0.0],
                angular_radius_deg: 0.0,
            },
            color_rgb: [1.0, 1.0, 1.0],
            intensity: 0.05,
        };
        let settings = PreparedRenderSettings::from_settings(RenderSettings {
            background_rgb: [0, 0, 0],
            ambient_intensity: 0.0,
            ..RenderSettings::default()
        });

        let rgb = composite_debug_rgb_hits(
            &[hit],
            &[],
            &[light],
            &settings,
            srgb_u8_to_linear_radiance([0, 0, 0]),
        );

        assert!(
            rgb[0] > 20 && rgb[1] > 20 && rgb[2] > 20,
            "transparent refractive surface should still show direct specular highlight, got {rgb:?}"
        );
    }

    #[test]
    fn transmissive_surface_traces_refracted_scene_geometry() {
        let hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [255, 255, 255],
            color_linear_rgb: srgb_u8_to_linear_rgb([255, 255, 255]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 0.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 0.0,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: normalize([0.5, 0.0, 0.866]),
            view_dir: [1.0, 0.0, 0.0],
        };
        let target = RenderObject {
            entity_u32: 2,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Triangles {
                bounds_min: [-1.001, -0.5, -0.5],
                bounds_max: [-0.999, 0.5, 0.5],
                triangles: vec![ray_facing_triangle(-1.0, [0, 0, 255, 255])],
                textures: Vec::new(),
                lights: Vec::new(),
                bvh: Vec::new(),
            }),
            color_rgb: [255, 255, 255],
            transform: Transform::default(),
        };
        let settings = PreparedRenderSettings::from_settings(RenderSettings {
            background_rgb: [0, 0, 0],
            ..RenderSettings::default()
        });

        let rgb = composite_debug_rgb_hits(
            &[hit],
            &[target],
            &[],
            &settings,
            srgb_u8_to_linear_radiance([0, 0, 0]),
        );

        assert!(
            rgb[2] > rgb[0] + 20 && rgb[2] > rgb[1] + 20,
            "refracted continuation should hit and shade the blue target"
        );
    }

    #[test]
    fn recursive_transmission_applies_current_medium_absorption() {
        let hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [255, 255, 255],
            color_linear_rgb: srgb_u8_to_linear_rgb([255, 255, 255]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 0.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: 1.0,
            volume_attenuation_color: [0.25, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 0.0,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [1.0, 0.0, 0.0],
            view_dir: [1.0, 0.0, 0.0],
        };
        let mut target_triangles =
            ray_facing_rect(-1.0, -0.3, 0.3, -0.3, 0.3, [255, 255, 255, 255]);
        for triangle in &mut target_triangles {
            triangle.unlit = true;
        }
        let target = RenderObject {
            entity_u32: 2,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Triangles {
                bounds_min: [-1.001, -0.3, -0.3],
                bounds_max: [-0.999, 0.3, 0.3],
                triangles: target_triangles,
                textures: Vec::new(),
                lights: Vec::new(),
                bvh: Vec::new(),
            }),
            color_rgb: [255, 255, 255],
            transform: Transform::default(),
        };
        let settings = PreparedRenderSettings::from_settings(RenderSettings {
            background_rgb: [0, 0, 0],
            ..RenderSettings::default()
        });

        let rgb = composite_debug_rgb_hits(
            &[hit],
            &[target],
            &[],
            &settings,
            srgb_u8_to_linear_radiance([0, 0, 0]),
        );

        assert!(
            rgb[1] > rgb[0] + 50 && rgb[2] > rgb[0] + 50,
            "recursive transmission should attenuate red while traveling inside the active medium, got {rgb:?}"
        );
    }

    #[test]
    fn rough_transmission_samples_off_axis_refracted_geometry() {
        let sharp_hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [255, 255, 255],
            color_linear_rgb: srgb_u8_to_linear_rgb([255, 255, 255]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 0.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 0.0,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [1.0, 0.0, 0.0],
            view_dir: [1.0, 0.0, 0.0],
        };
        let rough_hit = Hit {
            roughness: 1.0,
            ..sharp_hit
        };
        let mut center_triangles = ray_facing_rect(-1.0, -0.1, 0.1, -0.1, 0.1, [255, 0, 0, 255]);
        for triangle in &mut center_triangles {
            triangle.unlit = true;
        }
        let center_target = RenderObject {
            entity_u32: 2,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Triangles {
                bounds_min: [-1.001, -0.1, -0.1],
                bounds_max: [-0.999, 0.1, 0.1],
                triangles: center_triangles,
                textures: Vec::new(),
                lights: Vec::new(),
                bvh: Vec::new(),
            }),
            color_rgb: [255, 255, 255],
            transform: Transform::default(),
        };
        let mut off_axis_triangles =
            ray_facing_rect(-1.0, -0.25, 0.2, -0.35, -0.2, [0, 0, 255, 255]);
        for triangle in &mut off_axis_triangles {
            triangle.unlit = true;
        }
        let off_axis_target = RenderObject {
            entity_u32: 3,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Triangles {
                bounds_min: [-1.001, -0.25, -0.35],
                bounds_max: [-0.999, 0.2, -0.2],
                triangles: off_axis_triangles,
                textures: Vec::new(),
                lights: Vec::new(),
                bvh: Vec::new(),
            }),
            color_rgb: [255, 255, 255],
            transform: Transform::default(),
        };
        let one_sample_settings = PreparedRenderSettings::from_settings(RenderSettings {
            background_rgb: [0, 0, 0],
            rough_transmission_samples: 1,
            ..RenderSettings::default()
        });
        let multi_sample_settings = PreparedRenderSettings::from_settings(RenderSettings {
            background_rgb: [0, 0, 0],
            rough_transmission_samples: 16,
            ..RenderSettings::default()
        });

        let sharp_rgb = composite_debug_rgb_hits(
            &[sharp_hit],
            &[center_target.clone(), off_axis_target.clone()],
            &[],
            &one_sample_settings,
            srgb_u8_to_linear_radiance([0, 0, 0]),
        );
        let rough_one_sample_rgb = composite_debug_rgb_hits(
            &[rough_hit],
            &[center_target.clone(), off_axis_target.clone()],
            &[],
            &one_sample_settings,
            srgb_u8_to_linear_radiance([0, 0, 0]),
        );
        let rough_multi_sample_rgb = composite_debug_rgb_hits(
            &[rough_hit],
            &[center_target, off_axis_target],
            &[],
            &multi_sample_settings,
            srgb_u8_to_linear_radiance([0, 0, 0]),
        );

        assert!(
            sharp_rgb[0] > 100 && sharp_rgb[2] < 20,
            "sharp transmission should only see the centered red target, got {sharp_rgb:?}"
        );
        assert!(
            rough_one_sample_rgb[2] < 20,
            "one rough transmission sample should miss off-axis blue geometry, got {rough_one_sample_rgb:?}"
        );
        assert!(
            rough_multi_sample_rgb[2] > rough_one_sample_rgb[2] + 20,
            "more rough transmission samples should pick up off-axis blue geometry, got one {rough_one_sample_rgb:?} multi {rough_multi_sample_rgb:?}"
        );
        assert!(
            rough_multi_sample_rgb[0] < sharp_rgb[0],
            "rough transmission should spread energy away from the centered target"
        );
    }

    #[test]
    fn transmissive_surface_recurses_through_second_refractive_hit() {
        let source_hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [255, 255, 255],
            color_linear_rgb: srgb_u8_to_linear_rgb([255, 255, 255]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 0.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.001,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 0.0,
            ior: 1.0,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [1.0, 0.0, 0.0],
            view_dir: [1.0, 0.0, 0.0],
        };
        let mut redirecting_triangle = ray_facing_triangle(-1.0, [255, 255, 255, 0]);
        let redirecting_normal = normalize([0.5, 0.0, 0.866]);
        redirecting_triangle.normal_a = redirecting_normal;
        redirecting_triangle.normal_b = redirecting_normal;
        redirecting_triangle.normal_c = redirecting_normal;
        redirecting_triangle.alpha_cutoff = None;
        let redirecting_surface = RenderObject {
            entity_u32: 2,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Triangles {
                bounds_min: [-1.001, -0.5, -0.5],
                bounds_max: [-0.999, 0.5, 0.5],
                triangles: vec![redirecting_triangle],
                textures: Vec::new(),
                lights: Vec::new(),
                bvh: Vec::new(),
            }),
            color_rgb: [255, 255, 255],
            transform: Transform::default(),
        };

        let mut target_triangle_a = ray_facing_triangle(-2.0, [255, 0, 0, 255]);
        target_triangle_a.a = [-2.0, -0.5, -0.9];
        target_triangle_a.b = [-2.0, -0.5, -0.3];
        target_triangle_a.c = [-2.0, 0.5, -0.9];
        let mut target_triangle_b = ray_facing_triangle(-2.0, [255, 0, 0, 255]);
        target_triangle_b.a = [-2.0, 0.5, -0.9];
        target_triangle_b.b = [-2.0, -0.5, -0.3];
        target_triangle_b.c = [-2.0, 0.5, -0.3];
        let off_axis_target = RenderObject {
            entity_u32: 3,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Triangles {
                bounds_min: [-2.001, -0.5, -0.9],
                bounds_max: [-1.999, 0.5, -0.3],
                triangles: vec![target_triangle_a, target_triangle_b],
                textures: Vec::new(),
                lights: Vec::new(),
                bvh: Vec::new(),
            }),
            color_rgb: [255, 255, 255],
            transform: Transform::default(),
        };
        let settings = PreparedRenderSettings::from_settings(RenderSettings {
            background_rgb: [0, 0, 0],
            ..RenderSettings::default()
        });

        let rgb = composite_debug_rgb_hits(
            &[source_hit],
            &[redirecting_surface, off_axis_target],
            &[],
            &settings,
            srgb_u8_to_linear_radiance([0, 0, 0]),
        );

        assert!(
            rgb[0] > rgb[1] + 20 && rgb[0] > rgb[2] + 20,
            "recursive refraction should bend through the second surface into the off-axis red target"
        );
    }

    #[test]
    fn refracted_scene_hits_single_sided_transmissive_backface_exit() {
        let source_hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [255, 255, 255],
            color_linear_rgb: srgb_u8_to_linear_rgb([255, 255, 255]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 0.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.001,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 0.0,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.25, 0.25, -1.0],
            normal: [0.0, 0.0, 1.0],
            view_dir: [0.0, 0.0, -1.0],
        };
        let mut exit_triangle = test_triangle(None, false);
        exit_triangle.transmission_factor = 1.0;
        exit_triangle.color_rgba[3] = 0;
        exit_triangle.color_alpha = 0.0;
        let exit_surface = RenderObject {
            entity_u32: 1,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Triangles {
                bounds_min: [0.0, 0.0, 0.0],
                bounds_max: [1.0, 1.0, 0.0],
                triangles: vec![exit_triangle],
                textures: Vec::new(),
                lights: Vec::new(),
                bvh: Vec::new(),
            }),
            color_rgb: [255, 255, 255],
            transform: Transform::default(),
        };
        let settings = PreparedRenderSettings::from_settings(RenderSettings::default());

        let rgb = refracted_scene_rgb(
            &source_hit,
            [0.0, 0.0, 1.0],
            &[exit_surface],
            &[],
            &settings,
            1,
            &[],
        );

        assert!(
            rgb.is_some(),
            "refracted continuation should see transmissive backface exits of closed single-sided meshes"
        );
    }

    #[test]
    fn transmitted_volume_uses_hit_to_hit_path_length() {
        let entry = Hit {
            t: 1.0,
            entity_u32: 7,
            color_rgb: [255, 255, 255],
            color_linear_rgb: srgb_u8_to_linear_rgb([255, 255, 255]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 0.0,
            transmission_filter_rgb: [0.5, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.5,
            volume_attenuation_distance_m: 1.0,
            volume_attenuation_color: [0.25, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 0.5,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [0.0, 0.0, 1.0],
            view_dir: [0.0, 0.0, 1.0],
        };
        let exit = Hit { t: 3.0, ..entry };
        let settings = PreparedRenderSettings::from_settings(RenderSettings::default());
        let background = srgb_u8_to_linear_radiance([200, 200, 200]);

        let fallback = composite_debug_rgb_hits(&[entry], &[], &[], &settings, background);
        let path_traced = composite_debug_rgb_hits(&[entry, exit], &[], &[], &settings, background);

        assert!(
            path_traced[0] < fallback[0] - 40,
            "longer measured volume path should attenuate red more than fallback thickness"
        );
        assert_eq!(path_traced[1], fallback[1]);
        assert_eq!(path_traced[2], fallback[2]);
    }

    #[test]
    fn gltf_loader_preserves_khr_lights_punctual_point_light() {
        let (dir, gltf_path) = write_punctual_light_test_gltf("punctual-light-load-test");

        let mesh = load_gltf_mesh(&gltf_path, [1.0, 1.0, 1.0]).expect("load punctual light gltf");
        fs::remove_dir_all(&dir).ok();

        assert_eq!(mesh.triangles.len(), 1);
        assert_eq!(mesh.lights.len(), 1);
        assert_rgb_close(mesh.lights[0].color_rgb, [0.001, 1.0, 0.5], 1.0e-6);
        assert!((mesh.lights[0].intensity - 20.0).abs() < 1.0e-6);
        let RenderLightKind::Point { position, range_m } = mesh.lights[0].kind else {
            panic!("expected point light");
        };
        assert_eq!(position, [0.0, 0.0, 0.0]);
        assert_eq!(range_m, Some(10.0));
    }

    #[test]
    fn gltf_asset_light_contributes_to_debug_rgb_render() {
        let (dir, gltf_path) = write_punctual_light_test_gltf("punctual-light-render-test");
        let mut scene = SceneGraph::empty();
        scene.add_entity(EntityMetadata {
            id: EntityId("mesh".to_string()),
            name: "Mesh".to_string(),
            kind: "object".to_string(),
            robot_id: None,
            link_name: None,
        });
        scene.add_entity(EntityMetadata {
            id: EntityId("camera".to_string()),
            name: "Camera".to_string(),
            kind: "camera".to_string(),
            robot_id: None,
            link_name: None,
        });
        scene.root.children.push(SceneNode::mesh(
            "mesh",
            "Mesh",
            Geometry::MeshAsset {
                asset: gltf_path.display().to_string(),
                scale: [1.0, 1.0, 1.0],
                bounds: None,
            },
            Material::default(),
            Transform::default(),
        ));
        scene.root.children.push(SceneNode::camera(
            "camera",
            "Camera",
            CameraSpec {
                id: "camera".to_string(),
                name: "Camera".to_string(),
                transform: Transform::default(),
                fov_deg: 60.0,
                projection: CameraProjection::Perspective,
                resolution: [5, 5],
                intrinsics: None,
                distortion: None,
                depth_range_m: None,
                sensor_effects: None,
            },
        ));

        let renderer = NativeRenderer::new();
        let camera = find_camera(&scene.root, "camera").expect("camera");
        let entity_ids = entity_u32_map(&scene);
        let objects = render_objects(
            &scene.root,
            Transform::default(),
            &entity_ids,
            &renderer.mesh_cache,
            0.0,
            None,
        )
        .expect("objects");
        let lights = render_asset_lights(&objects);
        fs::remove_dir_all(&dir).ok();
        let settings = PreparedRenderSettings::from_settings(RenderSettings::default());

        assert_eq!(lights.len(), 1);
        let ambient = renderer.trace_debug_rgb(&camera, [5, 5], 2, 2, &objects, &[], &settings);
        let lit = renderer.trace_debug_rgb(&camera, [5, 5], 2, 2, &objects, &lights, &settings);

        assert!(
            lit[1] > ambient[1],
            "GLTF point light should brighten green channel"
        );
        assert!(lit[1] > lit[0], "green light should tint rendered hit");
        assert!(lit[1] > lit[2], "green light should tint rendered hit");
    }

    #[test]
    fn low_roughness_material_adds_stronger_debug_rgb_highlight() {
        let light_dir = fallback_light_direction();
        let matte = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [160, 160, 160],
            color_linear_rgb: srgb_u8_to_linear_rgb([160, 160, 160]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.7,
            roughness: 1.0,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: light_dir,
            view_dir: light_dir,
        };
        let shiny = Hit {
            roughness: 0.05,
            ..matte
        };

        assert!(shade_hit(&shiny, &[])[0] > shade_hit(&matte, &[])[0]);
    }

    #[test]
    fn ggx_direct_light_tints_metallic_specular_by_base_color() {
        let light = LightSample {
            direction: [0.0, 0.0, 1.0],
            color_rgb: [1.0, 1.0, 1.0],
            intensity: 1.0,
            max_t: None,
        };
        let dielectric = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [220, 0, 0],
            color_linear_rgb: srgb_u8_to_linear_rgb([220, 0, 0]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 0.35,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [0.0, 0.0, 1.0],
            view_dir: [0.0, 0.0, 1.0],
        };
        let metal = Hit {
            metallic: 1.0,
            ..dielectric
        };

        let dielectric_rgb = shade_pbr_direct_rgb(&dielectric, &light, [1.0, 1.0, 1.0]);
        let metal_rgb = shade_pbr_direct_rgb(&metal, &light, [1.0, 1.0, 1.0]);

        assert!(
            metal_rgb[0] > dielectric_rgb[0],
            "red metal should return stronger red specular energy"
        );
        assert!(
            metal_rgb[0] > metal_rgb[2] * 5.0,
            "metallic direct specular should be tinted by base color"
        );
        assert!(
            dielectric_rgb[1] > metal_rgb[1],
            "dielectric should keep diffuse green energy that metal suppresses"
        );
    }

    #[test]
    fn khr_materials_clearcoat_adds_secondary_specular_lobe() {
        let light = LightSample {
            direction: [0.0, 0.0, 1.0],
            color_rgb: [1.0, 1.0, 1.0],
            intensity: 1.0,
            max_t: None,
        };
        let base = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [0, 0, 0],
            color_linear_rgb: srgb_u8_to_linear_rgb([0, 0, 0]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 0.7,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 0.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [0.0, 0.0, 1.0],
            view_dir: [0.0, 0.0, 1.0],
        };
        let clearcoated = Hit {
            clearcoat_factor: 1.0,
            clearcoat_roughness: 0.04,
            clearcoat_normal: [0.0, 0.0, 1.0],
            ..base
        };

        let base_rgb = shade_pbr_direct_rgb(&base, &light, [1.0, 1.0, 1.0]);
        let clearcoat_rgb = shade_pbr_direct_rgb(&clearcoated, &light, [1.0, 1.0, 1.0]);

        assert!(
            base_rgb[0] <= 1.0e-6,
            "disabled base specular and black diffuse should not create a highlight"
        );
        assert!(
            clearcoat_rgb[0] > 1.0,
            "clearcoat should add its own secondary highlight"
        );
        assert!(
            (clearcoat_rgb[0] - clearcoat_rgb[1]).abs() < 1.0e-4,
            "clearcoat dielectric highlight should stay neutral"
        );

        let tilted_clearcoat = Hit {
            clearcoat_normal: normalize([1.0, 0.0, 1.0]),
            ..clearcoated
        };
        let tilted_rgb = shade_pbr_direct_rgb(&tilted_clearcoat, &light, [1.0, 1.0, 1.0]);
        assert!(
            tilted_rgb[0] < clearcoat_rgb[0] * 0.5,
            "clearcoat normal should steer the secondary highlight independently of the base normal"
        );
    }

    #[test]
    fn khr_materials_clearcoat_attenuates_underlying_base_layer() {
        let light = LightSample {
            direction: [0.0, 0.0, 1.0],
            color_rgb: [1.0, 1.0, 1.0],
            intensity: 1.0,
            max_t: None,
        };
        let base = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [255, 0, 0],
            color_linear_rgb: srgb_u8_to_linear_rgb([255, 0, 0]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 1.0,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 0.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [0.0, 0.0, 1.0],
            view_dir: [0.0, 0.0, 1.0],
        };
        let clearcoated = Hit {
            clearcoat_factor: 1.0,
            clearcoat_roughness: 1.0,
            ..base
        };

        let base_rgb = shade_pbr_direct_rgb(&base, &light, [1.0, 1.0, 1.0]);
        let clearcoated_rgb = shade_pbr_direct_rgb(&clearcoated, &light, [1.0, 1.0, 1.0]);

        assert!(
            clearcoated_rgb[0] < base_rgb[0] * 0.97,
            "clearcoat Fresnel should attenuate underlying red diffuse, base {base_rgb:?} clearcoated {clearcoated_rgb:?}"
        );
        assert!(
            clearcoated_rgb[1] > 0.0 && clearcoated_rgb[2] > 0.0,
            "clearcoat should still add a neutral dielectric highlight over the base layer"
        );
    }

    #[test]
    fn khr_materials_sheen_adds_colored_grazing_lobe() {
        let light = LightSample {
            direction: [0.0, 0.0, 1.0],
            color_rgb: [1.0, 1.0, 1.0],
            intensity: 1.0,
            max_t: None,
        };
        let base = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [0, 0, 0],
            color_linear_rgb: srgb_u8_to_linear_rgb([0, 0, 0]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 1.0,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 0.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [0.0, 0.0, 1.0],
            view_dir: normalize([0.0, 0.98, 0.20]),
        };
        let sheened = Hit {
            sheen_color: [1.0, 0.1, 0.0],
            sheen_roughness: 1.0,
            ..base
        };

        let base_rgb = shade_pbr_direct_rgb(&base, &light, [1.0, 1.0, 1.0]);
        let sheen_rgb = shade_pbr_direct_rgb(&sheened, &light, [1.0, 1.0, 1.0]);

        assert!(
            sheen_rgb[0] > base_rgb[0] + 10.0,
            "red sheen should add grazing light over the base material"
        );
        assert!(
            sheen_rgb[0] > sheen_rgb[1] * 5.0,
            "sheen color should tint the grazing lobe"
        );
    }

    #[test]
    fn khr_materials_anisotropy_shapes_specular_by_tangent_direction() {
        let light = LightSample {
            direction: normalize([0.7, 0.0, 0.714]),
            color_rgb: [1.0, 1.0, 1.0],
            intensity: 1.0,
            max_t: None,
        };
        let base = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [255, 255, 255],
            color_linear_rgb: srgb_u8_to_linear_rgb([255, 255, 255]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 1.0,
            roughness: 0.25,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.9,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [0.0, 0.0, 1.0],
            view_dir: [0.0, 0.0, 1.0],
        };
        let aligned = shade_pbr_direct_rgb(&base, &light, [1.0, 1.0, 1.0]);
        let orthogonal = shade_pbr_direct_rgb(
            &Hit {
                anisotropy_direction: [0.0, 1.0, 0.0],
                ..base
            },
            &light,
            [1.0, 1.0, 1.0],
        );

        assert!(
            aligned[0] > orthogonal[0] * 1.5,
            "anisotropy should strengthen highlight along tangent direction"
        );
    }

    #[test]
    fn khr_materials_iridescence_tints_dielectric_fresnel_by_thickness() {
        let light = LightSample {
            direction: [0.0, 0.0, 1.0],
            color_rgb: [1.0, 1.0, 1.0],
            intensity: 1.0,
            max_t: None,
        };
        let base = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [255, 255, 255],
            color_linear_rgb: srgb_u8_to_linear_rgb([255, 255, 255]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 0.08,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [0.0, 0.0, 1.0],
            view_dir: [0.0, 0.0, 1.0],
        };
        let iridescent = Hit {
            iridescence_factor: 1.0,
            iridescence_thickness_nm: 380.0,
            iridescence_ior: 1.45,
            ..base
        };

        let base_rgb = shade_pbr_direct_rgb(&base, &light, [1.0, 1.0, 1.0]);
        let iridescent_rgb = shade_pbr_direct_rgb(&iridescent, &light, [1.0, 1.0, 1.0]);

        assert!(
            (base_rgb[0] - base_rgb[1]).abs() < 1.0e-4
                && (base_rgb[1] - base_rgb[2]).abs() < 1.0e-4,
            "neutral dielectric should have neutral direct specular"
        );
        assert!(
            (iridescent_rgb[0] - iridescent_rgb[1]).abs() > 1.0
                || (iridescent_rgb[1] - iridescent_rgb[2]).abs() > 1.0,
            "iridescence should tint the dielectric specular lobe"
        );
        assert!(
            (iridescent_rgb[0] - base_rgb[0]).abs() > 1.0
                || (iridescent_rgb[1] - base_rgb[1]).abs() > 1.0
                || (iridescent_rgb[2] - base_rgb[2]).abs() > 1.0,
            "iridescence should change the rendered direct-light response"
        );
    }

    #[test]
    fn khr_materials_specular_strength_and_color_affect_direct_light_fresnel() {
        let light = LightSample {
            direction: [0.0, 0.0, 1.0],
            color_rgb: [1.0, 1.0, 1.0],
            intensity: 1.0,
            max_t: None,
        };
        let high_red_specular = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [0, 0, 0],
            color_linear_rgb: srgb_u8_to_linear_rgb([0, 0, 0]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 0.25,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 0.1, 0.1],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [0.0, 0.0, 1.0],
            view_dir: [0.0, 0.0, 1.0],
        };
        let low_red_specular = Hit {
            specular_factor: 0.1,
            ..high_red_specular
        };

        let high_rgb = shade_pbr_direct_rgb(&high_red_specular, &light, [1.0, 1.0, 1.0]);
        let low_rgb = shade_pbr_direct_rgb(&low_red_specular, &light, [1.0, 1.0, 1.0]);

        assert!(
            high_rgb[0] > low_rgb[0] * 5.0,
            "specularFactor should scale dielectric F0"
        );
        assert!(
            high_rgb[0] > high_rgb[1] * 5.0,
            "specularColorFactor should tint dielectric specular"
        );
    }

    #[test]
    fn khr_materials_ior_affects_dielectric_fresnel() {
        let light = LightSample {
            direction: [0.0, 0.0, 1.0],
            color_rgb: [1.0, 1.0, 1.0],
            intensity: 1.0,
            max_t: None,
        };
        let high_ior = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [0, 0, 0],
            color_linear_rgb: srgb_u8_to_linear_rgb([0, 0, 0]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 0.25,
            ior: 2.4,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [0.0, 0.0, 1.0],
            view_dir: [0.0, 0.0, 1.0],
        };
        let low_ior = Hit {
            ior: 1.1,
            ..high_ior
        };

        let high_rgb = shade_pbr_direct_rgb(&high_ior, &light, [1.0, 1.0, 1.0]);
        let low_rgb = shade_pbr_direct_rgb(&low_ior, &light, [1.0, 1.0, 1.0]);

        assert!(
            high_rgb[0] > low_rgb[0] * 10.0,
            "higher KHR_materials_ior should raise dielectric F0"
        );
    }

    #[test]
    fn khr_materials_unlit_ignores_lights_shadows_and_environment() {
        let hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [64, 128, 191],
            color_linear_rgb: srgb_u8_to_linear_rgb([64, 128, 191]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 0.0,
            metallic: 1.0,
            roughness: 0.04,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 0.0,
            specular_color: [0.0, 0.0, 0.0],
            unlit: true,
            point: [0.0, 0.0, 0.0],
            normal: [0.0, 0.0, -1.0],
            view_dir: [0.0, 0.0, 1.0],
        };
        let no_light_settings = PreparedRenderSettings::from_settings(RenderSettings {
            ambient_intensity: 0.0,
            ..RenderSettings::default()
        });
        let bright_environment_settings = PreparedRenderSettings::from_settings(RenderSettings {
            ambient_intensity: 2.0,
            environment: Some(EnvironmentSettings {
                sky_top_rgb: [255, 255, 255],
                sky_horizon_rgb: [255, 255, 255],
                ground_rgb: [255, 255, 255],
                intensity: 4.0,
                ambient_intensity: 4.0,
                map: None,
                map_rotation_deg: 0.0,
            }),
            ..RenderSettings::default()
        });
        let fallback_lights = fallback_lights();

        assert_eq!(
            shade_hit_with_lights(&hit, &[], &[], &no_light_settings),
            srgb_u8_to_linear_radiance([64, 128, 191])
        );
        assert_eq!(
            shade_hit_with_lights(&hit, &[], &fallback_lights, &bright_environment_settings),
            srgb_u8_to_linear_radiance([64, 128, 191])
        );
    }

    #[test]
    fn ggx_direct_light_reduces_on_axis_specular_as_roughness_increases() {
        let light = LightSample {
            direction: [0.0, 0.0, 1.0],
            color_rgb: [1.0, 1.0, 1.0],
            intensity: 1.0,
            max_t: None,
        };
        let smooth = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [180, 180, 180],
            color_linear_rgb: srgb_u8_to_linear_rgb([180, 180, 180]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 1.0,
            roughness: 0.18,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [0.0, 0.0, 1.0],
            view_dir: [0.0, 0.0, 1.0],
        };
        let rough = Hit {
            roughness: 0.85,
            ..smooth
        };

        let smooth_rgb = shade_pbr_direct_rgb(&smooth, &light, [1.0, 1.0, 1.0]);
        let rough_rgb = shade_pbr_direct_rgb(&rough, &light, [1.0, 1.0, 1.0]);

        assert!(
            smooth_rgb[0] > rough_rgb[0] * 5.0,
            "GGX on-axis highlight should be much tighter for low roughness"
        );
    }

    #[test]
    fn gltf_loader_uses_texture_indices_and_sampler_wrap_modes() {
        let (dir, gltf_path) = write_texture_index_sampler_test_gltf("texture-index-sampler-test");

        let mesh = load_gltf_mesh(&gltf_path, [1.0, 1.0, 1.0]).expect("load sampler textured gltf");
        fs::remove_dir_all(&dir).ok();

        assert_eq!(mesh.triangles.len(), 1);
        assert_eq!(mesh.textures.len(), 2);
        assert_eq!(mesh.triangles[0].texture_index, Some(1));
        assert_eq!(mesh.textures[1].wrap_s, TextureWrap::ClampToEdge);
        assert_eq!(mesh.textures[1].filter, TextureFilter::Linear);
        assert_eq!(
            interpolated_material_sample(&mesh.triangles[0], &mesh.textures, 0.2, 0.2).color_rgb,
            [0, 255, 0],
            "baseColorTexture index 1 should sample selected texture with clamp-to-edge wrapping"
        );
    }

    #[test]
    fn gltf_sampler_without_filter_uses_linear_auto_filtering() {
        assert_eq!(texture_filter_from_gltf(None, None), TextureFilter::Linear);
    }

    #[test]
    fn gltf_sampler_explicit_nearest_filter_stays_nearest() {
        assert_eq!(
            texture_filter_from_gltf(
                Some(gltf::texture::MagFilter::Nearest),
                Some(gltf::texture::MinFilter::Nearest)
            ),
            TextureFilter::Nearest
        );
    }

    #[test]
    fn gltf_loader_uses_base_color_texture_texcoord_set() {
        let (dir, gltf_path) = write_texcoord1_texture_test_gltf("texcoord1-texture-test");

        let mesh = load_gltf_mesh(&gltf_path, [1.0, 1.0, 1.0]).expect("load texcoord1 gltf");
        fs::remove_dir_all(&dir).ok();

        assert_eq!(mesh.triangles.len(), 1);
        assert_eq!(mesh.triangles[0].texture_index, Some(0));
        assert_eq!(mesh.triangles[0].texture_texcoord, 1);
        assert_eq!(
            interpolated_material_sample(&mesh.triangles[0], &mesh.textures, 0.2, 0.2).color_rgb,
            [0, 255, 0],
            "texCoord 1 should sample TEXCOORD_1, while TEXCOORD_0 would sample red"
        );
    }

    #[test]
    fn linear_texture_filter_blends_neighboring_texels() {
        let texture = TextureImage {
            width: 2,
            height: 1,
            rgba: vec![255, 0, 0, 255, 0, 255, 0, 255],
            wrap_s: TextureWrap::ClampToEdge,
            wrap_t: TextureWrap::ClampToEdge,
            filter: TextureFilter::Linear,
        };

        assert_eq!(sample_texture(&texture, [0.5, 0.0]), [128, 128, 0, 255]);
    }

    #[test]
    fn color_texture_linear_filter_blends_after_srgb_decode() {
        let texture = TextureImage {
            width: 2,
            height: 1,
            rgba: vec![0, 0, 0, 255, 255, 255, 255, 255],
            wrap_s: TextureWrap::ClampToEdge,
            wrap_t: TextureWrap::ClampToEdge,
            filter: TextureFilter::Linear,
        };

        assert_eq!(sample_texture(&texture, [0.5, 0.0]), [128, 128, 128, 255]);
        assert_rgb_close(
            sample_color_texture_linear_rgb(&texture, [0.5, 0.0]),
            [0.5, 0.5, 0.5],
            1.0e-6,
        );
    }

    #[test]
    fn base_color_texture_decodes_srgb_for_linear_shading_color() {
        let mut triangle = test_triangle(None, true);
        triangle.color_rgba = [128, 255, 255, 255];
        triangle.color_linear_rgb = [0.5, 1.0, 1.0];
        let texture = TextureImage {
            width: 1,
            height: 1,
            rgba: vec![128, 128, 128, 255],
            wrap_s: TextureWrap::ClampToEdge,
            wrap_t: TextureWrap::ClampToEdge,
            filter: TextureFilter::Nearest,
        };

        let sample = interpolated_material_sample(&triangle, &[texture], 0.2, 0.2);

        assert_eq!(sample.color_rgb, [64, 128, 128]);
        let texture_linear = srgb_u8_to_linear_rgb([128, 128, 128]);
        assert_rgb_close(
            sample.color_linear_rgb,
            [
                0.5 * texture_linear[0],
                texture_linear[1],
                texture_linear[2],
            ],
            1.0e-6,
        );
        assert!(
            sample.color_linear_rgb[1] < 128.0 / 255.0,
            "sRGB texture mid-gray must decode darker than a raw data channel"
        );
    }

    #[test]
    fn base_color_texture_linear_filter_uses_linear_color_space() {
        let mut triangle = test_triangle(None, true);
        triangle.texcoord_a = Some([0.5, 0.0]);
        triangle.texcoord_b = Some([0.5, 0.0]);
        triangle.texcoord_c = Some([0.5, 0.0]);
        let texture = TextureImage {
            width: 2,
            height: 1,
            rgba: vec![0, 0, 0, 255, 255, 255, 255, 255],
            wrap_s: TextureWrap::ClampToEdge,
            wrap_t: TextureWrap::ClampToEdge,
            filter: TextureFilter::Linear,
        };

        let sample = interpolated_material_sample(&triangle, &[texture], 0.0, 0.0);

        assert_eq!(sample.color_rgb, [128, 128, 128]);
        assert_rgb_close(sample.color_linear_rgb, [0.5, 0.5, 0.5], 1.0e-6);
    }

    #[test]
    fn vertex_color_uses_linear_values_for_shading_without_u8_quantization() {
        let mut triangle = test_triangle(None, true);
        triangle.texture_index = None;
        triangle.alpha_mode = MaterialAlphaMode::Blend;
        triangle.vertex_color_a = Some([26, 128, 230, 64]);
        triangle.vertex_color_b = Some([26, 128, 230, 64]);
        triangle.vertex_color_c = Some([26, 128, 230, 64]);
        triangle.vertex_color_linear_a = Some([0.1, 0.5, 0.9, 0.25]);
        triangle.vertex_color_linear_b = Some([0.1, 0.5, 0.9, 0.25]);
        triangle.vertex_color_linear_c = Some([0.1, 0.5, 0.9, 0.25]);

        let sample = interpolated_material_sample(&triangle, &[], 0.2, 0.2);

        assert_eq!(sample.color_rgb, [26, 128, 230]);
        assert_rgb_close(sample.color_linear_rgb, [0.1, 0.5, 0.9], 1.0e-6);
        assert!((sample.alpha - 0.25).abs() < 1.0e-6);
        assert!(
            (sample.color_linear_rgb[0] - (26.0 / 255.0)).abs() > 1.0e-3,
            "linear vertex color must not be reconstructed from rounded display bytes"
        );
    }

    #[test]
    fn metallic_roughness_texture_channels_stay_data_linear() {
        let mut triangle = test_triangle(None, true);
        triangle.texture_index = None;
        triangle.metallic_factor = 0.5;
        triangle.roughness_factor = 0.5;
        triangle.metallic_roughness_texture_index = Some(0);
        let texture = TextureImage {
            width: 1,
            height: 1,
            rgba: vec![0, 128, 128, 255],
            wrap_s: TextureWrap::ClampToEdge,
            wrap_t: TextureWrap::ClampToEdge,
            filter: TextureFilter::Nearest,
        };

        let sample = interpolated_material_sample(&triangle, &[texture], 0.2, 0.2);

        assert!((sample.roughness - (0.5 * 128.0 / 255.0)).abs() < 1.0e-6);
        assert!((sample.metallic - (0.5 * 128.0 / 255.0)).abs() < 1.0e-6);
    }

    #[test]
    fn gltf_loader_preserves_vertex_colors() {
        let (dir, gltf_path) = write_vertex_color_test_gltf("vertex-color-test");

        let mesh = load_gltf_mesh(&gltf_path, [1.0, 1.0, 1.0]).expect("load vertex-color gltf");
        fs::remove_dir_all(&dir).ok();

        assert_eq!(mesh.triangles.len(), 1);
        assert_eq!(mesh.triangles[0].vertex_color_a, Some([255, 0, 0, 128]));
        assert_eq!(
            mesh.triangles[0].vertex_color_linear_a,
            Some([1.0, 0.0, 0.0, 0.5])
        );
        let sample = interpolated_material_sample(&mesh.triangles[0], &[], 0.0, 0.0);
        assert_eq!(sample.color_rgb, [255, 0, 0]);
        assert_rgb_close(sample.color_linear_rgb, [1.0, 0.0, 0.0], 1.0e-6);
        assert_eq!(mesh.triangles[0].alpha_mode, MaterialAlphaMode::Opaque);
        assert_eq!(sample.alpha, 1.0);
    }

    #[test]
    fn gltf_loader_preserves_emissive_strength_for_hdr_contribution() {
        let (dir, gltf_path) = write_emissive_test_gltf("emissive-test");

        let mesh = load_gltf_mesh(&gltf_path, [1.0, 1.0, 1.0]).expect("load emissive gltf");
        fs::remove_dir_all(&dir).ok();

        assert_eq!(mesh.triangles.len(), 1);
        assert_eq!(mesh.triangles[0].emissive_rgb, [765.0, 255.0, 0.0]);
        let sample = interpolated_material_sample(&mesh.triangles[0], &mesh.textures, 0.0, 0.0);
        assert_eq!(sample.emission_rgb, [765.0, 255.0, 0.0]);

        let hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: sample.color_rgb,
            color_linear_rgb: sample.color_linear_rgb,
            emission_rgb: sample.emission_rgb,
            alpha: sample.alpha,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: sample.occlusion,
            metallic: sample.metallic,
            roughness: sample.roughness,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [0.0, 0.0, -1.0],
            view_dir: [0.0, 0.0, 1.0],
        };
        let settings = PreparedRenderSettings::from_settings(RenderSettings::default());
        let shaded = shade_hit_with_lights(&hit, &[], &[], &settings);
        assert!(
            shaded[0] >= sample.emission_rgb[0],
            "HDR emission should contribute before tone mapping"
        );
    }

    #[test]
    fn emissive_gltf_triangle_contributes_asset_light_to_other_surfaces() {
        let (dir, gltf_path) = write_emissive_test_gltf("emissive-light-test");

        let mesh = load_gltf_mesh(&gltf_path, [1.0, 1.0, 1.0]).expect("load emissive gltf");
        fs::remove_dir_all(&dir).ok();
        let object = RenderObject {
            entity_u32: 1,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Triangles {
                bounds_min: [0.0, 0.0, 0.0],
                bounds_max: [1.0, 1.0, 0.0],
                triangles: mesh.triangles,
                textures: mesh.textures,
                lights: Vec::new(),
                bvh: Vec::new(),
            }),
            color_rgb: [255, 255, 255],
            transform: Transform::default(),
        };

        let lights = render_asset_lights(std::slice::from_ref(&object));
        assert_eq!(lights.len(), 1);
        let RenderLightKind::AreaTriangle {
            a,
            b,
            c,
            normal,
            double_sided,
        } = lights[0].kind
        else {
            panic!("expected emissive triangle area light");
        };
        assert_rgb_close(a, [0.0, 0.0, 0.0], 1.0e-6);
        assert_rgb_close(b, [1.0, 0.0, 0.0], 1.0e-6);
        assert_rgb_close(c, [0.0, 1.0, 0.0], 1.0e-6);
        assert_rgb_close(normal, [0.0, 0.0, 1.0], 1.0e-6);
        assert!(!double_sided);
        assert_rgb_close(lights[0].color_rgb, [1.0, 1.0 / 3.0, 0.0], 1.0e-6);
        assert!((lights[0].intensity - 3.0).abs() < 1.0e-6);

        let hit = Hit {
            t: 1.0,
            entity_u32: 2,
            color_rgb: [255, 255, 255],
            color_linear_rgb: srgb_u8_to_linear_rgb([255, 255, 255]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 1.0,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [1.0 / 3.0, 1.0 / 3.0, 1.0],
            normal: [0.0, 0.0, -1.0],
            view_dir: [0.0, 0.0, -1.0],
        };
        let settings = PreparedRenderSettings::from_settings(RenderSettings {
            ambient_intensity: 0.0,
            ..RenderSettings::default()
        });

        let dark = shade_hit_with_lights(&hit, &[], &[], &settings);
        let lit = shade_hit_with_lights(&hit, &[], &lights, &settings);

        assert!(lit[0] > dark[0] + 100.0);
        assert!(lit[1] > dark[1] + 30.0);
        assert!(lit[0] > lit[1] * 2.0);
        assert!(lit[2] < 1.0);
    }

    #[test]
    fn emissive_triangle_light_averages_textured_emission_across_surface() {
        let mut triangle = test_triangle(None, false);
        triangle.texture_index = None;
        triangle.emissive_rgb = [255.0, 0.0, 0.0];
        triangle.emissive_texture_index = Some(0);
        triangle.texcoord_a = Some([0.0, 0.0]);
        triangle.texcoord_b = Some([1.0, 0.0]);
        triangle.texcoord_c = Some([0.0, 1.0]);
        let mut rgba = vec![255_u8, 0, 0, 255].repeat(16);
        let center_pixel = (1 + 1 * 4) * 4;
        rgba[center_pixel] = 0;
        rgba[center_pixel + 1] = 0;
        rgba[center_pixel + 2] = 0;
        let texture = TextureImage {
            width: 4,
            height: 4,
            rgba,
            wrap_s: TextureWrap::ClampToEdge,
            wrap_t: TextureWrap::ClampToEdge,
            filter: TextureFilter::Nearest,
        };

        let centroid = interpolated_emission_sample(
            &triangle,
            std::slice::from_ref(&texture),
            1.0 / 3.0,
            1.0 / 3.0,
        );
        let lights = emissive_triangle_lights(&[triangle], &[texture]);

        assert_eq!(centroid, [0.0, 0.0, 0.0]);
        assert_eq!(lights.len(), 1);
        assert!(
            lights[0].intensity > 0.05,
            "surface-averaged emissive extraction should preserve non-centroid emission, got {:?}",
            lights[0]
        );
        assert_rgb_close(lights[0].color_rgb, [1.0, 0.0, 0.0], 1.0e-6);
    }

    #[test]
    fn emissive_triangle_light_respects_alpha_visibility() {
        let mut opaque_triangle = test_triangle(None, false);
        opaque_triangle.texture_index = None;
        opaque_triangle.emissive_rgb = [255.0, 0.0, 0.0];
        opaque_triangle.alpha_mode = MaterialAlphaMode::Opaque;

        let opaque_lights = emissive_triangle_lights(&[opaque_triangle], &[]);
        assert_eq!(opaque_lights.len(), 1);
        assert!((opaque_lights[0].intensity - 1.0).abs() < 1.0e-6);

        let mut blended_triangle = opaque_triangle;
        blended_triangle.alpha_mode = MaterialAlphaMode::Blend;
        blended_triangle.color_alpha = 0.25;
        let blended_lights = emissive_triangle_lights(&[blended_triangle], &[]);
        assert_eq!(blended_lights.len(), 1);
        assert!((blended_lights[0].intensity - 0.25).abs() < 1.0e-6);

        let mut masked_triangle = opaque_triangle;
        masked_triangle.alpha_mode = MaterialAlphaMode::Mask;
        masked_triangle.alpha_cutoff = Some(0.5);
        masked_triangle.color_alpha = 0.25;
        assert!(emissive_triangle_lights(&[masked_triangle], &[]).is_empty());
    }

    #[test]
    fn double_sided_emissive_triangle_lights_from_back_side() {
        let mut single_sided_triangle = test_triangle(None, false);
        single_sided_triangle.texture_index = None;
        single_sided_triangle.emissive_rgb = [255.0, 255.0, 255.0];
        let mut double_sided_triangle = single_sided_triangle;
        double_sided_triangle.double_sided = true;

        let single_sided_light = emissive_triangle_lights(&[single_sided_triangle], &[]);
        let double_sided_light = emissive_triangle_lights(&[double_sided_triangle], &[]);
        assert_eq!(single_sided_light.len(), 1);
        assert_eq!(double_sided_light.len(), 1);

        let hit = Hit {
            t: 1.0,
            entity_u32: 2,
            color_rgb: [255, 255, 255],
            color_linear_rgb: srgb_u8_to_linear_rgb([255, 255, 255]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 1.0,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [1.0 / 3.0, 1.0 / 3.0, -1.0],
            normal: [0.0, 0.0, 1.0],
            view_dir: [0.0, 0.0, 1.0],
        };
        let settings = PreparedRenderSettings::from_settings(RenderSettings {
            ambient_intensity: 0.0,
            ..RenderSettings::default()
        });

        let single_sided = shade_hit_with_lights(&hit, &[], &single_sided_light, &settings);
        let double_sided = shade_hit_with_lights(&hit, &[], &double_sided_light, &settings);

        assert!(
            single_sided[0] < 1.0,
            "single-sided emissive triangles should not illuminate from behind"
        );
        assert!(
            double_sided[0] > single_sided[0] + 20.0,
            "double-sided emissive triangles should illuminate from behind"
        );
    }

    #[test]
    fn area_light_samples_preserve_partial_visibility_when_centroid_is_blocked() {
        let hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [255, 255, 255],
            color_linear_rgb: srgb_u8_to_linear_rgb([255, 255, 255]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 1.0,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [-0.266_666_68, -0.266_666_68, 0.0],
            normal: [0.0, 0.0, 1.0],
            view_dir: [0.0, 0.0, 1.0],
        };
        let blocker = RenderObject {
            entity_u32: 2,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Sphere { radius: 0.06 }),
            color_rgb: [255, 255, 255],
            transform: Transform::translated([-0.266_666_68, -0.266_666_68, 0.5]),
        };
        let light = RenderLight {
            kind: RenderLightKind::AreaTriangle {
                a: [-0.8, -0.8, 1.0],
                b: [-0.8, 0.8, 1.0],
                c: [0.8, -0.8, 1.0],
                normal: [0.0, 0.0, -1.0],
                double_sided: false,
            },
            color_rgb: [1.0, 1.0, 1.0],
            intensity: 8.0,
        };
        let centroid_settings = PreparedRenderSettings::from_settings(RenderSettings {
            ambient_intensity: 0.0,
            area_light_samples: 1,
            ..RenderSettings::default()
        });
        let sampled_settings = PreparedRenderSettings::from_settings(RenderSettings {
            ambient_intensity: 0.0,
            area_light_samples: 16,
            ..RenderSettings::default()
        });

        let centroid_only = shade_hit_with_lights(
            &hit,
            std::slice::from_ref(&blocker),
            &[light],
            &centroid_settings,
        );
        let sampled = shade_hit_with_lights(&hit, &[blocker], &[light], &sampled_settings);

        assert!(
            centroid_only[0] < 1.0,
            "single centroid sample should be fully shadowed by the blocker"
        );
        assert!(
            sampled[0] > centroid_only[0] + 50.0,
            "additional area-light samples should see unblocked emitter regions"
        );
    }

    #[test]
    fn directional_light_angular_radius_softens_shadow() {
        let hit = matte_test_hit([0.0, 0.0, 1.0]);
        let blocker = RenderObject {
            entity_u32: 2,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Sphere { radius: 0.04 }),
            color_rgb: [255, 255, 255],
            transform: Transform::translated([0.0, 0.0, 0.5]),
        };
        let hard_sun = RenderLight {
            kind: RenderLightKind::Directional {
                direction: [0.0, 0.0, 1.0],
                angular_radius_deg: 0.0,
            },
            color_rgb: [1.0, 1.0, 1.0],
            intensity: 1.0,
        };
        let soft_sun = RenderLight {
            kind: RenderLightKind::Directional {
                direction: [0.0, 0.0, 1.0],
                angular_radius_deg: 8.0,
            },
            color_rgb: [1.0, 1.0, 1.0],
            intensity: 1.0,
        };
        let hard_settings = PreparedRenderSettings::from_settings(RenderSettings {
            ambient_intensity: 0.0,
            soft_shadow_samples: 1,
            ..RenderSettings::default()
        });
        let soft_settings = PreparedRenderSettings::from_settings(RenderSettings {
            ambient_intensity: 0.0,
            soft_shadow_samples: 16,
            ..RenderSettings::default()
        });

        let hard = shade_hit_with_lights(
            &hit,
            std::slice::from_ref(&blocker),
            &[hard_sun],
            &hard_settings,
        );
        let soft = shade_hit_with_lights(&hit, &[blocker], &[soft_sun], &soft_settings);

        assert!(
            hard[0] < 1.0,
            "zero-radius directional light should be fully shadowed by the centered blocker"
        );
        assert!(
            soft[0] > hard[0] + 10.0,
            "finite angular-radius directional light should preserve partially unblocked sun samples, hard {hard:?} soft {soft:?}"
        );
    }

    #[test]
    fn gltf_loader_preserves_normal_texture_and_tangents() {
        let (dir, gltf_path) = write_normal_mapped_test_gltf("normal-map-test");

        let mesh = load_gltf_mesh(&gltf_path, [1.0, 1.0, 1.0]).expect("load normal-mapped gltf");
        fs::remove_dir_all(&dir).ok();

        assert_eq!(mesh.triangles.len(), 1);
        assert_eq!(mesh.textures.len(), 1);
        assert_eq!(mesh.triangles[0].normal_texture_index, Some(0));
        assert_eq!(mesh.triangles[0].tangent_a, Some([1.0, 0.0, 0.0, 1.0]));
        let normal = mapped_normal(
            &mesh.triangles[0],
            &mesh.textures,
            0.2,
            0.2,
            [0.0, 0.0, 1.0],
        );
        assert!(
            normal[0] > 0.9,
            "normal map should tilt the shaded normal toward tangent X, got {normal:?}"
        );
    }

    #[test]
    fn normal_map_uses_uv_derived_tangent_when_gltf_tangents_are_missing() {
        let mut triangle = test_triangle(None, false);
        triangle.tangent_a = None;
        triangle.tangent_b = None;
        triangle.tangent_c = None;
        triangle.texcoord_a = Some([0.0, 0.0]);
        triangle.texcoord_b = Some([1.0, 0.0]);
        triangle.texcoord_c = Some([0.0, 1.0]);
        triangle.normal_texture_index = Some(0);
        let texture = TextureImage {
            width: 1,
            height: 1,
            rgba: vec![255, 128, 128, 255],
            wrap_s: TextureWrap::Repeat,
            wrap_t: TextureWrap::Repeat,
            filter: TextureFilter::Nearest,
        };

        let normal = mapped_normal(&triangle, &[texture], 0.2, 0.2, [0.0, 0.0, 1.0]);

        assert!(
            normal[0] > 0.9,
            "normal map should derive tangent from triangle UVs when tangents are missing, got {normal:?}"
        );
    }

    #[test]
    fn gltf_loader_keeps_normal_texture_when_tangents_are_missing() {
        let (dir, gltf_path) =
            write_normal_mapped_without_tangent_test_gltf("normal-map-no-tangent-test");

        let mesh = load_gltf_mesh(&gltf_path, [1.0, 1.0, 1.0]).expect("load normal-mapped gltf");
        fs::remove_dir_all(&dir).ok();

        assert_eq!(mesh.triangles.len(), 1);
        assert_eq!(mesh.textures.len(), 1);
        assert_eq!(mesh.triangles[0].normal_texture_index, Some(0));
        assert_eq!(mesh.triangles[0].tangent_a, None);
        let normal = mapped_normal(
            &mesh.triangles[0],
            &mesh.textures,
            0.2,
            0.2,
            [0.0, 0.0, 1.0],
        );
        assert!(
            normal[0] > 0.9,
            "loaded normal map should derive tangent from UVs when GLTF lacks tangents, got {normal:?}"
        );
    }

    #[test]
    fn renderer_reuses_mesh_asset_cache_for_repeated_assets() {
        let (dir, gltf_path) = write_test_gltf("cache-test");
        let asset = gltf_path.display().to_string();
        let mut scene = SceneGraph::empty();
        for id in ["mesh-a", "mesh-b"] {
            scene.add_entity(EntityMetadata {
                id: EntityId(id.to_string()),
                name: id.to_string(),
                kind: "object".to_string(),
                robot_id: None,
                link_name: None,
            });
            scene.root.children.push(SceneNode::mesh(
                id,
                id,
                Geometry::MeshAsset {
                    asset: asset.clone(),
                    scale: [1.0, 1.0, 1.0],
                    bounds: None,
                },
                Material::default(),
                Transform::default(),
            ));
        }

        let renderer = NativeRenderer::new();
        renderer
            .render(
                &scene,
                None,
                &ObservationRequest {
                    camera_id: None,
                    views: vec![ObservationView::State],
                    resolution: [1, 1],
                    segmentation_policy: None,
                    shutter_policy: None,
                    render_settings: None,
                },
            )
            .expect("render state");
        renderer
            .render(
                &scene,
                None,
                &ObservationRequest {
                    camera_id: None,
                    views: vec![ObservationView::State],
                    resolution: [1, 1],
                    segmentation_policy: None,
                    shutter_policy: None,
                    render_settings: None,
                },
            )
            .expect("render state again");
        let cache_len = renderer.mesh_cache.lock().expect("cache lock").len();
        fs::remove_dir_all(&dir).ok();

        assert_eq!(cache_len, 1);
    }

    #[test]
    fn renderer_mesh_cache_separates_selected_gltf_material_variants() {
        let (dir, gltf_path) = write_material_variants_test_gltf("variant-cache-test");
        let asset = gltf_path.display().to_string();
        let mut scene = SceneGraph::empty();
        scene.add_entity(EntityMetadata {
            id: EntityId("mesh".to_string()),
            name: "mesh".to_string(),
            kind: "object".to_string(),
            robot_id: None,
            link_name: None,
        });
        scene.root.children.push(SceneNode::mesh(
            "mesh",
            "mesh",
            Geometry::MeshAsset {
                asset,
                scale: [1.0, 1.0, 1.0],
                bounds: None,
            },
            Material::default(),
            Transform::default(),
        ));

        let renderer = NativeRenderer::new();
        let entity_ids = entity_u32_map(&scene);
        let default_objects = render_objects(
            &scene.root,
            Transform::default(),
            &entity_ids,
            &renderer.mesh_cache,
            0.0,
            None,
        )
        .expect("default objects");
        let variant_objects = render_objects(
            &scene.root,
            Transform::default(),
            &entity_ids,
            &renderer.mesh_cache,
            0.0,
            Some("blue"),
        )
        .expect("variant objects");
        let cache_len = renderer.mesh_cache.lock().expect("cache lock").len();
        fs::remove_dir_all(&dir).ok();

        let RenderGeometry::Triangles {
            triangles: default_triangles,
            ..
        } = default_objects[0].geometry.as_ref()
        else {
            panic!("expected default triangle geometry");
        };
        let RenderGeometry::Triangles {
            triangles: variant_triangles,
            ..
        } = variant_objects[0].geometry.as_ref()
        else {
            panic!("expected variant triangle geometry");
        };

        assert_eq!(default_triangles[0].color_rgba, [255, 0, 0, 255]);
        assert_eq!(variant_triangles[0].color_rgba, [0, 0, 255, 255]);
        assert_eq!(cache_len, 2);
    }

    #[test]
    fn shadow_blocker_darkens_debug_rgb_hit() {
        let hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [200, 200, 200],
            color_linear_rgb: srgb_u8_to_linear_rgb([200, 200, 200]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 1.0,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [0.0, 0.0, 1.0],
            view_dir: [0.0, 0.0, 1.0],
        };
        let light_dir = fallback_light_direction();
        let blocker = RenderObject {
            entity_u32: 2,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Sphere { radius: 0.1 }),
            color_rgb: [255, 255, 255],
            transform: Transform::translated(scale(light_dir, 0.5)),
        };

        let lit = shade_hit(&hit, &[]);
        let shadowed = shade_hit(&hit, &[blocker]);

        assert!(shadowed[0] < lit[0], "shadow should darken hit");
    }

    #[test]
    fn directional_light_direction_illuminates_surface_it_points_at() {
        let mut root = SceneNode::group("root", "Root");
        root.children.push(SceneNode::light(
            "sun",
            "Sun",
            LightSpec {
                id: "sun".to_string(),
                name: "Sun".to_string(),
                transform: Transform::default(),
                kind: LightKind::Directional {
                    direction: [0.0, 0.0, -1.0],
                    angular_radius_deg: 0.0,
                },
                color_rgb: [255, 255, 255],
                intensity: 1.0,
            },
        ));
        let lights = render_lights(&root, Transform::default());
        let settings = PreparedRenderSettings::from_settings(RenderSettings {
            ambient_intensity: 0.0,
            ..RenderSettings::default()
        });
        let upward_hit = matte_test_hit([0.0, 0.0, 1.0]);
        let downward_hit = matte_test_hit([0.0, 0.0, -1.0]);

        let samples = light_samples(&lights[0], upward_hit.point, &settings);
        let upward = shade_hit_with_lights(&upward_hit, &[], &lights, &settings);
        let downward = shade_hit_with_lights(&downward_hit, &[], &lights, &settings);

        assert!(
            dot(upward_hit.normal, samples[0].direction) > 0.99,
            "directional light should expose a surface-to-light sample direction"
        );
        assert!(
            upward[0] > downward[0] + 10.0,
            "a downward-authored sun should light upward-facing surfaces"
        );
    }

    #[test]
    fn project_light_color_decodes_srgb_for_linear_shading() {
        let mut root = SceneNode::group("root", "Root");
        root.children.push(SceneNode::light(
            "red_sun",
            "Red Sun",
            LightSpec {
                id: "red_sun".to_string(),
                name: "Red Sun".to_string(),
                transform: Transform::default(),
                kind: LightKind::Directional {
                    direction: [0.0, 0.0, -1.0],
                    angular_radius_deg: 0.0,
                },
                color_rgb: [128, 64, 32],
                intensity: 1.0,
            },
        ));
        let lights = render_lights(&root, Transform::default());

        assert_rgb_close(
            lights[0].color_rgb,
            srgb_u8_to_linear_rgb([128, 64, 32]),
            1.0e-6,
        );
    }

    #[test]
    fn point_light_attenuation_uses_inverse_square_distance() {
        let one_meter = point_light_attenuation(1.0, None);
        let two_meters = point_light_attenuation(2.0, None);
        let four_meters = point_light_attenuation(4.0, None);

        assert!((one_meter - 1.0).abs() < 1.0e-6);
        assert!((two_meters - 0.25).abs() < 1.0e-6);
        assert!((four_meters - 0.0625).abs() < 1.0e-6);
        assert!((two_meters / one_meter - 0.25).abs() < 1.0e-6);
    }

    #[test]
    fn point_light_attenuation_smoothly_reaches_zero_at_range() {
        let inside = point_light_attenuation(1.0, Some(2.0));
        let at_range = point_light_attenuation(2.0, Some(2.0));
        let outside = point_light_attenuation(3.0, Some(2.0));

        assert!((inside - 0.9375).abs() < 1.0e-6);
        assert_eq!(at_range, 0.0);
        assert_eq!(outside, 0.0);
    }

    #[test]
    fn spot_light_angular_attenuation_uses_squared_gltf_falloff() {
        let inner_cos = 1.0;
        let outer_cos = 0.0;

        assert_eq!(
            spot_light_angular_attenuation(1.0, inner_cos, outer_cos),
            1.0
        );
        assert_eq!(
            spot_light_angular_attenuation(0.0, inner_cos, outer_cos),
            0.0
        );
        assert!((spot_light_angular_attenuation(0.5, inner_cos, outer_cos) - 0.25).abs() < 1.0e-6);
    }

    #[test]
    fn spot_light_samples_apply_squared_cone_attenuation() {
        let light = RenderLight {
            kind: RenderLightKind::Spot {
                position: [0.0, 0.0, 0.0],
                direction: [0.0, 0.0, -1.0],
                inner_cos: 1.0,
                outer_cos: 0.0,
                range_m: None,
            },
            color_rgb: [1.0, 1.0, 1.0],
            intensity: 4.0,
        };
        let settings = PreparedRenderSettings::from_settings(RenderSettings {
            soft_shadow_samples: 1,
            ..RenderSettings::default()
        });
        let halfway_between_inner_and_outer_cones = [3.0_f32.sqrt() * 0.5, 0.0, -0.5];

        let samples = light_samples(&light, halfway_between_inner_and_outer_cones, &settings);

        assert_eq!(samples.len(), 1);
        assert!((samples[0].intensity - 1.0).abs() < 1.0e-6);
    }

    #[test]
    fn soft_shadow_radius_partially_lights_point_light_penumbra() {
        let hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [200, 200, 200],
            color_linear_rgb: srgb_u8_to_linear_rgb([200, 200, 200]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 1.0,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [0.0, 0.0, 1.0],
            view_dir: [0.0, 0.0, 1.0],
        };
        let blocker = RenderObject {
            entity_u32: 2,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Sphere { radius: 0.12 }),
            color_rgb: [255, 255, 255],
            transform: Transform::translated([0.0, 0.0, 0.5]),
        };
        let light = RenderLight {
            kind: RenderLightKind::Point {
                position: [0.0, 0.0, 1.0],
                range_m: None,
            },
            color_rgb: [1.0, 1.0, 1.0],
            intensity: 2.0,
        };
        let hard_settings = PreparedRenderSettings::from_settings(RenderSettings {
            ambient_intensity: 0.0,
            soft_shadow_samples: 1,
            soft_shadow_radius_m: 0.0,
            ..RenderSettings::default()
        });
        let soft_settings = PreparedRenderSettings::from_settings(RenderSettings {
            ambient_intensity: 0.0,
            soft_shadow_samples: 16,
            soft_shadow_radius_m: 0.35,
            ..RenderSettings::default()
        });
        let clear = shade_hit_with_lights(&hit, &[], &[light], &hard_settings);
        let hard_shadow = shade_hit_with_lights(
            &hit,
            std::slice::from_ref(&blocker),
            &[light],
            &hard_settings,
        );
        let soft_shadow = shade_hit_with_lights(&hit, &[blocker], &[light], &soft_settings);

        assert!(hard_shadow[0] < clear[0] * 0.45);
        assert!(
            soft_shadow[0] > hard_shadow[0] * 1.2,
            "finite light radius should allow unblocked samples through the penumbra"
        );
        assert!(
            soft_shadow[0] < clear[0] * 0.95,
            "blocker should still shadow some finite-light samples"
        );
    }

    #[test]
    fn transparent_shadow_blocker_keeps_partial_light_visibility() {
        let hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [200, 200, 200],
            color_linear_rgb: srgb_u8_to_linear_rgb([200, 200, 200]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 1.0,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [1.0, 0.0, 0.0],
            view_dir: [0.0, 0.0, 1.0],
        };
        let transparent_blocker = RenderObject {
            entity_u32: 2,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Triangles {
                bounds_min: [0.5, -0.5, -0.5],
                bounds_max: [0.5, 0.5, 0.5],
                triangles: vec![ray_facing_triangle(0.5, [255, 255, 255, 128])],
                textures: Vec::new(),
                lights: Vec::new(),
                bvh: Vec::new(),
            }),
            color_rgb: [255, 255, 255],
            transform: Transform::default(),
        };
        let opaque_blocker = RenderObject {
            geometry: Arc::new(RenderGeometry::Triangles {
                bounds_min: [0.5, -0.5, -0.5],
                bounds_max: [0.5, 0.5, 0.5],
                triangles: vec![ray_facing_triangle(0.5, [255, 255, 255, 255])],
                textures: Vec::new(),
                lights: Vec::new(),
                bvh: Vec::new(),
            }),
            ..transparent_blocker.clone()
        };

        let clear = shadow_visibility(&hit, [1.0, 0.0, 0.0], Some(1.0), &[]);
        let transparent =
            shadow_visibility(&hit, [1.0, 0.0, 0.0], Some(1.0), &[transparent_blocker]);
        let opaque = shadow_visibility(&hit, [1.0, 0.0, 0.0], Some(1.0), &[opaque_blocker]);

        assert!(transparent < clear);
        assert!(transparent > opaque);
        assert!(transparent > 0.45 && transparent < 0.55);
        assert!(opaque <= 1.0e-6);
    }

    #[test]
    fn transmissive_shadow_blocker_tints_direct_light() {
        let hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [200, 200, 200],
            color_linear_rgb: srgb_u8_to_linear_rgb([200, 200, 200]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 1.0,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [1.0, 0.0, 0.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [0.0, 1.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [1.0, 0.0, 0.0],
            view_dir: [1.0, 0.0, 0.0],
        };
        let mut red_transmissive = ray_facing_triangle(0.5, [255, 255, 255, 255]);
        red_transmissive.diffuse_transmission_factor = 1.0;
        red_transmissive.diffuse_transmission_color_factor = [1.0, 0.02, 0.02];
        let blocker = RenderObject {
            entity_u32: 2,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Triangles {
                bounds_min: [0.5, -0.5, -0.5],
                bounds_max: [0.5, 0.5, 0.5],
                triangles: vec![red_transmissive],
                textures: Vec::new(),
                lights: Vec::new(),
                bvh: Vec::new(),
            }),
            color_rgb: [255, 255, 255],
            transform: Transform::default(),
        };
        let light = super::RenderLight {
            kind: RenderLightKind::Directional {
                direction: [1.0, 0.0, 0.0],
                angular_radius_deg: 0.0,
            },
            color_rgb: [1.0, 1.0, 1.0],
            intensity: 1.0,
        };
        let settings = PreparedRenderSettings::from_settings(RenderSettings {
            ambient_intensity: 0.0,
            ..RenderSettings::default()
        });

        let visibility =
            shadow_visibility_rgb(&hit, [1.0, 0.0, 0.0], Some(1.0), &[blocker.clone()]);
        let clear = shade_hit_with_lights(&hit, &[], &[light], &settings);
        let tinted = shade_hit_with_lights(&hit, &[blocker], &[light], &settings);

        assert!(visibility[0] > 0.95);
        assert!(visibility[1] < 0.30);
        assert!(visibility[2] < 0.30);
        assert!(tinted[0] > clear[0] * 0.95);
        assert!(tinted[1] < clear[1] * 0.35);
        assert!(tinted[2] < clear[2] * 0.35);
    }

    #[test]
    fn nearest_shadow_visibility_matches_sorted_transparent_hit_stack() {
        let hit = matte_test_hit([1.0, 0.0, 0.0]);
        let transparent = RenderObject {
            entity_u32: 2,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Triangles {
                bounds_min: [0.3, -0.5, -0.5],
                bounds_max: [0.3, 0.5, 0.5],
                triangles: vec![ray_facing_triangle(0.3, [255, 255, 255, 128])],
                textures: Vec::new(),
                lights: Vec::new(),
                bvh: Vec::new(),
            }),
            color_rgb: [255, 255, 255],
            transform: Transform::default(),
        };
        let mut blue_transmissive = ray_facing_triangle(0.6, [255, 255, 255, 255]);
        blue_transmissive.diffuse_transmission_factor = 1.0;
        blue_transmissive.diffuse_transmission_color_factor = [0.1, 0.2, 1.0];
        let transmissive = RenderObject {
            entity_u32: 3,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Triangles {
                bounds_min: [0.6, -0.5, -0.5],
                bounds_max: [0.6, 0.5, 0.5],
                triangles: vec![blue_transmissive],
                textures: Vec::new(),
                lights: Vec::new(),
                bvh: Vec::new(),
            }),
            color_rgb: [255, 255, 255],
            transform: Transform::default(),
        };
        let objects = vec![transmissive, transparent];

        let streaming = shadow_visibility_rgb(&hit, [1.0, 0.0, 0.0], Some(1.0), &objects);
        let sorted =
            shadow_visibility_rgb_collecting_all_hits(&hit, [1.0, 0.0, 0.0], Some(1.0), &objects);

        assert_rgb_close(streaming, sorted, 1.0e-5);
        assert!(
            streaming[2] > streaming[0] * 5.0,
            "stacked transparent/transmissive blockers should preserve blue tint, got {streaming:?}"
        );
    }

    #[test]
    fn debug_rgb_composites_transparent_front_hit_over_back_hit() {
        let front = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [200, 0, 0],
            color_linear_rgb: srgb_u8_to_linear_rgb([200, 0, 0]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 0.5,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 1.0,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [0.0, 0.0, 1.0],
            view_dir: [0.0, 0.0, 1.0],
        };
        let back = Hit {
            t: 2.0,
            entity_u32: 2,
            color_rgb: [0, 0, 200],
            color_linear_rgb: srgb_u8_to_linear_rgb([0, 0, 200]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 1.0,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, -1.0],
            normal: [0.0, 0.0, 1.0],
            view_dir: [0.0, 0.0, 1.0],
        };

        let settings = PreparedRenderSettings::from_settings(RenderSettings {
            background_rgb: [8, 8, 8],
            ..RenderSettings::default()
        });
        let color = composite_debug_rgb_hits(
            &[front, back],
            &[],
            &fallback_lights(),
            &settings,
            [8.0, 8.0, 8.0],
        );

        assert!(color[0] > 0, "front red should contribute");
        assert!(color[2] > 0, "back blue should show through");
    }

    #[test]
    fn segmentation_policy_skips_hits_below_min_alpha() {
        let transparent_front = RenderObject {
            entity_u32: 1,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Triangles {
                bounds_min: [1.0, -0.5, -0.5],
                bounds_max: [1.0, 0.5, 0.5],
                triangles: vec![ray_facing_triangle(1.0, [255, 0, 0, 128])],
                textures: Vec::new(),
                lights: Vec::new(),
                bvh: Vec::new(),
            }),
            color_rgb: [255, 0, 0],
            transform: Transform::default(),
        };
        let opaque_back = RenderObject {
            entity_u32: 2,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Triangles {
                bounds_min: [2.0, -0.5, -0.5],
                bounds_max: [2.0, 0.5, 0.5],
                triangles: vec![ray_facing_triangle(2.0, [0, 0, 255, 255])],
                textures: Vec::new(),
                lights: Vec::new(),
                bvh: Vec::new(),
            }),
            color_rgb: [0, 0, 255],
            transform: Transform::default(),
        };
        let camera = CameraSpec {
            id: "camera".to_string(),
            name: "Camera".to_string(),
            transform: Transform::default(),
            fov_deg: 60.0,
            projection: CameraProjection::Perspective,
            resolution: [5, 5],
            intrinsics: None,
            distortion: None,
            depth_range_m: None,
            sensor_effects: None,
        };
        let renderer = NativeRenderer::new();
        let objects = [transparent_front, opaque_back];

        let default_hit = renderer
            .trace_segmentation(&camera, [5, 5], 2, 2, &objects, None)
            .expect("default segmentation hit");
        let opaque_hit = renderer
            .trace_segmentation(
                &camera,
                [5, 5],
                2,
                2,
                &objects,
                Some(SegmentationPolicy { min_alpha: 1.0 }),
            )
            .expect("opaque segmentation hit");

        assert_eq!(default_hit.entity_u32, 1);
        assert_eq!(opaque_hit.entity_u32, 2);
    }

    #[test]
    fn debug_rgb_shutter_averages_sample_colors() {
        let camera = CameraSpec {
            id: "camera".to_string(),
            name: "Camera".to_string(),
            transform: Transform::default(),
            fov_deg: 60.0,
            projection: CameraProjection::Perspective,
            resolution: [5, 5],
            intrinsics: None,
            distortion: None,
            depth_range_m: None,
            sensor_effects: None,
        };
        let sample = |entity_u32, color_rgba| {
            let objects = vec![RenderObject {
                entity_u32,
                bounds: None,
                geometry: Arc::new(RenderGeometry::Triangles {
                    bounds_min: [0.999, -0.5, -0.5],
                    bounds_max: [1.001, 0.5, 0.5],
                    triangles: vec![ray_facing_triangle(1.0, color_rgba)],
                    textures: Vec::new(),
                    lights: Vec::new(),
                    bvh: Vec::new(),
                }),
                color_rgb: [255, 255, 255],
                transform: Transform::default(),
            }];
            RenderSceneSample {
                timestamp_sec: if entity_u32 == 1 { 0.0 } else { 1.0 },
                camera: Some(camera.clone()),
                object_bvh: Arc::new(ObjectBvh::from_objects(&objects)),
                objects,
                lights: fallback_lights(),
            }
        };
        let renderer = NativeRenderer::new();
        let red = sample(1, [255, 0, 0, 255]);
        let blue = sample(2, [0, 0, 255, 255]);

        let settings = PreparedRenderSettings::from_settings(RenderSettings::default());
        let color = renderer.trace_debug_rgb_shutter([5, 5], 2, 2, &[red, blue], None, &settings);

        assert!(color[0] > 20, "red sample should contribute to exposure");
        assert!(color[2] > 20, "blue sample should contribute to exposure");
        assert!(
            color[0] < 250 && color[2] < 250,
            "averaged exposure should not equal either single saturated sample"
        );
    }

    #[test]
    fn debug_rgb_rolling_shutter_uses_row_specific_sample_times() {
        let camera = CameraSpec {
            id: "camera".to_string(),
            name: "Camera".to_string(),
            transform: Transform::default(),
            fov_deg: 60.0,
            projection: CameraProjection::Perspective,
            resolution: [5, 2],
            intrinsics: None,
            distortion: None,
            depth_range_m: None,
            sensor_effects: None,
        };
        let sample = |timestamp_sec, entity_u32, color_rgba| {
            let objects = vec![RenderObject {
                entity_u32,
                bounds: None,
                geometry: Arc::new(RenderGeometry::Triangles {
                    bounds_min: [0.999, -10.0, -10.0],
                    bounds_max: [1.001, 10.0, 10.0],
                    triangles: vec![Triangle {
                        a: [1.0, -10.0, -10.0],
                        b: [1.0, -10.0, 10.0],
                        c: [1.0, 10.0, -10.0],
                        normal_a: [-1.0, 0.0, 0.0],
                        normal_b: [-1.0, 0.0, 0.0],
                        normal_c: [-1.0, 0.0, 0.0],
                        color_rgba,
                        color_linear_rgb: srgb_u8_to_linear_rgb([
                            color_rgba[0],
                            color_rgba[1],
                            color_rgba[2],
                        ]),
                        color_alpha: f32::from(color_rgba[3]) / 255.0,
                        vertex_color_a: None,
                        vertex_color_b: None,
                        vertex_color_c: None,
                        vertex_color_linear_a: None,
                        vertex_color_linear_b: None,
                        vertex_color_linear_c: None,
                        texcoord_a: None,
                        texcoord_b: None,
                        texcoord_c: None,
                        texcoord1_a: None,
                        texcoord1_b: None,
                        texcoord1_c: None,
                        tangent_a: None,
                        tangent_b: None,
                        tangent_c: None,
                        texture_index: None,
                        texture_texcoord: 0,
                        texture_transform: super::TextureTransform2D::default(),
                        metallic_factor: 0.0,
                        roughness_factor: 1.0,
                        metallic_roughness_texture_index: None,
                        metallic_roughness_texture_texcoord: 0,
                        metallic_roughness_texture_transform: super::TextureTransform2D::default(),
                        specular_glossiness_texture_index: None,
                        specular_glossiness_texture_texcoord: 0,
                        specular_glossiness_texture_transform: super::TextureTransform2D::default(),
                        ior: 1.5,
                        transmission_factor: 0.0,
                        transmission_texture_index: None,
                        transmission_texture_texcoord: 0,
                        transmission_texture_transform: super::TextureTransform2D::default(),
                        diffuse_transmission_factor: 0.0,
                        diffuse_transmission_texture_index: None,
                        diffuse_transmission_texture_texcoord: 0,
                        diffuse_transmission_texture_transform: super::TextureTransform2D::default(
                        ),
                        diffuse_transmission_color_factor: [1.0, 1.0, 1.0],
                        diffuse_transmission_color_texture_index: None,
                        diffuse_transmission_color_texture_texcoord: 0,
                        diffuse_transmission_color_texture_transform:
                            super::TextureTransform2D::default(),
                        dispersion: 0.0,
                        volume_thickness_factor: 0.0,
                        volume_thickness_texture_index: None,
                        volume_thickness_texture_texcoord: 0,
                        volume_thickness_texture_transform: super::TextureTransform2D::default(),
                        volume_attenuation_distance: f32::INFINITY,
                        volume_attenuation_color: [1.0, 1.0, 1.0],
                        clearcoat_factor: 0.0,
                        clearcoat_texture_index: None,
                        clearcoat_texture_texcoord: 0,
                        clearcoat_texture_transform: super::TextureTransform2D::default(),
                        clearcoat_roughness_factor: 1.0,
                        clearcoat_roughness_texture_index: None,
                        clearcoat_roughness_texture_texcoord: 0,
                        clearcoat_roughness_texture_transform: super::TextureTransform2D::default(),
                        clearcoat_normal_texture_index: None,
                        clearcoat_normal_texture_texcoord: 0,
                        clearcoat_normal_texture_transform: super::TextureTransform2D::default(),
                        clearcoat_normal_scale: 1.0,
                        sheen_color_factor: [0.0, 0.0, 0.0],
                        sheen_color_texture_index: None,
                        sheen_color_texture_texcoord: 0,
                        sheen_color_texture_transform: super::TextureTransform2D::default(),
                        sheen_roughness_factor: 1.0,
                        sheen_roughness_texture_index: None,
                        sheen_roughness_texture_texcoord: 0,
                        sheen_roughness_texture_transform: super::TextureTransform2D::default(),
                        anisotropy_strength: 0.0,
                        anisotropy_rotation: 0.0,
                        anisotropy_texture_index: None,
                        anisotropy_texture_texcoord: 0,
                        anisotropy_texture_transform: super::TextureTransform2D::default(),
                        iridescence_factor: 0.0,
                        iridescence_texture_index: None,
                        iridescence_texture_texcoord: 0,
                        iridescence_texture_transform: super::TextureTransform2D::default(),
                        iridescence_ior: 1.3,
                        iridescence_thickness_minimum_nm: 100.0,
                        iridescence_thickness_maximum_nm: 400.0,
                        iridescence_thickness_texture_index: None,
                        iridescence_thickness_texture_texcoord: 0,
                        iridescence_thickness_texture_transform: super::TextureTransform2D::default(
                        ),
                        specular_factor: 1.0,
                        specular_texture_index: None,
                        specular_texture_texcoord: 0,
                        specular_texture_transform: super::TextureTransform2D::default(),
                        specular_color_factor: [1.0, 1.0, 1.0],
                        specular_color_texture_index: None,
                        specular_color_texture_texcoord: 0,
                        specular_color_texture_transform: super::TextureTransform2D::default(),
                        normal_texture_index: None,
                        normal_texture_texcoord: 0,
                        normal_texture_transform: super::TextureTransform2D::default(),
                        normal_scale: 1.0,
                        emissive_rgb: [0.0, 0.0, 0.0],
                        emissive_texture_index: None,
                        emissive_texture_texcoord: 0,
                        emissive_texture_transform: super::TextureTransform2D::default(),
                        occlusion_texture_index: None,
                        occlusion_texture_texcoord: 0,
                        occlusion_texture_transform: super::TextureTransform2D::default(),
                        occlusion_strength: 1.0,
                        unlit: false,
                        alpha_cutoff: None,
                        alpha_mode: MaterialAlphaMode::Opaque,
                        double_sided: false,
                    }],
                    textures: Vec::new(),
                    lights: Vec::new(),
                    bvh: Vec::new(),
                }),
                color_rgb: [255, 255, 255],
                transform: Transform::default(),
            }];
            RenderSceneSample {
                timestamp_sec,
                camera: Some(camera.clone()),
                object_bvh: Arc::new(ObjectBvh::from_objects(&objects)),
                objects,
                lights: fallback_lights(),
            }
        };
        let renderer = NativeRenderer::new();
        let red = sample(0.0, 1, [255, 0, 0, 255]);
        let blue = sample(1.0, 2, [0, 0, 255, 255]);
        let settings = PreparedRenderSettings::from_settings(RenderSettings::default());
        let policy = Some(ShutterPolicy {
            exposure_sec: 0.0,
            samples: 1,
            mode: ShutterMode::RollingTopToBottom,
            readout_sec: 1.0,
        });

        let top = renderer.trace_debug_rgb_shutter(
            [5, 2],
            2,
            0,
            &[red.clone(), blue.clone()],
            policy,
            &settings,
        );
        let bottom =
            renderer.trace_debug_rgb_shutter([5, 2], 2, 1, &[red, blue], policy, &settings);

        assert!(top[0] > top[2], "top row should use earlier red sample");
        assert!(
            bottom[2] > bottom[0],
            "bottom row should use later blue sample"
        );
    }

    #[test]
    fn debug_rgb_composites_transparent_layers_within_same_mesh() {
        let object = RenderObject {
            entity_u32: 1,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Triangles {
                bounds_min: [1.0, -0.5, -0.5],
                bounds_max: [2.0, 0.5, 0.5],
                triangles: vec![
                    ray_facing_triangle(1.0, [255, 0, 0, 128]),
                    ray_facing_triangle(2.0, [0, 0, 255, 255]),
                ],
                textures: Vec::new(),
                lights: Vec::new(),
                bvh: Vec::new(),
            }),
            color_rgb: [255, 255, 255],
            transform: Transform::default(),
        };
        let camera = CameraSpec {
            id: "camera".to_string(),
            name: "Camera".to_string(),
            transform: Transform::default(),
            fov_deg: 60.0,
            projection: CameraProjection::Perspective,
            resolution: [5, 5],
            intrinsics: None,
            distortion: None,
            depth_range_m: None,
            sensor_effects: None,
        };

        let settings = PreparedRenderSettings::from_settings(RenderSettings::default());
        let color =
            NativeRenderer::new().trace_debug_rgb(&camera, [5, 5], 2, 2, &[object], &[], &settings);

        assert!(
            color[0] > 20,
            "front transparent red layer should contribute"
        );
        assert!(
            color[2] > 20,
            "back blue layer in the same mesh should contribute"
        );
    }

    #[test]
    fn render_settings_control_ambient_background_and_tone_mapping() {
        let hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [200, 200, 200],
            color_linear_rgb: srgb_u8_to_linear_rgb([200, 200, 200]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 1.0,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [0.0, 0.0, 1.0],
            view_dir: [0.0, 0.0, 1.0],
        };
        let dark_settings = RenderSettings {
            ambient_intensity: 0.0,
            ..RenderSettings::default()
        };
        let bright_settings = RenderSettings {
            ambient_intensity: 1.0,
            ambient_rgb: [255, 255, 255],
            ..RenderSettings::default()
        };

        let dark_prepared = PreparedRenderSettings::from_settings(dark_settings.clone());
        let bright_prepared = PreparedRenderSettings::from_settings(bright_settings.clone());

        let dark = tone_map_rgb(
            shade_hit_with_lights(&hit, &[], &[], &dark_prepared),
            &dark_settings,
        );
        let bright = tone_map_rgb(
            shade_hit_with_lights(&hit, &[], &[], &bright_prepared),
            &bright_settings,
        );
        let background_settings = PreparedRenderSettings::from_settings(RenderSettings {
            background_rgb: [1, 2, 3],
            ..RenderSettings::default()
        });
        let background = composite_debug_rgb_hits(
            &[],
            &[],
            &[],
            &background_settings,
            srgb_u8_to_linear_radiance([1, 2, 3]),
        );
        let linear_settings = RenderSettings::default();
        let linear = tone_map_rgb([1000.0, 1000.0, 1000.0], &linear_settings);
        let reinhard_settings = RenderSettings {
            tone_mapping: ToneMapping::Reinhard,
            ..RenderSettings::default()
        };
        let reinhard = tone_map_rgb([1000.0, 1000.0, 1000.0], &reinhard_settings);

        assert!(bright[0] > dark[0], "ambient intensity should brighten RGB");
        assert_eq!(background, [1, 2, 3]);
        assert!(
            reinhard[0] < linear[0],
            "tone mapping should compress highlights"
        );
    }

    #[test]
    fn ambient_rgb_decodes_srgb_for_linear_shading() {
        let hit = matte_test_hit([0.0, 0.0, 1.0]);
        let settings = RenderSettings {
            ambient_rgb: [128, 128, 128],
            ambient_intensity: 1.0,
            ..RenderSettings::default()
        };
        let prepared = PreparedRenderSettings::from_settings(settings);
        let shaded = shade_hit_with_lights(&hit, &[], &[], &prepared);
        let ambient_linear = srgb_u8_to_linear_rgb([128, 128, 128]);

        assert_rgb_close(
            shaded,
            [
                hit.color_linear_rgb[0] * 255.0 * ambient_linear[0],
                hit.color_linear_rgb[1] * 255.0 * ambient_linear[1],
                hit.color_linear_rgb[2] * 255.0 * ambient_linear[2],
            ],
            1.0e-4,
        );
    }

    #[test]
    fn render_settings_environment_samples_sky_horizon_ground_and_ambient() {
        let environment = EnvironmentSettings {
            sky_top_rgb: [10, 20, 30],
            sky_horizon_rgb: [40, 50, 60],
            ground_rgb: [70, 80, 90],
            map: None,
            map_rotation_deg: 0.0,
            intensity: 2.0,
            ambient_intensity: 0.5,
        };
        let settings = RenderSettings {
            ambient_intensity: 1.0,
            ambient_rgb: [0, 0, 0],
            environment: Some(environment),
            ..RenderSettings::default()
        };
        let settings = PreparedRenderSettings::from_settings(settings);
        let hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [200, 200, 200],
            color_linear_rgb: srgb_u8_to_linear_rgb([200, 200, 200]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 1.0,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [0.0, 0.0, 1.0],
            view_dir: [0.0, 0.0, 1.0],
        };

        assert_rgb_close(
            environment_background_rgb([0.0, 0.0, 1.0], &settings),
            scale(srgb_u8_to_linear_radiance([10, 20, 30]), 2.0),
            1.0e-5,
        );
        assert_rgb_close(
            environment_background_rgb([1.0, 0.0, 0.0], &settings),
            scale(srgb_u8_to_linear_radiance([40, 50, 60]), 2.0),
            1.0e-5,
        );
        assert_rgb_close(
            environment_background_rgb([0.0, 0.0, -1.0], &settings),
            scale(srgb_u8_to_linear_radiance([70, 80, 90]), 2.0),
            1.0e-5,
        );
        assert!(
            shade_hit_with_lights(&hit, &[], &[], &settings)[2] > 0.0,
            "sky environment should contribute to ambient surface lighting"
        );
    }

    #[test]
    fn render_settings_environment_reflection_responds_to_roughness() {
        let settings = PreparedRenderSettings::from_settings(RenderSettings {
            ambient_intensity: 0.0,
            environment: Some(EnvironmentSettings {
                sky_top_rgb: [240, 240, 240],
                sky_horizon_rgb: [120, 120, 120],
                ground_rgb: [10, 10, 10],
                map: None,
                map_rotation_deg: 0.0,
                intensity: 1.0,
                ambient_intensity: 0.0,
            }),
            ..RenderSettings::default()
        });
        let matte = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [180, 180, 180],
            color_linear_rgb: srgb_u8_to_linear_rgb([180, 180, 180]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 1.0,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [0.0, 0.0, 1.0],
            view_dir: [0.0, 0.0, 1.0],
        };
        let glossy = Hit {
            roughness: 0.05,
            ..matte
        };

        let matte_reflection = environment_reflection_rgb(&matte, &settings);
        let glossy_reflection = environment_reflection_rgb(&glossy, &settings);

        assert!(
            glossy_reflection[0] > matte_reflection[0] * 3.0,
            "low roughness should produce stronger environment reflection"
        );
    }

    #[test]
    fn polished_material_reflects_nearby_scene_geometry() {
        let settings = PreparedRenderSettings::from_settings(RenderSettings {
            ambient_intensity: 0.0,
            background_rgb: [0, 0, 0],
            ..RenderSettings::default()
        });
        let base_hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [255, 255, 255],
            color_linear_rgb: srgb_u8_to_linear_rgb([255, 255, 255]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 1.0,
            roughness: 0.04,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [1.0, 0.0, 0.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [0.0, 1.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [1.0, 0.0, 0.0],
            view_dir: [1.0, 0.0, 0.0],
        };
        let rough_hit = Hit {
            roughness: 1.0,
            ..base_hit
        };
        let mut target_triangles = ray_facing_rect(1.0, -0.25, 0.25, -0.25, 0.25, [0, 0, 255, 255]);
        for triangle in &mut target_triangles {
            triangle.unlit = true;
        }
        let target = RenderObject {
            entity_u32: 2,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Triangles {
                bounds_min: [0.999, -0.25, -0.25],
                bounds_max: [1.001, 0.25, 0.25],
                triangles: target_triangles,
                textures: Vec::new(),
                lights: Vec::new(),
                bvh: Vec::new(),
            }),
            color_rgb: [255, 255, 255],
            transform: Transform::default(),
        };

        let polished = shade_hit_with_lights(&base_hit, &[target.clone()], &[], &settings);
        let rough = shade_hit_with_lights(&rough_hit, &[target], &[], &settings);

        assert!(
            polished[2] > polished[0] + 20.0 && polished[2] > polished[1] + 20.0,
            "polished metal should pick up the blue scene target in reflection, got {polished:?}"
        );
        assert!(
            polished[2] > rough[2] + 20.0,
            "rough material should not receive the sharp reflected scene target, polished {polished:?} rough {rough:?}"
        );
    }

    #[test]
    fn clearcoat_scene_reflection_uses_clearcoat_normal() {
        let settings = PreparedRenderSettings::from_settings(RenderSettings {
            ambient_intensity: 0.0,
            background_rgb: [0, 0, 0],
            ..RenderSettings::default()
        });
        let base_hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [255, 255, 255],
            color_linear_rgb: srgb_u8_to_linear_rgb([255, 255, 255]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 1.0,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 0.04,
            clearcoat_normal: [1.0, 0.0, 0.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [0.0, 1.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [0.0, 0.0, 1.0],
            view_dir: [1.0, 0.0, 0.0],
        };
        let clearcoated_hit = Hit {
            clearcoat_factor: 1.0,
            ..base_hit
        };
        let mut target_triangles = ray_facing_rect(1.0, -0.25, 0.25, -0.25, 0.25, [0, 0, 255, 255]);
        for triangle in &mut target_triangles {
            triangle.unlit = true;
        }
        let target = RenderObject {
            entity_u32: 2,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Triangles {
                bounds_min: [0.999, -0.25, -0.25],
                bounds_max: [1.001, 0.25, 0.25],
                triangles: target_triangles,
                textures: Vec::new(),
                lights: Vec::new(),
                bvh: Vec::new(),
            }),
            color_rgb: [255, 255, 255],
            transform: Transform::default(),
        };

        let base =
            scene_reflection_rgb(&base_hit, std::slice::from_ref(&target), &[], &settings, 1);
        let clearcoated = scene_reflection_rgb(&clearcoated_hit, &[target], &[], &settings, 1);

        assert!(
            base[2] < 1.0,
            "rough base lobe should not reflect the scene target, got {base:?}"
        );
        assert!(
            clearcoated[2] > clearcoated[0] + 5.0 && clearcoated[2] > clearcoated[1] + 5.0,
            "smooth clearcoat should reflect the blue scene target with its own normal, got {clearcoated:?}"
        );
    }

    #[test]
    fn clearcoat_attenuates_base_scene_reflection() {
        let settings = PreparedRenderSettings::from_settings(RenderSettings {
            ambient_intensity: 0.0,
            background_rgb: [0, 0, 0],
            ..RenderSettings::default()
        });
        let base_hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [255, 255, 255],
            color_linear_rgb: srgb_u8_to_linear_rgb([255, 255, 255]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 1.0,
            roughness: 0.04,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: normalize([0.05, 0.0, 1.0]),
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [0.0, 1.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [1.0, 0.0, 0.0],
            view_dir: [1.0, 0.0, 0.0],
        };
        let clearcoated_hit = Hit {
            clearcoat_factor: 1.0,
            ..base_hit
        };
        let mut target_triangles = ray_facing_rect(1.0, -0.25, 0.25, -0.25, 0.25, [0, 0, 255, 255]);
        for triangle in &mut target_triangles {
            triangle.unlit = true;
        }
        let target = RenderObject {
            entity_u32: 2,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Triangles {
                bounds_min: [0.999, -0.25, -0.25],
                bounds_max: [1.001, 0.25, 0.25],
                triangles: target_triangles,
                textures: Vec::new(),
                lights: Vec::new(),
                bvh: Vec::new(),
            }),
            color_rgb: [255, 255, 255],
            transform: Transform::default(),
        };

        let base =
            scene_reflection_rgb(&base_hit, std::slice::from_ref(&target), &[], &settings, 1);
        let clearcoated = scene_reflection_rgb(&clearcoated_hit, &[target], &[], &settings, 1);

        assert!(
            base[2] > 10.0,
            "base polished material should reflect the blue target, got {base:?}"
        );
        assert!(
            clearcoated[2] < base[2] * 0.5,
            "grazing rough clearcoat should attenuate the base scene reflection, base {base:?} clearcoated {clearcoated:?}"
        );
    }

    #[test]
    fn rough_reflection_samples_off_axis_scene_geometry() {
        let one_sample_settings = PreparedRenderSettings::from_settings(RenderSettings {
            ambient_intensity: 0.0,
            rough_reflection_samples: 1,
            ..RenderSettings::default()
        });
        let multi_sample_settings = PreparedRenderSettings::from_settings(RenderSettings {
            ambient_intensity: 0.0,
            rough_reflection_samples: 16,
            ..RenderSettings::default()
        });
        let hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [255, 255, 255],
            color_linear_rgb: srgb_u8_to_linear_rgb([255, 255, 255]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 1.0,
            roughness: 0.70,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [1.0, 0.0, 0.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [0.0, 1.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [1.0, 0.0, 0.0],
            view_dir: [1.0, 0.0, 0.0],
        };
        let mut target_triangles = ray_facing_rect(1.0, -0.25, 0.05, 0.04, 0.25, [0, 0, 255, 255]);
        for triangle in &mut target_triangles {
            triangle.unlit = true;
        }
        let target = RenderObject {
            entity_u32: 2,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Triangles {
                bounds_min: [0.999, -0.25, 0.04],
                bounds_max: [1.001, 0.05, 0.25],
                triangles: target_triangles,
                textures: Vec::new(),
                lights: Vec::new(),
                bvh: Vec::new(),
            }),
            color_rgb: [255, 255, 255],
            transform: Transform::default(),
        };

        let one_sample = scene_reflection_rgb(
            &hit,
            std::slice::from_ref(&target),
            &[],
            &one_sample_settings,
            1,
        );
        let multi_sample = scene_reflection_rgb(&hit, &[target], &[], &multi_sample_settings, 1);

        assert!(
            one_sample[2] < 1.0,
            "single sharp reflection ray should miss the off-axis blue target, got {one_sample:?}"
        );
        assert!(
            multi_sample[2] > one_sample[2] + 0.5,
            "multi-sampled rough reflection should pick up off-axis scene geometry, one {one_sample:?} multi {multi_sample:?}"
        );
    }

    #[test]
    fn specular_reflection_bounces_reflected_scene_geometry() {
        let one_bounce_settings = PreparedRenderSettings::from_settings(RenderSettings {
            ambient_intensity: 0.0,
            background_rgb: [0, 0, 0],
            specular_reflection_bounces: 1,
            ..RenderSettings::default()
        });
        let two_bounce_settings = PreparedRenderSettings::from_settings(RenderSettings {
            ambient_intensity: 0.0,
            background_rgb: [0, 0, 0],
            specular_reflection_bounces: 2,
            ..RenderSettings::default()
        });
        let hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [255, 255, 255],
            color_linear_rgb: srgb_u8_to_linear_rgb([255, 255, 255]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 1.0,
            roughness: 0.04,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [1.0, 0.0, 0.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [0.0, 1.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [1.0, 0.0, 0.0],
            view_dir: [1.0, 0.0, 0.0],
        };
        let mut mirror_triangles =
            ray_facing_rect(1.0, -0.25, 0.25, -0.25, 0.25, [255, 255, 255, 255]);
        let redirect_normal = normalize([1.0, 0.0, -1.0]);
        for triangle in &mut mirror_triangles {
            triangle.normal_a = redirect_normal;
            triangle.normal_b = redirect_normal;
            triangle.normal_c = redirect_normal;
            triangle.metallic_factor = 1.0;
            triangle.roughness_factor = 0.04;
        }
        let mirror = RenderObject {
            entity_u32: 2,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Triangles {
                bounds_min: [0.999, -0.25, -0.25],
                bounds_max: [1.001, 0.25, 0.25],
                triangles: mirror_triangles,
                textures: Vec::new(),
                lights: Vec::new(),
                bvh: Vec::new(),
            }),
            color_rgb: [255, 255, 255],
            transform: Transform::default(),
        };

        let mut target_triangles = z_facing_rect(1.0, 0.75, 1.25, -0.25, 0.25, [0, 0, 255, 255]);
        for triangle in &mut target_triangles {
            triangle.unlit = true;
        }
        let target = RenderObject {
            entity_u32: 3,
            bounds: None,
            geometry: Arc::new(RenderGeometry::Triangles {
                bounds_min: [0.75, -0.25, 0.999],
                bounds_max: [1.25, 0.25, 1.001],
                triangles: target_triangles,
                textures: Vec::new(),
                lights: Vec::new(),
                bvh: Vec::new(),
            }),
            color_rgb: [255, 255, 255],
            transform: Transform::default(),
        };

        let one_bounce = scene_reflection_rgb(
            &hit,
            &[mirror.clone(), target.clone()],
            &[],
            &one_bounce_settings,
            1,
        );
        let two_bounce =
            scene_reflection_rgb(&hit, &[mirror, target], &[], &two_bounce_settings, 2);

        assert!(
            one_bounce[2] < 1.0,
            "single reflection bounce should stop at the dark mirror, got {one_bounce:?}"
        );
        assert!(
            two_bounce[2] > one_bounce[2] + 20.0,
            "second reflection bounce should pick up the blue target, one {one_bounce:?} two {two_bounce:?}"
        );
    }

    #[test]
    fn render_settings_environment_reflection_tints_metallic_materials() {
        let settings = PreparedRenderSettings::from_settings(RenderSettings {
            ambient_intensity: 0.0,
            environment: Some(EnvironmentSettings {
                sky_top_rgb: [255, 255, 255],
                sky_horizon_rgb: [255, 255, 255],
                ground_rgb: [255, 255, 255],
                map: None,
                map_rotation_deg: 0.0,
                intensity: 1.0,
                ambient_intensity: 0.0,
            }),
            ..RenderSettings::default()
        });
        let dielectric = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [220, 20, 20],
            color_linear_rgb: srgb_u8_to_linear_rgb([220, 20, 20]),
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 0.0,
            roughness: 0.05,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [0.0, 0.0, 1.0],
            view_dir: [0.0, 0.0, 1.0],
        };
        let metal = Hit {
            metallic: 1.0,
            ..dielectric
        };

        let dielectric_reflection = environment_reflection_rgb(&dielectric, &settings);
        let metal_reflection = environment_reflection_rgb(&metal, &settings);

        assert!(
            metal_reflection[0] > dielectric_reflection[0] * 5.0,
            "metal should reflect much more red energy from its base color"
        );
        assert!(
            metal_reflection[0] > metal_reflection[2] * 5.0,
            "metallic reflection should be tinted by the material base color"
        );
    }

    #[test]
    fn reflection_probe_drives_material_lighting_without_replacing_visible_environment() {
        let settings = PreparedRenderSettings {
            settings: RenderSettings {
                ambient_intensity: 0.0,
                environment: Some(EnvironmentSettings {
                    sky_top_rgb: [255, 0, 0],
                    sky_horizon_rgb: [255, 0, 0],
                    ground_rgb: [255, 0, 0],
                    map: None,
                    map_rotation_deg: 0.0,
                    intensity: 1.0,
                    ambient_intensity: 0.0,
                }),
                reflection_probe: Some(ReflectionProbeSettings {
                    map: "probe.hdr".to_string(),
                    rotation_deg: 0.0,
                    intensity: 1.0,
                    ambient_intensity: 0.0,
                    position: None,
                    box_size_m: None,
                    influence_radius_m: None,
                    falloff_power: None,
                }),
                ..RenderSettings::default()
            },
            environment_map: None,
            reflection_probes: vec![PreparedReflectionProbe {
                settings: ReflectionProbeSettings {
                    map: "probe.hdr".to_string(),
                    rotation_deg: 0.0,
                    intensity: 1.0,
                    ambient_intensity: 0.0,
                    position: None,
                    box_size_m: None,
                    influence_radius_m: None,
                    falloff_power: None,
                },
                texture: Arc::new(EnvironmentTexture {
                    width: 1,
                    height: 1,
                    rgb: vec![[0.0, 255.0, 0.0]],
                    roughness_mips: build_environment_roughness_mips(1, 1, &[[0.0, 255.0, 0.0]]),
                    diffuse_irradiance: build_environment_diffuse_irradiance(
                        1,
                        1,
                        &[[0.0, 255.0, 0.0]],
                    ),
                }),
            }],
            environment_brdf_lut: Arc::new(build_environment_brdf_lut(32)),
        };
        let hit = Hit {
            t: 1.0,
            entity_u32: 1,
            color_rgb: [255, 255, 255],
            color_linear_rgb: [1.0, 1.0, 1.0],
            emission_rgb: [0.0, 0.0, 0.0],
            alpha: 1.0,
            transmission_filter_rgb: [1.0, 1.0, 1.0],
            diffuse_transmission: 0.0,
            diffuse_transmission_color: [1.0, 1.0, 1.0],
            dispersion: 0.0,
            volume_thickness_m: 0.0,
            volume_attenuation_distance_m: f32::INFINITY,
            volume_attenuation_color: [1.0, 1.0, 1.0],
            occlusion: 1.0,
            metallic: 1.0,
            roughness: 0.08,
            ior: 1.5,
            clearcoat_factor: 0.0,
            clearcoat_roughness: 1.0,
            clearcoat_normal: [0.0, 0.0, 1.0],
            sheen_color: [0.0, 0.0, 0.0],
            sheen_roughness: 1.0,
            anisotropy_strength: 0.0,
            anisotropy_direction: [1.0, 0.0, 0.0],
            iridescence_factor: 0.0,
            iridescence_thickness_nm: 0.0,
            iridescence_ior: 1.3,
            specular_factor: 1.0,
            specular_color: [1.0, 1.0, 1.0],
            unlit: false,
            point: [0.0, 0.0, 0.0],
            normal: [0.0, 0.0, 1.0],
            view_dir: [0.0, 0.0, 1.0],
        };

        let background = environment_background_rgb([0.0, 0.0, 1.0], &settings);
        let reflection = environment_reflection_rgb(&hit, &settings);

        assert!(background[0] > background[1] * 10.0);
        assert!(reflection[1] > reflection[0] * 10.0);
    }

    #[test]
    fn spatial_reflection_probe_applies_falloff_and_box_projection() {
        let probe = ReflectionProbeSettings {
            map: "probe.hdr".to_string(),
            rotation_deg: 0.0,
            intensity: 1.0,
            ambient_intensity: 0.0,
            position: Some([0.0, 0.0, 0.0]),
            box_size_m: Some([4.0, 4.0, 4.0]),
            influence_radius_m: Some(2.0),
            falloff_power: Some(2.0),
        };

        assert!((reflection_probe_influence_weight(&probe, [0.0, 0.0, 0.0]) - 1.0).abs() < 1.0e-5);
        assert!((reflection_probe_influence_weight(&probe, [1.0, 0.0, 0.0]) - 0.25).abs() < 1.0e-5);
        assert_eq!(
            reflection_probe_influence_weight(&probe, [2.5, 0.0, 0.0]),
            0.0
        );

        let corrected = reflection_probe_sample_direction(&probe, [1.0, 0.0, 0.0], [0.0, 1.0, 0.0]);
        assert!(
            corrected[0] > 0.40 && corrected[1] > 0.80,
            "box-projected probe direction should aim from probe center to the box hit point"
        );

        let outside = ReflectionProbeSettings {
            influence_radius_m: None,
            ..probe
        };
        assert_eq!(
            reflection_probe_influence_weight(&outside, [3.0, 0.0, 0.0]),
            0.0
        );
        assert_rgb_close(
            reflection_probe_sample_direction(&outside, [3.0, 0.0, 0.0], [0.0, 1.0, 0.0]),
            [0.0, 1.0, 0.0],
            1.0e-5,
        );
    }

    #[test]
    fn multiple_reflection_probes_blend_by_surface_location() {
        let probe_texture = |rgb: [f32; 3]| {
            Arc::new(EnvironmentTexture {
                width: 1,
                height: 1,
                rgb: vec![rgb],
                roughness_mips: build_environment_roughness_mips(1, 1, &[rgb]),
                diffuse_irradiance: build_environment_diffuse_irradiance(1, 1, &[rgb]),
            })
        };
        let probe_settings = |map: &str, position: [f32; 3]| ReflectionProbeSettings {
            map: map.to_string(),
            rotation_deg: 0.0,
            intensity: 1.0,
            ambient_intensity: 0.0,
            position: Some(position),
            box_size_m: None,
            influence_radius_m: Some(2.0),
            falloff_power: Some(1.0),
        };
        let settings = PreparedRenderSettings {
            settings: RenderSettings {
                environment: Some(EnvironmentSettings {
                    sky_top_rgb: [0, 255, 0],
                    sky_horizon_rgb: [0, 255, 0],
                    ground_rgb: [0, 255, 0],
                    map: None,
                    map_rotation_deg: 0.0,
                    intensity: 1.0,
                    ambient_intensity: 0.0,
                }),
                reflection_probes: vec![
                    probe_settings("left.hdr", [-1.0, 0.0, 0.0]),
                    probe_settings("right.hdr", [1.0, 0.0, 0.0]),
                ],
                ..RenderSettings::default()
            },
            environment_map: None,
            reflection_probes: vec![
                PreparedReflectionProbe {
                    settings: probe_settings("left.hdr", [-1.0, 0.0, 0.0]),
                    texture: probe_texture([255.0, 0.0, 0.0]),
                },
                PreparedReflectionProbe {
                    settings: probe_settings("right.hdr", [1.0, 0.0, 0.0]),
                    texture: probe_texture([0.0, 0.0, 255.0]),
                },
            ],
            environment_brdf_lut: Arc::new(build_environment_brdf_lut(32)),
        };

        let left = rough_environment_radiance_rgb(
            [1.0, 0.0, 0.0],
            [0.0, 0.0, 1.0],
            [-1.0, 0.0, 0.0],
            0.04,
            &settings,
        );
        let middle = rough_environment_radiance_rgb(
            [1.0, 0.0, 0.0],
            [0.0, 0.0, 1.0],
            [0.0, 0.0, 0.0],
            0.04,
            &settings,
        );
        let outside = rough_environment_radiance_rgb(
            [1.0, 0.0, 0.0],
            [0.0, 0.0, 1.0],
            [4.0, 0.0, 0.0],
            0.04,
            &settings,
        );

        assert!(
            left[0] > left[2] * 10.0,
            "left probe should dominate near its center"
        );
        assert!(
            middle[0] > 100.0 && middle[2] > 100.0,
            "middle point should blend red and blue probes"
        );
        assert!(
            outside[1] > outside[0] * 10.0 && outside[1] > outside[2] * 10.0,
            "outside probe influence should fall back to visible environment"
        );
    }

    #[test]
    fn scene_reflection_probes_merge_into_native_render_settings() {
        let request_probe = ReflectionProbeSettings {
            map: "request.hdr".to_string(),
            rotation_deg: 0.0,
            intensity: 1.0,
            ambient_intensity: 0.25,
            position: None,
            box_size_m: None,
            influence_radius_m: None,
            falloff_power: None,
        };
        let scene_probe = ReflectionProbeSettings {
            map: "scene.hdr".to_string(),
            rotation_deg: 90.0,
            intensity: 2.0,
            ambient_intensity: 0.5,
            position: Some([1.0, 0.0, 0.0]),
            box_size_m: Some([2.0, 2.0, 2.0]),
            influence_radius_m: Some(3.0),
            falloff_power: Some(2.0),
        };
        let mut scene = SceneGraph::empty();
        scene.reflection_probes.push(scene_probe.clone());
        let request = ObservationRequest {
            camera_id: None,
            views: vec![ObservationView::State],
            resolution: [1, 1],
            segmentation_policy: None,
            shutter_policy: None,
            render_settings: Some(RenderSettings {
                reflection_probes: vec![request_probe.clone()],
                ..RenderSettings::default()
            }),
        };

        let settings = render_settings_for_scene(&scene, &request).expect("merged settings");

        assert_eq!(settings.reflection_probes, vec![request_probe, scene_probe]);
    }

    #[test]
    fn scene_render_settings_supply_native_defaults_when_request_is_empty() {
        let settings_probe = ReflectionProbeSettings {
            map: "settings.hdr".to_string(),
            rotation_deg: 0.0,
            intensity: 1.0,
            ambient_intensity: 0.25,
            position: None,
            box_size_m: None,
            influence_radius_m: None,
            falloff_power: None,
        };
        let scene_probe = ReflectionProbeSettings {
            map: "scene.hdr".to_string(),
            rotation_deg: 0.0,
            intensity: 1.0,
            ambient_intensity: 0.25,
            position: Some([1.0, 0.0, 0.0]),
            box_size_m: None,
            influence_radius_m: Some(2.0),
            falloff_power: None,
        };
        let mut scene = SceneGraph::empty();
        scene.render_settings = Some(RenderSettings {
            background_rgb: [1, 2, 3],
            ambient_intensity: 0.75,
            tone_mapping: ToneMapping::Reinhard,
            reflection_probes: vec![settings_probe.clone()],
            ..RenderSettings::default()
        });
        scene.reflection_probes.push(scene_probe.clone());
        let request = ObservationRequest {
            camera_id: None,
            views: vec![ObservationView::State],
            resolution: [1, 1],
            segmentation_policy: None,
            shutter_policy: None,
            render_settings: None,
        };

        let settings = render_settings_for_scene(&scene, &request).expect("scene settings");

        assert_eq!(settings.background_rgb, [1, 2, 3]);
        assert_eq!(settings.ambient_intensity, 0.75);
        assert_eq!(settings.tone_mapping, ToneMapping::Reinhard);
        assert_eq!(
            settings.reflection_probes,
            vec![settings_probe, scene_probe]
        );
    }

    #[test]
    fn request_render_settings_override_scene_render_defaults() {
        let mut scene = SceneGraph::empty();
        scene.render_settings = Some(RenderSettings {
            background_rgb: [1, 2, 3],
            ambient_intensity: 0.75,
            ..RenderSettings::default()
        });
        let request = ObservationRequest {
            camera_id: None,
            views: vec![ObservationView::State],
            resolution: [1, 1],
            segmentation_policy: None,
            shutter_policy: None,
            render_settings: Some(RenderSettings {
                background_rgb: [9, 8, 7],
                ambient_intensity: 0.2,
                ..RenderSettings::default()
            }),
        };

        let settings = render_settings_for_scene(&scene, &request).expect("request settings");

        assert_eq!(settings.background_rgb, [9, 8, 7]);
        assert_eq!(settings.ambient_intensity, 0.2);
    }

    #[test]
    fn rough_environment_reflection_filters_environment_map_neighborhood() {
        let mut rgb = vec![[10.0, 20.0, 180.0]; 8 * 5];
        rgb[2 * 8 + 3] = [20.0, 220.0, 20.0];
        rgb[2 * 8 + 4] = [255.0, 0.0, 0.0];
        rgb[2 * 8 + 5] = [230.0, 210.0, 20.0];
        rgb[1 * 8 + 4] = [20.0, 180.0, 220.0];
        rgb[3 * 8 + 4] = [180.0, 20.0, 220.0];
        let settings = PreparedRenderSettings {
            settings: RenderSettings {
                environment: Some(EnvironmentSettings {
                    sky_top_rgb: [0, 0, 0],
                    sky_horizon_rgb: [0, 0, 0],
                    ground_rgb: [0, 0, 0],
                    map: None,
                    map_rotation_deg: 0.0,
                    intensity: 1.0,
                    ambient_intensity: 0.0,
                }),
                ..RenderSettings::default()
            },
            environment_map: Some(Arc::new(EnvironmentTexture {
                width: 8,
                height: 5,
                roughness_mips: build_environment_roughness_mips(8, 5, &rgb),
                diffuse_irradiance: build_environment_diffuse_irradiance(8, 5, &rgb),
                rgb,
            })),
            reflection_probes: Vec::new(),
            environment_brdf_lut: Arc::new(build_environment_brdf_lut(32)),
        };

        let glossy = rough_environment_radiance_rgb(
            [1.0, 0.0, 0.0],
            [0.0, 0.0, 1.0],
            [0.0, 0.0, 0.0],
            0.04,
            &settings,
        );
        let rough = rough_environment_radiance_rgb(
            [1.0, 0.0, 0.0],
            [0.0, 0.0, 1.0],
            [0.0, 0.0, 0.0],
            1.0,
            &settings,
        );

        assert!(
            rough[0] < glossy[0],
            "rough environment reflection should reduce sharp red texel energy"
        );
        assert!(
            rough[1] > glossy[1] + 20.0 || rough[2] > glossy[2] + 20.0,
            "rough environment reflection should include neighboring texel colors"
        );
    }

    #[test]
    fn environment_brdf_lut_modulates_grazing_and_rough_reflections() {
        let lut = build_environment_brdf_lut(16);
        let face_on_smooth = sample_environment_brdf_lut(&lut, 1.0, 0.04);
        let face_on_rough = sample_environment_brdf_lut(&lut, 1.0, 1.0);
        let grazing_smooth = sample_environment_brdf_lut(&lut, 0.02, 0.04);

        assert!(
            face_on_smooth[0] > face_on_rough[0],
            "rough reflection should receive a lower specular scale from the BRDF lookup"
        );
        assert!(
            grazing_smooth[1] > face_on_smooth[1],
            "grazing angles should receive more additive Fresnel bias"
        );
    }

    #[test]
    fn environment_texture_builds_prefiltered_roughness_mips() {
        let mut rgb = vec![[0.0, 0.0, 0.0]; 4 * 2];
        rgb[1] = [255.0, 0.0, 0.0];
        let texture = EnvironmentTexture {
            width: 4,
            height: 2,
            roughness_mips: build_environment_roughness_mips(4, 2, &rgb),
            diffuse_irradiance: build_environment_diffuse_irradiance(4, 2, &rgb),
            rgb,
        };

        assert!(
            texture.roughness_mips.len() >= 3,
            "roughness cache should include downsampled levels"
        );
        assert_eq!(texture.roughness_mips[0].width, 4);
        assert_eq!(texture.roughness_mips[1].width, 2);
        assert_eq!(texture.roughness_mips[2].width, 1);

        let sharp = sample_prefiltered_environment_texture(&texture, [0.0, -1.0, 0.0], 0.0, 0.0);
        let rough = sample_prefiltered_environment_texture(&texture, [0.0, -1.0, 0.0], 0.0, 1.0);
        assert!(
            rough[0] < sharp[0],
            "rough prefilter should blur a sharp environment highlight"
        );
    }

    #[test]
    fn environment_texture_builds_diffuse_irradiance_cache() {
        let mut rgb = vec![[0.0, 0.0, 0.0]; 8 * 5];
        rgb[1 * 8 + 4] = [0.0, 255.0, 0.0];
        rgb[2 * 8 + 4] = [255.0, 0.0, 0.0];
        let texture = EnvironmentTexture {
            width: 8,
            height: 5,
            roughness_mips: build_environment_roughness_mips(8, 5, &rgb),
            diffuse_irradiance: build_environment_diffuse_irradiance(8, 5, &rgb),
            rgb,
        };
        let irradiance = texture
            .diffuse_irradiance
            .as_ref()
            .expect("diffuse irradiance cache");

        assert!(irradiance.width <= texture.width);
        assert!(irradiance.height <= texture.height);

        let direct = sample_environment_texture(&texture, [1.0, 0.0, 0.0], 0.0);
        let diffuse = sample_diffuse_environment_texture(&texture, [1.0, 0.0, 0.0], 0.0);
        assert!(
            diffuse[1] > direct[1] + 1.0,
            "diffuse irradiance should include nearby hemisphere energy, not only the exact reflection texel"
        );
    }

    #[test]
    fn ldr_environment_images_decode_srgb_to_linear_radiance() {
        let image = image::RgbImage::from_raw(1, 1, vec![128, 128, 128]).expect("rgb image");
        let (_width, _height, rgb) =
            decoded_environment_image_rgb(image::DynamicImage::ImageRgb8(image));

        assert_rgb_close(rgb[0], srgb_u8_to_linear_radiance([128, 128, 128]), 1.0e-6);
        assert!(
            rgb[0][0] < 128.0,
            "LDR environment maps should decode sRGB before becoming linear radiance"
        );
    }

    #[test]
    fn float_environment_images_preserve_linear_radiance() {
        let image = image::Rgb32FImage::from_raw(1, 1, vec![0.5, 0.25, 2.0]).expect("rgb32f image");
        let (_width, _height, rgb) =
            decoded_environment_image_rgb(image::DynamicImage::ImageRgb32F(image));

        assert_rgb_close(rgb[0], [127.5, 63.75, 510.0], 1.0e-6);
    }

    #[test]
    fn render_settings_environment_map_overrides_procedural_sky() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "robotdreams-renderer-environment-map-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("create environment map temp dir");
        let map_path = dir.join("environment.png");
        fs::write(
            &map_path,
            encode_png_rgb(1, 1, &[250, 10, 20]).expect("encode environment map"),
        )
        .expect("write environment map");
        let renderer = NativeRenderer::new();
        let settings = renderer
            .prepare_render_settings(RenderSettings {
                environment: Some(EnvironmentSettings {
                    sky_top_rgb: [10, 20, 30],
                    sky_horizon_rgb: [40, 50, 60],
                    ground_rgb: [70, 80, 90],
                    map: Some(map_path.display().to_string()),
                    map_rotation_deg: 0.0,
                    intensity: 1.0,
                    ambient_intensity: 0.0,
                }),
                ..RenderSettings::default()
            })
            .expect("prepare environment map settings");
        fs::remove_dir_all(&dir).ok();

        assert_rgb_close(
            environment_background_rgb([0.0, 0.0, 1.0], &settings),
            srgb_u8_to_linear_radiance([250, 10, 20]),
            1.0e-6,
        );
    }

    #[test]
    fn render_settings_white_balance_adjusts_display_channels() {
        let neutral = tone_map_rgb([100.0, 100.0, 100.0], &RenderSettings::default());
        let warmed = tone_map_rgb(
            [100.0, 100.0, 100.0],
            &RenderSettings {
                white_balance_rgb: [1.4, 1.0, 0.6],
                ..RenderSettings::default()
            },
        );

        assert!(warmed[0] > neutral[0], "red multiplier should brighten red");
        assert!(warmed[2] < neutral[2], "blue multiplier should darken blue");
    }

    #[test]
    fn render_settings_color_temperature_changes_channel_gains() {
        let cool_balance = color_temperature_white_balance_rgb(9000.0);
        let warm_balance = color_temperature_white_balance_rgb(3000.0);
        let cool = tone_map_rgb(
            [100.0, 100.0, 100.0],
            &RenderSettings {
                color_temperature_kelvin: Some(9000.0),
                ..RenderSettings::default()
            },
        );
        let warm = tone_map_rgb(
            [100.0, 100.0, 100.0],
            &RenderSettings {
                color_temperature_kelvin: Some(3000.0),
                ..RenderSettings::default()
            },
        );

        assert!(
            cool_balance[0] > cool_balance[2],
            "cool illuminant white balance should compensate red more than blue"
        );
        assert!(
            warm_balance[2] > warm_balance[0],
            "warm illuminant white balance should compensate blue more than red"
        );
        assert!(cool[0] > warm[0]);
        assert!(warm[2] > cool[2]);
    }
}
