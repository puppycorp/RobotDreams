use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use clap::{Args, Parser, Subcommand, ValueEnum};
use robotdreams_core::scene_graph::{
    AUTO_CAMERA_ID, EnvironmentSettings, ObservationMetadata, ReflectionProbeSettings,
    RenderSettings, SceneNode, SceneNodeKind, SegmentationPolicy, ShutterMode, ShutterPolicy,
    ToneMapping, prepare_observation_scene,
};
use robotdreams_core::{ObservationRequest, ObservationView, RobotDreams, RobotDreamsSnapshot};
use robotdreams_recorder::{NativeRecorder, RecordingArtifact, RecordingRequest};
use robotdreams_renderer::{FrameBuffer, FrameKind, NativeRenderer, RenderOutput};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, Write};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

const DEFAULT_SOCKET: &str = "/tmp/robotdreams-daemon.sock";

#[derive(Debug, Parser)]
#[command(name = "robotdreams", about = "Robot Dreams CLI")]
struct Cli {
    #[arg(long, default_value = DEFAULT_SOCKET)]
    socket: PathBuf,

    #[arg(
        long,
        default_value = "examples/puppyarm/project.json",
        help = "Project to open if a CLI request needs to auto-start the daemon"
    )]
    project: PathBuf,

    #[arg(
        long,
        default_value = "127.0.0.1:8345",
        help = "Daemon web bind address used when auto-starting the daemon"
    )]
    daemon_bind: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Open(OpenArgs),
    Status,
    Close,
    RenderFrame(RenderFrameArgs),
    Headless(HeadlessArgs),
    Vbus(VbusArgs),
    #[command(subcommand)]
    Project(ProjectCommand),
    #[command(subcommand)]
    Scenario(ScenarioCommand),
    #[command(subcommand)]
    Simulation(SimulationCommand),
    #[command(subcommand)]
    Recording(RecordingCommand),
    #[command(subcommand)]
    Daemon(DaemonCommand),
    #[command(subcommand)]
    Bus(BusCommand),
}

#[derive(Debug, Args)]
struct OpenArgs {
    path: PathBuf,

    #[arg(long, default_value = "127.0.0.1:8345")]
    bind: String,

    #[arg(long)]
    no_browser: bool,
}

#[derive(Debug, Args)]
struct RenderFrameArgs {
    path: PathBuf,

    #[arg(long, short)]
    out: PathBuf,

    #[arg(long)]
    camera: Option<String>,

    #[arg(long, value_enum, default_value_t = NativeView::DebugRgb)]
    view: NativeView,

    #[arg(long, default_value_t = 1800)]
    width: u32,

    #[arg(long, default_value_t = 1000)]
    height: u32,

    #[arg(
        long,
        help = "Minimum alpha for segmentation labels; 0 includes transparent hits, 1 labels only opaque hits"
    )]
    segmentation_min_alpha: Option<f32>,

    #[arg(long, help = "Global shutter exposure window in seconds for debug RGB")]
    shutter_exposure_sec: Option<f32>,

    #[arg(
        long,
        help = "Number of debug RGB samples to average across the shutter exposure"
    )]
    shutter_samples: Option<u32>,

    #[arg(long, value_enum, help = "Debug RGB shutter scan mode")]
    shutter_mode: Option<NativeShutterMode>,

    #[arg(
        long,
        help = "Rolling shutter top-to-bottom frame readout time in seconds"
    )]
    shutter_readout_sec: Option<f32>,

    #[arg(long, help = "Debug RGB background color as r,g,b")]
    background_rgb: Option<String>,

    #[arg(long, help = "Debug RGB ambient light color as r,g,b")]
    ambient_rgb: Option<String>,

    #[arg(long, help = "Debug RGB ambient light intensity")]
    ambient_intensity: Option<f32>,

    #[arg(
        long,
        help = "Debug RGB sky color for upward environment rays as r,g,b"
    )]
    environment_sky_top_rgb: Option<String>,

    #[arg(long, help = "Debug RGB sky color at the horizon as r,g,b")]
    environment_sky_horizon_rgb: Option<String>,

    #[arg(
        long,
        help = "Debug RGB ground color for downward environment rays as r,g,b"
    )]
    environment_ground_rgb: Option<String>,

    #[arg(long, help = "Debug RGB equirectangular environment map image path")]
    environment_map: Option<String>,

    #[arg(
        long,
        help = "Yaw rotation in degrees for the equirectangular environment map"
    )]
    environment_map_rotation_deg: Option<f32>,

    #[arg(long, help = "Debug RGB environment ray-miss intensity")]
    environment_intensity: Option<f32>,

    #[arg(
        long,
        help = "Debug RGB environment contribution to ambient surface lighting"
    )]
    environment_ambient_intensity: Option<f32>,

    #[arg(
        long,
        help = "Debug RGB equirectangular reflection probe image path for material lighting"
    )]
    reflection_probe: Option<String>,

    #[arg(long, help = "Yaw rotation in degrees for the reflection probe map")]
    reflection_probe_rotation_deg: Option<f32>,

    #[arg(long, help = "Debug RGB reflection probe specular intensity")]
    reflection_probe_intensity: Option<f32>,

    #[arg(
        long,
        help = "Debug RGB reflection probe contribution to ambient surface lighting"
    )]
    reflection_probe_ambient_intensity: Option<f32>,

    #[arg(long, help = "Reflection probe world position as x,y,z in meters")]
    reflection_probe_position: Option<String>,

    #[arg(long, help = "Reflection probe box projection size as x,y,z in meters")]
    reflection_probe_box_size_m: Option<String>,

    #[arg(long, help = "Reflection probe spherical influence radius in meters")]
    reflection_probe_influence_radius_m: Option<f32>,

    #[arg(long, help = "Reflection probe distance falloff exponent")]
    reflection_probe_falloff_power: Option<f32>,

    #[arg(long, help = "Debug RGB white-balance channel multipliers as r,g,b")]
    white_balance_rgb: Option<String>,

    #[arg(long, help = "Debug RGB white-balance color temperature in Kelvin")]
    color_temperature_kelvin: Option<f32>,

    #[arg(long, value_enum, help = "Debug RGB tone mapping curve")]
    tone_mapping: Option<NativeToneMapping>,

    #[arg(long, help = "Debug RGB tone mapper exposure multiplier")]
    tone_exposure: Option<f32>,

    #[arg(long, help = "Debug RGB deterministic subpixel samples per pixel")]
    debug_rgb_samples_per_pixel: Option<u32>,

    #[arg(long, help = "Debug RGB geometry ambient-occlusion sample count")]
    ambient_occlusion_samples: Option<u32>,

    #[arg(long, help = "Debug RGB geometry ambient-occlusion radius in meters")]
    ambient_occlusion_radius_m: Option<f32>,

    #[arg(
        long,
        help = "Debug RGB geometry ambient-occlusion strength multiplier"
    )]
    ambient_occlusion_intensity: Option<f32>,

    #[arg(long, help = "Debug RGB indirect diffuse sample count")]
    indirect_diffuse_samples: Option<u32>,

    #[arg(long, help = "Debug RGB indirect diffuse radius in meters")]
    indirect_diffuse_radius_m: Option<f32>,

    #[arg(long, help = "Debug RGB indirect diffuse strength multiplier")]
    indirect_diffuse_intensity: Option<f32>,

    #[arg(long, help = "Debug RGB bounded indirect diffuse bounce count")]
    indirect_diffuse_bounces: Option<u32>,

    #[arg(long, help = "Debug RGB finite-light soft-shadow sample count")]
    soft_shadow_samples: Option<u32>,

    #[arg(long, help = "Debug RGB finite-light soft-shadow radius in meters")]
    soft_shadow_radius_m: Option<f32>,

    #[arg(long, help = "Debug RGB emissive triangle area-light sample count")]
    area_light_samples: Option<u32>,

    #[arg(long, help = "Debug RGB rough transmission sample count")]
    rough_transmission_samples: Option<u32>,

    #[arg(long, help = "Debug RGB rough scene-reflection sample count")]
    rough_reflection_samples: Option<u32>,

    #[arg(long, help = "Debug RGB bounded specular reflection bounce count")]
    specular_reflection_bounces: Option<u32>,

    #[arg(long, help = "Selected KHR_materials_variants material variant name")]
    gltf_material_variant: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum NativeView {
    DebugRgb,
    Depth,
    Segmentation,
    Normal,
    Albedo,
    MaterialProperties,
    WorldPosition,
    State,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum NativeToneMapping {
    Linear,
    Reinhard,
    Aces,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum NativeShutterMode {
    Global,
    RollingTopToBottom,
}

#[derive(Debug, Args)]
struct HeadlessArgs {
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

#[derive(Debug, Args)]
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

#[derive(Debug, Subcommand)]
enum DaemonCommand {
    Start(DaemonStartArgs),
    Stop,
}

#[derive(Debug, Args)]
struct DaemonStartArgs {
    #[arg(long, default_value = "127.0.0.1:8345")]
    bind: String,
}

#[derive(Debug, Subcommand)]
enum BusCommand {
    Start,
    Stop,
}

#[derive(Debug, Subcommand)]
enum ProjectCommand {
    List(ProjectOutputArgs),
    State(ProjectStateArgs),
    Command(ProjectCommandArgs),
}

#[derive(Debug, Subcommand)]
enum ScenarioCommand {
    Load(ScenarioLoadArgs),
    Reset(ScenarioArgs),
    State(ScenarioStateArgs),
    #[command(name = "export-sensors")]
    ExportSensors(ScenarioExportSensorsArgs),
    Smoke(ScenarioSmokeArgs),
}

#[derive(Debug, Subcommand)]
enum SimulationCommand {
    Start(SimulationArgs),
    Pause(SimulationArgs),
    Stop(SimulationArgs),
    Reset(SimulationArgs),
    State(SimulationStateArgs),
    RenderFrame(SimulationRenderFrameArgs),
    Record(SimulationRecordArgs),
}

#[derive(Debug, Subcommand)]
enum RecordingCommand {
    Inspect(RecordingInspectArgs),
    Assert(RecordingAssertArgs),
}

#[derive(Debug, Args)]
struct ProjectOutputArgs {
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct ProjectStateArgs {
    #[arg(long)]
    project: Option<String>,

    item: Option<String>,

    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct ProjectCommandArgs {
    #[arg(long)]
    project: Option<String>,

    #[arg(long)]
    simulation: Option<String>,

    target: String,
    action: String,

    #[arg(long)]
    payload: Option<String>,

    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct ScenarioLoadArgs {
    #[arg(long)]
    project: Option<String>,

    #[arg(long)]
    simulation: Option<String>,

    #[arg(long)]
    path: PathBuf,

    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct ScenarioArgs {
    #[arg(long)]
    project: Option<String>,

    #[arg(long)]
    simulation: Option<String>,

    #[arg(long)]
    id: Option<String>,

    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct ScenarioStateArgs {
    #[arg(long)]
    project: Option<String>,

    #[arg(long)]
    simulation: Option<String>,

    #[arg(long)]
    id: Option<String>,

    #[arg(long)]
    payload: Option<String>,

    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct ScenarioExportSensorsArgs {
    #[arg(long)]
    project: Option<String>,

    #[arg(long)]
    simulation: Option<String>,

    #[arg(long)]
    id: Option<String>,

    #[arg(long)]
    payload: String,

    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct ScenarioSmokeArgs {
    #[arg(long)]
    project: Option<String>,

    #[arg(long)]
    simulation: Option<String>,

    #[arg(long)]
    path: PathBuf,

    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct SimulationArgs {
    #[arg(long)]
    project: Option<String>,

    #[arg(long)]
    simulation: Option<String>,
}

#[derive(Debug, Args)]
struct SimulationStateArgs {
    #[arg(long)]
    project: Option<String>,

    #[arg(long)]
    simulation: Option<String>,

    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct SimulationRenderFrameArgs {
    #[arg(long)]
    project: Option<String>,

    #[arg(long)]
    simulation: Option<String>,

    #[arg(long, short)]
    out: PathBuf,

    #[arg(long)]
    camera: Option<String>,

    #[arg(long, value_enum, default_value_t = NativeView::DebugRgb)]
    view: NativeView,

    #[arg(long, default_value_t = 640)]
    width: u32,

    #[arg(long, default_value_t = 480)]
    height: u32,

    #[arg(long)]
    json: bool,

    #[arg(
        long,
        help = "Minimum alpha for segmentation labels; 0 includes transparent hits, 1 labels only opaque hits"
    )]
    segmentation_min_alpha: Option<f32>,

    #[arg(long, help = "Global shutter exposure window in seconds for debug RGB")]
    shutter_exposure_sec: Option<f32>,

    #[arg(
        long,
        help = "Number of debug RGB samples to average across the shutter exposure"
    )]
    shutter_samples: Option<u32>,

    #[arg(long, value_enum, help = "Debug RGB shutter scan mode")]
    shutter_mode: Option<NativeShutterMode>,

    #[arg(
        long,
        help = "Rolling shutter top-to-bottom frame readout time in seconds"
    )]
    shutter_readout_sec: Option<f32>,

    #[arg(long, help = "Debug RGB background color as r,g,b")]
    background_rgb: Option<String>,

    #[arg(long, help = "Debug RGB ambient light color as r,g,b")]
    ambient_rgb: Option<String>,

    #[arg(long, help = "Debug RGB ambient light intensity")]
    ambient_intensity: Option<f32>,

    #[arg(
        long,
        help = "Debug RGB sky color for upward environment rays as r,g,b"
    )]
    environment_sky_top_rgb: Option<String>,

    #[arg(long, help = "Debug RGB sky color at the horizon as r,g,b")]
    environment_sky_horizon_rgb: Option<String>,

    #[arg(
        long,
        help = "Debug RGB ground color for downward environment rays as r,g,b"
    )]
    environment_ground_rgb: Option<String>,

    #[arg(long, help = "Debug RGB equirectangular environment map image path")]
    environment_map: Option<String>,

    #[arg(
        long,
        help = "Yaw rotation in degrees for the equirectangular environment map"
    )]
    environment_map_rotation_deg: Option<f32>,

    #[arg(long, help = "Debug RGB environment ray-miss intensity")]
    environment_intensity: Option<f32>,

    #[arg(
        long,
        help = "Debug RGB environment contribution to ambient surface lighting"
    )]
    environment_ambient_intensity: Option<f32>,

    #[arg(
        long,
        help = "Debug RGB equirectangular reflection probe image path for material lighting"
    )]
    reflection_probe: Option<String>,

    #[arg(long, help = "Yaw rotation in degrees for the reflection probe map")]
    reflection_probe_rotation_deg: Option<f32>,

    #[arg(long, help = "Debug RGB reflection probe specular intensity")]
    reflection_probe_intensity: Option<f32>,

    #[arg(
        long,
        help = "Debug RGB reflection probe contribution to ambient surface lighting"
    )]
    reflection_probe_ambient_intensity: Option<f32>,

    #[arg(long, help = "Reflection probe world position as x,y,z in meters")]
    reflection_probe_position: Option<String>,

    #[arg(long, help = "Reflection probe box projection size as x,y,z in meters")]
    reflection_probe_box_size_m: Option<String>,

    #[arg(long, help = "Reflection probe spherical influence radius in meters")]
    reflection_probe_influence_radius_m: Option<f32>,

    #[arg(long, help = "Reflection probe distance falloff exponent")]
    reflection_probe_falloff_power: Option<f32>,

    #[arg(long, help = "Debug RGB white-balance channel multipliers as r,g,b")]
    white_balance_rgb: Option<String>,

    #[arg(long, help = "Debug RGB white-balance color temperature in Kelvin")]
    color_temperature_kelvin: Option<f32>,

    #[arg(long, value_enum, help = "Debug RGB tone mapping curve")]
    tone_mapping: Option<NativeToneMapping>,

    #[arg(long, help = "Debug RGB tone mapper exposure multiplier")]
    tone_exposure: Option<f32>,

    #[arg(long, help = "Debug RGB deterministic subpixel samples per pixel")]
    debug_rgb_samples_per_pixel: Option<u32>,

    #[arg(long, help = "Debug RGB geometry ambient-occlusion sample count")]
    ambient_occlusion_samples: Option<u32>,

    #[arg(long, help = "Debug RGB geometry ambient-occlusion radius in meters")]
    ambient_occlusion_radius_m: Option<f32>,

    #[arg(
        long,
        help = "Debug RGB geometry ambient-occlusion strength multiplier"
    )]
    ambient_occlusion_intensity: Option<f32>,

    #[arg(long, help = "Debug RGB indirect diffuse sample count")]
    indirect_diffuse_samples: Option<u32>,

    #[arg(long, help = "Debug RGB indirect diffuse radius in meters")]
    indirect_diffuse_radius_m: Option<f32>,

    #[arg(long, help = "Debug RGB indirect diffuse strength multiplier")]
    indirect_diffuse_intensity: Option<f32>,

    #[arg(long, help = "Debug RGB bounded indirect diffuse bounce count")]
    indirect_diffuse_bounces: Option<u32>,

    #[arg(long, help = "Debug RGB finite-light soft-shadow sample count")]
    soft_shadow_samples: Option<u32>,

    #[arg(long, help = "Debug RGB finite-light soft-shadow radius in meters")]
    soft_shadow_radius_m: Option<f32>,

    #[arg(long, help = "Debug RGB emissive triangle area-light sample count")]
    area_light_samples: Option<u32>,

    #[arg(long, help = "Debug RGB rough transmission sample count")]
    rough_transmission_samples: Option<u32>,

    #[arg(long, help = "Debug RGB rough scene-reflection sample count")]
    rough_reflection_samples: Option<u32>,

    #[arg(long, help = "Debug RGB bounded specular reflection bounce count")]
    specular_reflection_bounces: Option<u32>,

    #[arg(long, help = "Selected KHR_materials_variants material variant name")]
    gltf_material_variant: Option<String>,
}

#[derive(Debug, Args)]
struct SimulationRecordArgs {
    #[arg(long)]
    project: Option<String>,

    #[arg(long)]
    simulation: Option<String>,

    #[arg(long, short)]
    out: Option<PathBuf>,

    #[arg(long)]
    live: bool,

    #[arg(long)]
    trace_only: bool,

    #[arg(long)]
    camera: Option<String>,

    #[arg(long, value_enum, default_value_t = NativeView::DebugRgb)]
    view: NativeView,

    #[arg(long, default_value_t = 10.0)]
    seconds: f32,

    #[arg(long, default_value_t = 12)]
    fps: u32,

    #[arg(long, default_value_t = 1280)]
    width: u32,

    #[arg(long, default_value_t = 720)]
    height: u32,

    #[arg(
        long,
        help = "Minimum alpha for segmentation labels; 0 includes transparent hits, 1 labels only opaque hits"
    )]
    segmentation_min_alpha: Option<f32>,

    #[arg(long, help = "Global shutter exposure window in seconds for debug RGB")]
    shutter_exposure_sec: Option<f32>,

    #[arg(
        long,
        help = "Number of debug RGB samples to average across the shutter exposure"
    )]
    shutter_samples: Option<u32>,

    #[arg(long, value_enum, help = "Debug RGB shutter scan mode")]
    shutter_mode: Option<NativeShutterMode>,

    #[arg(
        long,
        help = "Rolling shutter top-to-bottom frame readout time in seconds"
    )]
    shutter_readout_sec: Option<f32>,

    #[arg(long, help = "Debug RGB background color as r,g,b")]
    background_rgb: Option<String>,

    #[arg(long, help = "Debug RGB ambient light color as r,g,b")]
    ambient_rgb: Option<String>,

    #[arg(long, help = "Debug RGB ambient light intensity")]
    ambient_intensity: Option<f32>,

    #[arg(
        long,
        help = "Debug RGB sky color for upward environment rays as r,g,b"
    )]
    environment_sky_top_rgb: Option<String>,

    #[arg(long, help = "Debug RGB sky color at the horizon as r,g,b")]
    environment_sky_horizon_rgb: Option<String>,

    #[arg(
        long,
        help = "Debug RGB ground color for downward environment rays as r,g,b"
    )]
    environment_ground_rgb: Option<String>,

    #[arg(long, help = "Debug RGB equirectangular environment map image path")]
    environment_map: Option<String>,

    #[arg(
        long,
        help = "Yaw rotation in degrees for the equirectangular environment map"
    )]
    environment_map_rotation_deg: Option<f32>,

    #[arg(long, help = "Debug RGB environment ray-miss intensity")]
    environment_intensity: Option<f32>,

    #[arg(
        long,
        help = "Debug RGB environment contribution to ambient surface lighting"
    )]
    environment_ambient_intensity: Option<f32>,

    #[arg(
        long,
        help = "Debug RGB equirectangular reflection probe image path for material lighting"
    )]
    reflection_probe: Option<String>,

    #[arg(long, help = "Yaw rotation in degrees for the reflection probe map")]
    reflection_probe_rotation_deg: Option<f32>,

    #[arg(long, help = "Debug RGB reflection probe specular intensity")]
    reflection_probe_intensity: Option<f32>,

    #[arg(
        long,
        help = "Debug RGB reflection probe contribution to ambient surface lighting"
    )]
    reflection_probe_ambient_intensity: Option<f32>,

    #[arg(long, help = "Reflection probe world position as x,y,z in meters")]
    reflection_probe_position: Option<String>,

    #[arg(long, help = "Reflection probe box projection size as x,y,z in meters")]
    reflection_probe_box_size_m: Option<String>,

    #[arg(long, help = "Reflection probe spherical influence radius in meters")]
    reflection_probe_influence_radius_m: Option<f32>,

    #[arg(long, help = "Reflection probe distance falloff exponent")]
    reflection_probe_falloff_power: Option<f32>,

    #[arg(long, help = "Debug RGB white-balance channel multipliers as r,g,b")]
    white_balance_rgb: Option<String>,

    #[arg(long, help = "Debug RGB white-balance color temperature in Kelvin")]
    color_temperature_kelvin: Option<f32>,

    #[arg(long, value_enum, help = "Debug RGB tone mapping curve")]
    tone_mapping: Option<NativeToneMapping>,

    #[arg(long, help = "Debug RGB tone mapper exposure multiplier")]
    tone_exposure: Option<f32>,

    #[arg(long, help = "Debug RGB deterministic subpixel samples per pixel")]
    debug_rgb_samples_per_pixel: Option<u32>,

    #[arg(long, help = "Debug RGB geometry ambient-occlusion sample count")]
    ambient_occlusion_samples: Option<u32>,

    #[arg(long, help = "Debug RGB geometry ambient-occlusion radius in meters")]
    ambient_occlusion_radius_m: Option<f32>,

    #[arg(
        long,
        help = "Debug RGB geometry ambient-occlusion strength multiplier"
    )]
    ambient_occlusion_intensity: Option<f32>,

    #[arg(long, help = "Debug RGB indirect diffuse sample count")]
    indirect_diffuse_samples: Option<u32>,

    #[arg(long, help = "Debug RGB indirect diffuse radius in meters")]
    indirect_diffuse_radius_m: Option<f32>,

    #[arg(long, help = "Debug RGB indirect diffuse strength multiplier")]
    indirect_diffuse_intensity: Option<f32>,

    #[arg(long, help = "Debug RGB bounded indirect diffuse bounce count")]
    indirect_diffuse_bounces: Option<u32>,

    #[arg(long, help = "Debug RGB finite-light soft-shadow sample count")]
    soft_shadow_samples: Option<u32>,

    #[arg(long, help = "Debug RGB finite-light soft-shadow radius in meters")]
    soft_shadow_radius_m: Option<f32>,

    #[arg(long, help = "Debug RGB emissive triangle area-light sample count")]
    area_light_samples: Option<u32>,

    #[arg(long, help = "Debug RGB rough transmission sample count")]
    rough_transmission_samples: Option<u32>,

    #[arg(long, help = "Debug RGB rough scene-reflection sample count")]
    rough_reflection_samples: Option<u32>,

    #[arg(long, help = "Debug RGB bounded specular reflection bounce count")]
    specular_reflection_bounces: Option<u32>,

    #[arg(long, help = "Selected KHR_materials_variants material variant name")]
    gltf_material_variant: Option<String>,

    #[arg(long, default_value_t = 3500)]
    wait_ms: u64,

    #[arg(long)]
    keep_frames: Option<PathBuf>,

    #[arg(
        long,
        help = "Write machine-readable simulation samples as JSONL. Defaults beside --out."
    )]
    trace_out: Option<PathBuf>,

    #[arg(long, default_value_t = 20.0)]
    trace_hz: f32,
}

#[derive(Debug, Args)]
struct RecordingInspectArgs {
    trace: PathBuf,

    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct RecordingAssertArgs {
    #[arg(long)]
    trace: PathBuf,

    #[arg(long)]
    ready: PathBuf,

    #[arg(long = "expect-servo-moved", value_delimiter = ',')]
    expect_servo_moved: Vec<u8>,

    #[arg(long = "expect-target-write", value_delimiter = ',')]
    expect_target_write: Vec<u8>,

    #[arg(long = "allow-servo-id", value_delimiter = ',')]
    allow_servo_id: Vec<u8>,

    #[arg(long, default_value_t = 1)]
    min_present_delta: i32,

    #[arg(long, default_value_t = 1)]
    min_target_delta: i32,

    #[arg(long)]
    require_transform_change: bool,

    #[arg(long)]
    json: bool,
}

#[derive(Debug, Serialize)]
#[serde(tag = "command", rename_all = "camelCase")]
enum DaemonRequest {
    Open {
        path: String,
    },
    Status,
    ProjectList,
    ProjectState {
        project: Option<String>,
        item: Option<String>,
    },
    ProjectCommand {
        project: Option<String>,
        simulation: Option<String>,
        target: String,
        action: String,
        payload: Option<serde_json::Value>,
    },
    ScenarioCommand {
        project: Option<String>,
        simulation: Option<String>,
        action: String,
        path: Option<String>,
        scenario: Option<String>,
        payload: Option<serde_json::Value>,
    },
    SimulationCommand {
        project: Option<String>,
        simulation: Option<String>,
        action: String,
    },
    RenderFrame {
        project: Option<String>,
        simulation: Option<String>,
        camera: Option<String>,
        views: Option<Vec<ObservationView>>,
        width: Option<u32>,
        height: Option<u32>,
        segmentation_policy: Option<SegmentationPolicy>,
        shutter_policy: Option<ShutterPolicy>,
        render_settings: Option<RenderSettings>,
    },
    BusStart,
    BusStop,
    Close,
    Shutdown,
}

#[derive(Debug, Deserialize)]
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

#[derive(Debug, Default, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct RecordingInspectSummary {
    schema: Option<String>,
    started_unix_ms: Option<u128>,
    ended_unix_ms: Option<u128>,
    duration_sec: Option<f64>,
    sample_count: u64,
    sample_error_count: u64,
    command_count: u64,
    bus_event_count: u64,
    servo_ids: Vec<u8>,
    virtual_bus_path: Option<String>,
    virtual_bus_paths: Vec<String>,
    virtual_bus_running_sample_count: u64,
    running_sample_count: u64,
    bus_command_events_by_servo: Vec<ServoCommandEvidence>,
    servo_snapshot_count_by_servo: Vec<ServoSnapshotCount>,
    servo_movement_by_servo: Vec<ServoMovementEvidence>,
    first_status: Option<String>,
    last_status: Option<String>,
    transform_change_count: u64,
    changed_transform_ids: Vec<String>,
    first_transform_change_sec: Option<f64>,
    last_transform_change_sec: Option<f64>,
    scenario: Option<ScenarioTraceSummary>,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct ServoCommandEvidence {
    id: u8,
    count: u64,
    first_elapsed_sec: Option<f64>,
    last_elapsed_sec: Option<f64>,
    target_positions: Vec<i16>,
    min_target_position: Option<i16>,
    max_target_position: Option<i16>,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct ServoSnapshotCount {
    id: u8,
    count: u64,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct ServoMovementEvidence {
    id: u8,
    sample_count: u64,
    moving_sample_count: u64,
    first_present_position: Option<i16>,
    last_present_position: Option<i16>,
    min_present_position: Option<i16>,
    max_present_position: Option<i16>,
    max_present_delta: i32,
    first_target_position: Option<i16>,
    last_target_position: Option<i16>,
    min_target_position: Option<i16>,
    max_target_position: Option<i16>,
    max_target_delta: i32,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct ScenarioTraceSummary {
    scenario_id: Option<String>,
    first_status: Option<String>,
    last_status: Option<String>,
    first_progress: Option<String>,
    last_progress: Option<String>,
    sample_count: u64,
    source: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RecordingAssertResult {
    schema: &'static str,
    ok: bool,
    trace: String,
    ready: String,
    summary: RecordingInspectSummary,
    cases: Vec<RecordingAssertionCase>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RecordingAssertionCase {
    name: String,
    ok: bool,
    severity: &'static str,
    details: serde_json::Value,
    first_elapsed_sec: Option<f64>,
}

#[cfg(unix)]
async fn send_request(socket: &Path, request: &DaemonRequest) -> Result<DaemonResponse> {
    let stream = tokio::net::UnixStream::connect(socket)
        .await
        .with_context(|| format!("connect daemon socket {}", socket.display()))?;
    let mut stream = BufReader::new(stream);
    let mut raw = serde_json::to_vec(request)?;
    raw.push(b'\n');
    stream.get_mut().write_all(&raw).await?;

    let mut response = String::new();
    stream.read_line(&mut response).await?;
    Ok(serde_json::from_str(&response)?)
}

#[cfg(not(unix))]
async fn send_request(_socket: &Path, _request: &DaemonRequest) -> Result<DaemonResponse> {
    bail!("daemon socket is only implemented on Unix")
}

fn daemon_exe() -> Result<PathBuf> {
    let current = std::env::current_exe()?;
    let Some(dir) = current.parent() else {
        bail!("cannot determine current executable directory");
    };
    Ok(dir.join("robotdreams-daemon"))
}

async fn start_daemon(socket: &Path, path: Option<&Path>, bind: &str) -> Result<()> {
    let exe = daemon_exe()?;
    let mut command = if exe.exists() {
        ProcessCommand::new(exe)
    } else {
        let mut command = ProcessCommand::new("cargo");
        command.args(["run", "-p", "robotdreams-daemon", "--"]);
        command
    };
    command
        .arg("--bind")
        .arg(bind)
        .arg("--socket")
        .arg(socket)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(path) = path {
        command.arg(path);
    }
    #[cfg(unix)]
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    command.spawn().context("spawn robotdreams-daemon")?;

    for _ in 0..100 {
        if send_request(socket, &DaemonRequest::Status).await.is_ok() {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    bail!("daemon did not become ready at {}", socket.display())
}

async fn open_project(socket: &Path, path: &Path, bind: &str) -> Result<DaemonResponse> {
    let request = DaemonRequest::Open {
        path: path.display().to_string(),
    };
    match send_request(socket, &request).await {
        Ok(response) if response.ok => Ok(response),
        Ok(_) => restart_daemon_with_project(socket, path, bind).await,
        Err(_) => {
            start_daemon(socket, Some(path), bind).await?;
            send_request(socket, &request).await
        }
    }
}

#[cfg(test)]
mod tests {
    use robotdreams_core::scene_graph::{
        EntityId, EntityMetadata, Geometry, SceneGraph, Transform, add_auto_camera,
    };

    use super::*;

    fn empty_render_setting_args() -> RenderSettingArgs {
        RenderSettingArgs {
            background_rgb: None,
            ambient_rgb: None,
            ambient_intensity: None,
            environment_sky_top_rgb: None,
            environment_sky_horizon_rgb: None,
            environment_ground_rgb: None,
            environment_map: None,
            environment_map_rotation_deg: None,
            environment_intensity: None,
            environment_ambient_intensity: None,
            reflection_probe: None,
            reflection_probe_rotation_deg: None,
            reflection_probe_intensity: None,
            reflection_probe_ambient_intensity: None,
            reflection_probe_position: None,
            reflection_probe_box_size_m: None,
            reflection_probe_influence_radius_m: None,
            reflection_probe_falloff_power: None,
            white_balance_rgb: None,
            color_temperature_kelvin: None,
            tone_mapping: None,
            tone_exposure: None,
            debug_rgb_samples_per_pixel: None,
            ambient_occlusion_samples: None,
            ambient_occlusion_radius_m: None,
            ambient_occlusion_intensity: None,
            indirect_diffuse_samples: None,
            indirect_diffuse_radius_m: None,
            indirect_diffuse_intensity: None,
            indirect_diffuse_bounces: None,
            soft_shadow_samples: None,
            soft_shadow_radius_m: None,
            area_light_samples: None,
            rough_transmission_samples: None,
            rough_reflection_samples: None,
            specular_reflection_bounces: None,
            gltf_material_variant: None,
        }
    }

    #[test]
    fn inspect_recording_summarizes_samples_commands_and_transform_changes() {
        let trace = r#"{"type":"recordingStart","schema":"robotdreams.recording.trace.v1","startedUnixMs":1}
{"type":"sample","index":0,"elapsedSec":0.0,"data":{"status":"stopped","busEvents":[],"robotScene":{"dynamicState":{"transforms":{"1":{"axisAngle":[0,0,1,0.0]}}}}}}
{"type":"sample","index":1,"elapsedSec":0.1,"data":{"status":"running","busEvents":[{"sequence":1,"instruction":"write","id":1,"ids":[1],"writes":[{"id":1,"targetPosition":2100}]}],"robotScene":{"dynamicState":{"transforms":{"1":{"axisAngle":[0,0,1,0.5]}}}}}}
{"type":"sample","index":2,"elapsedSec":0.2,"data":{"status":"running","busEvents":[{"sequence":2,"instruction":"syncWrite","ids":[2],"writes":[{"id":3,"targetPosition":2200}]}],"robotScene":{"dynamicState":{"transforms":{"1":{"axisAngle":[0,0,1,0.5]}}}}}}
{"type":"recordingEnd","endedUnixMs":2,"elapsedSec":0.3,"samples":3}
"#;

        let summary = inspect_recording_reader(std::io::Cursor::new(trace)).expect("inspect");
        assert_eq!(
            summary.schema.as_deref(),
            Some("robotdreams.recording.trace.v1")
        );
        assert_eq!(summary.sample_count, 3);
        assert_eq!(summary.command_count, 2);
        assert_eq!(summary.servo_ids, vec![1, 2, 3]);
        assert_eq!(summary.first_status.as_deref(), Some("stopped"));
        assert_eq!(summary.last_status.as_deref(), Some("running"));
        assert_eq!(summary.first_transform_change_sec, Some(0.1));
        assert_eq!(summary.last_transform_change_sec, Some(0.1));
        assert_eq!(summary.duration_sec, Some(0.3));
    }

    #[test]
    fn assert_recording_accepts_bus_movement_and_transform_evidence() {
        let (trace, ready) = write_assertion_artifacts(
            "valid",
            r#"{"type":"recordingStart","schema":"robotdreams.recording.trace.v1","startedUnixMs":1}
{"type":"sample","index":0,"elapsedSec":0.0,"virtualBusRunning":true,"virtualBusPath":"/dev/pts/4","data":{"status":"running","virtualBus":{"running":true,"path":"/dev/pts/4"},"servoSnapshots":[{"id":1,"targetPosition":2048,"presentPosition":2048,"moving":false}],"busEvents":[],"robotScene":{"dynamicState":{"transforms":{"tool":{"axisAngle":[0,0,1,0.0]}}}}}}
{"type":"sample","index":1,"elapsedSec":0.1,"virtualBusRunning":true,"virtualBusPath":"/dev/pts/4","data":{"status":"running","virtualBus":{"running":true,"path":"/dev/pts/4"},"servoSnapshots":[{"id":1,"targetPosition":2100,"presentPosition":2060,"moving":true}],"busEvents":[{"sequence":1,"instruction":"write","id":1,"ids":[1],"writes":[{"id":1,"targetPosition":2100}]}],"robotScene":{"dynamicState":{"transforms":{"tool":{"axisAngle":[0,0,1,0.5]}}}}}}
{"type":"recordingEnd","endedUnixMs":2,"elapsedSec":0.2,"samples":2}
"#,
        );
        let result = assert_recording(&assert_args(trace, ready)).expect("assert");
        assert!(result.ok);
        assert_case(&result, "busTargetWrites", true);
        assert_case(&result, "presentPositionMovement", true);
        assert_case(&result, "transformChanges", true);
    }

    #[test]
    fn assert_recording_rejects_scenario_success_without_physical_evidence() {
        let (trace, ready) = write_assertion_artifacts(
            "scenario-only",
            r#"{"type":"recordingStart","schema":"robotdreams.recording.trace.v1","startedUnixMs":1}
{"type":"sample","index":0,"elapsedSec":0.0,"virtualBusRunning":true,"virtualBusPath":"/dev/pts/4","data":{"status":"running","virtualBus":{"running":true,"path":"/dev/pts/4"},"servoSnapshots":[{"id":1,"targetPosition":2048,"presentPosition":2048,"moving":false}],"busEvents":[],"scenario":{"scenarioId":"puppybot-ball-to-bin","status":"complete","progress":"complete"},"robotScene":{"dynamicState":{"transforms":{"tool":{"axisAngle":[0,0,1,0.0]}}}}}}
{"type":"sample","index":1,"elapsedSec":0.1,"virtualBusRunning":true,"virtualBusPath":"/dev/pts/4","data":{"status":"running","virtualBus":{"running":true,"path":"/dev/pts/4"},"servoSnapshots":[{"id":1,"targetPosition":2048,"presentPosition":2048,"moving":false}],"busEvents":[],"scenario":{"scenarioId":"puppybot-ball-to-bin","status":"complete","progress":"complete"},"robotScene":{"dynamicState":{"transforms":{"tool":{"axisAngle":[0,0,1,0.0]}}}}}}
{"type":"recordingEnd","endedUnixMs":2,"elapsedSec":0.2,"samples":2}
"#,
        );
        let result = assert_recording(&assert_args(trace, ready)).expect("assert");
        assert!(!result.ok);
        assert_case(&result, "busTargetWrites", false);
        assert_case(&result, "presentPositionMovement", false);
    }

    #[test]
    fn assert_recording_rejects_bus_command_without_present_position_movement() {
        let (trace, ready) = write_assertion_artifacts(
            "command-only",
            r#"{"type":"recordingStart","schema":"robotdreams.recording.trace.v1","startedUnixMs":1}
{"type":"sample","index":0,"elapsedSec":0.0,"virtualBusRunning":true,"virtualBusPath":"/dev/pts/4","data":{"status":"running","virtualBus":{"running":true,"path":"/dev/pts/4"},"servoSnapshots":[{"id":1,"targetPosition":2048,"presentPosition":2048,"moving":false}],"busEvents":[],"robotScene":{"dynamicState":{"transforms":{"tool":{"axisAngle":[0,0,1,0.0]}}}}}}
{"type":"sample","index":1,"elapsedSec":0.1,"virtualBusRunning":true,"virtualBusPath":"/dev/pts/4","data":{"status":"running","virtualBus":{"running":true,"path":"/dev/pts/4"},"servoSnapshots":[{"id":1,"targetPosition":2100,"presentPosition":2048,"moving":false}],"busEvents":[{"sequence":1,"instruction":"write","id":1,"ids":[1],"writes":[{"id":1,"targetPosition":2100}]}],"robotScene":{"dynamicState":{"transforms":{"tool":{"axisAngle":[0,0,1,0.0]}}}}}}
{"type":"recordingEnd","endedUnixMs":2,"elapsedSec":0.2,"samples":2}
"#,
        );
        let result = assert_recording(&assert_args(trace, ready)).expect("assert");
        assert!(!result.ok);
        assert_case(&result, "busTargetWrites", true);
        assert_case(&result, "presentPositionMovement", false);
    }

    #[test]
    fn assert_recording_rejects_present_position_movement_without_bus_command() {
        let (trace, ready) = write_assertion_artifacts(
            "movement-only",
            r#"{"type":"recordingStart","schema":"robotdreams.recording.trace.v1","startedUnixMs":1}
{"type":"sample","index":0,"elapsedSec":0.0,"virtualBusRunning":true,"virtualBusPath":"/dev/pts/4","data":{"status":"running","virtualBus":{"running":true,"path":"/dev/pts/4"},"servoSnapshots":[{"id":1,"targetPosition":2048,"presentPosition":2048,"moving":false}],"busEvents":[],"robotScene":{"dynamicState":{"transforms":{"tool":{"axisAngle":[0,0,1,0.0]}}}}}}
{"type":"sample","index":1,"elapsedSec":0.1,"virtualBusRunning":true,"virtualBusPath":"/dev/pts/4","data":{"status":"running","virtualBus":{"running":true,"path":"/dev/pts/4"},"servoSnapshots":[{"id":1,"targetPosition":2048,"presentPosition":2060,"moving":true}],"busEvents":[],"robotScene":{"dynamicState":{"transforms":{"tool":{"axisAngle":[0,0,1,0.5]}}}}}}
{"type":"recordingEnd","endedUnixMs":2,"elapsedSec":0.2,"samples":2}
"#,
        );
        let result = assert_recording(&assert_args(trace, ready)).expect("assert");
        assert!(!result.ok);
        assert_case(&result, "busTargetWrites", false);
    }

    #[test]
    fn assert_recording_rejects_sample_errors() {
        let (trace, ready) = write_assertion_artifacts(
            "sample-error",
            r#"{"type":"recordingStart","schema":"robotdreams.recording.trace.v1","startedUnixMs":1}
{"type":"sample","index":0,"elapsedSec":0.0,"virtualBusRunning":true,"virtualBusPath":"/dev/pts/4","data":{"status":"running","virtualBus":{"running":true,"path":"/dev/pts/4"},"servoSnapshots":[{"id":1,"targetPosition":2048,"presentPosition":2048,"moving":false}],"busEvents":[],"robotScene":{"dynamicState":{"transforms":{"tool":{"axisAngle":[0,0,1,0.0]}}}}}}
{"type":"sampleError","index":1,"elapsedSec":0.1,"error":"socket closed"}
{"type":"sample","index":2,"elapsedSec":0.2,"virtualBusRunning":true,"virtualBusPath":"/dev/pts/4","data":{"status":"running","virtualBus":{"running":true,"path":"/dev/pts/4"},"servoSnapshots":[{"id":1,"targetPosition":2100,"presentPosition":2060,"moving":true}],"busEvents":[{"sequence":1,"instruction":"write","id":1,"ids":[1],"writes":[{"id":1,"targetPosition":2100}]}],"robotScene":{"dynamicState":{"transforms":{"tool":{"axisAngle":[0,0,1,0.5]}}}}}}
{"type":"recordingEnd","endedUnixMs":2,"elapsedSec":0.3,"samples":3}
"#,
        );
        let result = assert_recording(&assert_args(trace, ready)).expect("assert");
        assert!(!result.ok);
        assert_case(&result, "noSampleErrors", false);
    }

    #[test]
    fn filter_new_bus_events_removes_previously_seen_sequences() {
        let data = serde_json::json!({
            "status": "running",
            "busEvents": [
                {"sequence": 1, "instruction": "write"},
                {"sequence": 2, "instruction": "write"}
            ]
        });
        let mut last_sequence = 1;
        let filtered = filter_new_bus_events(data, &mut last_sequence);
        let events = filtered
            .get("busEvents")
            .and_then(|events| events.as_array())
            .expect("events");
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].get("sequence").and_then(|value| value.as_u64()),
            Some(2)
        );
        assert_eq!(last_sequence, 2);
    }

    #[test]
    fn attach_scenario_data_adds_semantic_state_to_trace_sample_data() {
        let data = serde_json::json!({
            "status": "running",
            "busEvents": []
        });
        let scenario = serde_json::json!({
            "scenarioId": "puppybot-ball-to-bin",
            "status": "complete",
            "progress": "complete"
        });

        let merged = attach_scenario_data(data, Some(scenario.clone()));

        assert_eq!(merged.get("scenario"), Some(&scenario));
    }

    #[test]
    fn cli_parses_scenario_load_command() {
        let cli = Cli::try_parse_from([
            "robotdreams",
            "scenario",
            "load",
            "--path",
            "scenarios/place_ball_to_bin.robotdreams.json",
            "--json",
        ])
        .expect("parse scenario load command");

        let Command::Scenario(ScenarioCommand::Load(args)) = cli.command else {
            panic!("expected scenario load command");
        };
        assert_eq!(
            args.path,
            PathBuf::from("scenarios/place_ball_to_bin.robotdreams.json")
        );
        assert!(args.json);
    }

    #[test]
    fn cli_parses_native_render_frame_views() {
        let cli = Cli::try_parse_from([
            "robotdreams",
            "render-frame",
            "examples/puppyarm/project.json",
            "--out",
            "frame.bin",
            "--camera",
            "overhead_camera",
            "--view",
            "depth",
            "--width",
            "8",
            "--height",
            "6",
            "--segmentation-min-alpha",
            "1.0",
            "--shutter-exposure-sec",
            "0.02",
            "--shutter-samples",
            "3",
            "--shutter-mode",
            "rolling-top-to-bottom",
            "--shutter-readout-sec",
            "0.04",
            "--background-rgb",
            "1,2,3",
            "--ambient-rgb",
            "4,5,6",
            "--ambient-intensity",
            "0.7",
            "--environment-sky-top-rgb",
            "20,30,40",
            "--environment-sky-horizon-rgb",
            "50,60,70",
            "--environment-ground-rgb",
            "80,90,100",
            "--environment-map",
            "sky.hdr",
            "--environment-map-rotation-deg",
            "45",
            "--environment-intensity",
            "1.25",
            "--environment-ambient-intensity",
            "0.4",
            "--reflection-probe",
            "probe.hdr",
            "--reflection-probe-rotation-deg",
            "90",
            "--reflection-probe-intensity",
            "0.75",
            "--reflection-probe-ambient-intensity",
            "0.2",
            "--reflection-probe-position",
            "1.0,2.0,3.0",
            "--reflection-probe-box-size-m",
            "4.0,5.0,6.0",
            "--reflection-probe-influence-radius-m",
            "7.0",
            "--reflection-probe-falloff-power",
            "2.0",
            "--white-balance-rgb",
            "1.1,0.9,0.8",
            "--color-temperature-kelvin",
            "4500",
            "--tone-mapping",
            "reinhard",
            "--tone-exposure",
            "1.5",
            "--debug-rgb-samples-per-pixel",
            "4",
            "--ambient-occlusion-samples",
            "16",
            "--ambient-occlusion-radius-m",
            "0.6",
            "--ambient-occlusion-intensity",
            "0.75",
            "--indirect-diffuse-samples",
            "12",
            "--indirect-diffuse-radius-m",
            "0.9",
            "--indirect-diffuse-intensity",
            "0.5",
            "--indirect-diffuse-bounces",
            "3",
            "--soft-shadow-samples",
            "8",
            "--soft-shadow-radius-m",
            "0.12",
            "--area-light-samples",
            "9",
            "--rough-transmission-samples",
            "13",
            "--rough-reflection-samples",
            "15",
            "--specular-reflection-bounces",
            "3",
            "--gltf-material-variant",
            "dirty",
        ])
        .expect("parse render-frame");

        let Command::RenderFrame(args) = cli.command else {
            panic!("expected render-frame command");
        };
        assert_eq!(args.camera.as_deref(), Some("overhead_camera"));
        assert_eq!(args.view, NativeView::Depth);
        assert_eq!(args.width, 8);
        assert_eq!(args.height, 6);
        assert_eq!(args.segmentation_min_alpha, Some(1.0));
        assert_eq!(args.shutter_exposure_sec, Some(0.02));
        assert_eq!(args.shutter_samples, Some(3));
        assert_eq!(
            args.shutter_mode,
            Some(NativeShutterMode::RollingTopToBottom)
        );
        assert_eq!(args.shutter_readout_sec, Some(0.04));
        assert_eq!(args.background_rgb.as_deref(), Some("1,2,3"));
        assert_eq!(args.ambient_rgb.as_deref(), Some("4,5,6"));
        assert_eq!(args.ambient_intensity, Some(0.7));
        assert_eq!(args.environment_sky_top_rgb.as_deref(), Some("20,30,40"));
        assert_eq!(
            args.environment_sky_horizon_rgb.as_deref(),
            Some("50,60,70")
        );
        assert_eq!(args.environment_ground_rgb.as_deref(), Some("80,90,100"));
        assert_eq!(args.environment_map.as_deref(), Some("sky.hdr"));
        assert_eq!(args.environment_map_rotation_deg, Some(45.0));
        assert_eq!(args.environment_intensity, Some(1.25));
        assert_eq!(args.environment_ambient_intensity, Some(0.4));
        assert_eq!(args.reflection_probe.as_deref(), Some("probe.hdr"));
        assert_eq!(args.reflection_probe_rotation_deg, Some(90.0));
        assert_eq!(args.reflection_probe_intensity, Some(0.75));
        assert_eq!(args.reflection_probe_ambient_intensity, Some(0.2));
        assert_eq!(
            args.reflection_probe_position.as_deref(),
            Some("1.0,2.0,3.0")
        );
        assert_eq!(
            args.reflection_probe_box_size_m.as_deref(),
            Some("4.0,5.0,6.0")
        );
        assert_eq!(args.reflection_probe_influence_radius_m, Some(7.0));
        assert_eq!(args.reflection_probe_falloff_power, Some(2.0));
        assert_eq!(args.white_balance_rgb.as_deref(), Some("1.1,0.9,0.8"));
        assert_eq!(args.color_temperature_kelvin, Some(4500.0));
        assert_eq!(args.tone_mapping, Some(NativeToneMapping::Reinhard));
        assert_eq!(args.tone_exposure, Some(1.5));
        assert_eq!(args.debug_rgb_samples_per_pixel, Some(4));
        assert_eq!(args.ambient_occlusion_samples, Some(16));
        assert_eq!(args.ambient_occlusion_radius_m, Some(0.6));
        assert_eq!(args.ambient_occlusion_intensity, Some(0.75));
        assert_eq!(args.indirect_diffuse_samples, Some(12));
        assert_eq!(args.indirect_diffuse_radius_m, Some(0.9));
        assert_eq!(args.indirect_diffuse_intensity, Some(0.5));
        assert_eq!(args.indirect_diffuse_bounces, Some(3));
        assert_eq!(args.soft_shadow_samples, Some(8));
        assert_eq!(args.soft_shadow_radius_m, Some(0.12));
        assert_eq!(args.area_light_samples, Some(9));
        assert_eq!(args.rough_transmission_samples, Some(13));
        assert_eq!(args.rough_reflection_samples, Some(15));
        assert_eq!(args.specular_reflection_bounces, Some(3));
        assert_eq!(args.gltf_material_variant.as_deref(), Some("dirty"));
    }

    #[test]
    fn cli_parses_native_render_frame_auto_camera() {
        let cli = Cli::try_parse_from([
            "robotdreams",
            "render-frame",
            "examples/puppyarm/project.json",
            "--out",
            "frame.png",
            "--camera",
            "auto",
        ])
        .expect("parse render-frame auto camera");

        let Command::RenderFrame(args) = cli.command else {
            panic!("expected render-frame command");
        };
        assert_eq!(args.camera.as_deref(), Some("auto"));
    }

    #[test]
    fn cli_maps_inspection_png_views_to_native_renderer_request() {
        for (view, observation, frame, wire_name) in [
            (
                NativeView::Normal,
                ObservationView::Normal,
                FrameKind::Normal,
                "normal",
            ),
            (
                NativeView::Albedo,
                ObservationView::Albedo,
                FrameKind::Albedo,
                "albedo",
            ),
            (
                NativeView::MaterialProperties,
                ObservationView::MaterialProperties,
                FrameKind::MaterialProperties,
                "materialProperties",
            ),
        ] {
            assert_eq!(observation_view(view), observation);
            assert_eq!(frame_kind(view), Some(frame));
            assert_eq!(frame_kind_wire_name(view).expect("wire name"), wire_name);
            assert_eq!(frame_file_name(7, view), "frame-00007.png");
        }
    }

    #[test]
    fn cli_maps_world_position_to_binary_native_renderer_request() {
        let view = NativeView::WorldPosition;

        assert_eq!(observation_view(view), ObservationView::WorldPosition);
        assert_eq!(frame_kind(view), Some(FrameKind::WorldPosition));
        assert_eq!(
            frame_kind_wire_name(view).expect("wire name"),
            "worldPosition"
        );
        assert_eq!(frame_file_name(7, view), "frame-00007.bin");
    }

    #[test]
    fn cli_render_setting_overrides_inherit_project_base_settings() {
        let mut args = empty_render_setting_args();
        args.rough_reflection_samples = Some(8);
        args.environment_intensity = Some(1.5);
        let base = RenderSettings {
            soft_shadow_samples: 12,
            environment: Some(EnvironmentSettings {
                sky_top_rgb: [10, 20, 30],
                sky_horizon_rgb: [40, 50, 60],
                ground_rgb: [70, 80, 90],
                map: Some("studio.hdr".to_string()),
                map_rotation_deg: 15.0,
                intensity: 1.0,
                ambient_intensity: 0.42,
            }),
            ..RenderSettings::default()
        };

        let settings = render_settings_with_base(args, Some(base)).expect("settings");
        let settings = settings.expect("overrides should produce render settings");

        assert_eq!(settings.rough_reflection_samples, 8);
        assert_eq!(settings.soft_shadow_samples, 12);
        let environment = settings.environment.expect("environment");
        assert_eq!(environment.sky_top_rgb, [10, 20, 30]);
        assert_eq!(environment.map.as_deref(), Some("studio.hdr"));
        assert_eq!(environment.map_rotation_deg, 15.0);
        assert_eq!(environment.intensity, 1.5);
        assert_eq!(environment.ambient_intensity, 0.42);
    }

    #[test]
    fn add_auto_camera_inserts_renderable_camera_node() {
        let mut scene = SceneGraph::empty();
        scene.add_entity(EntityMetadata {
            id: EntityId("box".to_string()),
            name: "Box".to_string(),
            kind: "object".to_string(),
            robot_id: None,
            link_name: None,
        });
        scene.root.children.push(SceneNode::mesh(
            "box",
            "Box",
            Geometry::Box {
                size: [1.0, 1.0, 1.0],
            },
            robotdreams_core::scene_graph::Material::default(),
            Transform::default(),
        ));

        add_auto_camera(&mut scene, [320, 240]);

        assert!(
            scene
                .entities
                .contains_key(&EntityId(format!("camera:{AUTO_CAMERA_ID}")))
        );
        assert!(
            camera_id_from_scene(&scene.root).as_deref() == Some(AUTO_CAMERA_ID),
            "auto camera should be discoverable in scene graph"
        );
    }

    #[test]
    fn cli_parses_native_recording_options() {
        let cli = Cli::try_parse_from([
            "robotdreams",
            "simulation",
            "record",
            "--out",
            "recording.mp4",
            "--camera",
            "overhead_camera",
            "--view",
            "debug-rgb",
            "--seconds",
            "1",
            "--fps",
            "2",
        ])
        .expect("parse simulation record");

        let Command::Simulation(SimulationCommand::Record(args)) = cli.command else {
            panic!("expected simulation record command");
        };
        assert_eq!(args.out.as_deref(), Some(Path::new("recording.mp4")));
        assert_eq!(args.camera.as_deref(), Some("overhead_camera"));
        assert_eq!(args.view, NativeView::DebugRgb);
        assert_eq!(args.fps, 2);
    }

    #[test]
    fn cli_parses_live_native_recording_options() {
        let cli = Cli::try_parse_from([
            "robotdreams",
            "simulation",
            "record",
            "--live",
            "--project",
            "puppyarm",
            "--simulation",
            "sim-a",
            "--keep-frames",
            "frames",
            "--trace-out",
            "trace.jsonl",
            "--camera",
            "auto",
            "--seconds",
            "0.1",
            "--fps",
            "2",
        ])
        .expect("parse live simulation record");

        let Command::Simulation(SimulationCommand::Record(args)) = cli.command else {
            panic!("expected simulation record command");
        };
        assert!(args.live);
        assert_eq!(args.project.as_deref(), Some("puppyarm"));
        assert_eq!(args.simulation.as_deref(), Some("sim-a"));
        assert_eq!(args.keep_frames.as_deref(), Some(Path::new("frames")));
        assert_eq!(args.trace_out.as_deref(), Some(Path::new("trace.jsonl")));
    }

    #[test]
    fn cli_parses_daemon_simulation_render_frame_options() {
        let cli = Cli::try_parse_from([
            "robotdreams",
            "simulation",
            "render-frame",
            "--project",
            "puppyarm",
            "--simulation",
            "sim-a",
            "--out",
            "live.png",
            "--camera",
            "auto",
            "--view",
            "debug-rgb",
            "--width",
            "64",
            "--height",
            "48",
            "--segmentation-min-alpha",
            "0.5",
            "--shutter-exposure-sec",
            "0.03",
            "--shutter-samples",
            "4",
            "--shutter-mode",
            "rolling-top-to-bottom",
            "--shutter-readout-sec",
            "0.05",
            "--background-rgb",
            "7,8,9",
            "--ambient-rgb",
            "10,11,12",
            "--ambient-intensity",
            "0.25",
            "--environment-sky-top-rgb",
            "14,24,34",
            "--environment-map",
            "studio.png",
            "--environment-intensity",
            "0.8",
            "--reflection-probe",
            "studio-probe.png",
            "--reflection-probe-intensity",
            "1.4",
            "--reflection-probe-position",
            "0.0,0.0,1.0",
            "--reflection-probe-box-size-m",
            "3.0,3.0,2.0",
            "--white-balance-rgb",
            "0.8,1.0,1.2",
            "--color-temperature-kelvin",
            "7200",
            "--tone-mapping",
            "aces",
            "--tone-exposure",
            "0.9",
        ])
        .expect("parse simulation render-frame");

        let Command::Simulation(SimulationCommand::RenderFrame(args)) = cli.command else {
            panic!("expected simulation render-frame command");
        };
        assert_eq!(args.project.as_deref(), Some("puppyarm"));
        assert_eq!(args.simulation.as_deref(), Some("sim-a"));
        assert_eq!(args.camera.as_deref(), Some("auto"));
        assert_eq!(args.width, 64);
        assert_eq!(args.height, 48);
        assert_eq!(args.segmentation_min_alpha, Some(0.5));
        assert_eq!(args.shutter_exposure_sec, Some(0.03));
        assert_eq!(args.shutter_samples, Some(4));
        assert_eq!(
            args.shutter_mode,
            Some(NativeShutterMode::RollingTopToBottom)
        );
        assert_eq!(args.shutter_readout_sec, Some(0.05));
        assert_eq!(args.background_rgb.as_deref(), Some("7,8,9"));
        assert_eq!(args.ambient_rgb.as_deref(), Some("10,11,12"));
        assert_eq!(args.ambient_intensity, Some(0.25));
        assert_eq!(args.environment_sky_top_rgb.as_deref(), Some("14,24,34"));
        assert_eq!(args.environment_map.as_deref(), Some("studio.png"));
        assert_eq!(args.environment_intensity, Some(0.8));
        assert_eq!(args.reflection_probe.as_deref(), Some("studio-probe.png"));
        assert_eq!(args.reflection_probe_intensity, Some(1.4));
        assert_eq!(
            args.reflection_probe_position.as_deref(),
            Some("0.0,0.0,1.0")
        );
        assert_eq!(
            args.reflection_probe_box_size_m.as_deref(),
            Some("3.0,3.0,2.0")
        );
        assert_eq!(args.white_balance_rgb.as_deref(), Some("0.8,1.0,1.2"));
        assert_eq!(args.color_temperature_kelvin, Some(7200.0));
        assert_eq!(args.tone_mapping, Some(NativeToneMapping::Aces));
        assert_eq!(args.tone_exposure, Some(0.9));
    }

    #[test]
    fn daemon_trace_sample_data_omits_inline_frame_bytes() {
        let data = serde_json::json!({
            "metadata": {"timestamp_sec": 1.0},
            "frames": [{
                "kind": "debugRgb",
                "width": 2,
                "height": 1,
                "bytesBase64": "AAAA",
            }],
        });

        let sample = daemon_trace_sample_data(&data, Some(Path::new("frames/frame-00000.png")));

        assert!(
            sample
                .pointer("/frames/0/bytesBase64")
                .and_then(|value| value.as_str())
                .is_none()
        );
        assert_eq!(
            sample
                .pointer("/frames/0/path")
                .and_then(|value| value.as_str()),
            Some("frames/frame-00000.png")
        );
    }

    fn assert_args(trace: PathBuf, ready: PathBuf) -> RecordingAssertArgs {
        RecordingAssertArgs {
            trace,
            ready,
            expect_servo_moved: vec![1],
            expect_target_write: vec![1],
            allow_servo_id: vec![1],
            min_present_delta: 1,
            min_target_delta: 1,
            require_transform_change: true,
            json: true,
        }
    }

    fn assert_case(result: &RecordingAssertResult, name: &str, expected: bool) {
        let case = result
            .cases
            .iter()
            .find(|case| case.name == name)
            .unwrap_or_else(|| panic!("missing case {name}"));
        assert_eq!(case.ok, expected, "case {name}");
    }

    fn write_assertion_artifacts(name: &str, trace: &str) -> (PathBuf, PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "robotdreams-recording-assert-test-{}-{name}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let trace_path = dir.join("trace.jsonl");
        let ready_path = dir.join("ready.json");
        std::fs::write(&trace_path, trace).expect("write trace");
        std::fs::write(
            &ready_path,
            r#"{"type":"recordingReady","virtualBusRunning":true,"virtualBusPath":"/dev/pts/4"}"#,
        )
        .expect("write ready");
        (trace_path, ready_path)
    }
}

async fn restart_daemon_with_project(
    socket: &Path,
    path: &Path,
    bind: &str,
) -> Result<DaemonResponse> {
    let _ = send_request(socket, &DaemonRequest::Shutdown).await;
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    start_daemon(socket, Some(path), bind).await?;
    send_request(
        socket,
        &DaemonRequest::Open {
            path: path.display().to_string(),
        },
    )
    .await
}

async fn send_request_or_start(
    socket: &Path,
    request: &DaemonRequest,
    project_path: &Path,
    bind: &str,
) -> Result<DaemonResponse> {
    match send_request(socket, request).await {
        Ok(response) => Ok(response),
        Err(_) => {
            start_daemon(socket, Some(project_path), bind).await?;
            send_request(socket, request).await
        }
    }
}

fn print_response(response: DaemonResponse) -> Result<()> {
    if !response.ok {
        bail!(
            "{}",
            response
                .message
                .unwrap_or_else(|| "daemon command failed".to_string())
        );
    }
    if let Some(message) = response.message {
        println!("{message}");
    }
    if let Some(project_id) = response.project_id {
        println!("Project: {project_id}");
    }
    if let Some(url) = response.url {
        println!("URL: {url}");
    }
    println!("Already open: {}", response.already_open);
    println!("Virtual bus running: {}", response.virtual_bus_running);
    if let Some(path) = response.virtual_bus_path {
        println!("Virtual bus path: {path}");
    }
    if let Some(data) = response.data {
        println!("{}", serde_json::to_string_pretty(&data)?);
    }
    Ok(())
}

fn print_project_data(response: DaemonResponse, json: bool) -> Result<()> {
    if !response.ok {
        bail!(
            "{}",
            response
                .message
                .unwrap_or_else(|| "daemon command failed".to_string())
        );
    }

    let Some(data) = response.data else {
        return print_response(response);
    };

    if json {
        println!("{}", serde_json::to_string_pretty(&data)?);
        return Ok(());
    }

    match data {
        serde_json::Value::Array(items) => {
            for item in items {
                let id = item
                    .get("id")
                    .and_then(|value| value.as_str())
                    .unwrap_or("-");
                let item_type = item
                    .get("type")
                    .and_then(|value| value.as_str())
                    .unwrap_or("-");
                let name = item
                    .get("name")
                    .and_then(|value| value.as_str())
                    .unwrap_or("-");
                println!("{id}\t{item_type}\t{name}");
            }
        }
        value => {
            println!("{}", serde_json::to_string_pretty(&value)?);
        }
    }

    Ok(())
}

fn parse_payload(raw: Option<String>) -> Result<Option<serde_json::Value>> {
    raw.map(|raw| serde_json::from_str(&raw).context("parse --payload JSON"))
        .transpose()
}

fn recording_bus_events(data: &serde_json::Value) -> &[serde_json::Value] {
    data.get("busEvents")
        .and_then(|events| events.as_array())
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

fn recording_servo_snapshots(data: &serde_json::Value) -> &[serde_json::Value] {
    data.get("servoSnapshots")
        .or_else(|| data.pointer("/virtualBus/snapshots"))
        .and_then(|snapshots| snapshots.as_array())
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

fn read_i16(value: &serde_json::Value, key: &str) -> Option<i16> {
    value
        .get(key)
        .and_then(|value| value.as_i64())
        .and_then(|value| i16::try_from(value).ok())
}

fn read_u8(value: &serde_json::Value, key: &str) -> Option<u8> {
    value
        .get(key)
        .and_then(|value| value.as_u64())
        .and_then(|value| u8::try_from(value).ok())
}

fn insert_path(paths: &mut BTreeSet<String>, path: Option<&str>) {
    let Some(path) = path else {
        return;
    };
    let path = path.trim();
    if !path.is_empty() {
        paths.insert(path.to_string());
    }
}

fn is_write_like_instruction(instruction: Option<&str>) -> bool {
    matches!(
        instruction,
        Some("write" | "regWrite" | "syncWrite" | "action" | "syncWritePos")
    )
}

fn collect_bus_event_servo_ids(event: &serde_json::Value, ids: &mut BTreeSet<u8>) {
    for id in event
        .get("ids")
        .and_then(|value| value.as_array())
        .into_iter()
        .flatten()
    {
        if let Some(id) = id.as_u64().and_then(|id| u8::try_from(id).ok()) {
            ids.insert(id);
        }
    }
    for write in event
        .get("writes")
        .and_then(|value| value.as_array())
        .into_iter()
        .flatten()
    {
        if let Some(id) = write
            .get("id")
            .and_then(|id| id.as_u64())
            .and_then(|id| u8::try_from(id).ok())
        {
            ids.insert(id);
        }
    }
    if let Some(id) = event
        .get("id")
        .and_then(|id| id.as_u64())
        .and_then(|id| u8::try_from(id).ok())
    {
        ids.insert(id);
    }
}

fn collect_bus_event_target_writes(
    event: &serde_json::Value,
    elapsed_sec: Option<f64>,
    targets: &mut BTreeMap<u8, ServoCommandAccumulator>,
) {
    let instruction = event.get("instruction").and_then(|value| value.as_str());
    if !is_write_like_instruction(instruction) {
        return;
    }

    for write in event
        .get("writes")
        .and_then(|value| value.as_array())
        .into_iter()
        .flatten()
    {
        let Some(id) = read_u8(write, "id") else {
            continue;
        };
        let Some(target_position) = read_i16(write, "targetPosition") else {
            continue;
        };
        targets
            .entry(id)
            .or_insert_with(|| ServoCommandAccumulator::new(id))
            .record(target_position, elapsed_sec);
    }

    if let Some(id) = read_u8(event, "id")
        && let Some(target_position) = read_i16(event, "targetPosition")
    {
        targets
            .entry(id)
            .or_insert_with(|| ServoCommandAccumulator::new(id))
            .record(target_position, elapsed_sec);
    }
}

fn collect_servo_snapshot(
    snapshot: &serde_json::Value,
    movement: &mut BTreeMap<u8, ServoMovementAccumulator>,
) {
    let Some(id) = read_u8(snapshot, "id") else {
        return;
    };
    let present_position = read_i16(snapshot, "presentPosition");
    let target_position = read_i16(snapshot, "targetPosition");
    let moving = snapshot
        .get("moving")
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    movement
        .entry(id)
        .or_insert_with(|| ServoMovementAccumulator::new(id))
        .record(present_position, target_position, moving);
}

fn transform_ids_changed_between(
    previous: &serde_json::Value,
    current: &serde_json::Value,
) -> BTreeSet<String> {
    let mut ids = BTreeSet::new();
    let Some(current_map) = current.as_object() else {
        if previous != current {
            ids.insert("*".to_string());
        }
        return ids;
    };
    let previous_map = previous.as_object();
    for (id, value) in current_map {
        if previous_map.and_then(|map| map.get(id)) != Some(value) {
            ids.insert(id.clone());
        }
    }
    if let Some(previous_map) = previous_map {
        for id in previous_map.keys() {
            if !current_map.contains_key(id) {
                ids.insert(id.clone());
            }
        }
    }
    ids
}

fn update_scenario_summary(summary: &mut Option<ScenarioTraceSummary>, data: &serde_json::Value) {
    let Some(scenario) = data.get("scenario") else {
        return;
    };
    let entry = summary.get_or_insert_with(|| ScenarioTraceSummary {
        source: "tracePayloadOrDaemonState".to_string(),
        ..ScenarioTraceSummary::default()
    });
    entry.sample_count += 1;
    if entry.scenario_id.is_none() {
        entry.scenario_id = scenario
            .get("scenarioId")
            .or_else(|| scenario.get("id"))
            .and_then(|value| value.as_str())
            .map(str::to_string);
    }
    if let Some(status) = scenario.get("status").and_then(|value| value.as_str()) {
        if entry.first_status.is_none() {
            entry.first_status = Some(status.to_string());
        }
        entry.last_status = Some(status.to_string());
    }
    if let Some(progress) = scenario.get("progress").and_then(|value| value.as_str()) {
        if entry.first_progress.is_none() {
            entry.first_progress = Some(progress.to_string());
        }
        entry.last_progress = Some(progress.to_string());
    }
}

#[derive(Debug)]
struct ServoCommandAccumulator {
    id: u8,
    count: u64,
    first_elapsed_sec: Option<f64>,
    last_elapsed_sec: Option<f64>,
    target_positions: BTreeSet<i16>,
    min_target_position: Option<i16>,
    max_target_position: Option<i16>,
}

impl ServoCommandAccumulator {
    fn new(id: u8) -> Self {
        Self {
            id,
            count: 0,
            first_elapsed_sec: None,
            last_elapsed_sec: None,
            target_positions: BTreeSet::new(),
            min_target_position: None,
            max_target_position: None,
        }
    }

    fn record(&mut self, target_position: i16, elapsed_sec: Option<f64>) {
        self.count += 1;
        if self.first_elapsed_sec.is_none() {
            self.first_elapsed_sec = elapsed_sec;
        }
        self.last_elapsed_sec = elapsed_sec;
        self.target_positions.insert(target_position);
        self.min_target_position = Some(
            self.min_target_position
                .map(|current| current.min(target_position))
                .unwrap_or(target_position),
        );
        self.max_target_position = Some(
            self.max_target_position
                .map(|current| current.max(target_position))
                .unwrap_or(target_position),
        );
    }

    fn finish(self) -> ServoCommandEvidence {
        ServoCommandEvidence {
            id: self.id,
            count: self.count,
            first_elapsed_sec: self.first_elapsed_sec,
            last_elapsed_sec: self.last_elapsed_sec,
            target_positions: self.target_positions.into_iter().collect(),
            min_target_position: self.min_target_position,
            max_target_position: self.max_target_position,
        }
    }
}

#[derive(Debug)]
struct ServoMovementAccumulator {
    id: u8,
    sample_count: u64,
    moving_sample_count: u64,
    first_present_position: Option<i16>,
    last_present_position: Option<i16>,
    min_present_position: Option<i16>,
    max_present_position: Option<i16>,
    first_target_position: Option<i16>,
    last_target_position: Option<i16>,
    min_target_position: Option<i16>,
    max_target_position: Option<i16>,
}

impl ServoMovementAccumulator {
    fn new(id: u8) -> Self {
        Self {
            id,
            sample_count: 0,
            moving_sample_count: 0,
            first_present_position: None,
            last_present_position: None,
            min_present_position: None,
            max_present_position: None,
            first_target_position: None,
            last_target_position: None,
            min_target_position: None,
            max_target_position: None,
        }
    }

    fn record(
        &mut self,
        present_position: Option<i16>,
        target_position: Option<i16>,
        moving: bool,
    ) {
        self.sample_count += 1;
        if moving {
            self.moving_sample_count += 1;
        }
        if let Some(present_position) = present_position {
            if self.first_present_position.is_none() {
                self.first_present_position = Some(present_position);
            }
            self.last_present_position = Some(present_position);
            self.min_present_position = Some(
                self.min_present_position
                    .map(|current| current.min(present_position))
                    .unwrap_or(present_position),
            );
            self.max_present_position = Some(
                self.max_present_position
                    .map(|current| current.max(present_position))
                    .unwrap_or(present_position),
            );
        }
        if let Some(target_position) = target_position {
            if self.first_target_position.is_none() {
                self.first_target_position = Some(target_position);
            }
            self.last_target_position = Some(target_position);
            self.min_target_position = Some(
                self.min_target_position
                    .map(|current| current.min(target_position))
                    .unwrap_or(target_position),
            );
            self.max_target_position = Some(
                self.max_target_position
                    .map(|current| current.max(target_position))
                    .unwrap_or(target_position),
            );
        }
    }

    fn snapshot_count(&self) -> ServoSnapshotCount {
        ServoSnapshotCount {
            id: self.id,
            count: self.sample_count,
        }
    }

    fn finish(self) -> ServoMovementEvidence {
        let max_present_delta = match (self.min_present_position, self.max_present_position) {
            (Some(min), Some(max)) => i32::from(max) - i32::from(min),
            _ => 0,
        };
        let max_target_delta = match (self.min_target_position, self.max_target_position) {
            (Some(min), Some(max)) => i32::from(max) - i32::from(min),
            _ => 0,
        };
        ServoMovementEvidence {
            id: self.id,
            sample_count: self.sample_count,
            moving_sample_count: self.moving_sample_count,
            first_present_position: self.first_present_position,
            last_present_position: self.last_present_position,
            min_present_position: self.min_present_position,
            max_present_position: self.max_present_position,
            max_present_delta,
            first_target_position: self.first_target_position,
            last_target_position: self.last_target_position,
            min_target_position: self.min_target_position,
            max_target_position: self.max_target_position,
            max_target_delta,
        }
    }
}

fn inspect_recording_reader(reader: impl BufRead) -> Result<RecordingInspectSummary> {
    let mut summary = RecordingInspectSummary::default();
    let mut servo_ids = BTreeSet::new();
    let mut virtual_bus_paths = BTreeSet::new();
    let mut target_writes = BTreeMap::new();
    let mut servo_movement = BTreeMap::new();
    let mut changed_transform_ids = BTreeSet::new();
    let mut previous_transforms: Option<serde_json::Value> = None;

    for (line_index, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("read trace line {}", line_index + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let row: serde_json::Value = serde_json::from_str(&line)
            .with_context(|| format!("parse trace line {}", line_index + 1))?;
        let row_type = row.get("type").and_then(|value| value.as_str());
        match row_type {
            Some("recordingStart") => {
                summary.schema = row
                    .get("schema")
                    .and_then(|value| value.as_str())
                    .map(str::to_string);
                summary.started_unix_ms = row
                    .get("startedUnixMs")
                    .and_then(|value| value.as_u64().map(u128::from));
            }
            Some("recordingEnd") => {
                summary.ended_unix_ms = row
                    .get("endedUnixMs")
                    .and_then(|value| value.as_u64().map(u128::from));
                summary.duration_sec = row.get("elapsedSec").and_then(|value| value.as_f64());
            }
            Some("sampleError") => {
                summary.sample_error_count += 1;
            }
            Some("sample") => {
                summary.sample_count += 1;
                let elapsed_sec = row.get("elapsedSec").and_then(|value| value.as_f64());
                let top_level_bus_running = row
                    .get("virtualBusRunning")
                    .and_then(|value| value.as_bool())
                    .unwrap_or(false);
                insert_path(
                    &mut virtual_bus_paths,
                    row.get("virtualBusPath").and_then(|value| value.as_str()),
                );
                let Some(data) = row.get("data") else {
                    if top_level_bus_running {
                        summary.virtual_bus_running_sample_count += 1;
                    }
                    continue;
                };
                insert_path(
                    &mut virtual_bus_paths,
                    data.pointer("/virtualBus/path")
                        .and_then(|value| value.as_str()),
                );

                if let Some(status) = data.get("status").and_then(|value| value.as_str()) {
                    if summary.first_status.is_none() {
                        summary.first_status = Some(status.to_string());
                    }
                    summary.last_status = Some(status.to_string());
                    if status == "running" {
                        summary.running_sample_count += 1;
                    }
                }
                if top_level_bus_running
                    || data
                        .pointer("/virtualBus/running")
                        .and_then(|value| value.as_bool())
                        .unwrap_or(false)
                {
                    summary.virtual_bus_running_sample_count += 1;
                }

                let bus_events = recording_bus_events(data);
                summary.command_count += bus_events.len() as u64;
                summary.bus_event_count += bus_events.len() as u64;
                for event in bus_events {
                    collect_bus_event_servo_ids(event, &mut servo_ids);
                    collect_bus_event_target_writes(event, elapsed_sec, &mut target_writes);
                }

                for snapshot in recording_servo_snapshots(data) {
                    if let Some(id) = read_u8(snapshot, "id") {
                        servo_ids.insert(id);
                    }
                    collect_servo_snapshot(snapshot, &mut servo_movement);
                }

                if let Some(transforms) = data.pointer("/robotScene/dynamicState/transforms") {
                    match previous_transforms.as_ref() {
                        Some(previous) if previous != transforms => {
                            summary.transform_change_count += 1;
                            changed_transform_ids
                                .extend(transform_ids_changed_between(previous, transforms));
                            if summary.first_transform_change_sec.is_none() {
                                summary.first_transform_change_sec = elapsed_sec;
                            }
                            summary.last_transform_change_sec = elapsed_sec;
                        }
                        _ => {}
                    }
                    previous_transforms = Some(transforms.clone());
                }
                update_scenario_summary(&mut summary.scenario, data);
            }
            _ => {}
        }
    }

    summary.servo_ids = servo_ids.into_iter().collect();
    summary.virtual_bus_paths = virtual_bus_paths.into_iter().collect();
    summary.virtual_bus_path = match summary.virtual_bus_paths.as_slice() {
        [path] => Some(path.clone()),
        _ => None,
    };
    summary.bus_command_events_by_servo = target_writes
        .into_values()
        .map(ServoCommandAccumulator::finish)
        .collect();
    summary.servo_snapshot_count_by_servo = servo_movement
        .values()
        .map(|state| state.snapshot_count())
        .collect();
    summary.servo_movement_by_servo = servo_movement
        .into_values()
        .map(ServoMovementAccumulator::finish)
        .collect();
    summary.changed_transform_ids = changed_transform_ids.into_iter().collect();
    Ok(summary)
}

fn inspect_recording(args: RecordingInspectArgs) -> Result<()> {
    let file = std::fs::File::open(&args.trace)
        .with_context(|| format!("open trace {}", args.trace.display()))?;
    let summary = inspect_recording_reader(std::io::BufReader::new(file))?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&summary)?);
        return Ok(());
    }

    println!("Trace {}", args.trace.display());
    if let Some(schema) = &summary.schema {
        println!("Schema: {schema}");
    }
    if let Some(duration) = summary.duration_sec {
        println!("Duration: {duration:.3} s");
    }
    println!("Samples: {}", summary.sample_count);
    println!("Sample errors: {}", summary.sample_error_count);
    println!("Commands: {}", summary.command_count);
    println!("Running samples: {}", summary.running_sample_count);
    println!(
        "Virtual bus running samples: {}",
        summary.virtual_bus_running_sample_count
    );
    if let Some(path) = &summary.virtual_bus_path {
        println!("Virtual bus path: {path}");
    } else if !summary.virtual_bus_paths.is_empty() {
        println!(
            "Virtual bus paths: {}",
            summary.virtual_bus_paths.join(", ")
        );
    }
    println!(
        "Servo ids: {}",
        summary
            .servo_ids
            .iter()
            .map(u8::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!(
        "Status: {} -> {}",
        summary.first_status.as_deref().unwrap_or("-"),
        summary.last_status.as_deref().unwrap_or("-")
    );
    if let Some(first) = summary.first_transform_change_sec {
        println!(
            "Transform changes: first {first:.3} s, last {:.3} s",
            summary.last_transform_change_sec.unwrap_or(first)
        );
    } else {
        println!("Transform changes: none detected");
    }
    Ok(())
}

fn assertion_case(
    name: &str,
    ok: bool,
    details: serde_json::Value,
    first_elapsed_sec: Option<f64>,
) -> RecordingAssertionCase {
    RecordingAssertionCase {
        name: name.to_string(),
        ok,
        severity: "mandatory",
        details,
        first_elapsed_sec,
    }
}

fn ready_virtual_bus_path(ready: &serde_json::Value) -> Option<String> {
    ready
        .get("virtualBusPath")
        .or_else(|| ready.pointer("/simulation/virtualBus/path"))
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn ready_virtual_bus_running(ready: &serde_json::Value) -> bool {
    ready
        .get("virtualBusRunning")
        .or_else(|| ready.pointer("/simulation/virtualBus/running"))
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
}

fn command_evidence(summary: &RecordingInspectSummary, id: u8) -> Option<&ServoCommandEvidence> {
    summary
        .bus_command_events_by_servo
        .iter()
        .find(|evidence| evidence.id == id)
}

fn movement_evidence(summary: &RecordingInspectSummary, id: u8) -> Option<&ServoMovementEvidence> {
    summary
        .servo_movement_by_servo
        .iter()
        .find(|evidence| evidence.id == id)
}

fn expected_target_write_ids(args: &RecordingAssertArgs) -> Vec<u8> {
    let mut ids = BTreeSet::new();
    if args.expect_target_write.is_empty() {
        ids.extend(args.expect_servo_moved.iter().copied());
    } else {
        ids.extend(args.expect_target_write.iter().copied());
    }
    ids.into_iter().collect()
}

fn read_ready_file(path: &Path) -> Result<serde_json::Value> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("read ready file {}", path.display()))?;
    serde_json::from_str(&raw).with_context(|| format!("parse ready file {}", path.display()))
}

fn assert_recording(args: &RecordingAssertArgs) -> Result<RecordingAssertResult> {
    let trace_file = std::fs::File::open(&args.trace)
        .with_context(|| format!("open trace {}", args.trace.display()))?;
    let summary = inspect_recording_reader(std::io::BufReader::new(trace_file))?;
    let ready = read_ready_file(&args.ready)?;
    let ready_path = ready_virtual_bus_path(&ready);
    let expected_target_write_ids = expected_target_write_ids(args);
    let expected_moved_ids = args
        .expect_servo_moved
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();

    let mut cases = Vec::new();
    cases.push(assertion_case(
        "traceComplete",
        summary.schema.as_deref() == Some("robotdreams.recording.trace.v1")
            && summary.sample_count >= 2
            && summary.ended_unix_ms.is_some(),
        serde_json::json!({
            "schema": &summary.schema,
            "sampleCount": summary.sample_count,
            "endedUnixMs": &summary.ended_unix_ms,
        }),
        None,
    ));
    cases.push(assertion_case(
        "noSampleErrors",
        summary.sample_error_count == 0,
        serde_json::json!({
            "sampleErrorCount": summary.sample_error_count,
        }),
        None,
    ));
    cases.push(assertion_case(
        "readyVirtualBus",
        ready_virtual_bus_running(&ready) && ready_path.is_some(),
        serde_json::json!({
            "readyVirtualBusRunning": ready_virtual_bus_running(&ready),
            "readyVirtualBusPath": &ready_path,
        }),
        None,
    ));

    let bus_path_match = ready_path.as_ref().is_some_and(|ready_path| {
        !summary.virtual_bus_paths.is_empty()
            && summary
                .virtual_bus_paths
                .iter()
                .all(|trace_path| trace_path == ready_path)
    });
    cases.push(assertion_case(
        "virtualBusPathMatches",
        bus_path_match,
        serde_json::json!({
            "readyVirtualBusPath": &ready_path,
            "traceVirtualBusPaths": &summary.virtual_bus_paths,
        }),
        None,
    ));
    cases.push(assertion_case(
        "simulationRunning",
        summary.running_sample_count > 0 && summary.virtual_bus_running_sample_count > 0,
        serde_json::json!({
            "runningSampleCount": summary.running_sample_count,
            "virtualBusRunningSampleCount": summary.virtual_bus_running_sample_count,
        }),
        None,
    ));

    let target_write_ok = if expected_target_write_ids.is_empty() {
        !summary.bus_command_events_by_servo.is_empty()
    } else {
        expected_target_write_ids
            .iter()
            .all(|id| command_evidence(&summary, *id).is_some_and(|evidence| evidence.count > 0))
    };
    cases.push(assertion_case(
        "busTargetWrites",
        target_write_ok,
        serde_json::json!({
            "expectedServoIds": &expected_target_write_ids,
            "observed": &summary.bus_command_events_by_servo,
        }),
        summary
            .bus_command_events_by_servo
            .iter()
            .filter_map(|evidence| evidence.first_elapsed_sec)
            .min_by(f64::total_cmp),
    ));

    let moved_ids = if expected_moved_ids.is_empty() {
        summary
            .servo_movement_by_servo
            .iter()
            .filter(|evidence| evidence.max_present_delta >= args.min_present_delta)
            .map(|evidence| evidence.id)
            .collect::<Vec<_>>()
    } else {
        expected_moved_ids.clone()
    };
    let present_movement_ok = !moved_ids.is_empty()
        && moved_ids.iter().all(|id| {
            movement_evidence(&summary, *id).is_some_and(|evidence| {
                evidence.max_present_delta >= args.min_present_delta
                    && (evidence.max_target_delta >= args.min_target_delta
                        || command_evidence(&summary, *id).is_some())
            })
        });
    cases.push(assertion_case(
        "presentPositionMovement",
        present_movement_ok,
        serde_json::json!({
            "expectedServoIds": &moved_ids,
            "minPresentDelta": args.min_present_delta,
            "minTargetDelta": args.min_target_delta,
            "observed": &summary.servo_movement_by_servo,
        }),
        None,
    ));

    if !args.allow_servo_id.is_empty() {
        let allowed = args.allow_servo_id.iter().copied().collect::<BTreeSet<_>>();
        let unexpected = summary
            .servo_ids
            .iter()
            .copied()
            .filter(|id| !allowed.contains(id))
            .collect::<Vec<_>>();
        cases.push(assertion_case(
            "allowedServoIds",
            unexpected.is_empty(),
            serde_json::json!({
                "allowedServoIds": &args.allow_servo_id,
                "observedServoIds": &summary.servo_ids,
                "unexpectedServoIds": &unexpected,
            }),
            None,
        ));
    }

    cases.push(assertion_case(
        "transformChanges",
        !args.require_transform_change || summary.transform_change_count > 0,
        serde_json::json!({
            "required": args.require_transform_change,
            "transformChangeCount": summary.transform_change_count,
            "changedTransformIds": &summary.changed_transform_ids,
            "firstTransformChangeSec": summary.first_transform_change_sec,
            "lastTransformChangeSec": summary.last_transform_change_sec,
        }),
        summary.first_transform_change_sec,
    ));

    let ok = cases.iter().all(|case| case.ok);
    Ok(RecordingAssertResult {
        schema: "robotdreams.recording.assert.v1",
        ok,
        trace: args.trace.display().to_string(),
        ready: args.ready.display().to_string(),
        summary,
        cases,
    })
}

fn print_assert_result(result: &RecordingAssertResult, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(result)?);
    } else {
        println!(
            "Recording assert: {}",
            if result.ok { "ok" } else { "failed" }
        );
        println!("Trace {}", result.trace);
        println!("Ready {}", result.ready);
        for case in &result.cases {
            println!("{}: {}", case.name, if case.ok { "ok" } else { "failed" });
        }
    }
    if result.ok {
        Ok(())
    } else {
        bail!("recording assertion failed")
    }
}

fn assert_recording_command(args: RecordingAssertArgs) -> Result<()> {
    let result = assert_recording(&args)?;
    print_assert_result(&result, args.json)
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    std::env::temp_dir().join(format!("{prefix}-{}-{millis}", std::process::id()))
}

fn encode_frames_to_mp4(frame_dir: &Path, frame_count: u32, fps: u32, out: &Path) -> Result<()> {
    if frame_count == 0 {
        bail!("cannot encode an empty recording");
    }
    if let Some(parent) = out.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create output directory {}", parent.display()))?;
    }

    let location = frame_dir.join("frame-%05d.png");
    let caps = format!("image/png,framerate={}/1", fps.max(1));
    let raw_caps = format!("video/x-raw,format=I420,framerate={}/1", fps.max(1));
    let bitrate = "4000000";
    let gop_size = fps.max(1).to_string();
    let stop_index = frame_count.saturating_sub(1).to_string();
    let out_location = format!("location={}", out.display());

    let status = ProcessCommand::new("gst-launch-1.0")
        .arg("-q")
        .arg("multifilesrc")
        .arg(format!("location={}", location.display()))
        .arg("start-index=0")
        .arg(format!("stop-index={stop_index}"))
        .arg(format!("num-buffers={frame_count}"))
        .arg(format!("caps={caps}"))
        .arg("!")
        .arg("pngdec")
        .arg("!")
        .arg("videoconvert")
        .arg("!")
        .arg(raw_caps)
        .arg("!")
        .arg("openh264enc")
        .arg(format!("bitrate={bitrate}"))
        .arg(format!("gop-size={gop_size}"))
        .arg("!")
        .arg("h264parse")
        .arg("config-interval=-1")
        .arg("!")
        .arg("video/x-h264,stream-format=avc,alignment=au")
        .arg("!")
        .arg("mp4mux")
        .arg("faststart=true")
        .arg("!")
        .arg("filesink")
        .arg(out_location)
        .status()
        .context("run gst-launch-1.0 to encode MP4")?;

    if !status.success() {
        bail!("gst-launch-1.0 failed with {status}");
    }

    Ok(())
}

fn default_record_trace_path(video_out: &Path) -> PathBuf {
    let mut trace_out = video_out.to_path_buf();
    let file_name = video_out
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("recording.mp4");
    if let Some(stripped) = file_name.strip_suffix(".mp4") {
        trace_out.set_file_name(format!("{stripped}.trace.jsonl"));
    } else {
        trace_out.set_file_name(format!("{file_name}.trace.jsonl"));
    }
    trace_out
}

fn write_trace_line(file: &mut std::fs::File, value: serde_json::Value) -> Result<()> {
    serde_json::to_writer(&mut *file, &value)?;
    file.write_all(b"\n")?;
    file.flush()?;
    Ok(())
}

#[cfg(test)]
fn filter_new_bus_events(
    mut data: serde_json::Value,
    last_bus_event_sequence: &mut u64,
) -> serde_json::Value {
    let Some(events) = data
        .get_mut("busEvents")
        .and_then(|events| events.as_array_mut())
    else {
        return data;
    };

    let mut newest = *last_bus_event_sequence;
    let filtered = events
        .iter()
        .filter_map(|event| {
            let sequence = event.get("sequence").and_then(|value| value.as_u64())?;
            if sequence <= *last_bus_event_sequence {
                return None;
            }
            newest = newest.max(sequence);
            Some(event.clone())
        })
        .collect::<Vec<_>>();
    *events = filtered;
    *last_bus_event_sequence = newest;
    data
}

#[cfg(test)]
fn attach_scenario_data(
    mut data: serde_json::Value,
    scenario_data: Option<serde_json::Value>,
) -> serde_json::Value {
    if let Some(scenario_data) = scenario_data
        && let Some(object) = data.as_object_mut()
    {
        object.insert("scenario".to_string(), scenario_data);
    }
    data
}

fn unix_time_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

fn camera_id_from_scene(node: &SceneNode) -> Option<String> {
    if let SceneNodeKind::Camera(camera) = &node.kind {
        return Some(camera.id.clone());
    }
    node.children.iter().find_map(camera_id_from_scene)
}

fn default_camera_id(dreams: &RobotDreams) -> Result<String> {
    let scene = dreams.scene_graph();
    camera_id_from_scene(&scene.root).ok_or_else(|| anyhow!("project does not define a camera"))
}

fn observation_view(view: NativeView) -> ObservationView {
    match view {
        NativeView::DebugRgb => ObservationView::DebugRgb,
        NativeView::Depth => ObservationView::Depth,
        NativeView::Segmentation => ObservationView::Segmentation,
        NativeView::Normal => ObservationView::Normal,
        NativeView::Albedo => ObservationView::Albedo,
        NativeView::MaterialProperties => ObservationView::MaterialProperties,
        NativeView::WorldPosition => ObservationView::WorldPosition,
        NativeView::State => ObservationView::State,
    }
}

fn frame_kind(view: NativeView) -> Option<FrameKind> {
    match view {
        NativeView::DebugRgb => Some(FrameKind::DebugRgb),
        NativeView::Depth => Some(FrameKind::Depth),
        NativeView::Segmentation => Some(FrameKind::Segmentation),
        NativeView::Normal => Some(FrameKind::Normal),
        NativeView::Albedo => Some(FrameKind::Albedo),
        NativeView::MaterialProperties => Some(FrameKind::MaterialProperties),
        NativeView::WorldPosition => Some(FrameKind::WorldPosition),
        NativeView::State => None,
    }
}

fn observation_request(
    dreams: &RobotDreams,
    camera: Option<String>,
    view: NativeView,
    resolution: [u32; 2],
    segmentation_policy: Option<SegmentationPolicy>,
    shutter_policy: Option<ShutterPolicy>,
    render_settings: Option<RenderSettings>,
) -> Result<ObservationRequest> {
    let camera_id = if matches!(view, NativeView::State) {
        None
    } else {
        Some(match camera.as_deref() {
            Some("auto") => AUTO_CAMERA_ID.to_string(),
            Some(camera) => camera.to_string(),
            None => default_camera_id(dreams)?,
        })
    };
    Ok(ObservationRequest {
        camera_id,
        views: vec![observation_view(view)],
        resolution,
        segmentation_policy,
        shutter_policy,
        render_settings,
    })
}

fn segmentation_policy(min_alpha: Option<f32>) -> Option<SegmentationPolicy> {
    min_alpha.map(|min_alpha| SegmentationPolicy { min_alpha })
}

fn shutter_policy(
    exposure_sec: Option<f32>,
    samples: Option<u32>,
    mode: Option<NativeShutterMode>,
    readout_sec: Option<f32>,
) -> Option<ShutterPolicy> {
    if exposure_sec.is_none() && samples.is_none() && mode.is_none() && readout_sec.is_none() {
        return None;
    }
    Some(ShutterPolicy {
        exposure_sec: exposure_sec.unwrap_or(0.0),
        samples: samples.unwrap_or(1),
        mode: match mode.unwrap_or(NativeShutterMode::Global) {
            NativeShutterMode::Global => ShutterMode::Global,
            NativeShutterMode::RollingTopToBottom => ShutterMode::RollingTopToBottom,
        },
        readout_sec: readout_sec.unwrap_or(0.0),
    })
}

#[derive(Clone, Debug, Default)]
struct RenderSettingArgs {
    background_rgb: Option<String>,
    ambient_rgb: Option<String>,
    ambient_intensity: Option<f32>,
    environment_sky_top_rgb: Option<String>,
    environment_sky_horizon_rgb: Option<String>,
    environment_ground_rgb: Option<String>,
    environment_map: Option<String>,
    environment_map_rotation_deg: Option<f32>,
    environment_intensity: Option<f32>,
    environment_ambient_intensity: Option<f32>,
    reflection_probe: Option<String>,
    reflection_probe_rotation_deg: Option<f32>,
    reflection_probe_intensity: Option<f32>,
    reflection_probe_ambient_intensity: Option<f32>,
    reflection_probe_position: Option<String>,
    reflection_probe_box_size_m: Option<String>,
    reflection_probe_influence_radius_m: Option<f32>,
    reflection_probe_falloff_power: Option<f32>,
    white_balance_rgb: Option<String>,
    color_temperature_kelvin: Option<f32>,
    tone_mapping: Option<NativeToneMapping>,
    tone_exposure: Option<f32>,
    debug_rgb_samples_per_pixel: Option<u32>,
    ambient_occlusion_samples: Option<u32>,
    ambient_occlusion_radius_m: Option<f32>,
    ambient_occlusion_intensity: Option<f32>,
    indirect_diffuse_samples: Option<u32>,
    indirect_diffuse_radius_m: Option<f32>,
    indirect_diffuse_intensity: Option<f32>,
    indirect_diffuse_bounces: Option<u32>,
    soft_shadow_samples: Option<u32>,
    soft_shadow_radius_m: Option<f32>,
    area_light_samples: Option<u32>,
    rough_transmission_samples: Option<u32>,
    rough_reflection_samples: Option<u32>,
    specular_reflection_bounces: Option<u32>,
    gltf_material_variant: Option<String>,
}

fn render_settings(args: RenderSettingArgs) -> Result<Option<RenderSettings>> {
    render_settings_with_base(args, None)
}

fn render_settings_with_base(
    args: RenderSettingArgs,
    base: Option<RenderSettings>,
) -> Result<Option<RenderSettings>> {
    if args.background_rgb.is_none()
        && args.ambient_rgb.is_none()
        && args.ambient_intensity.is_none()
        && args.environment_sky_top_rgb.is_none()
        && args.environment_sky_horizon_rgb.is_none()
        && args.environment_ground_rgb.is_none()
        && args.environment_map.is_none()
        && args.environment_map_rotation_deg.is_none()
        && args.environment_intensity.is_none()
        && args.environment_ambient_intensity.is_none()
        && args.reflection_probe.is_none()
        && args.reflection_probe_rotation_deg.is_none()
        && args.reflection_probe_intensity.is_none()
        && args.reflection_probe_ambient_intensity.is_none()
        && args.reflection_probe_position.is_none()
        && args.reflection_probe_box_size_m.is_none()
        && args.reflection_probe_influence_radius_m.is_none()
        && args.reflection_probe_falloff_power.is_none()
        && args.white_balance_rgb.is_none()
        && args.color_temperature_kelvin.is_none()
        && args.tone_mapping.is_none()
        && args.tone_exposure.is_none()
        && args.debug_rgb_samples_per_pixel.is_none()
        && args.ambient_occlusion_samples.is_none()
        && args.ambient_occlusion_radius_m.is_none()
        && args.ambient_occlusion_intensity.is_none()
        && args.indirect_diffuse_samples.is_none()
        && args.indirect_diffuse_radius_m.is_none()
        && args.indirect_diffuse_intensity.is_none()
        && args.indirect_diffuse_bounces.is_none()
        && args.soft_shadow_samples.is_none()
        && args.soft_shadow_radius_m.is_none()
        && args.area_light_samples.is_none()
        && args.rough_transmission_samples.is_none()
        && args.rough_reflection_samples.is_none()
        && args.specular_reflection_bounces.is_none()
        && args.gltf_material_variant.is_none()
    {
        return Ok(None);
    }
    let mut settings = base.unwrap_or_default();
    if let Some(background_rgb) = args.background_rgb {
        settings.background_rgb = parse_rgb(&background_rgb)?;
    }
    if let Some(ambient_rgb) = args.ambient_rgb {
        settings.ambient_rgb = parse_rgb(&ambient_rgb)?;
    }
    if let Some(ambient_intensity) = args.ambient_intensity {
        settings.ambient_intensity = ambient_intensity;
    }
    if args.environment_sky_top_rgb.is_some()
        || args.environment_sky_horizon_rgb.is_some()
        || args.environment_ground_rgb.is_some()
        || args.environment_map.is_some()
        || args.environment_map_rotation_deg.is_some()
        || args.environment_intensity.is_some()
        || args.environment_ambient_intensity.is_some()
    {
        let mut environment = settings.environment.take().unwrap_or(EnvironmentSettings {
            sky_top_rgb: [96, 160, 255],
            sky_horizon_rgb: [180, 205, 235],
            ground_rgb: [70, 72, 68],
            map: None,
            map_rotation_deg: 0.0,
            intensity: 1.0,
            ambient_intensity: 0.35,
        });
        if let Some(sky_top_rgb) = args.environment_sky_top_rgb {
            environment.sky_top_rgb = parse_rgb(&sky_top_rgb)?;
        }
        if let Some(sky_horizon_rgb) = args.environment_sky_horizon_rgb {
            environment.sky_horizon_rgb = parse_rgb(&sky_horizon_rgb)?;
        }
        if let Some(ground_rgb) = args.environment_ground_rgb {
            environment.ground_rgb = parse_rgb(&ground_rgb)?;
        }
        if let Some(map) = args.environment_map {
            environment.map = Some(map);
        }
        if let Some(map_rotation_deg) = args.environment_map_rotation_deg {
            environment.map_rotation_deg = map_rotation_deg;
        }
        if let Some(intensity) = args.environment_intensity {
            environment.intensity = intensity;
        }
        if let Some(ambient_intensity) = args.environment_ambient_intensity {
            environment.ambient_intensity = ambient_intensity;
        }
        settings.environment = Some(environment);
    }
    if args.reflection_probe.is_some()
        || args.reflection_probe_rotation_deg.is_some()
        || args.reflection_probe_intensity.is_some()
        || args.reflection_probe_ambient_intensity.is_some()
        || args.reflection_probe_position.is_some()
        || args.reflection_probe_box_size_m.is_some()
        || args.reflection_probe_influence_radius_m.is_some()
        || args.reflection_probe_falloff_power.is_some()
    {
        let Some(map) = args.reflection_probe else {
            bail!("--reflection-probe is required when reflection probe settings are provided");
        };
        settings.reflection_probe = Some(ReflectionProbeSettings {
            map,
            rotation_deg: args.reflection_probe_rotation_deg.unwrap_or(0.0),
            intensity: args.reflection_probe_intensity.unwrap_or(1.0),
            ambient_intensity: args.reflection_probe_ambient_intensity.unwrap_or(0.35),
            position: args
                .reflection_probe_position
                .as_deref()
                .map(parse_vec3_f32)
                .transpose()?,
            box_size_m: args
                .reflection_probe_box_size_m
                .as_deref()
                .map(parse_vec3_f32)
                .transpose()?,
            influence_radius_m: args.reflection_probe_influence_radius_m,
            falloff_power: args.reflection_probe_falloff_power,
        });
    }
    if let Some(white_balance_rgb) = args.white_balance_rgb {
        settings.white_balance_rgb = parse_rgb_f32(&white_balance_rgb)?;
    }
    if let Some(color_temperature_kelvin) = args.color_temperature_kelvin {
        settings.color_temperature_kelvin = Some(color_temperature_kelvin);
    }
    if let Some(tone_mapping) = args.tone_mapping {
        settings.tone_mapping = match tone_mapping {
            NativeToneMapping::Linear => ToneMapping::Linear,
            NativeToneMapping::Reinhard => ToneMapping::Reinhard,
            NativeToneMapping::Aces => ToneMapping::Aces,
        };
    }
    if let Some(tone_exposure) = args.tone_exposure {
        settings.tone_exposure = tone_exposure;
    }
    if let Some(samples) = args.debug_rgb_samples_per_pixel {
        settings.debug_rgb_samples_per_pixel = samples.max(1);
    }
    if let Some(samples) = args.ambient_occlusion_samples {
        settings.ambient_occlusion_samples = samples;
    }
    if let Some(radius) = args.ambient_occlusion_radius_m {
        settings.ambient_occlusion_radius_m = radius.max(0.0);
    }
    if let Some(intensity) = args.ambient_occlusion_intensity {
        settings.ambient_occlusion_intensity = intensity.max(0.0);
    }
    if let Some(samples) = args.indirect_diffuse_samples {
        settings.indirect_diffuse_samples = samples;
    }
    if let Some(radius) = args.indirect_diffuse_radius_m {
        settings.indirect_diffuse_radius_m = radius.max(0.0);
    }
    if let Some(intensity) = args.indirect_diffuse_intensity {
        settings.indirect_diffuse_intensity = intensity.max(0.0);
    }
    if let Some(bounces) = args.indirect_diffuse_bounces {
        settings.indirect_diffuse_bounces = bounces;
    }
    if let Some(samples) = args.soft_shadow_samples {
        settings.soft_shadow_samples = samples.max(1);
    }
    if let Some(radius) = args.soft_shadow_radius_m {
        settings.soft_shadow_radius_m = radius.max(0.0);
    }
    if let Some(samples) = args.area_light_samples {
        settings.area_light_samples = samples.max(1);
    }
    if let Some(samples) = args.rough_transmission_samples {
        settings.rough_transmission_samples = samples.max(1);
    }
    if let Some(samples) = args.rough_reflection_samples {
        settings.rough_reflection_samples = samples.max(1);
    }
    if let Some(bounces) = args.specular_reflection_bounces {
        settings.specular_reflection_bounces = bounces;
    }
    if let Some(variant) = args.gltf_material_variant {
        settings.gltf_material_variant = Some(variant);
    }
    Ok(Some(settings))
}

fn parse_rgb(value: &str) -> Result<[u8; 3]> {
    let parts = value
        .split(',')
        .map(str::trim)
        .map(str::parse::<u8>)
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("parse RGB color '{value}' as r,g,b"))?;
    let [r, g, b]: [u8; 3] = parts.try_into().map_err(|_| {
        anyhow!("RGB color '{value}' must have exactly three comma-separated bytes")
    })?;
    Ok([r, g, b])
}

fn parse_rgb_f32(value: &str) -> Result<[f32; 3]> {
    let parts = value
        .split(',')
        .map(str::trim)
        .map(str::parse::<f32>)
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("parse RGB float triplet '{value}' as r,g,b"))?;
    let [r, g, b]: [f32; 3] = parts.try_into().map_err(|_| {
        anyhow!("RGB float triplet '{value}' must have exactly three comma-separated values")
    })?;
    Ok([r, g, b])
}

fn parse_vec3_f32(value: &str) -> Result<[f32; 3]> {
    let parts = value
        .split(',')
        .map(str::trim)
        .map(str::parse::<f32>)
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("parse vector '{value}' as x,y,z"))?;
    let [x, y, z]: [f32; 3] = parts
        .try_into()
        .map_err(|_| anyhow!("vector '{value}' must have exactly three comma-separated values"))?;
    Ok([x, y, z])
}

fn create_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create output directory {}", parent.display()))?;
    }
    Ok(())
}

fn metadata_path(out: &Path) -> PathBuf {
    let file_name = out
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("frame");
    out.with_file_name(format!("{file_name}.metadata.json"))
}

fn write_metadata(out: &Path, metadata: &ObservationMetadata) -> Result<PathBuf> {
    let path = metadata_path(out);
    create_parent_dir(&path)?;
    std::fs::write(&path, serde_json::to_vec_pretty(metadata)?)
        .with_context(|| format!("write metadata {}", path.display()))?;
    Ok(path)
}

fn location_json(location: &robotdreams_core::SceneLocation) -> serde_json::Value {
    serde_json::json!({
        "position": location.position,
        "rotation": location.rotation,
    })
}

fn robot_transform_json(snapshot: &RobotDreamsSnapshot) -> serde_json::Value {
    let mut transforms = serde_json::Map::new();
    for robot in &snapshot.robots {
        transforms.insert(
            format!("robot:{}:base", robot.id),
            location_json(&robot.base),
        );
        for (name, link) in &robot.links {
            if let Some(location) = &link.location {
                transforms.insert(
                    format!("robot:{}:link:{name}", robot.id),
                    location_json(location),
                );
            }
        }
        if let Some(tcp) = &robot.tcp
            && let Some(location) = &tcp.location
        {
            transforms.insert(format!("robot:{}:tcp", robot.id), location_json(location));
        }
    }
    serde_json::Value::Object(transforms)
}

fn servo_snapshot_json(snapshot: &RobotDreamsSnapshot) -> Vec<serde_json::Value> {
    snapshot
        .servo_snapshots
        .iter()
        .flat_map(|bus| {
            bus.snapshots.iter().map(|servo| {
                serde_json::json!({
                    "busId": bus.bus_id,
                    "id": servo.id,
                    "mode": servo.mode,
                    "torqueEnabled": servo.torque_enabled,
                    "moving": servo.moving,
                    "targetPosition": servo.target_position,
                    "presentPosition": servo.present_position,
                    "presentSpeed": servo.present_speed,
                    "presentLoad": servo.present_load,
                    "currentRaw": servo.current_raw,
                    "temperatureC": servo.temperature_c,
                    "voltageTenths": servo.voltage_tenths,
                })
            })
        })
        .collect()
}

fn hardware_json(snapshot: &RobotDreamsSnapshot) -> serde_json::Value {
    let buses = snapshot
        .hardware
        .buses
        .iter()
        .map(|bus| {
            let devices = bus
                .devices
                .iter()
                .map(|device| match device {
                    robotdreams_core::HardwareDeviceRuntime::Servo(servo) => serde_json::json!({
                        "type": "servo",
                        "id": servo.id,
                        "name": servo.name,
                        "profile": servo.profile,
                        "drivesRobot": servo.drives_robot,
                        "drivesJoint": servo.drives_joint,
                        "zeroOffset": servo.zero_offset,
                        "direction": servo.direction,
                        "targetPosition": servo.target_position,
                        "presentPosition": servo.present_position,
                        "torqueEnabled": servo.torque_enabled,
                    }),
                    robotdreams_core::HardwareDeviceRuntime::DcMotor(motor) => serde_json::json!({
                        "type": "dcMotor",
                        "id": motor.id,
                        "name": motor.name,
                        "profile": motor.profile,
                        "drivesRobot": motor.drives_robot,
                        "drivesWheel": motor.drives_wheel,
                        "direction": motor.direction,
                        "maxSpeedMps": motor.max_speed_mps,
                        "commandSpeed": motor.command_speed,
                    }),
                    robotdreams_core::HardwareDeviceRuntime::Imu(imu) => serde_json::json!({
                        "type": "imu",
                        "id": imu.id,
                        "name": imu.name,
                        "profile": imu.profile,
                        "mountedRobot": imu.mounted_robot,
                        "mountedLink": imu.mounted_link,
                    }),
                    robotdreams_core::HardwareDeviceRuntime::IoBoard(io) => serde_json::json!({
                        "type": "ioBoard",
                        "id": io.id,
                        "name": io.name,
                        "profile": io.profile,
                        "digitalInputs": io.digital_inputs,
                        "digitalOutputs": io.digital_outputs,
                        "analogInputs": io.analog_inputs,
                    }),
                })
                .collect::<Vec<_>>();
            serde_json::json!({
                "id": bus.id,
                "name": bus.name,
                "transportType": bus.transport_type,
                "devicePath": bus.device_path,
                "baud": bus.baud,
                "protocol": bus.protocol,
                "devices": devices,
            })
        })
        .collect::<Vec<_>>();
    serde_json::json!({ "buses": buses })
}

fn snapshot_json(snapshot: &RobotDreamsSnapshot) -> serde_json::Value {
    let servo_snapshots = servo_snapshot_json(snapshot);
    serde_json::json!({
        "status": "running",
        "clockSec": snapshot.clock_sec,
        "hardware": hardware_json(snapshot),
        "servoSnapshots": servo_snapshots,
        "virtualBus": {
            "running": !snapshot.servo_snapshots.is_empty(),
            "snapshots": servo_snapshot_json(snapshot),
        },
        "robots": snapshot.robots.iter().map(|robot| {
            serde_json::json!({
                "id": robot.id,
                "name": robot.name,
                "base": location_json(&robot.base),
                "joints": robot.joints.iter().map(|(name, joint)| {
                    (name.clone(), serde_json::json!({
                        "urdfName": joint.urdf_name,
                        "semanticName": joint.semantic_name,
                        "positionRad": joint.position_rad,
                        "location": joint.location.as_ref().map(location_json),
                    }))
                }).collect::<serde_json::Map<_, _>>(),
                "tcp": robot.tcp.as_ref().map(|tcp| serde_json::json!({
                    "name": tcp.name,
                    "link": tcp.link,
                    "location": tcp.location.as_ref().map(location_json),
                })),
            })
        }).collect::<Vec<_>>(),
        "robotScene": {
            "dynamicState": {
                "transforms": robot_transform_json(snapshot),
            }
        },
    })
}

fn write_snapshot_json(path: &Path, snapshot: &RobotDreamsSnapshot) -> Result<()> {
    create_parent_dir(path)?;
    std::fs::write(path, serde_json::to_vec_pretty(&snapshot_json(snapshot))?)
        .with_context(|| format!("write state {}", path.display()))
}

fn write_native_trace(
    trace_out: &Path,
    project_path: &Path,
    video_out: Option<&Path>,
    request: &RecordingRequest,
    snapshots: &[RobotDreamsSnapshot],
) -> Result<()> {
    create_parent_dir(trace_out)?;
    let mut file = std::fs::File::create(trace_out)
        .with_context(|| format!("create trace {}", trace_out.display()))?;
    let start_clock = snapshots
        .first()
        .map(|snapshot| snapshot.clock_sec)
        .unwrap_or(0.0);
    write_trace_line(
        &mut file,
        serde_json::json!({
            "type": "recordingStart",
            "schema": "robotdreams.recording.trace.v1",
            "startedUnixMs": unix_time_millis(),
            "projectPath": project_path.display().to_string(),
            "videoOut": video_out.map(|path| path.display().to_string()),
            "seconds": request.seconds,
            "fps": request.fps,
            "width": request.observation.resolution[0],
            "height": request.observation.resolution[1],
            "native": true,
        }),
    )?;
    for (index, snapshot) in snapshots.iter().enumerate() {
        write_trace_line(
            &mut file,
            serde_json::json!({
                "type": "sample",
                "index": index,
                "elapsedSec": snapshot.clock_sec - start_clock,
                "ok": true,
                "virtualBusRunning": !snapshot.servo_snapshots.is_empty(),
                "data": snapshot_json(snapshot),
            }),
        )?;
    }
    let elapsed_sec = snapshots
        .last()
        .map(|snapshot| snapshot.clock_sec - start_clock)
        .unwrap_or(0.0);
    write_trace_line(
        &mut file,
        serde_json::json!({
            "type": "recordingEnd",
            "endedUnixMs": unix_time_millis(),
            "elapsedSec": elapsed_sec,
            "samples": snapshots.len(),
        }),
    )?;
    Ok(())
}

fn frame_for_view<'a>(output: &'a RenderOutput, view: NativeView) -> Result<&'a FrameBuffer> {
    let Some(kind) = frame_kind(view) else {
        bail!("state view does not produce a frame buffer");
    };
    output
        .frames
        .iter()
        .find(|frame| frame.kind == kind)
        .ok_or_else(|| anyhow!("renderer did not produce {kind:?} frame"))
}

fn write_frame_buffer(path: &Path, frame: &FrameBuffer) -> Result<()> {
    create_parent_dir(path)?;
    std::fs::write(path, &frame.bytes).with_context(|| format!("write frame {}", path.display()))
}

fn frame_kind_wire_name(view: NativeView) -> Result<&'static str> {
    match view {
        NativeView::DebugRgb => Ok("debugRgb"),
        NativeView::Depth => Ok("depth"),
        NativeView::Segmentation => Ok("segmentation"),
        NativeView::Normal => Ok("normal"),
        NativeView::Albedo => Ok("albedo"),
        NativeView::MaterialProperties => Ok("materialProperties"),
        NativeView::WorldPosition => Ok("worldPosition"),
        NativeView::State => bail!("state view does not produce a frame buffer"),
    }
}

fn daemon_frame_from_data(
    data: &serde_json::Value,
    view: NativeView,
    fallback_resolution: [u32; 2],
) -> Result<FrameBuffer> {
    let expected_kind = frame_kind_wire_name(view)?;
    let frame = data
        .get("frames")
        .and_then(|frames| frames.as_array())
        .and_then(|frames| {
            frames.iter().find(|frame| {
                frame.get("kind").and_then(|kind| kind.as_str()) == Some(expected_kind)
            })
        })
        .ok_or_else(|| {
            anyhow!("daemon render-frame response did not include {expected_kind} frame")
        })?;
    let width = frame
        .get("width")
        .and_then(|value| value.as_u64())
        .and_then(|value| u32::try_from(value).ok())
        .unwrap_or(fallback_resolution[0]);
    let height = frame
        .get("height")
        .and_then(|value| value.as_u64())
        .and_then(|value| u32::try_from(value).ok())
        .unwrap_or(fallback_resolution[1]);
    let bytes = frame
        .get("bytesBase64")
        .and_then(|value| value.as_str())
        .ok_or_else(|| anyhow!("daemon render-frame response did not include bytesBase64"))
        .and_then(|bytes| {
            BASE64_STANDARD
                .decode(bytes)
                .map_err(|err| anyhow!("decode daemon frame bytes: {err}"))
        })?;
    let kind = frame_kind(view).expect("state handled by frame_kind_wire_name");
    Ok(FrameBuffer {
        kind,
        width,
        height,
        bytes,
    })
}

fn write_daemon_frame_metadata(out: &Path, data: &serde_json::Value) -> Result<Option<PathBuf>> {
    let Some(metadata) = data.get("metadata") else {
        return Ok(None);
    };
    let metadata_path = metadata_path(out);
    create_parent_dir(&metadata_path)?;
    std::fs::write(&metadata_path, serde_json::to_vec_pretty(metadata)?)
        .with_context(|| format!("write metadata {}", metadata_path.display()))?;
    Ok(Some(metadata_path))
}

fn daemon_trace_sample_data(
    data: &serde_json::Value,
    frame_path: Option<&Path>,
) -> serde_json::Value {
    let mut data = data.clone();
    if let Some(frames) = data
        .get_mut("frames")
        .and_then(|frames| frames.as_array_mut())
    {
        for frame in frames {
            if let Some(object) = frame.as_object_mut() {
                object.remove("bytesBase64");
                if let Some(path) = frame_path {
                    object.insert(
                        "path".to_string(),
                        serde_json::Value::String(path.display().to_string()),
                    );
                }
            }
        }
    }
    data
}

fn write_daemon_render_frame(
    response: DaemonResponse,
    args: SimulationRenderFrameArgs,
) -> Result<()> {
    if !response.ok {
        bail!(
            "{}",
            response
                .message
                .unwrap_or_else(|| "daemon render-frame failed".to_string())
        );
    }
    let Some(data) = response.data else {
        bail!("daemon render-frame response did not include data");
    };

    if args.json {
        println!("{}", serde_json::to_string_pretty(&data)?);
    }

    if matches!(args.view, NativeView::State) {
        create_parent_dir(&args.out)?;
        std::fs::write(&args.out, serde_json::to_vec_pretty(&data)?)
            .with_context(|| format!("write state {}", args.out.display()))?;
        println!("Wrote {}", args.out.display());
        return Ok(());
    }

    let frame = daemon_frame_from_data(&data, args.view, [args.width, args.height])?;
    write_frame_buffer(&args.out, &frame)?;
    if let Some(metadata_path) = write_daemon_frame_metadata(&args.out, &data)? {
        println!("Metadata {}", metadata_path.display());
    }
    println!("Wrote {}", args.out.display());
    Ok(())
}

fn frame_file_name(index: usize, view: NativeView) -> String {
    let extension = match view {
        NativeView::DebugRgb
        | NativeView::Normal
        | NativeView::Albedo
        | NativeView::MaterialProperties => "png",
        NativeView::Depth | NativeView::Segmentation | NativeView::WorldPosition => "bin",
        NativeView::State => "json",
    };
    format!("frame-{index:05}.{extension}")
}

fn write_recorded_frames(
    frame_dir: &Path,
    view: NativeView,
    frames: &[robotdreams_recorder::RecordedFrame],
) -> Result<()> {
    std::fs::create_dir_all(frame_dir)
        .with_context(|| format!("create frame directory {}", frame_dir.display()))?;
    for frame in frames {
        let buffer = frame_for_view(&frame.output, view)?;
        let path = frame_dir.join(frame_file_name(frame.index, view));
        write_frame_buffer(&path, buffer)?;
        let metadata_path = metadata_path(&path);
        std::fs::write(
            &metadata_path,
            serde_json::to_vec_pretty(&frame.output.metadata)?,
        )
        .with_context(|| format!("write metadata {}", metadata_path.display()))?;
    }
    Ok(())
}

fn render_frame(args: RenderFrameArgs) -> Result<()> {
    let dreams = RobotDreams::open(&args.path)
        .map_err(|err| anyhow!("{err}"))
        .with_context(|| format!("open project {}", args.path.display()))?;
    if matches!(args.view, NativeView::State) {
        write_snapshot_json(&args.out, &dreams.snapshot())?;
        println!("Wrote {}", args.out.display());
        return Ok(());
    }

    let request = observation_request(
        &dreams,
        args.camera,
        args.view,
        [args.width.max(1), args.height.max(1)],
        segmentation_policy(args.segmentation_min_alpha),
        shutter_policy(
            args.shutter_exposure_sec,
            args.shutter_samples,
            args.shutter_mode,
            args.shutter_readout_sec,
        ),
        render_settings_with_base(
            RenderSettingArgs {
                background_rgb: args.background_rgb,
                ambient_rgb: args.ambient_rgb,
                ambient_intensity: args.ambient_intensity,
                environment_sky_top_rgb: args.environment_sky_top_rgb,
                environment_sky_horizon_rgb: args.environment_sky_horizon_rgb,
                environment_ground_rgb: args.environment_ground_rgb,
                environment_map: args.environment_map,
                environment_map_rotation_deg: args.environment_map_rotation_deg,
                environment_intensity: args.environment_intensity,
                environment_ambient_intensity: args.environment_ambient_intensity,
                reflection_probe: args.reflection_probe,
                reflection_probe_rotation_deg: args.reflection_probe_rotation_deg,
                reflection_probe_intensity: args.reflection_probe_intensity,
                reflection_probe_ambient_intensity: args.reflection_probe_ambient_intensity,
                reflection_probe_position: args.reflection_probe_position,
                reflection_probe_box_size_m: args.reflection_probe_box_size_m,
                reflection_probe_influence_radius_m: args.reflection_probe_influence_radius_m,
                reflection_probe_falloff_power: args.reflection_probe_falloff_power,
                white_balance_rgb: args.white_balance_rgb,
                color_temperature_kelvin: args.color_temperature_kelvin,
                tone_mapping: args.tone_mapping,
                tone_exposure: args.tone_exposure,
                debug_rgb_samples_per_pixel: args.debug_rgb_samples_per_pixel,
                ambient_occlusion_samples: args.ambient_occlusion_samples,
                ambient_occlusion_radius_m: args.ambient_occlusion_radius_m,
                ambient_occlusion_intensity: args.ambient_occlusion_intensity,
                indirect_diffuse_samples: args.indirect_diffuse_samples,
                indirect_diffuse_radius_m: args.indirect_diffuse_radius_m,
                indirect_diffuse_intensity: args.indirect_diffuse_intensity,
                indirect_diffuse_bounces: args.indirect_diffuse_bounces,
                soft_shadow_samples: args.soft_shadow_samples,
                soft_shadow_radius_m: args.soft_shadow_radius_m,
                area_light_samples: args.area_light_samples,
                rough_transmission_samples: args.rough_transmission_samples,
                rough_reflection_samples: args.rough_reflection_samples,
                specular_reflection_bounces: args.specular_reflection_bounces,
                gltf_material_variant: args.gltf_material_variant,
            },
            dreams.scene_graph().render_settings.clone(),
        )?,
    )?;
    let scene = prepare_observation_scene(dreams.scene_graph(), &request);
    let output = NativeRenderer::new()
        .render(&scene, Some(dreams.snapshot()), &request)
        .map_err(|err| anyhow!(err))?;
    let frame = frame_for_view(&output, args.view)?;
    write_frame_buffer(&args.out, frame)?;
    let metadata = write_metadata(&args.out, &output.metadata)?;

    println!("Wrote {}", args.out.display());
    println!("Metadata {}", metadata.display());
    Ok(())
}

fn record_simulation(project_path: &Path, args: SimulationRecordArgs) -> Result<()> {
    let mut dreams = RobotDreams::open(project_path)
        .map_err(|err| anyhow!("{err}"))
        .with_context(|| format!("open project {}", project_path.display()))?;
    let fps = args.fps.max(1);
    let seconds = args.seconds.max(0.0);
    let is_state = matches!(args.view, NativeView::State);
    let trace_path = if args.trace_only || is_state {
        Some(args.trace_out.clone().or(args.out.clone()).context(
            "state and --trace-only recordings require --out or --trace-out for the JSONL trace",
        )?)
    } else {
        args.trace_out
            .clone()
            .or_else(|| args.out.as_deref().map(default_record_trace_path))
    };

    if matches!(
        args.view,
        NativeView::Depth
            | NativeView::Segmentation
            | NativeView::Normal
            | NativeView::Albedo
            | NativeView::MaterialProperties
            | NativeView::WorldPosition
    ) && !args.trace_only
        && args.keep_frames.is_none()
    {
        bail!(
            "depth, segmentation, normal, albedo, material-properties, and world-position recording require --keep-frames"
        );
    }

    let request = observation_request(
        &dreams,
        args.camera.clone(),
        args.view,
        [args.width.max(1), args.height.max(1)],
        segmentation_policy(args.segmentation_min_alpha),
        shutter_policy(
            args.shutter_exposure_sec,
            args.shutter_samples,
            args.shutter_mode,
            args.shutter_readout_sec,
        ),
        render_settings_with_base(
            RenderSettingArgs {
                background_rgb: args.background_rgb.clone(),
                ambient_rgb: args.ambient_rgb.clone(),
                ambient_intensity: args.ambient_intensity,
                environment_sky_top_rgb: args.environment_sky_top_rgb.clone(),
                environment_sky_horizon_rgb: args.environment_sky_horizon_rgb.clone(),
                environment_ground_rgb: args.environment_ground_rgb.clone(),
                environment_map: args.environment_map.clone(),
                environment_map_rotation_deg: args.environment_map_rotation_deg,
                environment_intensity: args.environment_intensity,
                environment_ambient_intensity: args.environment_ambient_intensity,
                reflection_probe: args.reflection_probe.clone(),
                reflection_probe_rotation_deg: args.reflection_probe_rotation_deg,
                reflection_probe_intensity: args.reflection_probe_intensity,
                reflection_probe_ambient_intensity: args.reflection_probe_ambient_intensity,
                reflection_probe_position: args.reflection_probe_position.clone(),
                reflection_probe_box_size_m: args.reflection_probe_box_size_m.clone(),
                reflection_probe_influence_radius_m: args.reflection_probe_influence_radius_m,
                reflection_probe_falloff_power: args.reflection_probe_falloff_power,
                white_balance_rgb: args.white_balance_rgb.clone(),
                color_temperature_kelvin: args.color_temperature_kelvin,
                tone_mapping: args.tone_mapping,
                tone_exposure: args.tone_exposure,
                debug_rgb_samples_per_pixel: args.debug_rgb_samples_per_pixel,
                ambient_occlusion_samples: args.ambient_occlusion_samples,
                ambient_occlusion_radius_m: args.ambient_occlusion_radius_m,
                ambient_occlusion_intensity: args.ambient_occlusion_intensity,
                indirect_diffuse_samples: args.indirect_diffuse_samples,
                indirect_diffuse_radius_m: args.indirect_diffuse_radius_m,
                indirect_diffuse_intensity: args.indirect_diffuse_intensity,
                indirect_diffuse_bounces: args.indirect_diffuse_bounces,
                soft_shadow_samples: args.soft_shadow_samples,
                soft_shadow_radius_m: args.soft_shadow_radius_m,
                area_light_samples: args.area_light_samples,
                rough_transmission_samples: args.rough_transmission_samples,
                rough_reflection_samples: args.rough_reflection_samples,
                specular_reflection_bounces: args.specular_reflection_bounces,
                gltf_material_variant: args.gltf_material_variant.clone(),
            },
            dreams.scene_graph().render_settings.clone(),
        )?,
    )?;
    let mut artifacts = vec![RecordingArtifact::StateTrace];
    if !args.trace_only && !is_state {
        if matches!(args.view, NativeView::DebugRgb) && args.keep_frames.is_none() {
            artifacts.push(RecordingArtifact::VideoFrames);
        } else {
            artifacts.push(RecordingArtifact::Frames);
        }
    }
    let recording_request = RecordingRequest {
        observation: request,
        fps: fps as f32,
        seconds,
        artifacts,
    };
    let output = NativeRecorder::new()
        .record(&mut dreams, &recording_request)
        .map_err(|err| anyhow!(err))?;

    if let Some(trace_path) = trace_path.as_ref() {
        write_native_trace(
            trace_path,
            project_path,
            args.out.as_deref(),
            &recording_request,
            &output.state_trace,
        )?;
        println!("Trace {}", trace_path.display());
    }

    if args.trace_only || is_state {
        return Ok(());
    }

    if let Some(frame_dir) = args.keep_frames.as_ref() {
        write_recorded_frames(frame_dir, args.view, &output.frames)?;
        println!("Frames: {} at {fps} fps", output.frame_count);
        println!("Wrote frames {}", frame_dir.display());
        return Ok(());
    }

    let video_out = args.out.as_ref().context(
        "debug-rgb recording requires --out unless --keep-frames or --trace-only is set",
    )?;
    let stream = output
        .video_stream
        .as_ref()
        .context("native recorder did not produce a video frame stream")?;
    let frame_dir = unique_temp_dir("robotdreams-record-frames");
    std::fs::create_dir_all(&frame_dir)
        .with_context(|| format!("create frame directory {}", frame_dir.display()))?;
    for (index, frame) in stream.frames.iter().enumerate() {
        write_frame_buffer(
            &frame_dir.join(frame_file_name(index, NativeView::DebugRgb)),
            frame,
        )?;
    }
    encode_frames_to_mp4(&frame_dir, stream.frames.len() as u32, fps, video_out)?;
    let _ = std::fs::remove_dir_all(&frame_dir);

    println!("Frames: {} at {fps} fps", stream.frames.len());
    println!("Wrote {}", video_out.display());
    Ok(())
}

async fn record_live_simulation(
    socket: &Path,
    project_path: &Path,
    bind: &str,
    args: SimulationRecordArgs,
) -> Result<()> {
    let fps = args.fps.max(1);
    let seconds = args.seconds.max(0.0);
    let is_state = matches!(args.view, NativeView::State);
    let trace_path = if args.trace_only || is_state {
        Some(args.trace_out.clone().or(args.out.clone()).context(
            "state and --trace-only recordings require --out or --trace-out for the JSONL trace",
        )?)
    } else {
        args.trace_out
            .clone()
            .or_else(|| args.out.as_deref().map(default_record_trace_path))
    };

    if matches!(
        args.view,
        NativeView::Depth
            | NativeView::Segmentation
            | NativeView::Normal
            | NativeView::Albedo
            | NativeView::MaterialProperties
            | NativeView::WorldPosition
    ) && !args.trace_only
        && args.keep_frames.is_none()
    {
        bail!(
            "depth, segmentation, normal, albedo, material-properties, and world-position recording require --keep-frames"
        );
    }

    let frame_count = (seconds * fps as f32).ceil() as usize + 1;
    let frame_interval = std::time::Duration::from_secs_f32(1.0 / fps as f32);
    let render_settings = render_settings(RenderSettingArgs {
        background_rgb: args.background_rgb.clone(),
        ambient_rgb: args.ambient_rgb.clone(),
        ambient_intensity: args.ambient_intensity,
        environment_sky_top_rgb: args.environment_sky_top_rgb.clone(),
        environment_sky_horizon_rgb: args.environment_sky_horizon_rgb.clone(),
        environment_ground_rgb: args.environment_ground_rgb.clone(),
        environment_map: args.environment_map.clone(),
        environment_map_rotation_deg: args.environment_map_rotation_deg,
        environment_intensity: args.environment_intensity,
        environment_ambient_intensity: args.environment_ambient_intensity,
        reflection_probe: args.reflection_probe.clone(),
        reflection_probe_rotation_deg: args.reflection_probe_rotation_deg,
        reflection_probe_intensity: args.reflection_probe_intensity,
        reflection_probe_ambient_intensity: args.reflection_probe_ambient_intensity,
        reflection_probe_position: args.reflection_probe_position.clone(),
        reflection_probe_box_size_m: args.reflection_probe_box_size_m.clone(),
        reflection_probe_influence_radius_m: args.reflection_probe_influence_radius_m,
        reflection_probe_falloff_power: args.reflection_probe_falloff_power,
        white_balance_rgb: args.white_balance_rgb.clone(),
        color_temperature_kelvin: args.color_temperature_kelvin,
        tone_mapping: args.tone_mapping,
        tone_exposure: args.tone_exposure,
        debug_rgb_samples_per_pixel: args.debug_rgb_samples_per_pixel,
        ambient_occlusion_samples: args.ambient_occlusion_samples,
        ambient_occlusion_radius_m: args.ambient_occlusion_radius_m,
        ambient_occlusion_intensity: args.ambient_occlusion_intensity,
        indirect_diffuse_samples: args.indirect_diffuse_samples,
        indirect_diffuse_radius_m: args.indirect_diffuse_radius_m,
        indirect_diffuse_intensity: args.indirect_diffuse_intensity,
        indirect_diffuse_bounces: args.indirect_diffuse_bounces,
        soft_shadow_samples: args.soft_shadow_samples,
        soft_shadow_radius_m: args.soft_shadow_radius_m,
        area_light_samples: args.area_light_samples,
        rough_transmission_samples: args.rough_transmission_samples,
        rough_reflection_samples: args.rough_reflection_samples,
        specular_reflection_bounces: args.specular_reflection_bounces,
        gltf_material_variant: args.gltf_material_variant.clone(),
    })?;
    let wants_frames = !args.trace_only && !is_state;
    let temp_frame_dir = if wants_frames
        && args.keep_frames.is_none()
        && matches!(args.view, NativeView::DebugRgb)
    {
        let dir = unique_temp_dir("robotdreams-live-record-frames");
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("create frame directory {}", dir.display()))?;
        Some(dir)
    } else {
        None
    };

    let mut trace_file = if let Some(trace_path) = trace_path.as_ref() {
        create_parent_dir(trace_path)?;
        Some(
            std::fs::File::create(trace_path)
                .with_context(|| format!("create trace {}", trace_path.display()))?,
        )
    } else {
        None
    };

    if let Some(file) = trace_file.as_mut() {
        write_trace_line(
            file,
            serde_json::json!({
                "type": "recordingStart",
                "schema": "robotdreams.recording.trace.v1",
                "startedUnixMs": unix_time_millis(),
                "projectPath": project_path.display().to_string(),
                "project": args.project.clone(),
                "simulation": args.simulation.clone(),
                "videoOut": args.out.as_ref().map(|path| path.display().to_string()),
                "seconds": seconds,
                "fps": fps,
                "width": args.width.max(1),
                "height": args.height.max(1),
                "native": true,
                "liveDaemon": true,
            }),
        )?;
    }

    let mut start_clock = None;
    let mut written_frames = 0_usize;
    for index in 0..frame_count {
        if index > 0 {
            tokio::time::sleep(frame_interval).await;
        }

        let response = send_request_or_start(
            socket,
            &DaemonRequest::RenderFrame {
                project: args.project.clone(),
                simulation: args.simulation.clone(),
                camera: args.camera.clone(),
                views: Some(vec![observation_view(args.view)]),
                width: Some(args.width.max(1)),
                height: Some(args.height.max(1)),
                segmentation_policy: segmentation_policy(args.segmentation_min_alpha),
                shutter_policy: shutter_policy(
                    args.shutter_exposure_sec,
                    args.shutter_samples,
                    args.shutter_mode,
                    args.shutter_readout_sec,
                ),
                render_settings: render_settings.clone(),
            },
            project_path,
            bind,
        )
        .await?;
        if !response.ok {
            bail!(
                "{}",
                response
                    .message
                    .unwrap_or_else(|| "daemon live recording sample failed".to_string())
            );
        }
        let virtual_bus_running = response.virtual_bus_running;
        let data = response
            .data
            .ok_or_else(|| anyhow!("daemon render-frame response did not include data"))?;

        let frame_path = if wants_frames {
            let frame_dir = args.keep_frames.as_ref().or(temp_frame_dir.as_ref());
            if let Some(frame_dir) = frame_dir {
                std::fs::create_dir_all(frame_dir)
                    .with_context(|| format!("create frame directory {}", frame_dir.display()))?;
                let path = frame_dir.join(frame_file_name(index, args.view));
                let frame = daemon_frame_from_data(&data, args.view, [args.width, args.height])?;
                write_frame_buffer(&path, &frame)?;
                let _ = write_daemon_frame_metadata(&path, &data)?;
                written_frames += 1;
                Some(path)
            } else {
                None
            }
        } else {
            None
        };

        let timestamp_sec = data
            .pointer("/metadata/timestamp_sec")
            .and_then(|value| value.as_f64())
            .unwrap_or(index as f64 / fps as f64);
        let start = *start_clock.get_or_insert(timestamp_sec);
        if let Some(file) = trace_file.as_mut() {
            write_trace_line(
                file,
                serde_json::json!({
                    "type": "sample",
                    "index": index,
                    "elapsedSec": timestamp_sec - start,
                    "ok": true,
                    "virtualBusRunning": virtual_bus_running,
                    "framePath": frame_path.as_ref().map(|path| path.display().to_string()),
                    "data": daemon_trace_sample_data(&data, frame_path.as_deref()),
                }),
            )?;
        }
    }

    if let Some(file) = trace_file.as_mut() {
        write_trace_line(
            file,
            serde_json::json!({
                "type": "recordingEnd",
                "endedUnixMs": unix_time_millis(),
                "elapsedSec": seconds,
                "samples": frame_count,
            }),
        )?;
    }

    if let Some(trace_path) = trace_path.as_ref() {
        println!("Trace {}", trace_path.display());
    }

    if args.trace_only || is_state {
        return Ok(());
    }

    if let Some(frame_dir) = args.keep_frames.as_ref() {
        println!("Frames: {written_frames} at {fps} fps");
        println!("Wrote frames {}", frame_dir.display());
        return Ok(());
    }

    let video_out = args.out.as_ref().context(
        "debug-rgb live recording requires --out unless --keep-frames or --trace-only is set",
    )?;
    let frame_dir = temp_frame_dir
        .as_ref()
        .context("debug-rgb live recording did not create temporary frames")?;
    encode_frames_to_mp4(frame_dir, written_frames as u32, fps, video_out)?;
    let _ = std::fs::remove_dir_all(frame_dir);

    println!("Frames: {written_frames} at {fps} fps");
    println!("Wrote {}", video_out.display());
    Ok(())
}

async fn delegate_to_daemon(args: impl IntoIterator<Item = String>) -> Result<()> {
    let status = tokio::process::Command::new(daemon_exe()?)
        .args(args)
        .status()
        .await?;
    if !status.success() {
        bail!("robotdreams-daemon exited with {status}");
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Open(args) => {
            let response = open_project(&cli.socket, &args.path, &args.bind).await?;
            print_response(response)?;
        }
        Command::Status => {
            print_response(
                send_request_or_start(
                    &cli.socket,
                    &DaemonRequest::Status,
                    &cli.project,
                    &cli.daemon_bind,
                )
                .await?,
            )?;
        }
        Command::Close => {
            print_response(send_request(&cli.socket, &DaemonRequest::Close).await?)?;
        }
        Command::RenderFrame(args) => {
            render_frame(args)?;
        }
        Command::Bus(BusCommand::Start) => {
            print_response(
                send_request_or_start(
                    &cli.socket,
                    &DaemonRequest::BusStart,
                    &cli.project,
                    &cli.daemon_bind,
                )
                .await?,
            )?;
        }
        Command::Bus(BusCommand::Stop) => {
            print_response(
                send_request_or_start(
                    &cli.socket,
                    &DaemonRequest::BusStop,
                    &cli.project,
                    &cli.daemon_bind,
                )
                .await?,
            )?;
        }
        Command::Project(ProjectCommand::List(args)) => {
            print_project_data(
                send_request_or_start(
                    &cli.socket,
                    &DaemonRequest::ProjectList,
                    &cli.project,
                    &cli.daemon_bind,
                )
                .await?,
                args.json,
            )?;
        }
        Command::Project(ProjectCommand::State(args)) => {
            print_project_data(
                send_request_or_start(
                    &cli.socket,
                    &DaemonRequest::ProjectState {
                        project: args.project,
                        item: args.item,
                    },
                    &cli.project,
                    &cli.daemon_bind,
                )
                .await?,
                args.json,
            )?;
        }
        Command::Project(ProjectCommand::Command(args)) => {
            let payload = parse_payload(args.payload)?;
            print_project_data(
                send_request_or_start(
                    &cli.socket,
                    &DaemonRequest::ProjectCommand {
                        project: args.project,
                        simulation: args.simulation,
                        target: args.target,
                        action: args.action,
                        payload,
                    },
                    &cli.project,
                    &cli.daemon_bind,
                )
                .await?,
                args.json,
            )?;
        }
        Command::Scenario(command) => match command {
            ScenarioCommand::Load(args) => {
                print_project_data(
                    send_request_or_start(
                        &cli.socket,
                        &DaemonRequest::ScenarioCommand {
                            project: args.project,
                            simulation: args.simulation,
                            action: "load".to_string(),
                            path: Some(args.path.display().to_string()),
                            scenario: None,
                            payload: None,
                        },
                        &cli.project,
                        &cli.daemon_bind,
                    )
                    .await?,
                    args.json,
                )?;
            }
            ScenarioCommand::Reset(args) => {
                print_project_data(
                    send_request_or_start(
                        &cli.socket,
                        &DaemonRequest::ScenarioCommand {
                            project: args.project,
                            simulation: args.simulation,
                            action: "reset".to_string(),
                            path: None,
                            scenario: args.id,
                            payload: None,
                        },
                        &cli.project,
                        &cli.daemon_bind,
                    )
                    .await?,
                    args.json,
                )?;
            }
            ScenarioCommand::State(args) => {
                let payload = parse_payload(args.payload)?;
                print_project_data(
                    send_request_or_start(
                        &cli.socket,
                        &DaemonRequest::ScenarioCommand {
                            project: args.project,
                            simulation: args.simulation,
                            action: "state".to_string(),
                            path: None,
                            scenario: args.id,
                            payload,
                        },
                        &cli.project,
                        &cli.daemon_bind,
                    )
                    .await?,
                    args.json,
                )?;
            }
            ScenarioCommand::ExportSensors(args) => {
                let payload = parse_payload(Some(args.payload))?;
                print_project_data(
                    send_request_or_start(
                        &cli.socket,
                        &DaemonRequest::ScenarioCommand {
                            project: args.project,
                            simulation: args.simulation,
                            action: "export-sensors".to_string(),
                            path: None,
                            scenario: args.id,
                            payload,
                        },
                        &cli.project,
                        &cli.daemon_bind,
                    )
                    .await?,
                    args.json,
                )?;
            }
            ScenarioCommand::Smoke(args) => {
                print_project_data(
                    send_request_or_start(
                        &cli.socket,
                        &DaemonRequest::ScenarioCommand {
                            project: args.project,
                            simulation: args.simulation,
                            action: "smoke".to_string(),
                            path: Some(args.path.display().to_string()),
                            scenario: None,
                            payload: None,
                        },
                        &cli.project,
                        &cli.daemon_bind,
                    )
                    .await?,
                    args.json,
                )?;
            }
        },
        Command::Simulation(command) => match command {
            SimulationCommand::Start(args) => {
                print_project_data(
                    send_request_or_start(
                        &cli.socket,
                        &DaemonRequest::SimulationCommand {
                            project: args.project,
                            simulation: args.simulation,
                            action: "start".to_string(),
                        },
                        &cli.project,
                        &cli.daemon_bind,
                    )
                    .await?,
                    false,
                )?;
            }
            SimulationCommand::Pause(args) => {
                print_project_data(
                    send_request_or_start(
                        &cli.socket,
                        &DaemonRequest::SimulationCommand {
                            project: args.project,
                            simulation: args.simulation,
                            action: "pause".to_string(),
                        },
                        &cli.project,
                        &cli.daemon_bind,
                    )
                    .await?,
                    false,
                )?;
            }
            SimulationCommand::Stop(args) => {
                print_project_data(
                    send_request_or_start(
                        &cli.socket,
                        &DaemonRequest::SimulationCommand {
                            project: args.project,
                            simulation: args.simulation,
                            action: "stop".to_string(),
                        },
                        &cli.project,
                        &cli.daemon_bind,
                    )
                    .await?,
                    false,
                )?;
            }
            SimulationCommand::Reset(args) => {
                print_project_data(
                    send_request_or_start(
                        &cli.socket,
                        &DaemonRequest::SimulationCommand {
                            project: args.project,
                            simulation: args.simulation,
                            action: "reset".to_string(),
                        },
                        &cli.project,
                        &cli.daemon_bind,
                    )
                    .await?,
                    false,
                )?;
            }
            SimulationCommand::State(args) => {
                print_project_data(
                    send_request_or_start(
                        &cli.socket,
                        &DaemonRequest::SimulationCommand {
                            project: args.project,
                            simulation: args.simulation,
                            action: "state".to_string(),
                        },
                        &cli.project,
                        &cli.daemon_bind,
                    )
                    .await?,
                    args.json,
                )?;
            }
            SimulationCommand::RenderFrame(args) => {
                let view = observation_view(args.view);
                let response = send_request_or_start(
                    &cli.socket,
                    &DaemonRequest::RenderFrame {
                        project: args.project.clone(),
                        simulation: args.simulation.clone(),
                        camera: args.camera.clone(),
                        views: Some(vec![view]),
                        width: Some(args.width),
                        height: Some(args.height),
                        segmentation_policy: segmentation_policy(args.segmentation_min_alpha),
                        shutter_policy: shutter_policy(
                            args.shutter_exposure_sec,
                            args.shutter_samples,
                            args.shutter_mode,
                            args.shutter_readout_sec,
                        ),
                        render_settings: render_settings(RenderSettingArgs {
                            background_rgb: args.background_rgb.clone(),
                            ambient_rgb: args.ambient_rgb.clone(),
                            ambient_intensity: args.ambient_intensity,
                            environment_sky_top_rgb: args.environment_sky_top_rgb.clone(),
                            environment_sky_horizon_rgb: args.environment_sky_horizon_rgb.clone(),
                            environment_ground_rgb: args.environment_ground_rgb.clone(),
                            environment_map: args.environment_map.clone(),
                            environment_map_rotation_deg: args.environment_map_rotation_deg,
                            environment_intensity: args.environment_intensity,
                            environment_ambient_intensity: args.environment_ambient_intensity,
                            reflection_probe: args.reflection_probe.clone(),
                            reflection_probe_rotation_deg: args.reflection_probe_rotation_deg,
                            reflection_probe_intensity: args.reflection_probe_intensity,
                            reflection_probe_ambient_intensity: args
                                .reflection_probe_ambient_intensity,
                            reflection_probe_position: args.reflection_probe_position.clone(),
                            reflection_probe_box_size_m: args.reflection_probe_box_size_m.clone(),
                            reflection_probe_influence_radius_m: args
                                .reflection_probe_influence_radius_m,
                            reflection_probe_falloff_power: args.reflection_probe_falloff_power,
                            white_balance_rgb: args.white_balance_rgb.clone(),
                            color_temperature_kelvin: args.color_temperature_kelvin,
                            tone_mapping: args.tone_mapping,
                            tone_exposure: args.tone_exposure,
                            debug_rgb_samples_per_pixel: args.debug_rgb_samples_per_pixel,
                            ambient_occlusion_samples: args.ambient_occlusion_samples,
                            ambient_occlusion_radius_m: args.ambient_occlusion_radius_m,
                            ambient_occlusion_intensity: args.ambient_occlusion_intensity,
                            indirect_diffuse_samples: args.indirect_diffuse_samples,
                            indirect_diffuse_radius_m: args.indirect_diffuse_radius_m,
                            indirect_diffuse_intensity: args.indirect_diffuse_intensity,
                            indirect_diffuse_bounces: args.indirect_diffuse_bounces,
                            soft_shadow_samples: args.soft_shadow_samples,
                            soft_shadow_radius_m: args.soft_shadow_radius_m,
                            area_light_samples: args.area_light_samples,
                            rough_transmission_samples: args.rough_transmission_samples,
                            rough_reflection_samples: args.rough_reflection_samples,
                            specular_reflection_bounces: args.specular_reflection_bounces,
                            gltf_material_variant: args.gltf_material_variant.clone(),
                        })?,
                    },
                    &cli.project,
                    &cli.daemon_bind,
                )
                .await?;
                write_daemon_render_frame(response, args)?;
            }
            SimulationCommand::Record(args) => {
                if args.live {
                    record_live_simulation(&cli.socket, &cli.project, &cli.daemon_bind, args)
                        .await?;
                } else {
                    record_simulation(&cli.project, args)?;
                }
            }
        },
        Command::Recording(RecordingCommand::Inspect(args)) => {
            inspect_recording(args)?;
        }
        Command::Recording(RecordingCommand::Assert(args)) => {
            assert_recording_command(args)?;
        }
        Command::Daemon(DaemonCommand::Start(args)) => {
            start_daemon(&cli.socket, None, &args.bind).await?;
            print_response(send_request(&cli.socket, &DaemonRequest::Status).await?)?;
        }
        Command::Daemon(DaemonCommand::Stop) => {
            print_response(send_request(&cli.socket, &DaemonRequest::Shutdown).await?)?;
        }
        Command::Headless(args) => {
            let mut raw = vec![
                "--socket".to_string(),
                cli.socket.display().to_string(),
                "headless".to_string(),
                "--seconds".to_string(),
                args.seconds.to_string(),
                "--hz".to_string(),
                args.hz.to_string(),
            ];
            if args.start_virtual_bus {
                raw.push("--start-virtual-bus".to_string());
            }
            if args.json {
                raw.push("--json".to_string());
            }
            if let Some(path) = args.path {
                raw.push(path.display().to_string());
            }
            delegate_to_daemon(raw).await?;
        }
        Command::Vbus(args) => {
            delegate_to_daemon(vec![
                "--socket".to_string(),
                cli.socket.display().to_string(),
                "vbus".to_string(),
                "--first-servo-id".to_string(),
                args.first_servo_id.to_string(),
                "--last-servo-id".to_string(),
                args.last_servo_id.to_string(),
                "--step-seconds".to_string(),
                args.step_seconds.to_string(),
                "--idle-sleep-ms".to_string(),
                args.idle_sleep_ms.to_string(),
            ])
            .await?;
        }
    }

    Ok(())
}
