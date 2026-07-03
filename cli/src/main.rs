use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use clap::{Args, Parser, Subcommand};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio_tungstenite::tungstenite::Message;

const DEFAULT_SOCKET: &str = "/tmp/robotdreams-daemon.sock";
const DEFAULT_SIMULATION_ID: &str = "default";

#[derive(Debug, Parser)]
#[command(name = "robotdreams", about = "Robot Dreams CLI")]
struct Cli {
    #[arg(long, default_value = DEFAULT_SOCKET)]
    socket: PathBuf,

    #[arg(
        long,
        default_value = "project.json",
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

    #[arg(long, default_value = "127.0.0.1:8345")]
    bind: String,

    #[arg(long, default_value_t = 1800)]
    width: u32,

    #[arg(long, default_value_t = 1000)]
    height: u32,

    #[arg(long, default_value_t = 3500)]
    wait_ms: u64,

    #[arg(long)]
    chrome: Option<PathBuf>,
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
struct SimulationRecordArgs {
    #[arg(long)]
    project: Option<String>,

    #[arg(long)]
    simulation: Option<String>,

    #[arg(long, short)]
    out: Option<PathBuf>,

    #[arg(long)]
    trace_only: bool,

    #[arg(long, default_value_t = 10.0)]
    seconds: f32,

    #[arg(long, default_value_t = 12)]
    fps: u32,

    #[arg(long, default_value_t = 1280)]
    width: u32,

    #[arg(long, default_value_t = 720)]
    height: u32,

    #[arg(long, default_value_t = 3500)]
    wait_ms: u64,

    #[arg(long)]
    chrome: Option<PathBuf>,

    #[arg(long)]
    keep_frames: Option<PathBuf>,

    #[arg(
        long,
        help = "Write machine-readable simulation samples as JSONL. Defaults beside --out."
    )]
    trace_out: Option<PathBuf>,

    #[arg(long, default_value_t = 20.0)]
    trace_hz: f32,

    #[arg(
        long,
        help = "Write this file when the 3D scene is loaded and video frame capture is about to start."
    )]
    ready_file: Option<PathBuf>,
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
    use super::*;

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

fn find_chrome(explicit: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(path) = explicit {
        return Ok(path);
    }

    if let Ok(path) = std::env::var("ROBOTDREAMS_CHROME")
        && !path.trim().is_empty()
    {
        return Ok(PathBuf::from(path));
    }

    for candidate in ["google-chrome", "chromium", "chromium-browser"] {
        let status = ProcessCommand::new(candidate)
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        if status.map(|status| status.success()).unwrap_or(false) {
            return Ok(PathBuf::from(candidate));
        }
    }

    bail!("could not find Chrome; pass --chrome or set ROBOTDREAMS_CHROME")
}

fn unused_local_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").context("bind temporary local port")?;
    Ok(listener.local_addr()?.port())
}

fn devtools_get_json(port: u16, path: &str) -> Result<serde_json::Value> {
    let mut stream = TcpStream::connect(SocketAddr::from(([127, 0, 0, 1], port)))
        .with_context(|| format!("connect Chrome DevTools on port {port}"))?;
    stream.set_read_timeout(Some(std::time::Duration::from_millis(500)))?;
    stream.set_write_timeout(Some(std::time::Duration::from_millis(500)))?;
    let request =
        format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes())?;
    let mut raw = Vec::new();
    let mut buffer = [0_u8; 8192];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => raw.extend_from_slice(&buffer[..count]),
            Err(err)
                if err.kind() == ErrorKind::WouldBlock || err.kind() == ErrorKind::TimedOut =>
            {
                if raw.is_empty() {
                    return Err(err.into());
                }
                break;
            }
            Err(err) => return Err(err.into()),
        }
    }
    let response = String::from_utf8(raw).context("decode DevTools HTTP response")?;
    let Some((_, body)) = response.split_once("\r\n\r\n") else {
        bail!("invalid DevTools HTTP response");
    };
    Ok(serde_json::from_str(body)?)
}

async fn wait_for_devtools_page(port: u16, url: &str, wait_ms: u64) -> Result<String> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(wait_ms.max(1));
    loop {
        if let Ok(serde_json::Value::Array(targets)) = devtools_get_json(port, "/json/list") {
            for target in targets {
                let target_url = target.get("url").and_then(|value| value.as_str());
                let ws_url = target
                    .get("webSocketDebuggerUrl")
                    .and_then(|value| value.as_str());
                if target_url == Some(url)
                    && let Some(ws_url) = ws_url
                {
                    return Ok(ws_url.to_string());
                }
            }
        }

        if std::time::Instant::now() >= deadline {
            bail!("timed out waiting for Chrome DevTools page");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

struct CdpClient {
    socket: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    next_id: u64,
}

impl CdpClient {
    async fn connect(url: &str) -> Result<Self> {
        let (socket, _) = tokio_tungstenite::connect_async(url)
            .await
            .context("connect Chrome DevTools websocket")?;
        Ok(Self { socket, next_id: 0 })
    }

    async fn call(&mut self, method: &str, params: serde_json::Value) -> Result<serde_json::Value> {
        self.next_id += 1;
        let id = self.next_id;
        let request = serde_json::json!({
            "id": id,
            "method": method,
            "params": params,
        });
        self.socket
            .send(Message::Text(request.to_string()))
            .await
            .with_context(|| format!("send CDP {method}"))?;

        while let Some(message) = self.socket.next().await {
            let message = message?;
            let Message::Text(raw) = message else {
                continue;
            };
            let response: serde_json::Value = serde_json::from_str(&raw)?;
            if response.get("id").and_then(|value| value.as_u64()) != Some(id) {
                continue;
            }
            if let Some(error) = response.get("error") {
                bail!("CDP {method} failed: {error}");
            }
            return Ok(response
                .get("result")
                .cloned()
                .unwrap_or(serde_json::Value::Null));
        }

        bail!("Chrome DevTools websocket closed while waiting for {method}")
    }

    async fn evaluate(&mut self, expression: &str) -> Result<serde_json::Value> {
        let result = self
            .call(
                "Runtime.evaluate",
                serde_json::json!({
                    "expression": expression,
                    "returnByValue": true,
                    "awaitPromise": true,
                }),
            )
            .await?;
        Ok(result
            .get("result")
            .and_then(|result| result.get("value"))
            .cloned()
            .unwrap_or(serde_json::Value::Null))
    }
}

async fn wait_for_rendered_scene(client: &mut CdpClient, wait_ms: u64) -> Result<()> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(wait_ms.max(1));
    loop {
        let state = client
            .evaluate(
                r#"(() => {
                    const body = document.body?.innerText ?? "";
                    const canvas = document.querySelector("canvas");
                    const rect = canvas?.getBoundingClientRect();
                    return {
                        failed: body.includes("Failed to load component"),
                        canvasCount: document.querySelectorAll("canvas").length,
                        canvasWidth: rect?.width ?? 0,
                        canvasHeight: rect?.height ?? 0
                    };
                })()"#,
            )
            .await?;

        if state
            .get("failed")
            .and_then(|value| value.as_bool())
            .unwrap_or(false)
        {
            bail!("robot scene component failed to load");
        }

        let canvas_count = state
            .get("canvasCount")
            .and_then(|value| value.as_u64())
            .unwrap_or(0);
        let canvas_width = state
            .get("canvasWidth")
            .and_then(|value| value.as_f64())
            .unwrap_or(0.0);
        let canvas_height = state
            .get("canvasHeight")
            .and_then(|value| value.as_f64())
            .unwrap_or(0.0);
        if canvas_count > 0 && canvas_width > 0.0 && canvas_height > 0.0 {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            return Ok(());
        }

        if std::time::Instant::now() >= deadline {
            bail!("timed out waiting for rendered scene canvas");
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
}

async fn capture_screenshot_png(client: &mut CdpClient) -> Result<Vec<u8>> {
    let screenshot = client
        .call(
            "Page.captureScreenshot",
            serde_json::json!({
                "format": "png",
                "fromSurface": true
            }),
        )
        .await?;
    let Some(data) = screenshot.get("data").and_then(|value| value.as_str()) else {
        bail!("Chrome did not return screenshot data");
    };
    base64::engine::general_purpose::STANDARD
        .decode(data)
        .context("decode screenshot")
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

fn unix_time_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

fn write_record_ready_file(
    path: &Path,
    record_url: &str,
    video_out: Option<&Path>,
    simulation_start: &DaemonResponse,
) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create ready-file directory {}", parent.display()))?;
    }
    let value = serde_json::json!({
        "type": "recordingReady",
        "readyUnixMs": unix_time_millis(),
        "recordUrl": record_url,
        "videoOut": video_out.map(|path| path.display().to_string()),
        "projectId": simulation_start.project_id.clone(),
        "virtualBusRunning": simulation_start.virtual_bus_running,
        "virtualBusPath": simulation_start.virtual_bus_path.clone(),
        "simulation": simulation_start.data.clone(),
    });
    std::fs::write(path, format!("{}\n", serde_json::to_string(&value)?))
        .with_context(|| format!("write ready file {}", path.display()))
}

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

fn spawn_record_trace_sampler(
    socket: PathBuf,
    project: Option<String>,
    simulation: Option<String>,
    trace_out: PathBuf,
    video_out: Option<PathBuf>,
    record_url: String,
    seconds: f32,
    fps: u32,
    width: u32,
    height: u32,
    trace_hz: f32,
    stop: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<Result<PathBuf>> {
    tokio::spawn(async move {
        if let Some(parent) = trace_out.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create trace directory {}", parent.display()))?;
        }
        let mut file = std::fs::File::create(&trace_out)
            .with_context(|| format!("create trace {}", trace_out.display()))?;
        write_trace_line(
            &mut file,
            serde_json::json!({
                "type": "recordingStart",
                "schema": "robotdreams.recording.trace.v1",
                "startedUnixMs": unix_time_millis(),
                "project": project,
                "simulation": simulation,
                "recordUrl": record_url,
                "videoOut": video_out.as_ref().map(|path| path.display().to_string()),
                "seconds": seconds,
                "fps": fps,
                "width": width,
                "height": height,
                "traceHz": trace_hz.max(0.1),
            }),
        )?;

        let interval = std::time::Duration::from_secs_f64(1.0 / trace_hz.max(0.1) as f64);
        let started = std::time::Instant::now();
        let mut index: u64 = 0;
        let mut last_bus_event_sequence: u64 = 0;
        while !stop.load(Ordering::Relaxed) {
            let scenario_data = send_request(
                &socket,
                &DaemonRequest::ProjectCommand {
                    project: project.clone(),
                    simulation: simulation.clone(),
                    target: "scenario".to_string(),
                    action: "state".to_string(),
                    payload: None,
                },
            )
            .await
            .ok()
            .filter(|response| response.ok)
            .and_then(|response| response.data);
            let response = send_request(
                &socket,
                &DaemonRequest::SimulationCommand {
                    project: project.clone(),
                    simulation: simulation.clone(),
                    action: "state".to_string(),
                },
            )
            .await;
            let elapsed_sec = started.elapsed().as_secs_f64();
            match response {
                Ok(mut response) => {
                    response.data = response
                        .data
                        .map(|data| filter_new_bus_events(data, &mut last_bus_event_sequence))
                        .map(|data| attach_scenario_data(data, scenario_data.clone()));
                    write_trace_line(
                        &mut file,
                        serde_json::json!({
                            "type": "sample",
                            "index": index,
                            "elapsedSec": elapsed_sec,
                            "ok": response.ok,
                            "projectId": response.project_id,
                            "url": response.url,
                            "virtualBusRunning": response.virtual_bus_running,
                            "virtualBusPath": response.virtual_bus_path,
                            "data": response.data,
                        }),
                    )?;
                }
                Err(err) => {
                    write_trace_line(
                        &mut file,
                        serde_json::json!({
                            "type": "sampleError",
                            "index": index,
                            "elapsedSec": elapsed_sec,
                            "error": err.to_string(),
                        }),
                    )?;
                }
            }
            index += 1;
            tokio::time::sleep(interval).await;
        }

        write_trace_line(
            &mut file,
            serde_json::json!({
                "type": "recordingEnd",
                "endedUnixMs": unix_time_millis(),
                "elapsedSec": started.elapsed().as_secs_f64(),
                "samples": index,
            }),
        )?;
        Ok(trace_out)
    })
}

async fn render_frame(socket: &Path, args: RenderFrameArgs) -> Result<()> {
    let daemon_was_running = send_request(socket, &DaemonRequest::Status).await.is_ok();
    let response = open_project(socket, &args.path, &args.bind).await?;
    if !response.ok {
        bail!(
            "{}",
            response
                .message
                .unwrap_or_else(|| "daemon command failed".to_string())
        );
    }

    let Some(url) = response.url else {
        bail!("daemon did not return a project URL");
    };

    let chrome = find_chrome(args.chrome)?;
    if let Some(parent) = args.out.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create output directory {}", parent.display()))?;
    }

    let profile_dir =
        std::env::temp_dir().join(format!("robotdreams-render-frame-{}", std::process::id()));
    let window_size = format!("{},{}", args.width.max(1), args.height.max(1));
    let devtools_port = unused_local_port()?;

    let mut child = tokio::process::Command::new(chrome)
        .arg("--headless")
        .arg("--no-sandbox")
        .arg("--disable-gpu=false")
        .arg("--use-gl=swiftshader")
        .arg("--enable-unsafe-swiftshader")
        .arg(format!("--user-data-dir={}", profile_dir.display()))
        .arg(format!("--remote-debugging-port={devtools_port}"))
        .arg(format!("--window-size={window_size}"))
        .arg(&url)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("start headless Chrome")?;

    let capture_result = tokio::time::timeout(
        std::time::Duration::from_millis(args.wait_ms + 10_000),
        async {
            let ws_url = wait_for_devtools_page(devtools_port, &url, args.wait_ms).await?;
            let mut client = CdpClient::connect(&ws_url).await?;
            client.call("Page.enable", serde_json::json!({})).await?;
            client.call("Runtime.enable", serde_json::json!({})).await?;
            client
                .call("Page.bringToFront", serde_json::json!({}))
                .await?;
            wait_for_rendered_scene(&mut client, args.wait_ms).await?;
            let bytes = capture_screenshot_png(&mut client).await?;
            std::fs::write(&args.out, bytes)
                .with_context(|| format!("write screenshot {}", args.out.display()))?;
            Result::<()>::Ok(())
        },
    )
    .await
    .context("timed out capturing frame")?;

    let _ = child.kill().await;
    if !daemon_was_running {
        let _ = send_request(socket, &DaemonRequest::Shutdown).await;
    }
    capture_result?;

    println!("Rendered {url}");
    println!("Wrote {}", args.out.display());
    Ok(())
}

async fn record_simulation(
    socket: &Path,
    project_path: &Path,
    bind: &str,
    args: SimulationRecordArgs,
) -> Result<()> {
    let daemon_was_running = send_request(socket, &DaemonRequest::Status).await.is_ok();
    let open_response = open_project(socket, project_path, bind).await?;
    if !open_response.ok {
        bail!(
            "{}",
            open_response
                .message
                .unwrap_or_else(|| "project open failed".to_string())
        );
    }
    let state_response = send_request(
        socket,
        &DaemonRequest::ProjectState {
            project: args.project.clone(),
            item: Some("simulation".to_string()),
        },
    )
    .await?;
    if !state_response.ok {
        bail!(
            "{}",
            state_response
                .message
                .unwrap_or_else(|| "project state failed".to_string())
        );
    }
    let simulation_id = args
        .simulation
        .as_deref()
        .unwrap_or(DEFAULT_SIMULATION_ID)
        .trim();
    let simulation_id = if simulation_id.is_empty() {
        DEFAULT_SIMULATION_ID
    } else {
        simulation_id
    };
    let record_url = state_response
        .data
        .as_ref()
        .and_then(|data| data.get("viewportUrl"))
        .and_then(|value| value.as_str())
        .map(|url| url.to_string())
        .or_else(|| {
            state_response.url.as_ref().map(|url| {
                let project_base_url = url.trim_end_matches('/').trim_end_matches("/workbench");
                format!("{project_base_url}/simulation/{simulation_id}/viewport")
            })
        })
        .ok_or_else(|| anyhow!("daemon did not return a simulation viewport URL"))?;
    let stop_response = send_request(
        socket,
        &DaemonRequest::SimulationCommand {
            project: args.project.clone(),
            simulation: args.simulation.clone(),
            action: "stop".to_string(),
        },
    )
    .await?;
    if !stop_response.ok {
        bail!(
            "{}",
            stop_response
                .message
                .unwrap_or_else(|| "simulation stop failed".to_string())
        );
    }

    if args.trace_only {
        let trace_path = args
            .trace_out
            .clone()
            .or_else(|| args.out.as_deref().map(default_record_trace_path))
            .context("--trace-only requires --trace-out when --out is not provided")?;
        let trace_stop = Arc::new(AtomicBool::new(false));
        let trace_handle = spawn_record_trace_sampler(
            socket.to_path_buf(),
            args.project.clone(),
            args.simulation.clone(),
            trace_path,
            args.out.clone(),
            record_url.clone(),
            args.seconds.max(0.1),
            args.fps.max(1),
            args.width.max(1),
            args.height.max(1),
            args.trace_hz,
            trace_stop.clone(),
        );
        let start_response = send_request(
            socket,
            &DaemonRequest::SimulationCommand {
                project: args.project.clone(),
                simulation: args.simulation.clone(),
                action: "start".to_string(),
            },
        )
        .await?;
        if !start_response.ok {
            trace_stop.store(true, Ordering::Relaxed);
            let _ = trace_handle.await;
            bail!(
                "{}",
                start_response
                    .message
                    .unwrap_or_else(|| "simulation start failed".to_string())
            );
        }
        if let Some(ready_file) = args.ready_file.as_ref() {
            write_record_ready_file(
                ready_file,
                &record_url,
                args.out.as_deref(),
                &start_response,
            )?;
        }
        tokio::time::sleep(std::time::Duration::from_secs_f32(args.seconds.max(0.1))).await;
        trace_stop.store(true, Ordering::Relaxed);
        let trace_out = trace_handle.await.context("join trace sampler")??;
        if !daemon_was_running {
            let _ = send_request(socket, &DaemonRequest::Shutdown).await;
        }
        println!("Recorded trace {record_url}");
        println!("Trace {}", trace_out.display());
        return Ok(());
    }

    let video_out = args
        .out
        .clone()
        .context("simulation record requires --out unless --trace-only is set")?;
    let chrome = find_chrome(args.chrome)?;
    let fps = args.fps.max(1);
    let seconds = args.seconds.max(0.1);
    let trace_path = args
        .trace_out
        .clone()
        .unwrap_or_else(|| default_record_trace_path(&video_out));
    let frame_count = ((seconds * fps as f32).ceil() as u32).max(1);
    let frame_interval = std::time::Duration::from_secs_f64(1.0 / fps as f64);
    let frame_dir = args
        .keep_frames
        .clone()
        .unwrap_or_else(|| unique_temp_dir("robotdreams-record-frames"));
    std::fs::create_dir_all(&frame_dir)
        .with_context(|| format!("create frame directory {}", frame_dir.display()))?;

    let profile_dir = unique_temp_dir("robotdreams-record-chrome");
    let window_size = format!("{},{}", args.width.max(1), args.height.max(1));
    let devtools_port = unused_local_port()?;

    let mut child = tokio::process::Command::new(chrome)
        .arg("--headless")
        .arg("--no-sandbox")
        .arg("--disable-gpu=false")
        .arg("--use-gl=swiftshader")
        .arg("--enable-unsafe-swiftshader")
        .arg(format!("--user-data-dir={}", profile_dir.display()))
        .arg(format!("--remote-debugging-port={devtools_port}"))
        .arg(format!("--window-size={window_size}"))
        .arg(&record_url)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .context("start headless Chrome")?;
    let trace_stop = Arc::new(AtomicBool::new(false));
    let trace_handle = spawn_record_trace_sampler(
        socket.to_path_buf(),
        args.project.clone(),
        args.simulation.clone(),
        trace_path,
        Some(video_out.clone()),
        record_url.clone(),
        seconds,
        fps,
        args.width.max(1),
        args.height.max(1),
        args.trace_hz,
        trace_stop.clone(),
    );

    let capture_timeout = std::time::Duration::from_millis(args.wait_ms)
        + std::time::Duration::from_secs_f32(seconds)
        + std::time::Duration::from_secs(30);
    let capture_result: Result<()> = tokio::time::timeout(capture_timeout, async {
        let ws_url = wait_for_devtools_page(devtools_port, &record_url, args.wait_ms).await?;
        let mut client = CdpClient::connect(&ws_url).await?;
        client.call("Page.enable", serde_json::json!({})).await?;
        client.call("Runtime.enable", serde_json::json!({})).await?;
        client
            .call("Page.bringToFront", serde_json::json!({}))
            .await?;
        wait_for_rendered_scene(&mut client, args.wait_ms).await?;
        let start_response = send_request(
            socket,
            &DaemonRequest::SimulationCommand {
                project: args.project.clone(),
                simulation: args.simulation.clone(),
                action: "start".to_string(),
            },
        )
        .await?;
        if !start_response.ok {
            bail!(
                "{}",
                start_response
                    .message
                    .unwrap_or_else(|| "simulation start failed".to_string())
            );
        }
        if let Some(ready_file) = args.ready_file.as_ref() {
            write_record_ready_file(ready_file, &record_url, Some(&video_out), &start_response)?;
        }

        let started = std::time::Instant::now();
        for index in 0..frame_count {
            let frame_path = frame_dir.join(format!("frame-{index:05}.png"));
            let bytes = capture_screenshot_png(&mut client).await?;
            std::fs::write(&frame_path, bytes)
                .with_context(|| format!("write frame {}", frame_path.display()))?;
            let next_frame_at = started + frame_interval * (index + 1);
            let now = std::time::Instant::now();
            if next_frame_at > now {
                tokio::time::sleep(next_frame_at - now).await;
            }
        }
        Result::<()>::Ok(())
    })
    .await
    .context("timed out recording simulation frames")
    .and_then(|inner| inner);

    let _ = child.kill().await;
    let _ = std::fs::remove_dir_all(&profile_dir);
    trace_stop.store(true, Ordering::Relaxed);
    let trace_out = trace_handle.await.context("join trace sampler")??;
    capture_result?;

    encode_frames_to_mp4(&frame_dir, frame_count, fps, &video_out)?;

    if args.keep_frames.is_none() {
        let _ = std::fs::remove_dir_all(&frame_dir);
    }
    if !daemon_was_running {
        let _ = send_request(socket, &DaemonRequest::Shutdown).await;
    }

    println!("Recorded {record_url}");
    println!("Frames: {frame_count} at {fps} fps");
    println!("Wrote {}", video_out.display());
    println!("Trace {}", trace_out.display());
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
            render_frame(&cli.socket, args).await?;
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
            SimulationCommand::Record(args) => {
                record_simulation(&cli.socket, &cli.project, &cli.daemon_bind, args).await?;
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
