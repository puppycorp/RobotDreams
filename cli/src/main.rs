use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use serde::{Deserialize, Serialize};
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

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Open(OpenArgs),
    Status,
    Close,
    Headless(HeadlessArgs),
    Vbus(VbusArgs),
    #[command(subcommand)]
    Project(ProjectCommand),
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
    Start(OpenArgs),
    Stop,
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

#[derive(Debug, Args)]
struct ProjectOutputArgs {
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct ProjectStateArgs {
    item: Option<String>,

    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct ProjectCommandArgs {
    target: String,
    action: String,

    #[arg(long)]
    payload: Option<String>,

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

async fn start_daemon(socket: &Path, path: &Path, bind: &str) -> Result<()> {
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
        .arg(path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
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
        Ok(response) => Ok(response),
        Err(_) => {
            start_daemon(socket, path, bind).await?;
            send_request(socket, &request).await
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
            print_response(send_request(&cli.socket, &DaemonRequest::Status).await?)?;
        }
        Command::Close => {
            print_response(send_request(&cli.socket, &DaemonRequest::Close).await?)?;
        }
        Command::Bus(BusCommand::Start) => {
            print_response(send_request(&cli.socket, &DaemonRequest::BusStart).await?)?;
        }
        Command::Bus(BusCommand::Stop) => {
            print_response(send_request(&cli.socket, &DaemonRequest::BusStop).await?)?;
        }
        Command::Project(ProjectCommand::List(args)) => {
            print_project_data(
                send_request(&cli.socket, &DaemonRequest::ProjectList).await?,
                args.json,
            )?;
        }
        Command::Project(ProjectCommand::State(args)) => {
            print_project_data(
                send_request(
                    &cli.socket,
                    &DaemonRequest::ProjectState { item: args.item },
                )
                .await?,
                args.json,
            )?;
        }
        Command::Project(ProjectCommand::Command(args)) => {
            let payload = parse_payload(args.payload)?;
            print_project_data(
                send_request(
                    &cli.socket,
                    &DaemonRequest::ProjectCommand {
                        target: args.target,
                        action: args.action,
                        payload,
                    },
                )
                .await?,
                args.json,
            )?;
        }
        Command::Daemon(DaemonCommand::Start(args)) => {
            start_daemon(&cli.socket, &args.path, &args.bind).await?;
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
