use anyhow::{Context, Result, bail};
use base64::Engine;
use clap::{Args, Parser, Subcommand};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio_tungstenite::tungstenite::Message;

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
    RenderFrame(RenderFrameArgs),
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
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(data)
                .context("decode screenshot")?;
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
        Command::RenderFrame(args) => {
            render_frame(&cli.socket, args).await?;
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
