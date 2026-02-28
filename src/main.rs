#![allow(dead_code)]
#![allow(non_snake_case)]

mod physics;
mod robot_dreams;
mod urdf;

use clap::{Args as ClapArgs, Parser, Subcommand};
use std::collections::HashSet;
#[cfg(unix)]
use std::ffi::CString;
use std::io::{Read, Write};
#[cfg(unix)]
use std::io;
#[cfg(unix)]
use std::os::unix::io::RawFd;
use std::net::SocketAddr;
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
use wgui::*;

const SERVO_COUNT_SLIDER_ID: u32 = 1;
const APPLY_SERVO_COUNT_BUTTON_ID: u32 = 2;
const TARGET_POSITION_SLIDER_ID: u32 = 3;
const TOGGLE_AUTO_MOTION_BUTTON_ID: u32 = 4;
const SERIAL_PORT_INPUT_ID: u32 = 5;
const SERIAL_BAUD_INPUT_ID: u32 = 6;
const TOGGLE_SERIAL_BRIDGE_BUTTON_ID: u32 = 7;
const TOGGLE_VIRTUAL_BUS_BUTTON_ID: u32 = 8;

const DEFAULT_SERVO_COUNT: i32 = 6;
const DEFAULT_SERIAL_BAUD: &str = "1000000";
const SERIAL_PORT_PLACEHOLDER: &str = "COM6 or /dev/ttyUSB0";
const BUS_SERVICE_INTERVAL_MS: u64 = 5;
const UI_RENDER_INTERVAL_MS: u64 = 50;

#[derive(Debug, Parser)]
#[command(
    name = "robot_dreams",
    about = "Robot Dreams virtual servo bus UI and bridge"
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
    frame.iter()
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
            if err.kind() == io::ErrorKind::WouldBlock || err.kind() == io::ErrorKind::Interrupted
            {
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if let Some(Command::Vbus(vbus_args)) = args.command {
        let config = VirtualServoSimConfig {
            first_servo_id: vbus_args.first_servo_id,
            last_servo_id: vbus_args.last_servo_id,
            step_seconds: vbus_args.step_seconds,
            idle_sleep_ms: vbus_args.idle_sleep_ms,
        };
        run_virtual_servo_sim(config)?;
        return Ok(());
    }

    let bind_addr: SocketAddr = args.bind.parse().map_err(|err| {
        format!(
            "invalid --bind value '{}': {} (expected host:port)",
            args.bind, err
        )
    })?;

    simple_logger::SimpleLogger::new()
        .with_level(LevelFilter::Info)
        .without_timestamps()
        .init()
        .unwrap();

    let mut sim = FeetechBusSim::new();
    sim.set_servo_count(DEFAULT_SERVO_COUNT as u8);

    let initial_port = args
        .port
        .unwrap_or_else(|| default_serial_port().to_string());

    let mut state = AppState {
        desired_servo_count: DEFAULT_SERVO_COUNT,
        snapshots: sim.servo_snapshots(),
        auto_motion: false,
        serial_port: initial_port,
        follow_virtual_bus_port: false,
        serial_baud: args.baud.to_string(),
        serial_status: "Disconnected".to_string(),
        virtual_bus_status: "Stopped".to_string(),
        virtual_bus_running: false,
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
                        if s.id == SERVO_COUNT_SLIDER_ID {
                            state.desired_servo_count = s.value.clamp(1, 32);
                        }
                        if s.id == TARGET_POSITION_SLIDER_ID {
                            if let Some(inx) = s.inx {
                                let _ = sim.set_target_position(inx as u8, s.value.clamp(0, 4095) as i16);
                                state.snapshots = sim.servo_snapshots();
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
