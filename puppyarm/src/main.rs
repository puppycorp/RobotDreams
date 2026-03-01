use std::collections::{BTreeMap, HashSet};
#[cfg(unix)]
use std::ffi::CString;
#[cfg(unix)]
use std::io;
use std::net::{IpAddr, SocketAddr};
#[cfg(unix)]
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use clap::{Args as ClapArgs, Parser, Subcommand};
use log::LevelFilter;
use robot_utils::ik::{ArmGeom, JointCalib, Vec3, angle_to_ticks, ik_elbow_up};
use robot_utils::servo::protocol::port_handler::PortHandler;
use robot_utils::servo::protocol::serial_port::SerialPortHandler;
use robot_utils::servo::protocol::stservo_def::COMM_SUCCESS;
use robot_utils::servo::protocol::virtual_uart::VirtualUartPort;
use robot_utils::servo::scscl::{SCSCL_GOAL_POSITION_L, Scscl};
use robot_utils::servo::sim::FeetechBusSim;
use serialport::{SerialPortInfo, SerialPortType};
use wgui::*;

const TARGET_POSITION_SLIDER_ID: u32 = 1;
const SCAN_BUTTON_ID: u32 = 2;
const REFRESH_BUTTON_ID: u32 = 3;
const SEND_ALL_BUTTON_ID: u32 = 4;
const MOVE_TIME_INPUT_ID: u32 = 7;
const MOVE_SPEED_INPUT_ID: u32 = 8;
const IK_X_INPUT_ID: u32 = 9;
const IK_Y_INPUT_ID: u32 = 10;
const IK_Z_INPUT_ID: u32 = 11;
const IK_L1_INPUT_ID: u32 = 12;
const IK_L2_INPUT_ID: u32 = 13;
const IK_SHOULDER_R_INPUT_ID: u32 = 14;
const IK_SHOULDER_Z_INPUT_ID: u32 = 15;
const IK_WRIST_PITCH_INPUT_ID: u32 = 16;
const APPLY_IK_BUTTON_ID: u32 = 17;
const TOGGLE_ELBOW_BRANCH_BUTTON_ID: u32 = 18;
const PORT_INPUT_ID: u32 = 19;
const TOGGLE_CONNECTION_BUTTON_ID: u32 = 20;
const TOGGLE_VBUS_BUTTON_ID: u32 = 23;
const SERVO_ANGLE_INPUT_ID: u32 = 24;
const APPLY_SERVO_ANGLE_BUTTON_ID: u32 = 25;
const TOGGLE_STATUS_POLLING_BUTTON_ID: u32 = 26;

const PUPPYARM_FIRST_SERVO_ID: u8 = 1;
const PUPPYARM_SERVO_COUNT: u8 = 4;
const PUPPYARM_LAST_SERVO_ID: u8 = PUPPYARM_FIRST_SERVO_ID + PUPPYARM_SERVO_COUNT - 1;

#[derive(Debug, Parser)]
#[command(name = "puppyarm", about = "PuppyArm servo bus client")]
struct Args {
    #[arg(
        long,
        value_name = "PORT",
        help = "Serial port path (e.g. COM6 or /dev/ttyUSB0). If omitted, auto-selects a detected port."
    )]
    port: Option<String>,

    #[arg(long, default_value_t = 1_000_000)]
    baud: u32,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Scan PuppyArm servos (fixed IDs 1..4) and print detected servos.
    Scan,

    /// Read position/speed from one servo.
    Read {
        #[arg(long)]
        id: u8,
    },

    /// Send a position command, then read back current state.
    Move {
        #[arg(long)]
        id: u8,

        #[arg(long, value_name = "0..4095")]
        position: u16,

        #[arg(long, default_value_t = 0)]
        time: u16,

        #[arg(long, default_value_t = 400)]
        speed: u16,
    },

    /// Launch WGUI control panel for all discovered servos.
    Ui(UiArgs),

    /// Run a headless virtual servo bus and print its PTY path.
    Vbus(VbusArgs),
}

#[derive(Debug, ClapArgs)]
struct UiArgs {
    #[arg(long, default_value = "0.0.0.0:12348")]
    bind: String,
}

#[derive(Debug, ClapArgs)]
struct VbusArgs {
    #[arg(long, default_value_t = PUPPYARM_FIRST_SERVO_ID)]
    first_servo_id: u8,

    #[arg(long, default_value_t = PUPPYARM_LAST_SERVO_ID)]
    last_servo_id: u8,
}

#[derive(Debug, Clone)]
struct ServoUiState {
    id: u8,
    target_position: u16,
    target_angle_degrees: String,
    present_position: Option<u16>,
    present_speed: Option<i32>,
    last_status: String,
}

impl ServoUiState {
    fn new(id: u8, target_position: u16) -> Self {
        Self {
            id,
            target_position,
            target_angle_degrees: format_target_angle_degrees(id, target_position),
            present_position: None,
            present_speed: None,
            last_status: "no data".to_string(),
        }
    }
}

struct VirtualBus {
    stop: Arc<AtomicBool>,
    status: Arc<Mutex<String>>,
    slave_path: String,
    join: Option<thread::JoinHandle<()>>,
}

enum ClientPort {
    Serial(SerialPortHandler),
    #[cfg(unix)]
    UnixRaw(UnixRawPort),
}

#[cfg(unix)]
struct UnixRawPort {
    fd: RawFd,
    baudrate: u32,
    tx_time_per_byte_ms: f64,
    packet_start_time: Instant,
    packet_timeout_ms: f64,
    start_time: Instant,
}

#[cfg(unix)]
impl UnixRawPort {
    fn open(path: &str, baudrate: u32) -> io::Result<Self> {
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

            let mut port = Self {
                fd,
                baudrate,
                tx_time_per_byte_ms: 0.0,
                packet_start_time: Instant::now(),
                packet_timeout_ms: 0.0,
                start_time: Instant::now(),
            };
            port.update_tx_time_per_byte();
            Ok(port)
        }
    }

    fn update_tx_time_per_byte(&mut self) {
        self.tx_time_per_byte_ms = (1000.0 / self.baudrate as f64) * 10.0;
    }

    fn elapsed_ms(&self, since: Instant) -> f64 {
        since.elapsed().as_secs_f64() * 1000.0
    }
}

#[cfg(unix)]
impl Drop for UnixRawPort {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.fd);
        }
    }
}

fn format_packet_hex(packet: &[u8]) -> String {
    packet
        .iter()
        .map(|b| format!("{:02X}", b))
        .collect::<Vec<_>>()
        .join(" ")
}

fn log_serial_tx(packet: &[u8], written: usize) {
    if packet.is_empty() {
        return;
    }

    let servo_id = packet.get(2).copied().unwrap_or(0);
    let instruction = packet.get(4).copied().unwrap_or(0);
    let status = if written == packet.len() {
        "ok"
    } else {
        "partial"
    };
    log::info!(
        "[serial tx] status={} id={} instr=0x{:02X} written={}/{} bytes={}",
        status,
        servo_id,
        instruction,
        written,
        packet.len(),
        format_packet_hex(packet)
    );
}

#[cfg(unix)]
impl PortHandler for UnixRawPort {
    fn clear_port(&mut self) {
        let available = self.get_bytes_available();
        if available > 0 {
            let _ = self.read_port(available);
        }
    }

    fn read_port(&mut self, length: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(length);
        if length == 0 {
            return out;
        }
        unsafe {
            out.set_len(length);
            let read_len = libc::read(self.fd, out.as_mut_ptr() as *mut libc::c_void, length);
            if read_len <= 0 {
                out.clear();
            } else {
                out.truncate(read_len as usize);
            }
        }
        out
    }

    fn write_port(&mut self, packet: &[u8]) -> usize {
        if packet.is_empty() {
            return 0;
        }
        let start = Instant::now();
        let mut total = 0usize;
        while total < packet.len() {
            let slice = &packet[total..];
            let written =
                unsafe { libc::write(self.fd, slice.as_ptr() as *const libc::c_void, slice.len()) };
            if written > 0 {
                total += written as usize;
                continue;
            }
            if written == 0 {
                break;
            }

            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock || err.kind() == io::ErrorKind::Interrupted {
                if start.elapsed() > Duration::from_millis(200) {
                    break;
                }
                thread::sleep(Duration::from_micros(200));
                continue;
            }
            break;
        }
        total
    }

    fn set_packet_timeout(&mut self, packet_length: usize) {
        self.packet_start_time = Instant::now();
        self.packet_timeout_ms = (self.tx_time_per_byte_ms * packet_length as f64)
            + (self.tx_time_per_byte_ms * 3.0)
            + 50.0;
    }

    fn set_packet_timeout_millis(&mut self, msec: u64) {
        self.packet_start_time = Instant::now();
        self.packet_timeout_ms = msec as f64;
    }

    fn is_packet_timeout(&mut self) -> bool {
        if self.packet_timeout_ms <= 0.0 {
            return false;
        }
        if self.get_time_since_start() > self.packet_timeout_ms {
            self.packet_timeout_ms = 0.0;
            return true;
        }
        false
    }

    fn get_current_time(&self) -> f64 {
        self.elapsed_ms(self.start_time)
    }

    fn get_time_since_start(&self) -> f64 {
        self.elapsed_ms(self.packet_start_time)
    }

    fn set_baud_rate(&mut self, baudrate: u32) -> bool {
        self.baudrate = baudrate;
        self.update_tx_time_per_byte();
        true
    }

    fn get_baud_rate(&self) -> u32 {
        self.baudrate
    }

    fn get_bytes_available(&self) -> usize {
        unsafe {
            let mut bytes: libc::c_int = 0;
            if libc::ioctl(self.fd, libc::FIONREAD, &mut bytes) == 0 {
                bytes as usize
            } else {
                0
            }
        }
    }
}

impl PortHandler for ClientPort {
    fn clear_port(&mut self) {
        match self {
            ClientPort::Serial(p) => p.clear_port(),
            #[cfg(unix)]
            ClientPort::UnixRaw(p) => p.clear_port(),
        }
    }

    fn read_port(&mut self, length: usize) -> Vec<u8> {
        match self {
            ClientPort::Serial(p) => p.read_port(length),
            #[cfg(unix)]
            ClientPort::UnixRaw(p) => p.read_port(length),
        }
    }

    fn write_port(&mut self, packet: &[u8]) -> usize {
        let written = match self {
            ClientPort::Serial(p) => p.write_port(packet),
            #[cfg(unix)]
            ClientPort::UnixRaw(p) => p.write_port(packet),
        };
        log_serial_tx(packet, written);
        written
    }

    fn set_packet_timeout(&mut self, packet_length: usize) {
        match self {
            ClientPort::Serial(p) => p.set_packet_timeout(packet_length),
            #[cfg(unix)]
            ClientPort::UnixRaw(p) => p.set_packet_timeout(packet_length),
        }
    }

    fn set_packet_timeout_millis(&mut self, msec: u64) {
        match self {
            ClientPort::Serial(p) => p.set_packet_timeout_millis(msec),
            #[cfg(unix)]
            ClientPort::UnixRaw(p) => p.set_packet_timeout_millis(msec),
        }
    }

    fn is_packet_timeout(&mut self) -> bool {
        match self {
            ClientPort::Serial(p) => p.is_packet_timeout(),
            #[cfg(unix)]
            ClientPort::UnixRaw(p) => p.is_packet_timeout(),
        }
    }

    fn get_current_time(&self) -> f64 {
        match self {
            ClientPort::Serial(p) => p.get_current_time(),
            #[cfg(unix)]
            ClientPort::UnixRaw(p) => p.get_current_time(),
        }
    }

    fn get_time_since_start(&self) -> f64 {
        match self {
            ClientPort::Serial(p) => p.get_time_since_start(),
            #[cfg(unix)]
            ClientPort::UnixRaw(p) => p.get_time_since_start(),
        }
    }

    fn set_baud_rate(&mut self, baudrate: u32) -> bool {
        match self {
            ClientPort::Serial(p) => p.set_baud_rate(baudrate),
            #[cfg(unix)]
            ClientPort::UnixRaw(p) => p.set_baud_rate(baudrate),
        }
    }

    fn get_baud_rate(&self) -> u32 {
        match self {
            ClientPort::Serial(p) => p.get_baud_rate(),
            #[cfg(unix)]
            ClientPort::UnixRaw(p) => p.get_baud_rate(),
        }
    }

    fn get_bytes_available(&self) -> usize {
        match self {
            ClientPort::Serial(p) => p.get_bytes_available(),
            #[cfg(unix)]
            ClientPort::UnixRaw(p) => p.get_bytes_available(),
        }
    }
}

type PuppyClient = Scscl<ClientPort>;

impl VirtualBus {
    fn start(first_servo_id: u8, last_servo_id: u8) -> std::io::Result<Self> {
        if first_servo_id > last_servo_id {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "virtual bus range is invalid: from must be <= to",
            ));
        }

        let port = VirtualUartPort::new()?;
        let slave_path = port.slave_path().to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let status = Arc::new(Mutex::new(format!(
            "Running on {} (IDs {}..={})",
            slave_path, first_servo_id, last_servo_id
        )));

        let stop_in_thread = Arc::clone(&stop);
        let status_in_thread = Arc::clone(&status);

        let join = thread::spawn(move || {
            run_virtual_bus_loop(
                port,
                first_servo_id,
                last_servo_id,
                stop_in_thread,
                status_in_thread,
            );
        });

        Ok(Self {
            stop,
            status,
            slave_path,
            join: Some(join),
        })
    }

    fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }

    fn slave_path(&self) -> &str {
        &self.slave_path
    }

    fn status(&self) -> String {
        self.status
            .lock()
            .map(|s| s.clone())
            .unwrap_or_else(|_| "status unavailable".to_string())
    }
}

fn run_virtual_bus_loop(
    mut port: VirtualUartPort,
    first_servo_id: u8,
    last_servo_id: u8,
    stop: Arc<AtomicBool>,
    status: Arc<Mutex<String>>,
) {
    let mut sim = FeetechBusSim::new();
    for id in first_servo_id..=last_servo_id {
        sim.add_servo(id);
    }

    let mut buffer: Vec<u8> = Vec::new();
    let mut last_step = Instant::now();

    while !stop.load(Ordering::Relaxed) {
        let mut incoming = port.read_port(512);
        let has_io = !incoming.is_empty();
        if has_io {
            buffer.append(&mut incoming);
            let frames = extract_frames(&mut buffer);
            for frame in frames {
                if let Ok(Some(response)) = sim.handle_frame(&frame) {
                    let _ = port.write_port(&response);
                }
            }
        }

        let now = Instant::now();
        let dt = (now - last_step).as_secs_f32();
        if dt >= 0.005 {
            sim.step(dt);
            last_step = now;
        }

        if !has_io {
            thread::sleep(Duration::from_millis(2));
        }
    }

    if let Ok(mut s) = status.lock() {
        *s = "Stopped".to_string();
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

struct AppState {
    port_input: String,
    baud: u32,
    connected: bool,
    status_polling_enabled: bool,
    virtual_bus_running: bool,
    virtual_bus_status: String,
    move_time: String,
    move_speed: String,
    x: String,
    y: String,
    z: String,
    l1: String,
    l2: String,
    shoulder_r: String,
    shoulder_z: String,
    wrist_pitch: String,
    elbow_up_branch: bool,
    status: String,
    ik_status: String,
    servos: BTreeMap<u8, ServoUiState>,
}

impl AppState {
    fn new(initial_port: Option<String>, baud: u32) -> Self {
        let port_input = initial_port.unwrap_or_default();
        let initial_status = if port_input.is_empty() {
            "No serial port selected. Start virtual bus or enter a port and click Connect."
                .to_string()
        } else {
            format!("Ready to connect to {}", port_input)
        };
        let mut servos = BTreeMap::new();
        for id in PUPPYARM_FIRST_SERVO_ID..=PUPPYARM_LAST_SERVO_ID {
            servos.insert(id, ServoUiState::new(id, 0));
        }

        Self {
            port_input,
            baud,
            connected: false,
            status_polling_enabled: true,
            virtual_bus_running: false,
            virtual_bus_status: "stopped".to_string(),
            move_time: "0".to_string(),
            move_speed: "400".to_string(),
            x: "0.15".to_string(),
            y: "0.00".to_string(),
            z: "0.08".to_string(),
            l1: "0.11".to_string(),
            l2: "0.11".to_string(),
            shoulder_r: "0.00".to_string(),
            shoulder_z: "0.00".to_string(),
            wrist_pitch: "".to_string(),
            elbow_up_branch: true,
            status: initial_status,
            ik_status: "Set XYZ and click Apply IK".to_string(),
            servos,
        }
    }
}

fn score_port(info: &SerialPortInfo) -> i32 {
    let name = info.port_name.to_ascii_lowercase();
    let mut score = 0i32;

    score += match info.port_type {
        SerialPortType::UsbPort(_) => 40,
        SerialPortType::PciPort => 20,
        SerialPortType::Unknown => 10,
        SerialPortType::BluetoothPort => -30,
    };

    if name.contains("ttyusb")
        || name.contains("ttyacm")
        || name.contains("cu.usb")
        || name.starts_with("com")
    {
        score += 30;
    }
    if name.contains("usb") {
        score += 15;
    }
    if name.contains("bluetooth") {
        score -= 50;
    }

    score
}

fn resolve_port(
    port_arg: Option<String>,
    baud: u32,
) -> Result<(String, bool), Box<dyn std::error::Error>> {
    if let Some(port) = port_arg {
        let trimmed = port.trim();
        if trimmed.is_empty() {
            return Err("port cannot be empty".into());
        }
        return Ok((trimmed.to_string(), false));
    }

    let mut ports = serialport::available_ports()?;
    if ports.is_empty() {
        return Err(
            "no serial ports detected; pass --port explicitly (e.g. --port /dev/ttyUSB0)".into(),
        );
    }

    ports.sort_by(|a, b| {
        score_port(b)
            .cmp(&score_port(a))
            .then_with(|| a.port_name.cmp(&b.port_name))
    });

    let mut attempted = Vec::new();
    for info in ports {
        let port_name = info.port_name;
        match serialport::new(&port_name, baud)
            .timeout(Duration::from_millis(2))
            .open()
        {
            Ok(_) => return Ok((port_name, true)),
            Err(err) => attempted.push(format!("{port_name}: {err}")),
        }
    }

    let details = attempted.join("; ");
    Err(format!(
        "no usable serial ports detected at baud {baud}; pass --port explicitly. Tried: {details}"
    )
    .into())
}

fn parse_u16_field(label: &str, value: &str) -> Result<u16, String> {
    value
        .trim()
        .parse::<u16>()
        .map_err(|_| format!("{label} must be a non-negative integer"))
}

fn parse_f64_field(label: &str, value: &str) -> Result<f64, String> {
    value
        .trim()
        .parse::<f64>()
        .map_err(|_| format!("{label} must be a floating-point number"))
}

fn parse_motion_params(state: &AppState) -> Result<(u16, u16), String> {
    let time = parse_u16_field("move time", &state.move_time)?;
    let speed = parse_u16_field("move speed", &state.move_speed)?;
    Ok((time, speed))
}

fn joint_calib_for_servo(_id: u8) -> JointCalib {
    let ticks_per_rad = 4096.0 / std::f64::consts::TAU;
    JointCalib {
        offset_ticks: 2048,
        ticks_per_rad,
        sign: 1.0,
        tick_min: 0,
        tick_max: 4095,
    }
}

fn ticks_to_degrees(ticks: u16, calib: JointCalib) -> f64 {
    let scaled = (ticks as f64 - calib.offset_ticks as f64) / calib.ticks_per_rad;
    (scaled / calib.sign).to_degrees()
}

fn format_target_angle_degrees(servo_id: u8, ticks: u16) -> String {
    format!(
        "{:.1}",
        ticks_to_degrees(ticks, joint_calib_for_servo(servo_id))
    )
}

fn send_target(client: &mut PuppyClient, servo: &mut ServoUiState, time: u16, speed: u16) -> bool {
    let (result, error) = client.write_pos(servo.id, servo.target_position, time, speed);
    if result == COMM_SUCCESS {
        servo.last_status = "write ok".to_string();
        true
    } else {
        let message = client.handler.get_tx_rx_result(result);
        servo.last_status = format!("write failed: {message} (error byte {error})");
        false
    }
}

fn scan_bus(client: &mut PuppyClient, state: &mut AppState) {
    let mut discovered = 0usize;
    for id in PUPPYARM_FIRST_SERVO_ID..=PUPPYARM_LAST_SERVO_ID {
        let (_, result, _) = client.handler.ping(id);
        let servo = state
            .servos
            .entry(id)
            .or_insert_with(|| ServoUiState::new(id, 2048));
        if result == COMM_SUCCESS {
            discovered += 1;
            servo.last_status = "detected".to_string();
        } else {
            servo.present_position = None;
            servo.present_speed = None;
            servo.last_status = "not detected on scan".to_string();
        }
    }

    state.status = format!(
        "Scan complete: {} servo(s) found in fixed range {}..={}",
        discovered, PUPPYARM_FIRST_SERVO_ID, PUPPYARM_LAST_SERVO_ID
    );
}

fn refresh_servos(
    client: &mut PuppyClient,
    servos: &mut BTreeMap<u8, ServoUiState>,
) -> (usize, usize) {
    let mut ok = 0usize;
    let mut fail = 0usize;

    for servo in servos.values_mut() {
        let (position, speed, result, error) = client.read_pos_speed(servo.id);
        if result == COMM_SUCCESS {
            servo.present_position = Some(position);
            servo.present_speed = Some(speed);
            let (goal, goal_result, _goal_error) = client
                .handler
                .read_2byte_tx_rx(servo.id, SCSCL_GOAL_POSITION_L);
            if goal_result == COMM_SUCCESS {
                servo.target_position = goal;
                servo.target_angle_degrees = format_target_angle_degrees(servo.id, goal);
            }
            servo.last_status = "ok".to_string();
            ok += 1;
        } else {
            servo.present_position = None;
            servo.present_speed = None;
            let message = client.handler.get_tx_rx_result(result);
            servo.last_status = format!("read failed: {message} (error byte {error})");
            fail += 1;
        }
    }

    (ok, fail)
}

fn send_all_targets(client: &mut PuppyClient, state: &mut AppState) {
    if state.servos.is_empty() {
        state.status = "No servos to command. Run scan first.".to_string();
        return;
    }

    let (time, speed) = match parse_motion_params(state) {
        Ok(params) => params,
        Err(err) => {
            state.status = err;
            return;
        }
    };

    let mut ok = 0usize;
    for servo in state.servos.values_mut() {
        if send_target(client, servo, time, speed) {
            ok += 1;
        }
    }

    state.status = format!("Sent targets to {ok}/{} servo(s)", state.servos.len());
}

fn set_servo_angle_target(client: &mut PuppyClient, state: &mut AppState, servo_id: u8) {
    let (time, speed) = match parse_motion_params(state) {
        Ok(params) => params,
        Err(err) => {
            state.status = err;
            return;
        }
    };

    let Some(servo) = state.servos.get_mut(&servo_id) else {
        state.status = format!("Servo {} is not available. Run Scan first.", servo_id);
        return;
    };

    let angle_label = format!("servo {} angle", servo_id);
    let angle_deg = match parse_f64_field(&angle_label, &servo.target_angle_degrees) {
        Ok(value) => value,
        Err(err) => {
            state.status = err;
            return;
        }
    };

    let target_ticks = angle_to_ticks(angle_deg.to_radians(), joint_calib_for_servo(servo_id))
        .clamp(0, 4095) as u16;
    servo.target_position = target_ticks;
    servo.target_angle_degrees = format!("{angle_deg:.1}");

    if send_target(client, servo, time, speed) {
        state.status = format!(
            "Sent servo {} to {:.1} deg (ticks {})",
            servo_id, angle_deg, target_ticks
        );
    } else {
        state.status = format!("Command failed for servo {}", servo_id);
    }
}

fn apply_ik(client: &mut PuppyClient, state: &mut AppState) {
    let x = match parse_f64_field("x", &state.x) {
        Ok(value) => value,
        Err(err) => {
            state.ik_status = err;
            return;
        }
    };
    let y = match parse_f64_field("y", &state.y) {
        Ok(value) => value,
        Err(err) => {
            state.ik_status = err;
            return;
        }
    };
    let z = match parse_f64_field("z", &state.z) {
        Ok(value) => value,
        Err(err) => {
            state.ik_status = err;
            return;
        }
    };
    let l1 = match parse_f64_field("l1", &state.l1) {
        Ok(value) => value,
        Err(err) => {
            state.ik_status = err;
            return;
        }
    };
    let l2 = match parse_f64_field("l2", &state.l2) {
        Ok(value) => value,
        Err(err) => {
            state.ik_status = err;
            return;
        }
    };
    let shoulder_r = match parse_f64_field("shoulder_r", &state.shoulder_r) {
        Ok(value) => value,
        Err(err) => {
            state.ik_status = err;
            return;
        }
    };
    let shoulder_z = match parse_f64_field("shoulder_z", &state.shoulder_z) {
        Ok(value) => value,
        Err(err) => {
            state.ik_status = err;
            return;
        }
    };

    let wrist_pitch = if state.wrist_pitch.trim().is_empty() {
        None
    } else {
        match parse_f64_field("wrist_pitch", &state.wrist_pitch) {
            Ok(value) => Some(value),
            Err(err) => {
                state.ik_status = err;
                return;
            }
        }
    };

    let geom = ArmGeom {
        l1,
        l2,
        shoulder_r,
        shoulder_z,
        wrist_pitch,
    };

    let elbow_sign = if state.elbow_up_branch { 1.0 } else { -1.0 };
    let solution = match ik_elbow_up(Vec3 { x, y, z }, geom, elbow_sign) {
        Ok(solution) => solution,
        Err(err) => {
            state.ik_status = format!("IK failed: {err:?}");
            return;
        }
    };

    let (time, speed) = match parse_motion_params(state) {
        Ok(params) => params,
        Err(err) => {
            state.ik_status = err;
            return;
        }
    };

    let mut ids_and_angles = vec![
        (1u8, solution.q0_yaw),
        (2u8, solution.q1_shoulder),
        (3u8, solution.q2_elbow),
    ];
    if let Some(q3) = solution.q3_wrist {
        ids_and_angles.push((4u8, q3));
    }

    for (id, angle_rad) in ids_and_angles {
        let calib = joint_calib_for_servo(id);
        let target_ticks = angle_to_ticks(angle_rad, calib).clamp(0, 4095) as u16;
        let servo = state
            .servos
            .entry(id)
            .or_insert_with(|| ServoUiState::new(id, target_ticks));
        servo.target_position = target_ticks;
        servo.target_angle_degrees = format!("{:.1}", angle_rad.to_degrees());
        let _ = send_target(client, servo, time, speed);
    }

    state.ik_status = format!(
        "IK ok: q0={:.1} deg, q1={:.1} deg, q2={:.1} deg{}",
        solution.q0_yaw.to_degrees(),
        solution.q1_shoulder.to_degrees(),
        solution.q2_elbow.to_degrees(),
        solution
            .q3_wrist
            .map(|q| format!(", q3={:.1} deg", q.to_degrees()))
            .unwrap_or_default(),
    );
    state.status = "IK targets applied to servo IDs 1..4 (as available)".to_string();
}

fn render_servo_state(servo: &ServoUiState) -> Item {
    let present_position = servo
        .present_position
        .map(|v| v.to_string())
        .unwrap_or_else(|| "-".to_string());
    let present_speed = servo
        .present_speed
        .map(|v| v.to_string())
        .unwrap_or_else(|| "-".to_string());

    let target_degrees = ticks_to_degrees(servo.target_position, joint_calib_for_servo(servo.id));

    vstack([
        text(&format!("Servo {}", servo.id)),
        text(&format!(
            "present {} | speed {}",
            present_position, present_speed
        )),
        text(&format!(
            "target {} ({:.1} deg)",
            servo.target_position, target_degrees
        )),
        slider()
            .id(TARGET_POSITION_SLIDER_ID)
            .inx(servo.id as u32)
            .min(0)
            .max(4095)
            .ivalue(servo.target_position as i32),
        hstack([
            text("Angle (deg)"),
            text_input()
                .id(SERVO_ANGLE_INPUT_ID)
                .inx(servo.id as u32)
                .svalue(&servo.target_angle_degrees)
                .width(90),
            button("Set Angle")
                .id(APPLY_SERVO_ANGLE_BUTTON_ID)
                .inx(servo.id as u32),
        ])
        .spacing(8),
        text(&servo.last_status),
    ])
    .spacing(4)
    .padding(10)
    .border("1px solid #d6d6d6")
    .background_color("#f8f9fb")
    .into()
}

fn render(state: &AppState) -> Item {
    vstack([
        text("PuppyArm Servo Bus UI"),
        text(&format!("Baud {}", state.baud)),
        text(&state.status),
        hstack([
            text("Port"),
            text_input()
                .id(PORT_INPUT_ID)
                .placeholder("COM6 or /dev/ttyUSB0")
                .svalue(&state.port_input)
                .width(220),
            button(if state.connected {
                "Disconnect"
            } else {
                "Connect"
            })
            .id(TOGGLE_CONNECTION_BUTTON_ID),
        ])
        .spacing(8),
        hstack([
            text(&format!(
                "Virtual bus IDs {}..={}",
                PUPPYARM_FIRST_SERVO_ID, PUPPYARM_LAST_SERVO_ID
            )),
            button(if state.virtual_bus_running {
                "Stop Virtual Bus"
            } else {
                "Start Virtual Bus"
            })
            .id(TOGGLE_VBUS_BUTTON_ID),
        ])
        .spacing(8),
        text(&format!("Virtual bus: {}", state.virtual_bus_status)),
        hstack([
            text(&format!(
                "Scan fixed IDs {}..={}",
                PUPPYARM_FIRST_SERVO_ID, PUPPYARM_LAST_SERVO_ID
            )),
            button("Scan").id(SCAN_BUTTON_ID),
            button("Refresh").id(REFRESH_BUTTON_ID),
            button(if state.status_polling_enabled {
                "Pause Polling"
            } else {
                "Resume Polling"
            })
            .id(TOGGLE_STATUS_POLLING_BUTTON_ID),
        ])
        .spacing(8),
        text(if state.status_polling_enabled {
            "Status polling: enabled"
        } else {
            "Status polling: paused"
        }),
        hstack([
            text("Move time"),
            text_input()
                .id(MOVE_TIME_INPUT_ID)
                .svalue(&state.move_time)
                .width(80),
            text("speed"),
            text_input()
                .id(MOVE_SPEED_INPUT_ID)
                .svalue(&state.move_speed)
                .width(80),
            button("Send All Targets").id(SEND_ALL_BUTTON_ID),
        ])
        .spacing(8),
        text("Servos"),
        vstack(state.servos.values().map(render_servo_state)).spacing(8),
        text("Cartesian IK"),
        hstack([
            text("x"),
            text_input().id(IK_X_INPUT_ID).svalue(&state.x).width(80),
            text("y"),
            text_input().id(IK_Y_INPUT_ID).svalue(&state.y).width(80),
            text("z"),
            text_input().id(IK_Z_INPUT_ID).svalue(&state.z).width(80),
        ])
        .spacing(8),
        hstack([
            text("l1"),
            text_input().id(IK_L1_INPUT_ID).svalue(&state.l1).width(70),
            text("l2"),
            text_input().id(IK_L2_INPUT_ID).svalue(&state.l2).width(70),
            text("shoulder_r"),
            text_input()
                .id(IK_SHOULDER_R_INPUT_ID)
                .svalue(&state.shoulder_r)
                .width(80),
            text("shoulder_z"),
            text_input()
                .id(IK_SHOULDER_Z_INPUT_ID)
                .svalue(&state.shoulder_z)
                .width(80),
            text("wrist_pitch(rad)"),
            text_input()
                .id(IK_WRIST_PITCH_INPUT_ID)
                .placeholder("optional")
                .svalue(&state.wrist_pitch)
                .width(100),
        ])
        .spacing(8),
        hstack([
            button(if state.elbow_up_branch {
                "Elbow Branch: Up"
            } else {
                "Elbow Branch: Down"
            })
            .id(TOGGLE_ELBOW_BRANCH_BUTTON_ID),
            button("Apply IK").id(APPLY_IK_BUTTON_ID),
        ])
        .spacing(8),
        text(&state.ik_status),
    ])
    .spacing(10)
    .padding(16)
    .background_color("#ffffff")
    .into()
}

#[cfg(unix)]
fn is_unix_tty_path(path: &str) -> bool {
    path.starts_with("/dev/tty") || path.starts_with("/dev/cu")
}

fn open_client(port: &str, baud: u32) -> Result<PuppyClient, Box<dyn std::error::Error>> {
    #[cfg(unix)]
    {
        if is_unix_tty_path(port) {
            match UnixRawPort::open(port, baud) {
                Ok(raw) => return Ok(Scscl::new(ClientPort::UnixRaw(raw))),
                Err(raw_err) => match SerialPortHandler::open(port, baud) {
                    Ok(serial) => return Ok(Scscl::new(ClientPort::Serial(serial))),
                    Err(serial_err) => {
                        return Err(format!(
                            "failed to open {} as raw unix ({}) or serial ({})",
                            port, raw_err, serial_err
                        )
                        .into());
                    }
                },
            }
        }
    }

    match SerialPortHandler::open(port, baud) {
        Ok(serial) => Ok(Scscl::new(ClientPort::Serial(serial))),
        Err(serial_err) => {
            #[cfg(unix)]
            {
                match UnixRawPort::open(port, baud) {
                    Ok(raw) => Ok(Scscl::new(ClientPort::UnixRaw(raw))),
                    Err(raw_err) => Err(format!(
                        "failed to open {} as serial ({}) or raw unix ({})",
                        port, serial_err, raw_err
                    )
                    .into()),
                }
            }
            #[cfg(not(unix))]
            {
                Err(serial_err.into())
            }
        }
    }
}

fn run_scan(port: &str, baud: u32) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = open_client(port, baud)?;

    println!(
        "Scanning fixed PuppyArm range {}..={} on {} @ {} baud",
        PUPPYARM_FIRST_SERVO_ID, PUPPYARM_LAST_SERVO_ID, port, baud
    );
    let mut found = 0usize;
    for id in PUPPYARM_FIRST_SERVO_ID..=PUPPYARM_LAST_SERVO_ID {
        let (model, result, error) = client.handler.ping(id);
        if result == COMM_SUCCESS {
            found += 1;
            println!("id={id} model={model} error={error}");
        }
    }
    println!("Found {found} servo(s)");

    Ok(())
}

fn run_read(port: &str, baud: u32, id: u8) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = open_client(port, baud)?;
    let (pos, speed, result, error) = client.read_pos_speed(id);
    if result != COMM_SUCCESS {
        let message = client.handler.get_tx_rx_result(result);
        return Err(format!("read failed for id {id}: {message} (error byte {error})").into());
    }

    println!("id={id} position={pos} speed={speed} error={error}");
    Ok(())
}

fn run_move(
    port: &str,
    baud: u32,
    id: u8,
    position: u16,
    time: u16,
    speed: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    if position > 4095 {
        return Err(format!("position must be in range 0..=4095, got {position}").into());
    }

    let mut client = open_client(port, baud)?;

    let (result, error) = client.write_pos(id, position, time, speed);
    if result != COMM_SUCCESS {
        let message = client.handler.get_tx_rx_result(result);
        return Err(format!("write failed for id {id}: {message} (error byte {error})").into());
    }

    thread::sleep(Duration::from_millis(40));
    let (pos, measured_speed, read_result, read_error) = client.read_pos_speed(id);
    if read_result != COMM_SUCCESS {
        let message = client.handler.get_tx_rx_result(read_result);
        return Err(format!(
            "move sent, but readback failed for id {id}: {message} (error byte {read_error})"
        )
        .into());
    }

    println!(
        "id={id} commanded_position={position} read_position={pos} read_speed={measured_speed} error={read_error}"
    );

    Ok(())
}

fn run_vbus(args: VbusArgs) -> Result<(), Box<dyn std::error::Error>> {
    if args.first_servo_id > args.last_servo_id {
        return Err("first-servo-id must be <= last-servo-id".into());
    }

    let _bus = VirtualBus::start(args.first_servo_id, args.last_servo_id)?;
    println!("PuppyArm virtual bus running.");
    println!("Slave device: {}", _bus.slave_path());
    println!(
        "Servo IDs: {}..={}",
        args.first_servo_id, args.last_servo_id
    );
    println!("Press Ctrl-C to stop.");

    loop {
        thread::sleep(Duration::from_secs(1));
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
            "UI bind failed on {}: {}. Stop the existing process or use `ui --bind <addr:port>`.",
            bind_addr, err
        )
    })?;
    drop(listener);
    Ok(())
}

async fn run_ui(
    initial_port: Option<String>,
    baud: u32,
    ui_args: UiArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let bind_addr = ui_args.bind.parse()?;
    ensure_ui_bind_available(bind_addr)?;

    let browser_url = ui_url(bind_addr);
    println!("PuppyArm UI server listening on {}", browser_url);
    println!("Press Ctrl-C to stop.");

    let mut state = AppState::new(initial_port, baud);
    let mut client: Option<PuppyClient> = None;
    let mut virtual_bus: Option<VirtualBus> = None;

    if !state.port_input.trim().is_empty() {
        match open_client(state.port_input.trim(), baud) {
            Ok(mut connected_client) => {
                state.connected = true;
                state.status = format!("Connected to {} @ {} baud", state.port_input, baud);
                scan_bus(&mut connected_client, &mut state);
                let _ = refresh_servos(&mut connected_client, &mut state.servos);
                client = Some(connected_client);
            }
            Err(err) => {
                state.status = format!("Failed to connect {}: {err}", state.port_input);
            }
        }
    }

    let mut wgui = Wgui::new(bind_addr);
    let mut connected_clients = HashSet::new();
    let mut ticker = tokio::time::interval(Duration::from_millis(250));

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if let Some(bus) = virtual_bus.as_ref() {
                    state.virtual_bus_status = bus.status();
                } else {
                    state.virtual_bus_status = "stopped".to_string();
                }

                if state.status_polling_enabled {
                    if let Some(client_ref) = client.as_mut() {
                        let _ = refresh_servos(client_ref, &mut state.servos);
                    }
                }

                for id in &connected_clients {
                    wgui.render(*id, render(&state)).await;
                }
            }
            maybe_message = wgui.next() => {
                let Some(message) = maybe_message else {
                    break;
                };

                let client_id = message.client_id;
                match message.event {
                    ClientEvent::Disconnected { id: _ } => {
                        connected_clients.remove(&client_id);
                    }
                    ClientEvent::Connected { id: _ } => {
                        connected_clients.insert(client_id);
                        wgui.render(client_id, render(&state)).await;
                    }
                    ClientEvent::OnClick(click) => {
                        match click.id {
                            TOGGLE_CONNECTION_BUTTON_ID => {
                                if state.connected {
                                    client = None;
                                    state.connected = false;
                                    state.status = "Disconnected".to_string();
                                } else {
                                    let port_name = state.port_input.trim().to_string();
                                    if port_name.is_empty() {
                                        state.status = "Set a serial port first".to_string();
                                    } else {
                                        match open_client(&port_name, baud) {
                                            Ok(mut connected_client) => {
                                                state.connected = true;
                                                state.status = format!("Connected to {} @ {} baud", port_name, baud);
                                                scan_bus(&mut connected_client, &mut state);
                                                let _ = refresh_servos(&mut connected_client, &mut state.servos);
                                                client = Some(connected_client);
                                            }
                                            Err(err) => {
                                                state.status = format!("Failed to connect {}: {err}", port_name);
                                            }
                                        }
                                    }
                                }
                            }
                            TOGGLE_VBUS_BUTTON_ID => {
                                if let Some(mut bus) = virtual_bus.take() {
                                    let should_disconnect = state.connected && state.port_input == bus.slave_path();
                                    bus.stop();
                                    state.virtual_bus_running = false;
                                    state.virtual_bus_status = "stopped".to_string();
                                    if should_disconnect {
                                        client = None;
                                        state.connected = false;
                                    }
                                    state.status = "Virtual bus stopped".to_string();
                                } else {
                                    match VirtualBus::start(PUPPYARM_FIRST_SERVO_ID, PUPPYARM_LAST_SERVO_ID) {
                                        Ok(bus) => {
                                            let slave_path = bus.slave_path().to_string();
                                            state.virtual_bus_running = true;
                                            state.virtual_bus_status = bus.status();
                                            state.port_input = slave_path.clone();
                                            state.status = format!("Virtual bus started at {}", slave_path);
                                            virtual_bus = Some(bus);

                                            match open_client(&slave_path, baud) {
                                                Ok(mut connected_client) => {
                                                    state.connected = true;
                                                    scan_bus(&mut connected_client, &mut state);
                                                    let _ = refresh_servos(&mut connected_client, &mut state.servos);
                                                    client = Some(connected_client);
                                                    state.status = format!("Virtual bus ready and connected on {}", slave_path);
                                                }
                                                Err(err) => {
                                                    state.connected = false;
                                                    state.status = format!("Virtual bus started but connect failed: {err}");
                                                }
                                            }
                                        }
                                        Err(err) => {
                                            state.status = format!("Failed to start virtual bus: {err}");
                                        }
                                    }
                                }
                            }
                            TOGGLE_STATUS_POLLING_BUTTON_ID => {
                                state.status_polling_enabled = !state.status_polling_enabled;
                                if state.status_polling_enabled {
                                    state.status = "Status polling resumed".to_string();
                                    if let Some(client_ref) = client.as_mut() {
                                        let (ok, fail) = refresh_servos(client_ref, &mut state.servos);
                                        state.status = format!("Status polling resumed ({ok} ok, {fail} failed)");
                                    }
                                } else {
                                    state.status = "Status polling paused".to_string();
                                }
                            }
                            SCAN_BUTTON_ID => {
                                if let Some(client_ref) = client.as_mut() {
                                    scan_bus(client_ref, &mut state);
                                    let _ = refresh_servos(client_ref, &mut state.servos);
                                } else {
                                    state.status = "Not connected. Connect first.".to_string();
                                }
                            }
                            REFRESH_BUTTON_ID => {
                                if let Some(client_ref) = client.as_mut() {
                                    let (ok, fail) = refresh_servos(client_ref, &mut state.servos);
                                    state.status = format!("Refresh done: {ok} ok, {fail} failed");
                                } else {
                                    state.status = "Not connected. Connect first.".to_string();
                                }
                            }
                            SEND_ALL_BUTTON_ID => {
                                if let Some(client_ref) = client.as_mut() {
                                    send_all_targets(client_ref, &mut state);
                                } else {
                                    state.status = "Not connected. Connect first.".to_string();
                                }
                            }
                            APPLY_IK_BUTTON_ID => {
                                if let Some(client_ref) = client.as_mut() {
                                    apply_ik(client_ref, &mut state);
                                } else {
                                    state.status = "Not connected. Connect first.".to_string();
                                }
                            }
                            APPLY_SERVO_ANGLE_BUTTON_ID => {
                                if let Some(client_ref) = client.as_mut() {
                                    if let Some(inx) = click.inx {
                                        set_servo_angle_target(client_ref, &mut state, inx as u8);
                                    } else {
                                        state.status = "Missing servo index for angle command".to_string();
                                    }
                                } else {
                                    state.status = "Not connected. Connect first.".to_string();
                                }
                            }
                            TOGGLE_ELBOW_BRANCH_BUTTON_ID => {
                                state.elbow_up_branch = !state.elbow_up_branch;
                            }
                            _ => {}
                        }
                    }
                    ClientEvent::OnTextChanged(text_changed) => {
                        match text_changed.id {
                            PORT_INPUT_ID => state.port_input = text_changed.value,
                            MOVE_TIME_INPUT_ID => state.move_time = text_changed.value,
                            MOVE_SPEED_INPUT_ID => state.move_speed = text_changed.value,
                            IK_X_INPUT_ID => state.x = text_changed.value,
                            IK_Y_INPUT_ID => state.y = text_changed.value,
                            IK_Z_INPUT_ID => state.z = text_changed.value,
                            IK_L1_INPUT_ID => state.l1 = text_changed.value,
                            IK_L2_INPUT_ID => state.l2 = text_changed.value,
                            IK_SHOULDER_R_INPUT_ID => state.shoulder_r = text_changed.value,
                            IK_SHOULDER_Z_INPUT_ID => state.shoulder_z = text_changed.value,
                            IK_WRIST_PITCH_INPUT_ID => state.wrist_pitch = text_changed.value,
                            SERVO_ANGLE_INPUT_ID => {
                                if let Some(inx) = text_changed.inx {
                                    if let Some(servo) = state.servos.get_mut(&(inx as u8)) {
                                        servo.target_angle_degrees = text_changed.value;
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    ClientEvent::OnSliderChange(slider) => {
                        if slider.id == TARGET_POSITION_SLIDER_ID {
                            if let Some(inx) = slider.inx {
                                let servo_id = inx as u8;
                                let target = slider.value.clamp(0, 4095) as u16;

                                let servo = state
                                    .servos
                                    .entry(servo_id)
                                    .or_insert_with(|| ServoUiState::new(servo_id, target));
                                servo.target_position = target;
                                servo.target_angle_degrees =
                                    format_target_angle_degrees(servo_id, target);

                                if let Some(client_ref) = client.as_mut() {
                                    let motion = parse_motion_params(&state);
                                    let servo = state
                                        .servos
                                        .entry(servo_id)
                                        .or_insert_with(|| ServoUiState::new(servo_id, target));

                                    match motion {
                                        Ok((time, speed)) => {
                                            if send_target(client_ref, servo, time, speed) {
                                                state.status =
                                                    format!("Sent servo {} to {}", servo_id, target);
                                            } else {
                                                state.status =
                                                    format!("Command failed for servo {}", servo_id);
                                            }
                                        }
                                        Err(err) => {
                                            state.status = err;
                                        }
                                    }
                                } else {
                                    state.status = format!(
                                        "Not connected. Updated local target for servo {} to {}",
                                        servo_id, target
                                    );
                                }
                            }
                        }
                    }
                    _ => {}
                }

                for id in &connected_clients {
                    wgui.render(*id, render(&state)).await;
                }
            }
        }
    }

    if let Some(mut bus) = virtual_bus {
        bus.stop();
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    simple_logger::SimpleLogger::new()
        .with_level(LevelFilter::Info)
        .without_timestamps()
        .init()
        .unwrap();

    let Args {
        port,
        baud,
        command,
    } = Args::parse();

    match command {
        Command::Ui(ui_args) => {
            let initial_port = if let Some(port) = port {
                Some(port)
            } else {
                match resolve_port(None, baud) {
                    Ok((detected, _)) => {
                        println!("Auto-selected serial port: {detected}");
                        Some(detected)
                    }
                    Err(_) => {
                        println!(
                            "No usable serial port auto-detected. Use UI controls to start virtual bus or enter a port."
                        );
                        None
                    }
                }
            };

            run_ui(initial_port, baud, ui_args).await
        }
        Command::Vbus(vbus_args) => run_vbus(vbus_args),
        other_command => {
            let (resolved_port, auto_selected) = resolve_port(port, baud)?;
            if auto_selected {
                println!("Auto-selected serial port: {resolved_port}");
            }

            match other_command {
                Command::Scan => run_scan(&resolved_port, baud),
                Command::Read { id } => run_read(&resolved_port, baud, id),
                Command::Move {
                    id,
                    position,
                    time,
                    speed,
                } => run_move(&resolved_port, baud, id, position, time, speed),
                Command::Ui(_) | Command::Vbus(_) => unreachable!(),
            }
        }
    }
}
