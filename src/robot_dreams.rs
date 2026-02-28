use std::error::Error;
use std::path::Path;
#[cfg(unix)]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(unix)]
use std::sync::Arc;
#[cfg(unix)]
use std::thread;

use crate::physics::PhysicsWorld;
use crate::urdf::{UrdfModel, load_urdf};

pub struct RobotDreams {
    physics: PhysicsWorld,
    models: Vec<UrdfModel>,
    dt: f32,
}

impl RobotDreams {
    pub fn new() -> Self {
        Self {
            physics: PhysicsWorld::new(),
            models: Vec::new(),
            dt: 1.0 / 200.0,
        }
    }

    pub fn set_time_step(&mut self, dt: f32) {
        if dt > 0.0 {
            self.dt = dt;
            self.physics.set_time_step(dt);
        }
    }

    pub fn add_urdf(&mut self, path: impl AsRef<Path>) -> Result<usize, Box<dyn Error>> {
        let model = load_urdf(path)?;
        self.models.push(model);
        Ok(self.models.len() - 1)
    }

    pub fn step(&mut self, steps: usize) {
        for _ in 0..steps {
            self.physics.step();
        }
    }

    pub fn models(&self) -> &[UrdfModel] {
        &self.models
    }

    pub fn physics_mut(&mut self) -> &mut PhysicsWorld {
        &mut self.physics
    }
}

impl Default for RobotDreams {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct VirtualServoSimConfig {
    pub first_servo_id: u8,
    pub last_servo_id: u8,
    pub step_seconds: f32,
    pub idle_sleep_ms: u64,
}

impl Default for VirtualServoSimConfig {
    fn default() -> Self {
        Self {
            first_servo_id: 1,
            last_servo_id: 6,
            step_seconds: 0.005,
            idle_sleep_ms: 2,
        }
    }
}

#[cfg(unix)]
pub struct VirtualServoSimHandle {
    stop: Arc<AtomicBool>,
    join: Option<thread::JoinHandle<()>>,
    slave_path: String,
}

#[cfg(unix)]
impl VirtualServoSimHandle {
    pub fn slave_path(&self) -> &str {
        &self.slave_path
    }

    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

#[cfg(unix)]
impl Drop for VirtualServoSimHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(unix)]
fn validate_virtual_servo_sim_config(config: VirtualServoSimConfig) -> std::io::Result<()> {
    if config.first_servo_id > config.last_servo_id {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "first-servo-id must be less than or equal to last-servo-id",
        ));
    }

    if config.step_seconds <= 0.0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "step-seconds must be greater than 0",
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn create_virtual_servo_bus_sim(config: VirtualServoSimConfig) -> robot_utils::servo::sim::FeetechBusSim {
    let mut sim = robot_utils::servo::sim::FeetechBusSim::new();
    for id in config.first_servo_id..=config.last_servo_id {
        sim.add_servo(id);
    }
    sim
}

#[cfg(unix)]
fn run_virtual_servo_sim_loop(
    mut port: robot_utils::servo::protocol::virtual_uart::VirtualUartPort,
    config: VirtualServoSimConfig,
    stop: Option<Arc<AtomicBool>>,
) {
    use std::time::Duration;

    use robot_utils::servo::protocol::port_handler::PortHandler;

    let mut sim = create_virtual_servo_bus_sim(config);
    let mut buffer: Vec<u8> = Vec::new();
    let mut last_step = std::time::Instant::now();

    loop {
        if let Some(stop_flag) = &stop {
            if stop_flag.load(Ordering::Relaxed) {
                break;
            }
        }

        let mut data = port.read_port(512);
        let has_io = !data.is_empty();
        if has_io {
            buffer.append(&mut data);
        }

        let frames = extract_frames(&mut buffer);
        for frame in frames {
            match sim.handle_frame(&frame) {
                Ok(Some(response)) => {
                    let _ = port.write_port(&response);
                }
                Ok(None) => {}
                Err(_) => {}
            }
        }

        let now = std::time::Instant::now();
        let dt = (now - last_step).as_secs_f32();
        if dt >= config.step_seconds {
            sim.step(dt);
            last_step = now;
        }

        if !has_io {
            thread::sleep(Duration::from_millis(config.idle_sleep_ms));
        }
    }
}

#[cfg(unix)]
pub fn spawn_virtual_servo_sim(
    config: VirtualServoSimConfig,
) -> std::io::Result<VirtualServoSimHandle> {
    use robot_utils::servo::protocol::virtual_uart::VirtualUartPort;

    validate_virtual_servo_sim_config(config)?;

    let port = VirtualUartPort::new()?;
    let slave_path = port.slave_path().to_string();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_in_thread = Arc::clone(&stop);

    let join = thread::spawn(move || {
        run_virtual_servo_sim_loop(port, config, Some(stop_in_thread));
    });

    Ok(VirtualServoSimHandle {
        stop,
        join: Some(join),
        slave_path,
    })
}

#[cfg(unix)]
pub fn run_virtual_servo_sim(config: VirtualServoSimConfig) -> std::io::Result<()> {
    use robot_utils::servo::protocol::virtual_uart::VirtualUartPort;

    validate_virtual_servo_sim_config(config)?;

    let port = VirtualUartPort::new()?;
    let slave_path = port.slave_path().to_string();

    println!("Virtual servo bus ready.");
    println!("Slave device: {}", slave_path);
    println!(
        "Servo IDs: {}..={}",
        config.first_servo_id, config.last_servo_id
    );
    println!("Press Ctrl-C to stop.");

    run_virtual_servo_sim_loop(port, config, None);
    Ok(())
}

#[cfg(not(unix))]
pub fn run_virtual_servo_sim(_config: VirtualServoSimConfig) -> std::io::Result<()> {
    eprintln!("robot_dreams is only supported on Unix-like systems.");
    Ok(())
}

#[cfg(unix)]
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

        let frame: Vec<u8> = buffer.drain(0..total_len).collect();
        frames.push(frame);
    }

    frames
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::ffi::CString;
    use std::io;
    use std::os::unix::io::RawFd;
    use std::thread;
    use std::time::{Duration, Instant};

    use robot_utils::servo::protocol::port_handler::PortHandler;
    use robot_utils::servo::protocol::stservo_def::COMM_SUCCESS;
    use robot_utils::servo::scscl::{SCSCL_GOAL_POSITION_L, Scscl};

    struct UnixRawTestPort {
        fd: RawFd,
        baudrate: u32,
        tx_time_per_byte_ms: f64,
        packet_start_time: Instant,
        packet_timeout_ms: f64,
        start_time: Instant,
    }

    impl UnixRawTestPort {
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

    impl Drop for UnixRawTestPort {
        fn drop(&mut self) {
            unsafe {
                libc::close(self.fd);
            }
        }
    }

    impl PortHandler for UnixRawTestPort {
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
                let written = unsafe {
                    libc::write(
                        self.fd,
                        slice.as_ptr() as *const libc::c_void,
                        slice.len(),
                    )
                };
                if written > 0 {
                    total += written as usize;
                    continue;
                }
                if written == 0 {
                    break;
                }

                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::WouldBlock
                    || err.kind() == io::ErrorKind::Interrupted
                {
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

    fn angle_deg_to_ticks(angle_deg: f64) -> u16 {
        (((angle_deg + 180.0) / 360.0) * 4095.0)
            .round()
            .clamp(0.0, 4095.0) as u16
    }

    #[test]
    fn virtual_servo_bus_e2e_receives_angle_targets() {
        let mut sim = spawn_virtual_servo_sim(VirtualServoSimConfig {
            first_servo_id: 1,
            last_servo_id: 4,
            step_seconds: 0.002,
            idle_sleep_ms: 1,
        })
        .expect("spawn virtual servo sim");

        let mut client = {
            let deadline = Instant::now() + Duration::from_secs(1);
            loop {
                match UnixRawTestPort::open(sim.slave_path(), 1_000_000) {
                    Ok(port) => break Scscl::new(port),
                    Err(_) if Instant::now() < deadline => thread::sleep(Duration::from_millis(20)),
                    Err(err) => panic!("open client on {}: {err}", sim.slave_path()),
                }
            }
        };

        for id in 1..=4 {
            let (_model, result, error) = client.handler.ping(id);
            assert_eq!(
                result, COMM_SUCCESS,
                "ping failed for id {id} with error byte {error}"
            );
        }
        let (_model, missing_result, _missing_error) = client.handler.ping(5);
        assert_ne!(
            missing_result, COMM_SUCCESS,
            "servo 5 unexpectedly responded outside configured range"
        );

        let commands = [(1_u8, 80.0), (2_u8, -45.0), (3_u8, 20.0), (4_u8, 5.0)];
        for (id, angle_deg) in commands {
            let target_ticks = angle_deg_to_ticks(angle_deg);
            let (write_result, write_error) = client.write_pos(id, target_ticks, 0, 1500);
            assert_eq!(
                write_result, COMM_SUCCESS,
                "write failed for id {id} target {target_ticks} (error byte {write_error})"
            );

            let (goal_ticks, read_result, read_error) =
                client.handler.read_2byte_tx_rx(id, SCSCL_GOAL_POSITION_L);
            assert_eq!(
                read_result, COMM_SUCCESS,
                "goal read failed for id {id} (error byte {read_error})"
            );
            assert_eq!(
                goal_ticks, target_ticks,
                "goal register mismatch for id {id}"
            );
            let (_present, _speed, state_result, state_error) = client.read_pos_speed(id);
            assert_eq!(
                state_result, COMM_SUCCESS,
                "state read failed for id {id} (error byte {state_error})"
            );
        }

        sim.stop();
    }
}
