use std::error::Error;
use std::path::Path;

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
pub fn run_virtual_servo_sim(config: VirtualServoSimConfig) -> std::io::Result<()> {
    use std::thread;
    use std::time::Duration;

    use robot_utils::servo::protocol::port_handler::PortHandler;
    use robot_utils::servo::protocol::virtual_uart::VirtualUartPort;
    use robot_utils::servo::sim::FeetechBusSim;

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

    let mut sim = FeetechBusSim::new();
    let servo_ids = config.first_servo_id..=config.last_servo_id;
    for id in servo_ids.clone() {
        sim.add_servo(id);
    }

    let mut port = VirtualUartPort::new()?;

    println!("Virtual servo bus ready.");
    println!("Slave device: {}", port.slave_path());
    println!(
        "Servo IDs: {}..={}",
        config.first_servo_id, config.last_servo_id
    );
    println!("Press Ctrl-C to stop.");

    let mut buffer: Vec<u8> = Vec::new();

    let mut last_step = std::time::Instant::now();

    loop {
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
