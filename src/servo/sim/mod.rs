use std::collections::HashMap;

use crate::servo::protocol::feetech_scs::{FeetechScs, Instruction, Response};
use crate::servo::protocol::serial_bus::{BusServoProtocol, ProtocolError};

const REGISTER_COUNT: usize = 256;
const ERROR_RANGE: u8 = 0x10;

// ST3215 register addresses (little-endian for multi-byte values).
const REG_ID: usize = 0x05;
const REG_BAUD: usize = 0x06;
const REG_MIN_ANGLE: usize = 0x09;
const REG_MAX_ANGLE: usize = 0x0B;
const REG_MAX_TORQUE: usize = 0x10;
const REG_MODE: usize = 0x21;
const REG_TORQUE_SWITCH: usize = 0x28;
const REG_ACCEL: usize = 0x29;
const REG_TARGET_POS: usize = 0x2A;
const REG_TARGET_SPEED: usize = 0x2E;
const REG_TORQUE_LIMIT: usize = 0x30;
const REG_PRESENT_POS: usize = 0x38;
const REG_PRESENT_SPEED: usize = 0x3A;
const REG_PRESENT_LOAD: usize = 0x3C;
const REG_PRESENT_VOLT: usize = 0x3E;
const REG_PRESENT_TEMP: usize = 0x3F;
const REG_ASYNC_FLAG: usize = 0x40;
const REG_SERVO_STATUS: usize = 0x41;
const REG_MOVING: usize = 0x42;
const REG_PRESENT_CURRENT: usize = 0x45;

#[derive(Debug, Clone, Copy)]
pub struct VirtualServo {
    angle_rad: f32,
    angular_velocity: f32,
    inertia: f32,
    damping: f32,
    pending_torque: f32,
}

impl VirtualServo {
    pub fn new(inertia: f32) -> Self {
        let inertia = if inertia > 0.0 { inertia } else { 1.0 };
        Self {
            angle_rad: 0.0,
            angular_velocity: 0.0,
            inertia,
            damping: 0.0,
            pending_torque: 0.0,
        }
    }

    pub fn set_damping(&mut self, damping: f32) {
        self.damping = if damping < 0.0 { 0.0 } else { damping };
    }

    pub fn angle_rad(&self) -> f32 {
        self.angle_rad
    }

    pub fn angular_velocity(&self) -> f32 {
        self.angular_velocity
    }

    pub fn apply_rotational_force(&mut self, torque: f32) {
        self.pending_torque += torque;
    }

    pub fn counteract_physics_force(&mut self, physics_torque: f32) {
        self.pending_torque -= physics_torque;
    }

    pub fn step(&mut self, dt: f32) {
        if dt <= 0.0 {
            self.pending_torque = 0.0;
            return;
        }
        let accel = self.pending_torque / self.inertia;
        self.angular_velocity += accel * dt;
        if self.damping > 0.0 {
            let decay = (1.0 - self.damping * dt).clamp(0.0, 1.0);
            self.angular_velocity *= decay;
        }
        self.angle_rad += self.angular_velocity * dt;
        self.pending_torque = 0.0;
    }
}

#[derive(Debug, Clone)]
pub struct FeetechServo {
    registers: [u8; REGISTER_COUNT],
    pending_writes: Vec<(u8, Vec<u8>)>,
    position_steps: f32,
    velocity_steps_s: f32,
    last_velocity_steps_s: f32,
    temperature_c: f32,
}

#[derive(Debug, Clone)]
pub struct FeetechServoSnapshot {
    pub id: u8,
    pub mode: u8,
    pub torque_enabled: bool,
    pub target_position: i16,
    pub present_position: i16,
    pub present_speed: i16,
    pub present_load: i16,
    pub moving: bool,
    pub temperature_c: u8,
    pub current_raw: u16,
}

impl FeetechServo {
    pub fn new() -> Self {
        Self {
            registers: [0u8; REGISTER_COUNT],
            pending_writes: Vec::new(),
            position_steps: 0.0,
            velocity_steps_s: 0.0,
            last_velocity_steps_s: 0.0,
            temperature_c: 30.0,
        }
    }

    fn init_defaults(&mut self, id: u8) {
        self.registers[REG_ID] = id;
        self.registers[REG_BAUD] = 0; // 1,000,000 by default
        write_u16_le(&mut self.registers, REG_MIN_ANGLE, 0);
        write_u16_le(&mut self.registers, REG_MAX_ANGLE, 4095);
        write_u16_le(&mut self.registers, REG_MAX_TORQUE, 1000);
        self.registers[REG_MODE] = 0; // position mode
        self.registers[REG_TORQUE_SWITCH] = 1; // enabled by default for simulation
        self.registers[REG_ACCEL] = 0;
        write_u16_le(&mut self.registers, REG_TARGET_POS, 0);
        write_u16_le(&mut self.registers, REG_TARGET_SPEED, 0);
        write_u16_le(&mut self.registers, REG_TORQUE_LIMIT, 1000);
        write_u16_le(&mut self.registers, REG_PRESENT_POS, 0);
        write_u16_le(&mut self.registers, REG_PRESENT_SPEED, 0);
        write_u16_le(&mut self.registers, REG_PRESENT_LOAD, 0);
        self.registers[REG_PRESENT_VOLT] = 74; // 7.4V -> 0.1V units
        self.registers[REG_PRESENT_TEMP] = 30;
        self.registers[REG_ASYNC_FLAG] = 0;
        self.registers[REG_SERVO_STATUS] = 0;
        self.registers[REG_MOVING] = 0;
        write_u16_le(&mut self.registers, REG_PRESENT_CURRENT, 0);
    }

    fn read(&self, address: u8, length: u8) -> Result<Vec<u8>, ()> {
        let start = address as usize;
        let end = start + length as usize;
        if end > self.registers.len() {
            return Err(());
        }
        Ok(self.registers[start..end].to_vec())
    }

    fn write(&mut self, address: u8, data: &[u8]) -> bool {
        let start = address as usize;
        let end = start + data.len();
        if end > self.registers.len() {
            return false;
        }
        self.registers[start..end].copy_from_slice(data);
        true
    }

    fn queue_write(&mut self, address: u8, data: &[u8]) {
        self.pending_writes.push((address, data.to_vec()));
    }

    fn apply_pending(&mut self) {
        let pending = std::mem::take(&mut self.pending_writes);
        for (address, data) in pending {
            let _ = self.write(address, &data);
        }
    }

    fn update_sim(&mut self, dt: f32) {
        if dt <= 0.0 {
            return;
        }

        let torque_enabled = self.registers[REG_TORQUE_SWITCH] != 0;
        if !torque_enabled {
            self.velocity_steps_s = 0.0;
            self.registers[REG_MOVING] = 0;
            write_u16_le(&mut self.registers, REG_PRESENT_SPEED, 0);
            write_u16_le(&mut self.registers, REG_PRESENT_LOAD, 0);
            write_u16_le(&mut self.registers, REG_PRESENT_CURRENT, 0);
            self.temperature_c = cool_temperature(self.temperature_c, dt);
            self.registers[REG_PRESENT_TEMP] = self.temperature_c.round().clamp(0.0, 255.0) as u8;
            return;
        }

        let mode = self.registers[REG_MODE];
        if mode != 0 {
            // Only position mode is simulated for now.
            return;
        }

        let target = read_i16_le(&self.registers, REG_TARGET_POS) as f32;
        let mut min_angle = read_i16_le(&self.registers, REG_MIN_ANGLE) as f32;
        let mut max_angle = read_i16_le(&self.registers, REG_MAX_ANGLE) as f32;
        if min_angle == 0.0 && max_angle == 0.0 {
            min_angle = 0.0;
            max_angle = 4095.0;
        }
        let target = target.clamp(min_angle, max_angle);

        let max_speed = read_u16_le(&self.registers, REG_TARGET_SPEED) as f32;
        let max_speed = if max_speed <= 0.0 { 1000.0 } else { max_speed };
        let accel = self.registers[REG_ACCEL] as f32 * 100.0;
        let accel = if accel <= 0.0 { 1000.0 } else { accel };

        let delta = target - self.position_steps;
        if delta.abs() < 0.5 && self.velocity_steps_s.abs() < 0.5 {
            self.position_steps = target;
            self.velocity_steps_s = 0.0;
        } else {
            let desired_vel = delta.signum() * max_speed;
            let dv = (desired_vel - self.velocity_steps_s).clamp(-accel * dt, accel * dt);
            self.velocity_steps_s += dv;
            let prev_pos = self.position_steps;
            self.position_steps += self.velocity_steps_s * dt;
            if (target - prev_pos).signum() != (target - self.position_steps).signum() {
                self.position_steps = target;
                self.velocity_steps_s = 0.0;
            }
        }

        let accel_steps_s2 = (self.velocity_steps_s - self.last_velocity_steps_s) / dt;
        self.last_velocity_steps_s = self.velocity_steps_s;
        let position_error = (target - self.position_steps).abs();

        let torque_demand = (position_error * 0.05) + accel_steps_s2.abs() * 0.002;
        let current_raw = (torque_demand * 300.0).clamp(0.0, 1000.0);
        let load_raw = (torque_demand * 400.0).clamp(0.0, 1000.0);

        write_u16_le(
            &mut self.registers,
            REG_PRESENT_CURRENT,
            current_raw.round() as u16,
        );
        write_u16_le(
            &mut self.registers,
            REG_PRESENT_LOAD,
            encode_signed_speed(if self.velocity_steps_s < 0.0 { -load_raw } else { load_raw }),
        );

        self.temperature_c = heat_temperature(self.temperature_c, current_raw, dt);
        self.registers[REG_PRESENT_TEMP] = self.temperature_c.round().clamp(0.0, 255.0) as u8;

        write_u16_le(
            &mut self.registers,
            REG_PRESENT_POS,
            self.position_steps.round().clamp(-32767.0, 32767.0) as i16 as u16,
        );
        write_u16_le(
            &mut self.registers,
            REG_PRESENT_SPEED,
            encode_signed_speed(self.velocity_steps_s),
        );
        self.registers[REG_MOVING] = if self.velocity_steps_s.abs() > 0.5 { 1 } else { 0 };
    }
}

#[derive(Debug, Default)]
pub struct FeetechBusSim {
    protocol: FeetechScs,
    servos: HashMap<u8, FeetechServo>,
}

impl FeetechBusSim {
    pub fn new() -> Self {
        Self {
            protocol: FeetechScs,
            servos: HashMap::new(),
        }
    }

    pub fn add_servo(&mut self, id: u8) {
        self.servos.entry(id).or_insert_with(|| {
            let mut servo = FeetechServo::new();
            servo.init_defaults(id);
            servo
        });
    }

    pub fn remove_servo(&mut self, id: u8) -> bool {
        self.servos.remove(&id).is_some()
    }

    pub fn set_servo_count(&mut self, count: u8) {
        let count = count.clamp(1, 32);
        self.servos.retain(|id, _| *id <= count);
        for id in 1..=count {
            self.add_servo(id);
        }
    }

    pub fn set_target_position(&mut self, id: u8, target: i16) -> bool {
        let Some(servo) = self.servos.get_mut(&id) else {
            return false;
        };
        write_u16_le(&mut servo.registers, REG_TARGET_POS, target as u16);
        true
    }

    pub fn servo_snapshots(&self) -> Vec<FeetechServoSnapshot> {
        let mut ids: Vec<u8> = self.servos.keys().copied().collect();
        ids.sort_unstable();
        ids.into_iter()
            .filter_map(|id| {
                self.servos.get(&id).map(|servo| FeetechServoSnapshot {
                    id,
                    mode: servo.registers[REG_MODE],
                    torque_enabled: servo.registers[REG_TORQUE_SWITCH] != 0,
                    target_position: read_i16_le(&servo.registers, REG_TARGET_POS),
                    present_position: read_i16_le(&servo.registers, REG_PRESENT_POS),
                    present_speed: decode_signed_speed(read_u16_le(&servo.registers, REG_PRESENT_SPEED)),
                    present_load: decode_signed_speed(read_u16_le(&servo.registers, REG_PRESENT_LOAD)),
                    moving: servo.registers[REG_MOVING] != 0,
                    temperature_c: servo.registers[REG_PRESENT_TEMP],
                    current_raw: read_u16_le(&servo.registers, REG_PRESENT_CURRENT),
                })
            })
            .collect()
    }

    pub fn step(&mut self, dt: f32) {
        for servo in self.servos.values_mut() {
            servo.update_sim(dt);
        }
    }

    pub fn handle_frame(&mut self, frame: &[u8]) -> Result<Option<Vec<u8>>, ProtocolError> {
        let decoded = self.protocol.decode(frame)?;

        match &decoded.instruction {
            Instruction::SyncRead { address, length, ids } => {
                if !self.protocol.is_broadcast(decoded.id) {
                    return Ok(None);
                }
                let mut frames = Vec::new();
                for id in ids {
                    if let Some(servo) = self.servos.get_mut(id) {
                        let response = Self::read_response(*id, servo, *address, *length);
                        frames.extend_from_slice(&self.protocol.encode_response(response));
                    }
                }
                return Ok(Some(frames));
            }
            Instruction::SyncWrite {
                address,
                length,
                writes,
            } => {
                if !self.protocol.is_broadcast(decoded.id) {
                    return Ok(None);
                }
                let data_len = *length as usize;
                for (id, data) in writes {
                    if data.len() != data_len {
                        continue;
                    }
                    if let Some(servo) = self.servos.get_mut(id) {
                        let _ = servo.write(*address, data);
                    }
                }
                return Ok(None);
            }
            Instruction::Action => {
                if self.protocol.is_broadcast(decoded.id) {
                    for servo in self.servos.values_mut() {
                        servo.apply_pending();
                    }
                    return Ok(None);
                }
            }
            _ => {}
        }

        if self.protocol.is_broadcast(decoded.id) {
            for servo in self.servos.values_mut() {
                Self::apply_instruction(decoded.id, servo, &decoded.instruction);
            }
            return Ok(None);
        }

        let Some(servo) = self.servos.get_mut(&decoded.id) else {
            return Ok(None);
        };

        let response = Self::apply_instruction(decoded.id, servo, &decoded.instruction);
        Ok(response.map(|resp| self.protocol.encode_response(resp)))
    }

    fn apply_instruction(id: u8, servo: &mut FeetechServo, instruction: &Instruction) -> Option<Response> {
        match instruction {
            Instruction::Ping => Some(Response::Status {
                id,
                error: 0,
                params: Vec::new(),
            }),
            Instruction::Read { address, length } => {
                Some(Self::read_response(id, servo, *address, *length))
            }
            Instruction::Write { address, data } => {
                let ok = servo.write(*address, data);
                Some(Response::Status {
                    id,
                    error: if ok { 0 } else { ERROR_RANGE },
                    params: Vec::new(),
                })
            }
            Instruction::RegWrite { address, data } => {
                servo.queue_write(*address, data);
                Some(Response::Status {
                    id,
                    error: 0,
                    params: Vec::new(),
                })
            }
            Instruction::Action => {
                servo.apply_pending();
                Some(Response::Status {
                    id,
                    error: 0,
                    params: Vec::new(),
                })
            }
            Instruction::SyncRead { .. } | Instruction::SyncWrite { .. } => None,
        }
    }

    fn read_response(id: u8, servo: &FeetechServo, address: u8, length: u8) -> Response {
        match servo.read(address, length) {
            Ok(params) => Response::Status {
                id,
                error: 0,
                params,
            },
            Err(()) => Response::Status {
                id,
                error: ERROR_RANGE,
                params: vec![0u8; length as usize],
            },
        }
    }
}

fn read_u16_le(registers: &[u8; REGISTER_COUNT], address: usize) -> u16 {
    let lo = registers[address] as u16;
    let hi = registers[address + 1] as u16;
    lo | (hi << 8)
}

fn read_i16_le(registers: &[u8; REGISTER_COUNT], address: usize) -> i16 {
    let raw = read_u16_le(registers, address);
    if (raw & 0x8000) != 0 {
        -((raw & 0x7FFF) as i16)
    } else {
        raw as i16
    }
}

fn write_u16_le(registers: &mut [u8; REGISTER_COUNT], address: usize, value: u16) {
    registers[address] = (value & 0xFF) as u8;
    registers[address + 1] = (value >> 8) as u8;
}

fn encode_signed_speed(speed: f32) -> u16 {
    if speed < 0.0 {
        let mag = (-speed).round().clamp(0.0, 32767.0) as u16;
        mag | 0x8000
    } else {
        speed.round().clamp(0.0, 32767.0) as u16
    }
}

fn decode_signed_speed(raw: u16) -> i16 {
    let mag = (raw & 0x7FFF) as i16;
    if (raw & 0x8000) != 0 {
        -mag
    } else {
        mag
    }
}

fn heat_temperature(temp_c: f32, current_raw: f32, dt: f32) -> f32 {
    let ambient = 30.0;
    let heat_rate = 0.08;
    let cool_rate = 0.04;
    let current_norm = (current_raw / 1000.0).clamp(0.0, 1.0);
    let dtemp = (heat_rate * current_norm - cool_rate * (temp_c - ambient)) * dt * 60.0;
    (temp_c + dtemp).clamp(ambient, 100.0)
}

fn cool_temperature(temp_c: f32, dt: f32) -> f32 {
    let ambient = 30.0;
    let cool_rate = 0.06;
    let dtemp = -cool_rate * (temp_c - ambient) * dt * 60.0;
    (temp_c + dtemp).clamp(ambient, 100.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::servo::protocol::feetech_scs::Instruction;

    #[test]
    fn write_then_read_registers() {
        let mut sim = FeetechBusSim::new();
        sim.add_servo(1);

        let write_frame = sim
            .protocol
            .encode_instruction(1, Instruction::Write {
                address: 0x10,
                data: vec![0x12, 0x34],
            });

        let response = sim.handle_frame(&write_frame).expect("write");
        assert!(response.is_some());

        let read_frame = sim
            .protocol
            .encode_instruction(1, Instruction::Read {
                address: 0x10,
                length: 2,
            });

        let response = sim.handle_frame(&read_frame).expect("read");
        let response = response.expect("read response");

        let decoded = sim
            .protocol
            .decode_response(&response)
            .expect("decode response");

        match decoded {
            Response::Status { id, error, params } => {
                assert_eq!(id, 1);
                assert_eq!(error, 0);
                assert_eq!(params, vec![0x12, 0x34]);
            }
        }
    }

    #[test]
    fn virtual_servo_counteracts_torque() {
        let mut servo = VirtualServo::new(1.0);
        servo.apply_rotational_force(2.0);
        servo.counteract_physics_force(2.0);
        servo.step(0.1);
        assert!(servo.angular_velocity().abs() < 1e-6);
        assert!(servo.angle_rad().abs() < 1e-6);
    }
}
