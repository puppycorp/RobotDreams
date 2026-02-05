use std::collections::HashMap;

use crate::servo::protocol::feetech_scs::{FeetechScs, Instruction, Response};
use crate::servo::protocol::serial_bus::{BusServoProtocol, ProtocolError};

const REGISTER_COUNT: usize = 256;
const ERROR_RANGE: u8 = 0x10;

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
}

impl FeetechServo {
    pub fn new() -> Self {
        Self {
            registers: [0u8; REGISTER_COUNT],
            pending_writes: Vec::new(),
        }
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
        self.servos.entry(id).or_insert_with(FeetechServo::new);
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
