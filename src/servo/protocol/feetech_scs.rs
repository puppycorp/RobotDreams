use crate::servo::protocol::serial_bus::{BusServoProtocol, Decoded, ProtocolError};

const HEADER_BYTE: u8 = 0xFF;
const INSTRUCTION_PING: u8 = 0x01;
const INSTRUCTION_READ: u8 = 0x02;
const INSTRUCTION_WRITE: u8 = 0x03;
const INSTRUCTION_REG_WRITE: u8 = 0x04;
const INSTRUCTION_ACTION: u8 = 0x05;
const INSTRUCTION_SYNC_READ: u8 = 0x82;
const INSTRUCTION_SYNC_WRITE: u8 = 0x83;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Instruction {
    Ping,
    Read { address: u8, length: u8 },
    Write { address: u8, data: Vec<u8> },
    RegWrite { address: u8, data: Vec<u8> },
    Action,
    SyncRead { address: u8, length: u8, ids: Vec<u8> },
    SyncWrite { address: u8, length: u8, writes: Vec<(u8, Vec<u8>)> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    Status { id: u8, error: u8, params: Vec<u8> },
}

#[derive(Debug, Default, Clone, Copy)]
pub struct FeetechScs;

impl FeetechScs {
    pub fn encode_instruction(&self, id: u8, instruction: Instruction) -> Vec<u8> {
        let (instruction_byte, params) = match instruction {
            Instruction::Ping => (INSTRUCTION_PING, Vec::new()),
            Instruction::Read { address, length } => {
                (INSTRUCTION_READ, vec![address, length])
            }
            Instruction::Write { address, data } => {
                let mut params = Vec::with_capacity(1 + data.len());
                params.push(address);
                params.extend_from_slice(&data);
                (INSTRUCTION_WRITE, params)
            }
            Instruction::RegWrite { address, data } => {
                let mut params = Vec::with_capacity(1 + data.len());
                params.push(address);
                params.extend_from_slice(&data);
                (INSTRUCTION_REG_WRITE, params)
            }
            Instruction::Action => (INSTRUCTION_ACTION, Vec::new()),
            Instruction::SyncRead { address, length, ids } => {
                let mut params = Vec::with_capacity(2 + ids.len());
                params.push(address);
                params.push(length);
                params.extend_from_slice(&ids);
                (INSTRUCTION_SYNC_READ, params)
            }
            Instruction::SyncWrite { address, length, writes } => {
                let mut params = Vec::new();
                params.push(address);
                params.push(length);
                for (id, data) in writes {
                    params.push(id);
                    params.extend_from_slice(&data);
                }
                (INSTRUCTION_SYNC_WRITE, params)
            }
        };

        let length = (params.len() + 2) as u8;
        let checksum = checksum(id, length, instruction_byte, &params);

        let mut frame = Vec::with_capacity(6 + params.len());
        frame.push(HEADER_BYTE);
        frame.push(HEADER_BYTE);
        frame.push(id);
        frame.push(length);
        frame.push(instruction_byte);
        frame.extend_from_slice(&params);
        frame.push(checksum);
        frame
    }

    pub fn decode_response(&self, bytes: &[u8]) -> Result<Response, ProtocolError> {
        let (id, length, error, params) = parse_packet(bytes)?;

        if length != params.len() + 2 {
            return Err(ProtocolError::LengthMismatch);
        }

        let computed = checksum(id, length as u8, error, params);
        if computed != bytes[bytes.len() - 1] {
            return Err(ProtocolError::ChecksumMismatch);
        }

        Ok(Response::Status {
            id,
            error,
            params: params.to_vec(),
        })
    }
}

impl BusServoProtocol for FeetechScs {
    type Instruction = Instruction;
    type Response = Response;

    fn decode(&self, bytes: &[u8]) -> Result<Decoded<Self::Instruction>, ProtocolError> {
        let (id, length, instruction, params) = parse_packet(bytes)?;
        let checksum_byte = bytes[bytes.len() - 1];

        if length != params.len() + 2 {
            return Err(ProtocolError::LengthMismatch);
        }

        let computed = checksum(id, length as u8, instruction, params);
        if computed != checksum_byte {
            return Err(ProtocolError::ChecksumMismatch);
        }

        let instruction = match instruction {
            INSTRUCTION_PING => Instruction::Ping,
            INSTRUCTION_READ => {
                if params.len() != 2 {
                    return Err(ProtocolError::Malformed);
                }
                Instruction::Read {
                    address: params[0],
                    length: params[1],
                }
            }
            INSTRUCTION_WRITE => {
                if params.is_empty() {
                    return Err(ProtocolError::Malformed);
                }
                Instruction::Write {
                    address: params[0],
                    data: params[1..].to_vec(),
                }
            }
            INSTRUCTION_REG_WRITE => {
                if params.is_empty() {
                    return Err(ProtocolError::Malformed);
                }
                Instruction::RegWrite {
                    address: params[0],
                    data: params[1..].to_vec(),
                }
            }
            INSTRUCTION_ACTION => {
                if !params.is_empty() {
                    return Err(ProtocolError::Malformed);
                }
                Instruction::Action
            }
            INSTRUCTION_SYNC_READ => {
                if params.len() < 2 {
                    return Err(ProtocolError::Malformed);
                }
                Instruction::SyncRead {
                    address: params[0],
                    length: params[1],
                    ids: params[2..].to_vec(),
                }
            }
            INSTRUCTION_SYNC_WRITE => {
                if params.len() < 2 {
                    return Err(ProtocolError::Malformed);
                }
                let address = params[0];
                let length = params[1];
                let chunk_len = length as usize + 1;
                let tail = &params[2..];
                if chunk_len == 0 || tail.len() % chunk_len != 0 {
                    return Err(ProtocolError::Malformed);
                }
                let mut writes = Vec::new();
                for chunk in tail.chunks(chunk_len) {
                    let id = chunk[0];
                    let data = chunk[1..].to_vec();
                    writes.push((id, data));
                }
                Instruction::SyncWrite {
                    address,
                    length,
                    writes,
                }
            }
            _ => return Err(ProtocolError::UnsupportedInstruction),
        };

        Ok(Decoded { id, instruction })
    }

    fn encode_response(&self, response: Self::Response) -> Vec<u8> {
        match response {
            Response::Status { id, error, params } => {
                let length = (params.len() + 2) as u8;
                let checksum = checksum(id, length, error, &params);

                let mut frame = Vec::with_capacity(6 + params.len());
                frame.push(HEADER_BYTE);
                frame.push(HEADER_BYTE);
                frame.push(id);
                frame.push(length);
                frame.push(error);
                frame.extend_from_slice(&params);
                frame.push(checksum);
                frame
            }
        }
    }
}

fn parse_packet(bytes: &[u8]) -> Result<(u8, usize, u8, &[u8]), ProtocolError> {
    if bytes.len() < 6 {
        return Err(ProtocolError::TooShort);
    }
    if bytes[0] != HEADER_BYTE || bytes[1] != HEADER_BYTE {
        return Err(ProtocolError::BadHeader);
    }

    let id = bytes[2];
    let length = bytes[3] as usize;
    if length < 2 {
        return Err(ProtocolError::LengthMismatch);
    }

    let expected_len = length + 4;
    if bytes.len() != expected_len {
        return Err(ProtocolError::LengthMismatch);
    }

    let instruction_or_error = bytes[4];
    let params = &bytes[5..bytes.len() - 1];

    Ok((id, length, instruction_or_error, params))
}

fn checksum(id: u8, length: u8, instruction_or_error: u8, params: &[u8]) -> u8 {
    let mut sum = id.wrapping_add(length).wrapping_add(instruction_or_error);
    for byte in params {
        sum = sum.wrapping_add(*byte);
    }
    !sum
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_ping_roundtrip() {
        let protocol = FeetechScs;
        let frame = protocol.encode_instruction(1, Instruction::Ping);
        let decoded = protocol.decode(&frame).expect("decode ping");
        assert_eq!(decoded.id, 1);
        assert_eq!(decoded.instruction, Instruction::Ping);
    }
}
