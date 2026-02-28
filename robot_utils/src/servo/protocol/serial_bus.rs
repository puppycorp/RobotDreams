#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decoded<I> {
    pub id: u8,
    pub instruction: I,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    BadHeader,
    TooShort,
    LengthMismatch,
    ChecksumMismatch,
    Malformed,
    UnsupportedInstruction,
}

pub trait BusServoProtocol {
    type Instruction;
    type Response;

    fn decode(&self, bytes: &[u8]) -> Result<Decoded<Self::Instruction>, ProtocolError>;
    fn encode_response(&self, response: Self::Response) -> Vec<u8>;

    fn is_broadcast(&self, id: u8) -> bool {
        id == 0xFE
    }
}
