pub const BROADCAST_ID: u8 = 0xFE;
pub const MAX_ID: u8 = 0xFC;
pub const STS_END: u8 = 0;

pub const INST_PING: u8 = 0x01;
pub const INST_READ: u8 = 0x02;
pub const INST_WRITE: u8 = 0x03;
pub const INST_REG_WRITE: u8 = 0x04;
pub const INST_ACTION: u8 = 0x05;
pub const INST_SYNC_READ: u8 = 0x82;
pub const INST_SYNC_WRITE: u8 = 0x83;

pub const TXPACKET_MAX_LEN: usize = 250;
pub const RXPACKET_MAX_LEN: usize = 250;

pub const ERRBIT_VOLTAGE: u8 = 1;
pub const ERRBIT_ANGLE: u8 = 2;
pub const ERRBIT_OVERHEAT: u8 = 4;
pub const ERRBIT_OVERELE: u8 = 8;
pub const ERRBIT_OVERLOAD: u8 = 32;

pub const COMM_SUCCESS: i32 = 0;
pub const COMM_PORT_BUSY: i32 = -1;
pub const COMM_TX_FAIL: i32 = -2;
pub const COMM_RX_FAIL: i32 = -3;
pub const COMM_TX_ERROR: i32 = -4;
pub const COMM_RX_WAITING: i32 = -5;
pub const COMM_RX_TIMEOUT: i32 = -6;
pub const COMM_RX_CORRUPT: i32 = -7;
pub const COMM_NOT_AVAILABLE: i32 = -9;

pub const PKT_HEADER0: usize = 0;
pub const PKT_HEADER1: usize = 1;
pub const PKT_ID: usize = 2;
pub const PKT_LENGTH: usize = 3;
pub const PKT_INSTRUCTION: usize = 4;
pub const PKT_ERROR: usize = 4;
pub const PKT_PARAMETER0: usize = 5;
