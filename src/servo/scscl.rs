use crate::servo::protocol::group_sync_write::GroupSyncWrite;
use crate::servo::protocol::port_handler::PortHandler;
use crate::servo::protocol::protocol_packet_handler::ProtocolPacketHandler;
use crate::servo::protocol::stservo_def::BROADCAST_ID;

pub const SCSCL_1M: u8 = 0;
pub const SCSCL_0_5M: u8 = 1;
pub const SCSCL_250K: u8 = 2;
pub const SCSCL_128K: u8 = 3;
pub const SCSCL_115200: u8 = 4;
pub const SCSCL_76800: u8 = 5;
pub const SCSCL_57600: u8 = 6;
pub const SCSCL_38400: u8 = 7;

pub const SCSCL_MODEL_L: u8 = 3;
pub const SCSCL_MODEL_H: u8 = 4;

pub const SCSCL_ID: u8 = 5;
pub const SCSCL_BAUD_RATE: u8 = 6;
pub const SCSCL_MIN_ANGLE_LIMIT_L: u8 = 9;
pub const SCSCL_MIN_ANGLE_LIMIT_H: u8 = 10;
pub const SCSCL_MAX_ANGLE_LIMIT_L: u8 = 11;
pub const SCSCL_MAX_ANGLE_LIMIT_H: u8 = 12;
pub const SCSCL_CW_DEAD: u8 = 26;
pub const SCSCL_CCW_DEAD: u8 = 27;

pub const SCSCL_TORQUE_ENABLE: u8 = 40;
pub const SCSCL_GOAL_POSITION_L: u8 = 42;
pub const SCSCL_GOAL_POSITION_H: u8 = 43;
pub const SCSCL_GOAL_TIME_L: u8 = 44;
pub const SCSCL_GOAL_TIME_H: u8 = 45;
pub const SCSCL_GOAL_SPEED_L: u8 = 46;
pub const SCSCL_GOAL_SPEED_H: u8 = 47;
pub const SCSCL_LOCK: u8 = 48;

pub const SCSCL_PRESENT_POSITION_L: u8 = 56;
pub const SCSCL_PRESENT_POSITION_H: u8 = 57;
pub const SCSCL_PRESENT_SPEED_L: u8 = 58;
pub const SCSCL_PRESENT_SPEED_H: u8 = 59;
pub const SCSCL_PRESENT_LOAD_L: u8 = 60;
pub const SCSCL_PRESENT_LOAD_H: u8 = 61;
pub const SCSCL_PRESENT_VOLTAGE: u8 = 62;
pub const SCSCL_PRESENT_TEMPERATURE: u8 = 63;
pub const SCSCL_MOVING: u8 = 66;
pub const SCSCL_PRESENT_CURRENT_L: u8 = 69;
pub const SCSCL_PRESENT_CURRENT_H: u8 = 70;

#[derive(Debug)]
pub struct Scscl<P: PortHandler> {
    pub handler: ProtocolPacketHandler<P>,
    pub group_sync_write: GroupSyncWrite,
}

impl<P: PortHandler> Scscl<P> {
    pub fn new(port: P) -> Self {
        Self {
            handler: ProtocolPacketHandler::new(port, 1),
            group_sync_write: GroupSyncWrite::new(SCSCL_GOAL_POSITION_L, 6),
        }
    }

    pub fn write_pos(&mut self, scs_id: u8, position: u16, time: u16, speed: u16) -> (i32, u8) {
        let txpacket = [
            self.handler.scs_lobyte(position),
            self.handler.scs_hibyte(position),
            self.handler.scs_lobyte(time),
            self.handler.scs_hibyte(time),
            self.handler.scs_lobyte(speed),
            self.handler.scs_hibyte(speed),
        ];
        self.handler
            .write_tx_rx(scs_id, SCSCL_GOAL_POSITION_L, txpacket.len() as u8, &txpacket)
    }

    pub fn read_pos(&mut self, scs_id: u8) -> (u16, i32, u8) {
        self.handler.read_2byte_tx_rx(scs_id, SCSCL_PRESENT_POSITION_L)
    }

    pub fn read_speed(&mut self, scs_id: u8) -> (i32, i32, u8) {
        let (speed, result, error) = self.handler.read_2byte_tx_rx(scs_id, SCSCL_PRESENT_SPEED_L);
        (self.handler.scs_tohost(speed, 15), result, error)
    }

    pub fn read_pos_speed(&mut self, scs_id: u8) -> (u16, i32, i32, u8) {
        let (value, result, error) = self.handler.read_4byte_tx_rx(scs_id, SCSCL_PRESENT_POSITION_L);
        let pos = self.handler.scs_loword(value);
        let speed = self.handler.scs_hiword(value);
        (pos, self.handler.scs_tohost(speed, 15), result, error)
    }

    pub fn read_moving(&mut self, scs_id: u8) -> (u8, i32, u8) {
        self.handler.read_1byte_tx_rx(scs_id, SCSCL_MOVING)
    }

    pub fn sync_write_pos(&mut self, scs_id: u8, position: u16, time: u16, speed: u16) -> bool {
        let txpacket = [
            self.handler.scs_lobyte(position),
            self.handler.scs_hibyte(position),
            self.handler.scs_lobyte(time),
            self.handler.scs_hibyte(time),
            self.handler.scs_lobyte(speed),
            self.handler.scs_hibyte(speed),
        ];
        self.group_sync_write.add_param(scs_id, &txpacket)
    }

    pub fn reg_write_pos(&mut self, scs_id: u8, position: u16, time: u16, speed: u16) -> (i32, u8) {
        let txpacket = [
            self.handler.scs_lobyte(position),
            self.handler.scs_hibyte(position),
            self.handler.scs_lobyte(time),
            self.handler.scs_hibyte(time),
            self.handler.scs_lobyte(speed),
            self.handler.scs_hibyte(speed),
        ];
        self.handler
            .reg_write_tx_rx(scs_id, SCSCL_GOAL_POSITION_L, txpacket.len() as u8, &txpacket)
    }

    pub fn reg_action(&mut self) -> i32 {
        self.handler.action(BROADCAST_ID)
    }

    pub fn pwm_mode(&mut self, scs_id: u8) -> (i32, u8) {
        let txpacket = [0, 0, 0, 0];
        self.handler
            .write_tx_rx(scs_id, SCSCL_MIN_ANGLE_LIMIT_L, txpacket.len() as u8, &txpacket)
    }

    pub fn write_pwm(&mut self, scs_id: u8, time: i32) -> (i32, u8) {
        let value = self.handler.scs_toscs(time, 10);
        self.handler.write_2byte_tx_rx(scs_id, SCSCL_GOAL_TIME_L, value)
    }

    pub fn lock_eprom(&mut self, scs_id: u8) -> (i32, u8) {
        self.handler.write_1byte_tx_rx(scs_id, SCSCL_LOCK, 1)
    }

    pub fn unlock_eprom(&mut self, scs_id: u8) -> (i32, u8) {
        self.handler.write_1byte_tx_rx(scs_id, SCSCL_LOCK, 0)
    }
}
