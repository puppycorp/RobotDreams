use crate::servo::protocol::group_sync_write::GroupSyncWrite;
use crate::servo::protocol::port_handler::PortHandler;
use crate::servo::protocol::protocol_packet_handler::ProtocolPacketHandler;
use crate::servo::protocol::stservo_def::BROADCAST_ID;

pub const STS_1M: u8 = 0;
pub const STS_0_5M: u8 = 1;
pub const STS_250K: u8 = 2;
pub const STS_128K: u8 = 3;
pub const STS_115200: u8 = 4;
pub const STS_76800: u8 = 5;
pub const STS_57600: u8 = 6;
pub const STS_38400: u8 = 7;

pub const STS_MODEL_L: u8 = 3;
pub const STS_MODEL_H: u8 = 4;

pub const STS_ID: u8 = 5;
pub const STS_BAUD_RATE: u8 = 6;
pub const STS_MIN_ANGLE_LIMIT_L: u8 = 9;
pub const STS_MIN_ANGLE_LIMIT_H: u8 = 10;
pub const STS_MAX_ANGLE_LIMIT_L: u8 = 11;
pub const STS_MAX_ANGLE_LIMIT_H: u8 = 12;
pub const STS_CW_DEAD: u8 = 26;
pub const STS_CCW_DEAD: u8 = 27;
pub const STS_OFS_L: u8 = 31;
pub const STS_OFS_H: u8 = 32;
pub const STS_MODE: u8 = 33;

pub const STS_TORQUE_ENABLE: u8 = 40;
pub const STS_ACC: u8 = 41;
pub const STS_GOAL_POSITION_L: u8 = 42;
pub const STS_GOAL_POSITION_H: u8 = 43;
pub const STS_GOAL_TIME_L: u8 = 44;
pub const STS_GOAL_TIME_H: u8 = 45;
pub const STS_GOAL_SPEED_L: u8 = 46;
pub const STS_GOAL_SPEED_H: u8 = 47;
pub const STS_LOCK: u8 = 55;

pub const STS_PRESENT_POSITION_L: u8 = 56;
pub const STS_PRESENT_POSITION_H: u8 = 57;
pub const STS_PRESENT_SPEED_L: u8 = 58;
pub const STS_PRESENT_SPEED_H: u8 = 59;
pub const STS_PRESENT_LOAD_L: u8 = 60;
pub const STS_PRESENT_LOAD_H: u8 = 61;
pub const STS_PRESENT_VOLTAGE: u8 = 62;
pub const STS_PRESENT_TEMPERATURE: u8 = 63;
pub const STS_MOVING: u8 = 66;
pub const STS_PRESENT_CURRENT_L: u8 = 69;
pub const STS_PRESENT_CURRENT_H: u8 = 70;

#[derive(Debug)]
pub struct Sts<P: PortHandler> {
    pub handler: ProtocolPacketHandler<P>,
    pub group_sync_write: GroupSyncWrite,
}

impl<P: PortHandler> Sts<P> {
    pub fn new(port: P) -> Self {
        Self {
            handler: ProtocolPacketHandler::new(port, 0),
            group_sync_write: GroupSyncWrite::new(STS_ACC, 7),
        }
    }

    pub fn write_pos_ex(&mut self, sts_id: u8, position: u16, speed: u16, acc: u8) -> (i32, u8) {
        let txpacket = [
            acc,
            self.handler.scs_lobyte(position),
            self.handler.scs_hibyte(position),
            0,
            0,
            self.handler.scs_lobyte(speed),
            self.handler.scs_hibyte(speed),
        ];
        self.handler
            .write_tx_rx(sts_id, STS_ACC, txpacket.len() as u8, &txpacket)
    }

    pub fn read_pos(&mut self, sts_id: u8) -> (i32, i32, u8) {
        let (pos, result, error) = self.handler.read_2byte_tx_rx(sts_id, STS_PRESENT_POSITION_L);
        (self.handler.scs_tohost(pos, 15), result, error)
    }

    pub fn read_speed(&mut self, sts_id: u8) -> (i32, i32, u8) {
        let (speed, result, error) = self.handler.read_2byte_tx_rx(sts_id, STS_PRESENT_SPEED_L);
        (self.handler.scs_tohost(speed, 15), result, error)
    }

    pub fn read_pos_speed(&mut self, sts_id: u8) -> (i32, i32, i32, u8) {
        let (value, result, error) = self.handler.read_4byte_tx_rx(sts_id, STS_PRESENT_POSITION_L);
        let pos = self.handler.scs_loword(value);
        let speed = self.handler.scs_hiword(value);
        (
            self.handler.scs_tohost(pos, 15),
            self.handler.scs_tohost(speed, 15),
            result,
            error,
        )
    }

    pub fn read_moving(&mut self, sts_id: u8) -> (u8, i32, u8) {
        self.handler.read_1byte_tx_rx(sts_id, STS_MOVING)
    }

    pub fn sync_write_pos_ex(&mut self, sts_id: u8, position: u16, speed: u16, acc: u8) -> bool {
        let txpacket = [
            acc,
            self.handler.scs_lobyte(position),
            self.handler.scs_hibyte(position),
            0,
            0,
            self.handler.scs_lobyte(speed),
            self.handler.scs_hibyte(speed),
        ];
        self.group_sync_write.add_param(sts_id, &txpacket)
    }

    pub fn reg_write_pos_ex(
        &mut self,
        sts_id: u8,
        position: u16,
        speed: u16,
        acc: u8,
    ) -> (i32, u8) {
        let txpacket = [
            acc,
            self.handler.scs_lobyte(position),
            self.handler.scs_hibyte(position),
            0,
            0,
            self.handler.scs_lobyte(speed),
            self.handler.scs_hibyte(speed),
        ];
        self.handler
            .reg_write_tx_rx(sts_id, STS_ACC, txpacket.len() as u8, &txpacket)
    }

    pub fn reg_action(&mut self) -> i32 {
        self.handler.action(BROADCAST_ID)
    }

    pub fn wheel_mode(&mut self, sts_id: u8) -> (i32, u8) {
        self.handler.write_1byte_tx_rx(sts_id, STS_MODE, 1)
    }

    pub fn write_spec(&mut self, sts_id: u8, speed: i32, acc: u8) -> (i32, u8) {
        let speed = self.handler.scs_toscs(speed, 15);
        let txpacket = [acc, 0, 0, 0, 0, self.handler.scs_lobyte(speed), self.handler.scs_hibyte(speed)];
        self.handler
            .write_tx_rx(sts_id, STS_ACC, txpacket.len() as u8, &txpacket)
    }

    pub fn lock_eprom(&mut self, sts_id: u8) -> (i32, u8) {
        self.handler.write_1byte_tx_rx(sts_id, STS_LOCK, 1)
    }

    pub fn unlock_eprom(&mut self, sts_id: u8) -> (i32, u8) {
        self.handler.write_1byte_tx_rx(sts_id, STS_LOCK, 0)
    }
}
