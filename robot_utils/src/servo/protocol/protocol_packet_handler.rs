use crate::servo::protocol::port_handler::PortHandler;
use crate::servo::protocol::stservo_def::*;

#[derive(Debug)]
pub struct ProtocolPacketHandler<P: PortHandler> {
    port: P,
    scs_end: u8,
    is_using: bool,
}

impl<P: PortHandler> ProtocolPacketHandler<P> {
    pub fn new(port: P, protocol_end: u8) -> Self {
        Self {
            port,
            scs_end: protocol_end,
            is_using: false,
        }
    }

    pub fn port_mut(&mut self) -> &mut P {
        &mut self.port
    }

    pub fn into_port(self) -> P {
        self.port
    }

    pub fn scs_getend(&self) -> u8 {
        self.scs_end
    }

    pub fn scs_setend(&mut self, end: u8) {
        self.scs_end = end;
    }

    pub fn scs_tohost(&self, value: u16, bit: u8) -> i32 {
        if (value & (1 << bit)) != 0 {
            -((value & !(1 << bit)) as i32)
        } else {
            value as i32
        }
    }

    pub fn scs_toscs(&self, value: i32, bit: u8) -> u16 {
        if value < 0 {
            ((-value) as u16) | (1 << bit)
        } else {
            value as u16
        }
    }

    pub fn scs_makeword(&self, a: u8, b: u8) -> u16 {
        if self.scs_end == 0 {
            (a as u16) | ((b as u16) << 8)
        } else {
            (b as u16) | ((a as u16) << 8)
        }
    }

    pub fn scs_makedword(&self, a: u16, b: u16) -> u32 {
        (a as u32) | ((b as u32) << 16)
    }

    pub fn scs_loword(&self, value: u32) -> u16 {
        (value & 0xFFFF) as u16
    }

    pub fn scs_hiword(&self, value: u32) -> u16 {
        ((value >> 16) & 0xFFFF) as u16
    }

    pub fn scs_lobyte(&self, value: u16) -> u8 {
        if self.scs_end == 0 {
            (value & 0xFF) as u8
        } else {
            ((value >> 8) & 0xFF) as u8
        }
    }

    pub fn scs_hibyte(&self, value: u16) -> u8 {
        if self.scs_end == 0 {
            ((value >> 8) & 0xFF) as u8
        } else {
            (value & 0xFF) as u8
        }
    }

    pub fn get_protocol_version(&self) -> f32 {
        1.0
    }

    pub fn get_tx_rx_result(&self, result: i32) -> &'static str {
        match result {
            COMM_SUCCESS => "[TxRxResult] Communication success!",
            COMM_PORT_BUSY => "[TxRxResult] Port is in use!",
            COMM_TX_FAIL => "[TxRxResult] Failed transmit instruction packet!",
            COMM_RX_FAIL => "[TxRxResult] Failed get status packet from device!",
            COMM_TX_ERROR => "[TxRxResult] Incorrect instruction packet!",
            COMM_RX_WAITING => "[TxRxResult] Now receiving status packet!",
            COMM_RX_TIMEOUT => "[TxRxResult] There is no status packet!",
            COMM_RX_CORRUPT => "[TxRxResult] Incorrect status packet!",
            COMM_NOT_AVAILABLE => "[TxRxResult] Protocol does not support this function!",
            _ => "",
        }
    }

    pub fn get_rx_packet_error(&self, error: u8) -> &'static str {
        if (error & ERRBIT_VOLTAGE) != 0 {
            return "[ServoStatus] Input voltage error!";
        }
        if (error & ERRBIT_ANGLE) != 0 {
            return "[ServoStatus] Angle sen error!";
        }
        if (error & ERRBIT_OVERHEAT) != 0 {
            return "[ServoStatus] Overheat error!";
        }
        if (error & ERRBIT_OVERELE) != 0 {
            return "[ServoStatus] OverEle error!";
        }
        if (error & ERRBIT_OVERLOAD) != 0 {
            return "[ServoStatus] Overload error!";
        }
        ""
    }

    pub fn tx_packet(&mut self, txpacket: &mut [u8]) -> i32 {
        if self.is_using {
            return COMM_PORT_BUSY;
        }
        self.is_using = true;

        if txpacket.len() < 6 {
            self.is_using = false;
            return COMM_TX_ERROR;
        }

        let total_packet_length = txpacket[PKT_LENGTH] as usize + 4;
        if total_packet_length > TXPACKET_MAX_LEN || total_packet_length > txpacket.len() {
            self.is_using = false;
            return COMM_TX_ERROR;
        }

        txpacket[PKT_HEADER0] = 0xFF;
        txpacket[PKT_HEADER1] = 0xFF;

        let mut checksum: u8 = 0;
        for idx in 2..(total_packet_length - 1) {
            checksum = checksum.wrapping_add(txpacket[idx]);
        }
        txpacket[total_packet_length - 1] = (!checksum) & 0xFF;

        self.port.clear_port();
        let written = self.port.write_port(&txpacket[..total_packet_length]);
        if written != total_packet_length {
            self.is_using = false;
            return COMM_TX_FAIL;
        }

        COMM_SUCCESS
    }

    pub fn rx_packet(&mut self) -> (Vec<u8>, i32) {
        let mut rxpacket: Vec<u8> = Vec::new();
        let result: i32;
        let mut rx_length = 0usize;
        let mut wait_length = 6usize;

        loop {
            if rx_length < wait_length {
                let to_read = wait_length - rx_length;
                let mut chunk = self.port.read_port(to_read);
                rxpacket.append(&mut chunk);
                rx_length = rxpacket.len();
            }

            if rx_length >= wait_length {
                let mut header_index: Option<usize> = None;
                for idx in 0..(rx_length - 1) {
                    if rxpacket[idx] == 0xFF && rxpacket[idx + 1] == 0xFF {
                        header_index = Some(idx);
                        break;
                    }
                }

                let Some(idx) = header_index else {
                    if self.port.is_packet_timeout() {
                        result = if rx_length == 0 {
                            COMM_RX_TIMEOUT
                        } else {
                            COMM_RX_CORRUPT
                        };
                        break;
                    }
                    continue;
                };

                if idx == 0 {
                    if rxpacket[PKT_ID] > 0xFD
                        || rxpacket[PKT_LENGTH] as usize > RXPACKET_MAX_LEN
                        || rxpacket[PKT_ERROR] > 0x7F
                    {
                        rxpacket.remove(0);
                        rx_length -= 1;
                        continue;
                    }

                    let expected = rxpacket[PKT_LENGTH] as usize + PKT_LENGTH + 1;
                    if wait_length != expected {
                        wait_length = expected;
                        continue;
                    }

                    if rx_length < wait_length {
                        if self.port.is_packet_timeout() {
                            result = if rx_length == 0 {
                                COMM_RX_TIMEOUT
                            } else {
                                COMM_RX_CORRUPT
                            };
                            break;
                        }
                        continue;
                    }

                    let mut checksum: u8 = 0;
                    for i in 2..(wait_length - 1) {
                        checksum = checksum.wrapping_add(rxpacket[i]);
                    }
                    checksum = (!checksum) & 0xFF;

                    result = if rxpacket[wait_length - 1] == checksum {
                        COMM_SUCCESS
                    } else {
                        COMM_RX_CORRUPT
                    };
                    break;
                } else {
                    rxpacket.drain(0..idx);
                    rx_length = rxpacket.len();
                }
            } else if self.port.is_packet_timeout() {
                result = if rx_length == 0 {
                    COMM_RX_TIMEOUT
                } else {
                    COMM_RX_CORRUPT
                };
                break;
            }
        }

        self.is_using = false;
        (rxpacket, result)
    }

    pub fn tx_rx_packet(&mut self, txpacket: &mut [u8]) -> (Option<Vec<u8>>, i32, u8) {
        let mut error = 0u8;
        let result = self.tx_packet(txpacket);
        if result != COMM_SUCCESS {
            return (None, result, error);
        }

        if txpacket[PKT_ID] == BROADCAST_ID {
            self.is_using = false;
            return (None, result, error);
        }

        if txpacket[PKT_INSTRUCTION] == INST_READ {
            let param_len = txpacket[PKT_PARAMETER0 + 1] as usize;
            self.port.set_packet_timeout(param_len + 6);
        } else {
            self.port.set_packet_timeout(6);
        }

        let (rxpacket, rx_result) = loop {
            let (packet, result) = self.rx_packet();
            if result != COMM_SUCCESS || txpacket[PKT_ID] == packet[PKT_ID] {
                break (Some(packet), result);
            }
        };

        if rx_result == COMM_SUCCESS {
            if let Some(ref packet) = rxpacket {
                if packet.len() > PKT_ERROR {
                    error = packet[PKT_ERROR];
                }
            }
        }

        (rxpacket, rx_result, error)
    }

    pub fn ping(&mut self, sts_id: u8) -> (u16, i32, u8) {
        let mut model_number = 0u16;
        let mut error = 0u8;

        let mut txpacket = vec![0u8; 6];
        if sts_id >= BROADCAST_ID {
            return (model_number, COMM_NOT_AVAILABLE, error);
        }

        txpacket[PKT_ID] = sts_id;
        txpacket[PKT_LENGTH] = 2;
        txpacket[PKT_INSTRUCTION] = INST_PING;

        let (_, result, err) = self.tx_rx_packet(&mut txpacket);
        error = err;

        if result == COMM_SUCCESS {
            let (data, read_result, read_error) = self.read_tx_rx(sts_id, 3, 2);
            if read_result == COMM_SUCCESS {
                model_number = self.scs_makeword(data[0], data[1]);
                error = read_error;
            }
        }

        (model_number, result, error)
    }

    pub fn action(&mut self, sts_id: u8) -> i32 {
        let mut txpacket = vec![0u8; 6];
        txpacket[PKT_ID] = sts_id;
        txpacket[PKT_LENGTH] = 2;
        txpacket[PKT_INSTRUCTION] = INST_ACTION;
        let (_, result, _) = self.tx_rx_packet(&mut txpacket);
        result
    }

    pub fn read_tx(&mut self, sts_id: u8, address: u8, length: u8) -> i32 {
        let mut txpacket = vec![0u8; 8];
        if sts_id >= BROADCAST_ID {
            return COMM_NOT_AVAILABLE;
        }

        txpacket[PKT_ID] = sts_id;
        txpacket[PKT_LENGTH] = 4;
        txpacket[PKT_INSTRUCTION] = INST_READ;
        txpacket[PKT_PARAMETER0] = address;
        txpacket[PKT_PARAMETER0 + 1] = length;

        let result = self.tx_packet(&mut txpacket);
        if result == COMM_SUCCESS {
            self.port.set_packet_timeout(length as usize + 6);
        }
        result
    }

    pub fn read_rx(&mut self, sts_id: u8, length: usize) -> (Vec<u8>, i32, u8) {
        let mut result: i32;
        let mut error = 0u8;
        let mut data = Vec::new();

        loop {
            let (packet, rx_result) = self.rx_packet();
            result = rx_result;
            if result != COMM_SUCCESS || packet[PKT_ID] == sts_id {
                if result == COMM_SUCCESS {
                    error = packet[PKT_ERROR];
                    if packet.len() >= PKT_PARAMETER0 + length {
                        data.extend_from_slice(&packet[PKT_PARAMETER0..PKT_PARAMETER0 + length]);
                    }
                }
                break;
            }
        }

        (data, result, error)
    }

    pub fn read_tx_rx(&mut self, sts_id: u8, address: u8, length: u8) -> (Vec<u8>, i32, u8) {
        let mut txpacket = vec![0u8; 8];
        let mut data = Vec::new();

        if sts_id >= BROADCAST_ID {
            return (data, COMM_NOT_AVAILABLE, 0);
        }

        txpacket[PKT_ID] = sts_id;
        txpacket[PKT_LENGTH] = 4;
        txpacket[PKT_INSTRUCTION] = INST_READ;
        txpacket[PKT_PARAMETER0] = address;
        txpacket[PKT_PARAMETER0 + 1] = length;

        let (rxpacket, result, mut error) = self.tx_rx_packet(&mut txpacket);
        if result == COMM_SUCCESS {
            if let Some(packet) = rxpacket {
                error = packet[PKT_ERROR];
                let end = PKT_PARAMETER0 + length as usize;
                if packet.len() >= end {
                    data.extend_from_slice(&packet[PKT_PARAMETER0..end]);
                }
            }
        }

        (data, result, error)
    }

    pub fn read_1byte_tx(&mut self, sts_id: u8, address: u8) -> i32 {
        self.read_tx(sts_id, address, 1)
    }

    pub fn read_1byte_rx(&mut self, sts_id: u8) -> (u8, i32, u8) {
        let (data, result, error) = self.read_rx(sts_id, 1);
        let data_read = if result == COMM_SUCCESS { data[0] } else { 0 };
        (data_read, result, error)
    }

    pub fn read_1byte_tx_rx(&mut self, sts_id: u8, address: u8) -> (u8, i32, u8) {
        let (data, result, error) = self.read_tx_rx(sts_id, address, 1);
        let data_read = if result == COMM_SUCCESS { data[0] } else { 0 };
        (data_read, result, error)
    }

    pub fn read_2byte_tx(&mut self, sts_id: u8, address: u8) -> i32 {
        self.read_tx(sts_id, address, 2)
    }

    pub fn read_2byte_rx(&mut self, sts_id: u8) -> (u16, i32, u8) {
        let (data, result, error) = self.read_rx(sts_id, 2);
        let data_read = if result == COMM_SUCCESS {
            self.scs_makeword(data[0], data[1])
        } else {
            0
        };
        (data_read, result, error)
    }

    pub fn read_2byte_tx_rx(&mut self, sts_id: u8, address: u8) -> (u16, i32, u8) {
        let (data, result, error) = self.read_tx_rx(sts_id, address, 2);
        let data_read = if result == COMM_SUCCESS {
            self.scs_makeword(data[0], data[1])
        } else {
            0
        };
        (data_read, result, error)
    }

    pub fn read_4byte_tx(&mut self, sts_id: u8, address: u8) -> i32 {
        self.read_tx(sts_id, address, 4)
    }

    pub fn read_4byte_rx(&mut self, sts_id: u8) -> (u32, i32, u8) {
        let (data, result, error) = self.read_rx(sts_id, 4);
        let data_read = if result == COMM_SUCCESS {
            self.scs_makedword(
                self.scs_makeword(data[0], data[1]),
                self.scs_makeword(data[2], data[3]),
            )
        } else {
            0
        };
        (data_read, result, error)
    }

    pub fn read_4byte_tx_rx(&mut self, sts_id: u8, address: u8) -> (u32, i32, u8) {
        let (data, result, error) = self.read_tx_rx(sts_id, address, 4);
        let data_read = if result == COMM_SUCCESS {
            self.scs_makedword(
                self.scs_makeword(data[0], data[1]),
                self.scs_makeword(data[2], data[3]),
            )
        } else {
            0
        };
        (data_read, result, error)
    }

    pub fn write_tx_only(&mut self, sts_id: u8, address: u8, length: u8, data: &[u8]) -> i32 {
        let mut txpacket = vec![0u8; length as usize + 7];
        txpacket[PKT_ID] = sts_id;
        txpacket[PKT_LENGTH] = length + 3;
        txpacket[PKT_INSTRUCTION] = INST_WRITE;
        txpacket[PKT_PARAMETER0] = address;
        let end = PKT_PARAMETER0 + 1 + length as usize;
        txpacket[PKT_PARAMETER0 + 1..end].copy_from_slice(&data[..length as usize]);
        let result = self.tx_packet(&mut txpacket);
        self.is_using = false;
        result
    }

    pub fn write_tx_rx(&mut self, sts_id: u8, address: u8, length: u8, data: &[u8]) -> (i32, u8) {
        let mut txpacket = vec![0u8; length as usize + 7];
        txpacket[PKT_ID] = sts_id;
        txpacket[PKT_LENGTH] = length + 3;
        txpacket[PKT_INSTRUCTION] = INST_WRITE;
        txpacket[PKT_PARAMETER0] = address;
        let end = PKT_PARAMETER0 + 1 + length as usize;
        txpacket[PKT_PARAMETER0 + 1..end].copy_from_slice(&data[..length as usize]);
        let (_, result, error) = self.tx_rx_packet(&mut txpacket);
        (result, error)
    }

    pub fn write_1byte_tx_only(&mut self, sts_id: u8, address: u8, data: u8) -> i32 {
        self.write_tx_only(sts_id, address, 1, &[data])
    }

    pub fn write_1byte_tx_rx(&mut self, sts_id: u8, address: u8, data: u8) -> (i32, u8) {
        self.write_tx_rx(sts_id, address, 1, &[data])
    }

    pub fn write_2byte_tx_only(&mut self, sts_id: u8, address: u8, data: u16) -> i32 {
        let data_write = [self.scs_lobyte(data), self.scs_hibyte(data)];
        self.write_tx_only(sts_id, address, 2, &data_write)
    }

    pub fn write_2byte_tx_rx(&mut self, sts_id: u8, address: u8, data: u16) -> (i32, u8) {
        let data_write = [self.scs_lobyte(data), self.scs_hibyte(data)];
        self.write_tx_rx(sts_id, address, 2, &data_write)
    }

    pub fn write_4byte_tx_only(&mut self, sts_id: u8, address: u8, data: u32) -> i32 {
        let data_write = [
            self.scs_lobyte(self.scs_loword(data)),
            self.scs_hibyte(self.scs_loword(data)),
            self.scs_lobyte(self.scs_hiword(data)),
            self.scs_hibyte(self.scs_hiword(data)),
        ];
        self.write_tx_only(sts_id, address, 4, &data_write)
    }

    pub fn write_4byte_tx_rx(&mut self, sts_id: u8, address: u8, data: u32) -> (i32, u8) {
        let data_write = [
            self.scs_lobyte(self.scs_loword(data)),
            self.scs_hibyte(self.scs_loword(data)),
            self.scs_lobyte(self.scs_hiword(data)),
            self.scs_hibyte(self.scs_hiword(data)),
        ];
        self.write_tx_rx(sts_id, address, 4, &data_write)
    }

    pub fn reg_write_tx_only(&mut self, sts_id: u8, address: u8, length: u8, data: &[u8]) -> i32 {
        let mut txpacket = vec![0u8; length as usize + 7];
        txpacket[PKT_ID] = sts_id;
        txpacket[PKT_LENGTH] = length + 3;
        txpacket[PKT_INSTRUCTION] = INST_REG_WRITE;
        txpacket[PKT_PARAMETER0] = address;
        let end = PKT_PARAMETER0 + 1 + length as usize;
        txpacket[PKT_PARAMETER0 + 1..end].copy_from_slice(&data[..length as usize]);
        let result = self.tx_packet(&mut txpacket);
        self.is_using = false;
        result
    }

    pub fn reg_write_tx_rx(
        &mut self,
        sts_id: u8,
        address: u8,
        length: u8,
        data: &[u8],
    ) -> (i32, u8) {
        let mut txpacket = vec![0u8; length as usize + 7];
        txpacket[PKT_ID] = sts_id;
        txpacket[PKT_LENGTH] = length + 3;
        txpacket[PKT_INSTRUCTION] = INST_REG_WRITE;
        txpacket[PKT_PARAMETER0] = address;
        let end = PKT_PARAMETER0 + 1 + length as usize;
        txpacket[PKT_PARAMETER0 + 1..end].copy_from_slice(&data[..length as usize]);
        let (_, result, error) = self.tx_rx_packet(&mut txpacket);
        (result, error)
    }

    pub fn sync_read_tx(
        &mut self,
        start_address: u8,
        data_length: u8,
        param: &[u8],
        param_length: usize,
    ) -> i32 {
        let mut txpacket = vec![0u8; param_length + 8];
        txpacket[PKT_ID] = BROADCAST_ID;
        txpacket[PKT_LENGTH] = (param_length + 4) as u8;
        txpacket[PKT_INSTRUCTION] = INST_SYNC_READ;
        txpacket[PKT_PARAMETER0] = start_address;
        txpacket[PKT_PARAMETER0 + 1] = data_length;
        let end = PKT_PARAMETER0 + 2 + param_length;
        txpacket[PKT_PARAMETER0 + 2..end].copy_from_slice(&param[..param_length]);
        self.tx_packet(&mut txpacket)
    }

    pub fn sync_read_rx(&mut self, data_length: u8, param_length: usize) -> (i32, Vec<u8>) {
        let wait_length = (6 + data_length as usize) * param_length;
        self.port.set_packet_timeout(wait_length);

        let mut rxpacket: Vec<u8> = Vec::new();
        let mut rx_length = 0usize;
        let result: i32;

        loop {
            if rx_length < wait_length {
                let mut chunk = self.port.read_port(wait_length - rx_length);
                rxpacket.append(&mut chunk);
                rx_length = rxpacket.len();
            }

            if rx_length >= wait_length {
                result = COMM_SUCCESS;
                break;
            }

            if self.port.is_packet_timeout() {
                result = if rx_length == 0 {
                    COMM_RX_TIMEOUT
                } else {
                    COMM_RX_CORRUPT
                };
                break;
            }
        }

        self.is_using = false;
        (result, rxpacket)
    }

    pub fn sync_write_tx_only(
        &mut self,
        start_address: u8,
        data_length: u8,
        param: &[u8],
        param_length: usize,
    ) -> i32 {
        let mut txpacket = vec![0u8; param_length + 8];
        txpacket[PKT_ID] = BROADCAST_ID;
        txpacket[PKT_LENGTH] = (param_length + 4) as u8;
        txpacket[PKT_INSTRUCTION] = INST_SYNC_WRITE;
        txpacket[PKT_PARAMETER0] = start_address;
        txpacket[PKT_PARAMETER0 + 1] = data_length;
        let end = PKT_PARAMETER0 + 2 + param_length;
        txpacket[PKT_PARAMETER0 + 2..end].copy_from_slice(&param[..param_length]);
        let (_, result, _) = self.tx_rx_packet(&mut txpacket);
        result
    }
}
