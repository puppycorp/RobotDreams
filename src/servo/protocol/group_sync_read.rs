use std::collections::HashMap;

use crate::servo::protocol::port_handler::PortHandler;
use crate::servo::protocol::protocol_packet_handler::ProtocolPacketHandler;
use crate::servo::protocol::stservo_def::{COMM_NOT_AVAILABLE, COMM_RX_CORRUPT, COMM_SUCCESS};

#[derive(Debug)]
pub struct GroupSyncRead {
    start_address: u8,
    data_length: u8,
    last_result: bool,
    is_param_changed: bool,
    param: Vec<u8>,
    order: Vec<u8>,
    data_dict: HashMap<u8, Vec<u8>>,
}

impl GroupSyncRead {
    pub fn new(start_address: u8, data_length: u8) -> Self {
        let mut instance = Self {
            start_address,
            data_length,
            last_result: false,
            is_param_changed: false,
            param: Vec::new(),
            order: Vec::new(),
            data_dict: HashMap::new(),
        };
        instance.clear_param();
        instance
    }

    fn make_param(&mut self) {
        if self.data_dict.is_empty() {
            return;
        }
        self.param.clear();
        self.param.extend(self.order.iter().copied());
    }

    pub fn add_param(&mut self, sts_id: u8) -> bool {
        if self.data_dict.contains_key(&sts_id) {
            return false;
        }
        self.data_dict.insert(sts_id, Vec::new());
        self.order.push(sts_id);
        self.is_param_changed = true;
        true
    }

    pub fn remove_param(&mut self, sts_id: u8) {
        if self.data_dict.remove(&sts_id).is_some() {
            self.order.retain(|id| *id != sts_id);
            self.is_param_changed = true;
        }
    }

    pub fn clear_param(&mut self) {
        self.data_dict.clear();
        self.order.clear();
        self.param.clear();
    }

    pub fn tx_packet<P: PortHandler>(&mut self, handler: &mut ProtocolPacketHandler<P>) -> i32 {
        if self.data_dict.is_empty() {
            return COMM_NOT_AVAILABLE;
        }

        if self.is_param_changed || self.param.is_empty() {
            self.make_param();
        }

        handler.sync_read_tx(
            self.start_address,
            self.data_length,
            &self.param,
            self.order.len(),
        )
    }

    pub fn rx_packet<P: PortHandler>(&mut self, handler: &mut ProtocolPacketHandler<P>) -> i32 {
        self.last_result = true;
        if self.data_dict.is_empty() {
            return COMM_NOT_AVAILABLE;
        }

        let (result, rxpacket) = handler.sync_read_rx(self.data_length, self.order.len());
        if rxpacket.len() >= (self.data_length as usize + 6) {
            for sts_id in self.order.iter().copied() {
                let (data, read_result) =
                    self.read_rx(&rxpacket, sts_id, self.data_length as usize);
                if read_result != COMM_SUCCESS {
                    self.last_result = false;
                }
                self.data_dict.insert(sts_id, data.unwrap_or_default());
            }
        } else {
            self.last_result = false;
        }

        result
    }

    pub fn tx_rx_packet<P: PortHandler>(&mut self, handler: &mut ProtocolPacketHandler<P>) -> i32 {
        let result = self.tx_packet(handler);
        if result != COMM_SUCCESS {
            return result;
        }
        self.rx_packet(handler)
    }

    fn read_rx(&self, rxpacket: &[u8], sts_id: u8, data_length: usize) -> (Option<Vec<u8>>, i32) {
        let rx_length = rxpacket.len();
        let mut rx_index = 0usize;
        while (rx_index + 6 + data_length) <= rx_length {
            let mut headpacket = [0u8; 3];
            while rx_index < rx_length {
                headpacket[2] = headpacket[1];
                headpacket[1] = headpacket[0];
                headpacket[0] = rxpacket[rx_index];
                rx_index += 1;
                if headpacket[2] == 0xFF && headpacket[1] == 0xFF && headpacket[0] == sts_id {
                    break;
                }
            }

            if (rx_index + 3 + data_length) > rx_length {
                break;
            }

            if rxpacket[rx_index] != (data_length as u8 + 2) {
                rx_index += 1;
                continue;
            }
            rx_index += 1;
            let error = rxpacket[rx_index];
            rx_index += 1;
            let mut checksum: u8 = sts_id
                .wrapping_add(data_length as u8 + 2)
                .wrapping_add(error);
            let mut data = vec![error];
            data.extend_from_slice(&rxpacket[rx_index..rx_index + data_length]);
            for _ in 0..data_length {
                checksum = checksum.wrapping_add(rxpacket[rx_index]);
                rx_index += 1;
            }
            checksum = (!checksum) & 0xFF;
            if checksum != rxpacket[rx_index] {
                return (None, COMM_RX_CORRUPT);
            }
            return (Some(data), COMM_SUCCESS);
        }

        (None, COMM_RX_CORRUPT)
    }

    pub fn is_available(&self, sts_id: u8, address: u8, data_length: u8) -> (bool, u8) {
        if !self.data_dict.contains_key(&sts_id) {
            return (false, 0);
        }
        if address < self.start_address
            || (self.start_address + self.data_length - data_length) < address
        {
            return (false, 0);
        }
        let Some(data) = self.data_dict.get(&sts_id) else {
            return (false, 0);
        };
        if data.is_empty() || data.len() < (data_length as usize + 1) {
            return (false, 0);
        }
        (true, data[0])
    }

    pub fn get_data(&self, sts_id: u8, address: u8, data_length: u8, scs_end: u8) -> u32 {
        let Some(data) = self.data_dict.get(&sts_id) else {
            return 0;
        };
        let offset = (address - self.start_address) as usize + 1;
        match data_length {
            1 => data[offset] as u32,
            2 => {
                let (low, high) = if scs_end == 0 {
                    (data[offset], data[offset + 1])
                } else {
                    (data[offset + 1], data[offset])
                };
                (low as u32) | ((high as u32) << 8)
            }
            4 => {
                let (l0, l1, h0, h1) = if scs_end == 0 {
                    (
                        data[offset],
                        data[offset + 1],
                        data[offset + 2],
                        data[offset + 3],
                    )
                } else {
                    (
                        data[offset + 1],
                        data[offset],
                        data[offset + 3],
                        data[offset + 2],
                    )
                };
                let low = (l0 as u32) | ((l1 as u32) << 8);
                let high = (h0 as u32) | ((h1 as u32) << 8);
                low | (high << 16)
            }
            _ => 0,
        }
    }

    pub fn last_result(&self) -> bool {
        self.last_result
    }
}
