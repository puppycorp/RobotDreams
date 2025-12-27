use std::collections::HashMap;

use crate::servo::protocol::protocol_packet_handler::ProtocolPacketHandler;
use crate::servo::protocol::port_handler::PortHandler;
use crate::servo::protocol::stservo_def::COMM_NOT_AVAILABLE;

#[derive(Debug)]
pub struct GroupSyncWrite {
    start_address: u8,
    data_length: u8,
    is_param_changed: bool,
    param: Vec<u8>,
    order: Vec<u8>,
    data_dict: HashMap<u8, Vec<u8>>,
}

impl GroupSyncWrite {
    pub fn new(start_address: u8, data_length: u8) -> Self {
        let mut instance = Self {
            start_address,
            data_length,
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
        for sts_id in self.order.iter().copied() {
            let Some(data) = self.data_dict.get(&sts_id) else {
                return;
            };
            if data.is_empty() {
                return;
            }
            self.param.push(sts_id);
            self.param.extend_from_slice(data);
        }
    }

    pub fn add_param(&mut self, sts_id: u8, data: &[u8]) -> bool {
        if self.data_dict.contains_key(&sts_id) {
            return false;
        }
        if data.len() > self.data_length as usize {
            return false;
        }
        self.data_dict.insert(sts_id, data.to_vec());
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

    pub fn change_param(&mut self, sts_id: u8, data: &[u8]) -> bool {
        if !self.data_dict.contains_key(&sts_id) {
            return false;
        }
        if data.len() > self.data_length as usize {
            return false;
        }
        self.data_dict.insert(sts_id, data.to_vec());
        self.is_param_changed = true;
        true
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
        let param_length = self.order.len() * (1 + self.data_length as usize);
        handler.sync_write_tx_only(self.start_address, self.data_length, &self.param, param_length)
    }

    pub fn is_available(&self) -> bool {
        !self.data_dict.is_empty()
    }

    pub fn data_length(&self) -> u8 {
        self.data_length
    }

    pub fn start_address(&self) -> u8 {
        self.start_address
    }
}
