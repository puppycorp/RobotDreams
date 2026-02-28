use std::collections::VecDeque;
use std::time::Instant;

use crate::servo::sim::FeetechBusSim;

pub trait PortHandler {
    fn clear_port(&mut self);
    fn read_port(&mut self, length: usize) -> Vec<u8>;
    fn write_port(&mut self, packet: &[u8]) -> usize;
    fn set_packet_timeout(&mut self, packet_length: usize);
    fn set_packet_timeout_millis(&mut self, msec: u64);
    fn is_packet_timeout(&mut self) -> bool;
    fn get_current_time(&self) -> f64;
    fn get_time_since_start(&self) -> f64;
    fn set_baud_rate(&mut self, baudrate: u32) -> bool;
    fn get_baud_rate(&self) -> u32;
    fn get_bytes_available(&self) -> usize;
}

#[derive(Debug)]
pub struct SimPort {
    sim: FeetechBusSim,
    rx_buffer: VecDeque<u8>,
    baudrate: u32,
    tx_time_per_byte_ms: f64,
    packet_start_time: Instant,
    packet_timeout_ms: f64,
    start_time: Instant,
}

impl SimPort {
    pub fn new(sim: FeetechBusSim) -> Self {
        let mut port = Self {
            sim,
            rx_buffer: VecDeque::new(),
            baudrate: 1_000_000,
            tx_time_per_byte_ms: 0.0,
            packet_start_time: Instant::now(),
            packet_timeout_ms: 0.0,
            start_time: Instant::now(),
        };
        port.update_tx_time_per_byte();
        port
    }

    pub fn sim_mut(&mut self) -> &mut FeetechBusSim {
        &mut self.sim
    }

    fn update_tx_time_per_byte(&mut self) {
        self.tx_time_per_byte_ms = (1000.0 / self.baudrate as f64) * 10.0;
    }

    fn elapsed_ms(&self, since: Instant) -> f64 {
        let elapsed = since.elapsed();
        elapsed.as_secs_f64() * 1000.0
    }
}

impl PortHandler for SimPort {
    fn clear_port(&mut self) {
        self.rx_buffer.clear();
    }

    fn read_port(&mut self, length: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(length);
        for _ in 0..length {
            if let Some(byte) = self.rx_buffer.pop_front() {
                out.push(byte);
            } else {
                break;
            }
        }
        out
    }

    fn write_port(&mut self, packet: &[u8]) -> usize {
        if let Ok(Some(response)) = self.sim.handle_frame(packet) {
            for byte in response {
                self.rx_buffer.push_back(byte);
            }
        }
        packet.len()
    }

    fn set_packet_timeout(&mut self, packet_length: usize) {
        self.packet_start_time = Instant::now();
        self.packet_timeout_ms = (self.tx_time_per_byte_ms * packet_length as f64)
            + (self.tx_time_per_byte_ms * 3.0)
            + 50.0;
    }

    fn set_packet_timeout_millis(&mut self, msec: u64) {
        self.packet_start_time = Instant::now();
        self.packet_timeout_ms = msec as f64;
    }

    fn is_packet_timeout(&mut self) -> bool {
        if self.packet_timeout_ms <= 0.0 {
            return false;
        }
        if self.get_time_since_start() > self.packet_timeout_ms {
            self.packet_timeout_ms = 0.0;
            return true;
        }
        false
    }

    fn get_current_time(&self) -> f64 {
        self.elapsed_ms(self.start_time)
    }

    fn get_time_since_start(&self) -> f64 {
        self.elapsed_ms(self.packet_start_time)
    }

    fn set_baud_rate(&mut self, baudrate: u32) -> bool {
        self.baudrate = baudrate;
        self.update_tx_time_per_byte();
        true
    }

    fn get_baud_rate(&self) -> u32 {
        self.baudrate
    }

    fn get_bytes_available(&self) -> usize {
        self.rx_buffer.len()
    }
}

impl Default for SimPort {
    fn default() -> Self {
        Self::new(FeetechBusSim::new())
    }
}
