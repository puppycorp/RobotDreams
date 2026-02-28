use std::io::{Read, Write};
use std::time::{Duration, Instant};

use serialport::SerialPort;

use crate::servo::protocol::port_handler::PortHandler;

pub struct SerialPortHandler {
    port: Box<dyn SerialPort>,
    baudrate: u32,
    tx_time_per_byte_ms: f64,
    packet_start_time: Instant,
    packet_timeout_ms: f64,
    start_time: Instant,
}

impl SerialPortHandler {
    pub fn open(port_name: &str, baudrate: u32) -> std::io::Result<Self> {
        let port = serialport::new(port_name, baudrate)
            .timeout(Duration::from_millis(2))
            .open()?;

        let mut handler = Self {
            port,
            baudrate,
            tx_time_per_byte_ms: 0.0,
            packet_start_time: Instant::now(),
            packet_timeout_ms: 0.0,
            start_time: Instant::now(),
        };
        handler.update_tx_time_per_byte();
        Ok(handler)
    }

    pub fn from_port(port: Box<dyn SerialPort>, baudrate: u32) -> Self {
        let mut handler = Self {
            port,
            baudrate,
            tx_time_per_byte_ms: 0.0,
            packet_start_time: Instant::now(),
            packet_timeout_ms: 0.0,
            start_time: Instant::now(),
        };
        handler.update_tx_time_per_byte();
        handler
    }

    fn update_tx_time_per_byte(&mut self) {
        self.tx_time_per_byte_ms = (1000.0 / self.baudrate as f64) * 10.0;
    }

    fn elapsed_ms(&self, since: Instant) -> f64 {
        since.elapsed().as_secs_f64() * 1000.0
    }
}

impl PortHandler for SerialPortHandler {
    fn clear_port(&mut self) {
        let available = self.get_bytes_available();
        if available > 0 {
            let _ = self.read_port(available);
        }
    }

    fn read_port(&mut self, length: usize) -> Vec<u8> {
        if length == 0 {
            return Vec::new();
        }

        let mut out = vec![0u8; length];
        match self.port.read(&mut out) {
            Ok(read_len) => {
                out.truncate(read_len);
                out
            }
            Err(err)
                if err.kind() == std::io::ErrorKind::TimedOut
                    || err.kind() == std::io::ErrorKind::WouldBlock =>
            {
                Vec::new()
            }
            Err(_) => Vec::new(),
        }
    }

    fn write_port(&mut self, packet: &[u8]) -> usize {
        if packet.is_empty() {
            return 0;
        }

        if self.port.write_all(packet).is_err() {
            return 0;
        }
        let _ = self.port.flush();
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
        if self.port.set_baud_rate(baudrate).is_err() {
            return false;
        }
        self.baudrate = baudrate;
        self.update_tx_time_per_byte();
        true
    }

    fn get_baud_rate(&self) -> u32 {
        self.baudrate
    }

    fn get_bytes_available(&self) -> usize {
        self.port.bytes_to_read().unwrap_or(0) as usize
    }
}
