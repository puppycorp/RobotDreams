#[cfg(unix)]
use std::ffi::CStr;
#[cfg(unix)]
use std::io;
#[cfg(unix)]
#[cfg(unix)]
use std::os::unix::io::RawFd;
#[cfg(unix)]
use std::time::Instant;

#[cfg(unix)]
use crate::servo::protocol::port_handler::PortHandler;

#[cfg(unix)]
#[derive(Debug)]
pub struct VirtualUartPort {
    master_fd: RawFd,
    slave_path: String,
    baudrate: u32,
    tx_time_per_byte_ms: f64,
    packet_start_time: Instant,
    packet_timeout_ms: f64,
    start_time: Instant,
}

#[cfg(unix)]
impl VirtualUartPort {
    pub fn new() -> io::Result<Self> {
        unsafe {
            let fd = libc::posix_openpt(libc::O_RDWR | libc::O_NOCTTY);
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            if libc::grantpt(fd) != 0 {
                let err = io::Error::last_os_error();
                libc::close(fd);
                return Err(err);
            }
            if libc::unlockpt(fd) != 0 {
                let err = io::Error::last_os_error();
                libc::close(fd);
                return Err(err);
            }

            let name_ptr = libc::ptsname(fd);
            if name_ptr.is_null() {
                let err = io::Error::last_os_error();
                libc::close(fd);
                return Err(err);
            }

            let cstr = CStr::from_ptr(name_ptr);
            let slave_path = cstr.to_string_lossy().into_owned();

            let flags = libc::fcntl(fd, libc::F_GETFL);
            if flags >= 0 {
                libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
            }

            let mut port = Self {
                master_fd: fd,
                slave_path,
                baudrate: 1_000_000,
                tx_time_per_byte_ms: 0.0,
                packet_start_time: Instant::now(),
                packet_timeout_ms: 0.0,
                start_time: Instant::now(),
            };
            port.update_tx_time_per_byte();
            Ok(port)
        }
    }

    pub fn slave_path(&self) -> &str {
        &self.slave_path
    }

    fn update_tx_time_per_byte(&mut self) {
        self.tx_time_per_byte_ms = (1000.0 / self.baudrate as f64) * 10.0;
    }

    fn elapsed_ms(&self, since: Instant) -> f64 {
        since.elapsed().as_secs_f64() * 1000.0
    }
}

#[cfg(unix)]
impl Drop for VirtualUartPort {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.master_fd);
        }
    }
}

#[cfg(unix)]
impl PortHandler for VirtualUartPort {
    fn clear_port(&mut self) {
        let _ = self.read_port(self.get_bytes_available());
    }

    fn read_port(&mut self, length: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(length);
        if length == 0 {
            return out;
        }
        unsafe {
            out.set_len(length);
            let read_len = libc::read(
                self.master_fd,
                out.as_mut_ptr() as *mut libc::c_void,
                length,
            );
            if read_len <= 0 {
                out.clear();
            } else {
                out.truncate(read_len as usize);
            }
        }
        out
    }

    fn write_port(&mut self, packet: &[u8]) -> usize {
        if packet.is_empty() {
            return 0;
        }
        unsafe {
            let written = libc::write(
                self.master_fd,
                packet.as_ptr() as *const libc::c_void,
                packet.len(),
            );
            if written < 0 { 0 } else { written as usize }
        }
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
        unsafe {
            let mut bytes: libc::c_int = 0;
            if libc::ioctl(self.master_fd, libc::FIONREAD, &mut bytes) == 0 {
                bytes as usize
            } else {
                0
            }
        }
    }
}
