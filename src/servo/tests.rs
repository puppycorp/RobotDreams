use crate::servo::protocol::group_sync_read::GroupSyncRead;
use crate::servo::protocol::group_sync_write::GroupSyncWrite;
use crate::servo::protocol::port_handler::SimPort;
use crate::servo::protocol::protocol_packet_handler::ProtocolPacketHandler;
use crate::servo::protocol::stservo_def::COMM_SUCCESS;
use crate::servo::scscl::{Scscl, SCSCL_GOAL_POSITION_L};
use crate::servo::sim::FeetechBusSim;
use crate::servo::sts::{Sts, STS_ACC, STS_PRESENT_POSITION_L};

#[test]
fn write_read_roundtrip() {
    let mut sim = FeetechBusSim::new();
    sim.add_servo(1);

    let port = SimPort::new(sim);
    let mut handler = ProtocolPacketHandler::new(port, 0);

    let (result, error) = handler.write_tx_rx(1, 0x10, 2, &[0x12, 0x34]);
    assert_eq!(result, COMM_SUCCESS);
    assert_eq!(error, 0);

    let (data, result, error) = handler.read_tx_rx(1, 0x10, 2);
    assert_eq!(result, COMM_SUCCESS);
    assert_eq!(error, 0);
    assert_eq!(data, vec![0x12, 0x34]);
}

#[test]
fn reg_write_action_applies() {
    let mut sim = FeetechBusSim::new();
    sim.add_servo(1);

    let port = SimPort::new(sim);
    let mut handler = ProtocolPacketHandler::new(port, 0);

    let (result, error) = handler.reg_write_tx_rx(1, 0x20, 2, &[0xAA, 0xBB]);
    assert_eq!(result, COMM_SUCCESS);
    assert_eq!(error, 0);

    let (data, _, _) = handler.read_tx_rx(1, 0x20, 2);
    assert_eq!(data, vec![0x00, 0x00]);

    let result = handler.action(0xFE);
    assert_eq!(result, COMM_SUCCESS);

    let (data, _, _) = handler.read_tx_rx(1, 0x20, 2);
    assert_eq!(data, vec![0xAA, 0xBB]);
}

#[test]
fn sync_read_group_parses_frames() {
    let mut sim = FeetechBusSim::new();
    sim.add_servo(1);
    sim.add_servo(2);

    let port = SimPort::new(sim);
    let mut handler = ProtocolPacketHandler::new(port, 0);

    let _ = handler.write_tx_rx(1, 0x30, 2, &[0x10, 0x20]);
    let _ = handler.write_tx_rx(2, 0x30, 2, &[0x30, 0x40]);

    let mut group = GroupSyncRead::new(0x30, 2);
    assert!(group.add_param(1));
    assert!(group.add_param(2));

    let result = group.tx_rx_packet(&mut handler);
    assert_eq!(result, COMM_SUCCESS);

    let (available, error) = group.is_available(1, 0x30, 2);
    assert!(available);
    assert_eq!(error, 0);
    assert_eq!(group.get_data(1, 0x30, 2, handler.scs_getend()) as u16, 0x2010);

    let (available, error) = group.is_available(2, 0x30, 2);
    assert!(available);
    assert_eq!(error, 0);
    assert_eq!(group.get_data(2, 0x30, 2, handler.scs_getend()) as u16, 0x4030);
}

#[test]
fn sync_write_group_updates_registers() {
    let mut sim = FeetechBusSim::new();
    sim.add_servo(1);
    sim.add_servo(2);

    let port = SimPort::new(sim);
    let mut handler = ProtocolPacketHandler::new(port, 0);

    let mut group = GroupSyncWrite::new(0x40, 2);
    assert!(group.add_param(1, &[0x55, 0x66]));
    assert!(group.add_param(2, &[0x77, 0x88]));

    let result = group.tx_packet(&mut handler);
    assert_eq!(result, COMM_SUCCESS);

    let (data, _, _) = handler.read_tx_rx(1, 0x40, 2);
    assert_eq!(data, vec![0x55, 0x66]);

    let (data, _, _) = handler.read_tx_rx(2, 0x40, 2);
    assert_eq!(data, vec![0x77, 0x88]);
}

#[test]
fn sts_helper_writes_expected_registers() {
    let mut sim = FeetechBusSim::new();
    sim.add_servo(1);

    let port = SimPort::new(sim);
    let mut sts = Sts::new(port);

    let (result, error) = sts.write_pos_ex(1, 0x1234, 0x5678, 0x01);
    assert_eq!(result, COMM_SUCCESS);
    assert_eq!(error, 0);

    let (data, _, _) = sts.handler.read_tx_rx(1, STS_ACC, 7);
    assert_eq!(data, vec![0x01, 0x34, 0x12, 0x00, 0x00, 0x78, 0x56]);

    let _ = sts.handler.write_tx_rx(1, STS_PRESENT_POSITION_L, 2, &[0x78, 0x56]);
    let (pos, _, _) = sts.read_pos(1);
    assert_eq!(pos, 0x5678);
}

#[test]
fn scscl_endianness_matches_protocol_end() {
    let mut sim = FeetechBusSim::new();
    sim.add_servo(1);

    let port = SimPort::new(sim);
    let mut scscl = Scscl::new(port);

    let (result, error) = scscl.write_pos(1, 0x1234, 0x5678, 0x9ABC);
    assert_eq!(result, COMM_SUCCESS);
    assert_eq!(error, 0);

    let (data, _, _) = scscl.handler.read_tx_rx(1, SCSCL_GOAL_POSITION_L, 6);
    assert_eq!(data, vec![0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC]);
}

#[cfg(unix)]
mod virtual_uart {
    use std::fs::OpenOptions;
    use std::io::{Read, Write};
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::io::AsRawFd;
    use std::time::{Duration, Instant};

    use crate::servo::protocol::port_handler::PortHandler;
    use crate::servo::protocol::virtual_uart::VirtualUartPort;

    fn read_until_len<P: PortHandler>(port: &mut P, len: usize) -> Vec<u8> {
        let start = Instant::now();
        let mut out = Vec::new();
        while out.len() < len && start.elapsed() < Duration::from_secs(1) {
            let mut chunk = port.read_port(len - out.len());
            if chunk.is_empty() {
                std::thread::sleep(Duration::from_millis(5));
                continue;
            }
            out.append(&mut chunk);
        }
        out
    }

    fn set_raw(fd: i32) {
        unsafe {
            let mut term: libc::termios = std::mem::zeroed();
            if libc::tcgetattr(fd, &mut term) != 0 {
                return;
            }
            libc::cfmakeraw(&mut term);
            let _ = libc::tcsetattr(fd, libc::TCSANOW, &term);
        }
    }

    #[test]
    fn virtual_uart_transfers_bytes() {
        let mut port = VirtualUartPort::new().expect("create virtual uart");
        let slave_path = port.slave_path().to_string();

        let mut slave = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(&slave_path)
            .expect("open slave");
        set_raw(slave.as_raw_fd());

        slave.write_all(b"hello").expect("write to slave");
        let read = read_until_len(&mut port, 5);
        assert_eq!(read, b"hello");

        let written = port.write_port(b"abc");
        assert_eq!(written, 3);

        let mut buf = [0u8; 3];
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(1) {
            match slave.read(&mut buf) {
                Ok(0) => {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Ok(n) => {
                    assert_eq!(&buf[..n], b"abc");
                    return;
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(err) => panic!("read slave: {err}"),
            }
        }

        panic!("timed out reading from slave");
    }
}
