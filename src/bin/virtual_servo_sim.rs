#[cfg(unix)]
fn main() -> std::io::Result<()> {
    use std::thread;
    use std::time::Duration;

    use robotdreams::servo::protocol::port_handler::PortHandler;
    use robotdreams::servo::protocol::virtual_uart::VirtualUartPort;
    use robotdreams::servo::sim::FeetechBusSim;

    let mut sim = FeetechBusSim::new();
    let servo_ids = 1u8..=6u8;
    for id in servo_ids.clone() {
        sim.add_servo(id);
    }

    let mut port = VirtualUartPort::new()?;

    println!("Virtual servo bus ready.");
    println!("Slave device: {}", port.slave_path());
    println!("Servo IDs: 1..=6");
    println!("Press Ctrl-C to stop.");

    let mut buffer: Vec<u8> = Vec::new();

    let mut last_step = std::time::Instant::now();

    loop {
        let available = port.get_bytes_available();
        if available > 0 {
            let mut data = port.read_port(available);
            buffer.append(&mut data);
        }

        let frames = extract_frames(&mut buffer);
        for frame in frames {
            match sim.handle_frame(&frame) {
                Ok(Some(response)) => {
                    let _ = port.write_port(&response);
                }
                Ok(None) => {}
                Err(_) => {}
            }
        }

        let now = std::time::Instant::now();
        let dt = (now - last_step).as_secs_f32();
        if dt >= 0.005 {
            sim.step(dt);
            last_step = now;
        }

        if available == 0 {
            thread::sleep(Duration::from_millis(2));
        }
    }
}

#[cfg(not(unix))]
fn main() {
    eprintln!("virtual_servo_sim is only supported on Unix-like systems.");
}

#[cfg(unix)]
fn extract_frames(buffer: &mut Vec<u8>) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();

    loop {
        if buffer.len() < 2 {
            break;
        }

        let mut start = None;
        for idx in 0..(buffer.len() - 1) {
            if buffer[idx] == 0xFF && buffer[idx + 1] == 0xFF {
                start = Some(idx);
                break;
            }
        }

        let Some(start) = start else {
            buffer.clear();
            break;
        };

        if start > 0 {
            buffer.drain(0..start);
        }

        if buffer.len() < 4 {
            break;
        }

        let length = buffer[3] as usize;
        if length < 2 {
            buffer.drain(0..1);
            continue;
        }

        let total_len = length + 4;
        if total_len < 6 {
            buffer.drain(0..1);
            continue;
        }

        if buffer.len() < total_len {
            break;
        }

        let frame: Vec<u8> = buffer.drain(0..total_len).collect();
        frames.push(frame);
    }

    frames
}
