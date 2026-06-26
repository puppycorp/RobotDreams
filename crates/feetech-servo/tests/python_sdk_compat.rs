#![cfg(unix)]

use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use feetech_servo::servo::protocol::port_handler::PortHandler;
use feetech_servo::servo::protocol::virtual_uart::VirtualUartPort;
use feetech_servo::servo::sim::FeetechBusSim;

fn extract_frames(buffer: &mut Vec<u8>) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    loop {
        let Some(start) = buffer.windows(2).position(|window| window == [0xFF, 0xFF]) else {
            buffer.clear();
            break;
        };
        if start > 0 {
            buffer.drain(..start);
        }
        if buffer.len() < 4 {
            break;
        }
        let frame_len = buffer[3] as usize + 4;
        if buffer.len() < frame_len {
            break;
        }
        frames.push(buffer.drain(..frame_len).collect());
    }
    frames
}

fn create_sim() -> FeetechBusSim {
    let mut sim = FeetechBusSim::new();
    for id in 1..=4 {
        sim.add_servo(id);
    }
    sim
}

fn run_sim_loop(mut port: VirtualUartPort, stop: Arc<AtomicBool>) {
    let mut sim = create_sim();
    let mut buffer = Vec::new();
    let mut last_step = Instant::now();

    while !stop.load(Ordering::Relaxed) {
        let now = Instant::now();
        let dt = (now - last_step).as_secs_f32();
        if dt >= 0.002 {
            sim.step(dt);
            last_step = now;
        }

        let mut data = port.read_port(512);
        if data.is_empty() {
            thread::sleep(Duration::from_millis(1));
            continue;
        }

        buffer.append(&mut data);
        for frame in extract_frames(&mut buffer) {
            if let Ok(Some(response)) = sim.handle_frame(&frame) {
                let _ = port.write_port(&response);
            }
        }
    }
}

fn run_python_sdk_compat(slave_path: &str) {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let workspace_dir = format!("{manifest_dir}/../..");
    let script = format!("{manifest_dir}/tests/python_sdk_compat.py");
    let pythonpath = format!("{workspace_dir}/third_party");

    let output = Command::new("python3")
        .arg(script)
        .arg(slave_path)
        .env("PYTHONPATH", pythonpath)
        .output()
        .expect("run python SDK compatibility script");

    assert!(
        output.status.success(),
        "python SDK compatibility failed\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn python_sdk_client_exercises_supported_simulator_features() {
    let port = VirtualUartPort::new().expect("create virtual UART");
    let slave_path = port.slave_path().to_string();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_in_thread = Arc::clone(&stop);
    let sim_thread = thread::spawn(move || run_sim_loop(port, stop_in_thread));

    let result = std::panic::catch_unwind(|| run_python_sdk_compat(&slave_path));
    stop.store(true, Ordering::Relaxed);
    sim_thread.join().expect("join simulator thread");

    if let Err(payload) = result {
        std::panic::resume_unwind(payload);
    }
}
