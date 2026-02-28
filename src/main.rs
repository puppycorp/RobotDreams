#![allow(dead_code)]
#![allow(non_snake_case)]

mod ik;
mod physics;
mod robot_dreams;
mod servo;
mod urdf;

use std::collections::HashSet;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::servo::sim::{FeetechBusSim, FeetechServoSnapshot};
use log::Level;
use wgui::*;

const SERVO_COUNT_SLIDER_ID: u32 = 1;
const APPLY_SERVO_COUNT_BUTTON_ID: u32 = 2;
const TARGET_POSITION_SLIDER_ID: u32 = 3;
const TOGGLE_AUTO_MOTION_BUTTON_ID: u32 = 4;
const SERIAL_PORT_INPUT_ID: u32 = 5;
const SERIAL_BAUD_INPUT_ID: u32 = 6;
const TOGGLE_SERIAL_BRIDGE_BUTTON_ID: u32 = 7;

const DEFAULT_SERVO_COUNT: i32 = 6;
const DEFAULT_SERIAL_BAUD: &str = "1000000";

struct SerialBridge {
    stop: Arc<AtomicBool>,
    frames_rx: Receiver<Vec<u8>>,
    responses_tx: Sender<Vec<u8>>,
    status: Arc<Mutex<String>>,
    join: Option<thread::JoinHandle<()>>,
}

impl SerialBridge {
    fn start(port_name: String, baudrate: u32) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let status = Arc::new(Mutex::new(format!(
            "Opening {} @ {} baud...",
            port_name, baudrate
        )));
        let (frames_tx, frames_rx) = mpsc::channel::<Vec<u8>>();
        let (responses_tx, responses_rx) = mpsc::channel::<Vec<u8>>();
        let stop_in_thread = Arc::clone(&stop);
        let status_in_thread = Arc::clone(&status);

        let join = thread::spawn(move || {
            run_serial_bridge(
                port_name,
                baudrate,
                stop_in_thread,
                status_in_thread,
                frames_tx,
                responses_rx,
            );
        });

        Self {
            stop,
            frames_rx,
            responses_tx,
            status,
            join: Some(join),
        }
    }

    fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }

    fn status(&self) -> String {
        self.status
            .lock()
            .map(|s| s.clone())
            .unwrap_or_else(|_| "status unavailable".to_string())
    }
}

struct AppState {
    desired_servo_count: i32,
    snapshots: Vec<FeetechServoSnapshot>,
    auto_motion: bool,
    serial_port: String,
    serial_baud: String,
    serial_status: String,
}

fn render_servo_state(snapshot: &FeetechServoSnapshot) -> Item {
    let status = if snapshot.moving { "moving" } else { "idle" };
    let torque = if snapshot.torque_enabled { "on" } else { "off" };

    vstack([
        text(&format!("Servo {}", snapshot.id)),
        text(&format!(
            "mode {} | torque {} | status {}",
            snapshot.mode, torque, status
        )),
        text(&format!(
            "position {} | target {} | speed {}",
            snapshot.present_position, snapshot.target_position, snapshot.present_speed
        )),
        text(&format!(
            "load {} | current {} | temp {} C",
            snapshot.present_load, snapshot.current_raw, snapshot.temperature_c
        )),
        slider()
            .id(TARGET_POSITION_SLIDER_ID)
            .inx(snapshot.id as u32)
            .min(0)
            .max(4095)
            .ivalue(snapshot.target_position.clamp(0, 4095) as i32),
    ])
    .spacing(4)
    .padding(12)
    .border("1px solid #d6d6d6")
    .background_color("#f8f9fb")
    .into()
}

fn render(state: &AppState) -> Item {
    vstack([
        text("Robot Dreams Virtual Servo Bus"),
        hstack([
            text("Servo count"),
            slider()
                .id(SERVO_COUNT_SLIDER_ID)
                .min(1)
                .max(32)
                .ivalue(state.desired_servo_count)
                .width(240),
            text(&state.desired_servo_count.to_string()).width(28),
            button("Apply").id(APPLY_SERVO_COUNT_BUTTON_ID),
            button(if state.auto_motion {
                "Disable Auto Motion"
            } else {
                "Enable Auto Motion"
            })
            .id(TOGGLE_AUTO_MOTION_BUTTON_ID),
        ])
        .spacing(10),
        hstack([
            text("Serial port"),
            text_input()
                .id(SERIAL_PORT_INPUT_ID)
                .placeholder("COM6")
                .svalue(&state.serial_port)
                .width(120),
            text("Baud"),
            text_input()
                .id(SERIAL_BAUD_INPUT_ID)
                .placeholder(DEFAULT_SERIAL_BAUD)
                .svalue(&state.serial_baud)
                .width(110),
            button(if state.serial_status.starts_with("Connected") {
                "Disconnect"
            } else {
                "Connect"
            })
            .id(TOGGLE_SERIAL_BRIDGE_BUTTON_ID),
        ])
        .spacing(10),
        text(&format!("Serial bridge: {}", state.serial_status)),
        text("Connected servos"),
        vstack(state.snapshots.iter().map(render_servo_state)).spacing(8),
    ])
    .spacing(12)
    .padding(16)
    .background_color("#ffffff")
    .into()
}

fn apply_auto_motion(sim: &mut FeetechBusSim, snapshots: &[FeetechServoSnapshot], t: f32) {
    for (inx, snapshot) in snapshots.iter().enumerate() {
        let phase = t * 1.4 + inx as f32 * 0.8;
        let target = (2048.0 + phase.sin() * 1200.0).round() as i16;
        let _ = sim.set_target_position(snapshot.id, target);
    }
}

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

        frames.push(buffer.drain(0..total_len).collect());
    }

    frames
}

fn run_serial_bridge(
    port_name: String,
    baudrate: u32,
    stop: Arc<AtomicBool>,
    status: Arc<Mutex<String>>,
    frames_tx: Sender<Vec<u8>>,
    responses_rx: Receiver<Vec<u8>>,
) {
    let mut port = match serialport::new(&port_name, baudrate)
        .timeout(Duration::from_millis(10))
        .open()
    {
        Ok(port) => port,
        Err(err) => {
            if let Ok(mut s) = status.lock() {
                *s = format!("Failed to open {}: {}", port_name, err);
            }
            return;
        }
    };

    if let Ok(mut s) = status.lock() {
        *s = format!("Connected to {} @ {} baud", port_name, baudrate);
    }

    let mut buffer: Vec<u8> = Vec::new();
    let mut read_buf = [0u8; 512];

    while !stop.load(Ordering::Relaxed) {
        while let Ok(response) = responses_rx.try_recv() {
            if !response.is_empty() {
                let _ = port.write_all(&response);
            }
        }

        match port.read(&mut read_buf) {
            Ok(read_len) if read_len > 0 => {
                buffer.extend_from_slice(&read_buf[..read_len]);
                let frames = extract_frames(&mut buffer);
                for frame in frames {
                    if frames_tx.send(frame).is_err() {
                        if let Ok(mut s) = status.lock() {
                            *s = "Bridge disconnected from app state".to_string();
                        }
                        return;
                    }
                }
            }
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::TimedOut => {}
            Err(err) => {
                if let Ok(mut s) = status.lock() {
                    *s = format!("Serial error on {}: {}", port_name, err);
                }
                return;
            }
        }
    }

    if let Ok(mut s) = status.lock() {
        *s = "Disconnected".to_string();
    }
}

#[tokio::main]
async fn main() {
    simple_logger::init_with_level(Level::Info).unwrap();

    let mut sim = FeetechBusSim::new();
    sim.set_servo_count(DEFAULT_SERVO_COUNT as u8);

    let mut state = AppState {
        desired_servo_count: DEFAULT_SERVO_COUNT,
        snapshots: sim.servo_snapshots(),
        auto_motion: false,
        serial_port: "COM6".to_string(),
        serial_baud: DEFAULT_SERIAL_BAUD.to_string(),
        serial_status: "Disconnected".to_string(),
    };
    let mut serial_bridge: Option<SerialBridge> = None;

    let mut wgui = Wgui::new("0.0.0.0:12347".parse().unwrap());
    let mut client_ids = HashSet::new();
    let mut ticker = tokio::time::interval(Duration::from_millis(50));
    let mut last_step = Instant::now();
    let motion_start = Instant::now();

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let now = Instant::now();
                let dt = (now - last_step).as_secs_f32();
                last_step = now;

                if state.auto_motion {
                    apply_auto_motion(&mut sim, &state.snapshots, motion_start.elapsed().as_secs_f32());
                }

                if let Some(bridge) = serial_bridge.as_ref() {
                    state.serial_status = bridge.status();
                    while let Ok(frame) = bridge.frames_rx.try_recv() {
                        if let Ok(Some(response)) = sim.handle_frame(&frame) {
                            let _ = bridge.responses_tx.send(response);
                        }
                    }
                }
                sim.step(dt.max(0.001));
                state.snapshots = sim.servo_snapshots();

                for id in &client_ids {
                    wgui.render(*id, render(&state)).await;
                }
            }
            maybe_message = wgui.next() => {
                let Some(message) = maybe_message else {
                    break;
                };

                let client_id = message.client_id;
                match message.event {
                    ClientEvent::Disconnected { id: _ } => {
                        client_ids.remove(&client_id);
                    }
                    ClientEvent::Connected { id: _ } => {
                        client_ids.insert(client_id);
                        wgui.render(client_id, render(&state)).await;
                    }
                    ClientEvent::OnClick(o) => match o.id {
                        APPLY_SERVO_COUNT_BUTTON_ID => {
                            sim.set_servo_count(state.desired_servo_count as u8);
                            state.snapshots = sim.servo_snapshots();
                        }
                        TOGGLE_AUTO_MOTION_BUTTON_ID => {
                            state.auto_motion = !state.auto_motion;
                        }
                        TOGGLE_SERIAL_BRIDGE_BUTTON_ID => {
                            if let Some(mut bridge) = serial_bridge.take() {
                                bridge.stop();
                                state.serial_status = "Disconnected".to_string();
                            } else {
                                let parsed_baud = state.serial_baud.trim().parse::<u32>();
                                if state.serial_port.trim().is_empty() {
                                    state.serial_status = "Set a serial port first (e.g. COM6 or /dev/ttyUSB0)".to_string();
                                } else if let Ok(baudrate) = parsed_baud {
                                    serial_bridge = Some(SerialBridge::start(
                                        state.serial_port.trim().to_string(),
                                        baudrate,
                                    ));
                                } else {
                                    state.serial_status = "Baud must be an integer (e.g. 1000000)".to_string();
                                }
                            }
                        }
                        _ => {}
                    },
                    ClientEvent::OnTextChanged(t) => match t.id {
                        SERIAL_PORT_INPUT_ID => {
                            state.serial_port = t.value;
                        }
                        SERIAL_BAUD_INPUT_ID => {
                            state.serial_baud = t.value;
                        }
                        _ => {}
                    },
                    ClientEvent::OnSliderChange(s) => {
                        if s.id == SERVO_COUNT_SLIDER_ID {
                            state.desired_servo_count = s.value.clamp(1, 32);
                        }
                        if s.id == TARGET_POSITION_SLIDER_ID {
                            if let Some(inx) = s.inx {
                                let _ = sim.set_target_position(inx as u8, s.value.clamp(0, 4095) as i16);
                                state.snapshots = sim.servo_snapshots();
                            }
                        }
                    }
                    _ => {}
                }

                for id in &client_ids {
                    wgui.render(*id, render(&state)).await;
                }
            }
        }
    }

    if let Some(mut bridge) = serial_bridge {
        bridge.stop();
    }
}
