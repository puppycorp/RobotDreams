use std::collections::HashSet;
use std::time::{Duration, Instant};

use log::Level;
use RobotDreams::servo::sim::{FeetechBusSim, FeetechServoSnapshot};
use wgui::*;

const SERVO_COUNT_SLIDER_ID: u32 = 1;
const APPLY_SERVO_COUNT_BUTTON_ID: u32 = 2;
const TARGET_POSITION_SLIDER_ID: u32 = 3;
const TOGGLE_AUTO_MOTION_BUTTON_ID: u32 = 4;

const DEFAULT_SERVO_COUNT: i32 = 6;

struct AppState {
    desired_servo_count: i32,
    snapshots: Vec<FeetechServoSnapshot>,
    auto_motion: bool,
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

#[tokio::main]
async fn main() {
    simple_logger::init_with_level(Level::Info).unwrap();

    let mut sim = FeetechBusSim::new();
    sim.set_servo_count(DEFAULT_SERVO_COUNT as u8);

    let mut state = AppState {
        desired_servo_count: DEFAULT_SERVO_COUNT,
        snapshots: sim.servo_snapshots(),
        auto_motion: false,
    };

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
}
