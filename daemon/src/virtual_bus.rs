use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};
use std::{thread, vec::Vec};

#[cfg(unix)]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[cfg(not(unix))]
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(unix)]
use feetech_servo::servo::protocol::port_handler::PortHandler;
#[cfg(unix)]
use feetech_servo::servo::protocol::serial_bus::ProtocolError;
#[cfg(unix)]
use feetech_servo::servo::protocol::virtual_uart::VirtualUartPort;
use feetech_servo::servo::sim::{FeetechBusEvent, FeetechBusSim, FeetechServoSnapshot};
use robotdreams_core::RobotDreams;

use crate::hardware_runtime::{HardwareDeviceRuntime, HardwareRuntime, max_servo_count};

const MAX_RECENT_BUS_EVENTS: usize = 4096;

#[derive(Debug, Clone)]
pub(crate) struct TimedFeetechBusEvent {
    pub(crate) sequence: u64,
    pub(crate) unix_ms: u128,
    pub(crate) event: FeetechBusEvent,
}

fn unix_time_millis() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

fn append_recent_bus_event(
    events: &Arc<Mutex<VecDeque<TimedFeetechBusEvent>>>,
    next_event_sequence: &AtomicU64,
    event: FeetechBusEvent,
) {
    let sequence = next_event_sequence.fetch_add(1, Ordering::Relaxed);
    if let Ok(mut events) = events.lock() {
        events.push_back(TimedFeetechBusEvent {
            sequence,
            unix_ms: unix_time_millis(),
            event,
        });
        while events.len() > MAX_RECENT_BUS_EVENTS {
            events.pop_front();
        }
    }
}

fn extract_servo_frames(buffer: &mut Vec<u8>) -> Vec<Vec<u8>> {
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

#[cfg(unix)]
struct WorkbenchVirtualBus {
    sim: Arc<Mutex<FeetechBusSim>>,
    robotdreams: Option<Arc<Mutex<RobotDreams>>>,
    events: Arc<Mutex<VecDeque<TimedFeetechBusEvent>>>,
    next_event_sequence: Arc<AtomicU64>,
    stop: Option<Arc<AtomicBool>>,
    join: Option<thread::JoinHandle<()>>,
    path: Option<String>,
    status: String,
    configured: bool,
}

#[cfg(not(unix))]
struct WorkbenchVirtualBus {
    sim: Arc<Mutex<FeetechBusSim>>,
    robotdreams: Option<Arc<Mutex<RobotDreams>>>,
    events: Arc<Mutex<VecDeque<TimedFeetechBusEvent>>>,
    next_event_sequence: Arc<AtomicU64>,
    status: String,
    configured: bool,
}

#[cfg(unix)]
impl WorkbenchVirtualBus {
    fn new() -> Self {
        Self {
            sim: Arc::new(Mutex::new(FeetechBusSim::new())),
            robotdreams: None,
            events: Arc::new(Mutex::new(VecDeque::new())),
            next_event_sequence: Arc::new(AtomicU64::new(1)),
            stop: None,
            join: None,
            path: None,
            status: "Stopped".to_string(),
            configured: false,
        }
    }

    fn new_with_robotdreams(robotdreams: Arc<Mutex<RobotDreams>>) -> Self {
        Self {
            sim: Arc::new(Mutex::new(FeetechBusSim::new())),
            robotdreams: Some(robotdreams),
            events: Arc::new(Mutex::new(VecDeque::new())),
            next_event_sequence: Arc::new(AtomicU64::new(1)),
            stop: None,
            join: None,
            path: None,
            status: "Stopped".to_string(),
            configured: false,
        }
    }

    fn is_running(&self) -> bool {
        self.stop.is_some()
    }

    fn path(&self) -> Option<&str> {
        self.path.as_deref()
    }

    fn set_servo_count(&self, servo_count: u8) {
        if self.robotdreams.is_some() {
            return;
        }
        if let Ok(mut sim) = self.sim.lock() {
            sim.set_servo_count(servo_count.max(1));
        }
    }

    fn configure_from_hardware(&mut self, hardware_runtime: &HardwareRuntime) {
        if self.robotdreams.is_some() {
            self.configured = true;
            return;
        }
        if self.is_running() || self.configured {
            return;
        }
        self.reset_from_hardware(hardware_runtime);
    }

    fn reset_from_hardware(&mut self, hardware_runtime: &HardwareRuntime) {
        if self.robotdreams.is_some() {
            self.configured = true;
            return;
        }
        if let Ok(mut sim) = self.sim.lock() {
            sim.set_servo_count(max_servo_count(hardware_runtime));
            for bus in &hardware_runtime.buses {
                for device in &bus.devices {
                    let HardwareDeviceRuntime::Servo(servo) = device else {
                        continue;
                    };
                    if let Ok(servo_id) = u8::try_from(servo.id) {
                        let _ = sim.set_target_position(servo_id, servo.zero_offset);
                    }
                }
            }
            sim.step(3.0);
            self.configured = true;
        }
    }

    fn set_target_position(&self, id: u8, target: i16) {
        if let Some(robotdreams) = &self.robotdreams {
            if let Ok(mut dreams) = robotdreams.lock()
                && let Some(bus_id) = dreams.first_virtual_bus_id().map(str::to_string)
            {
                dreams.set_virtual_servo_target(&bus_id, id, target);
                dreams.advance_seconds(3.0);
            }
            return;
        }
        let step_seconds = if self.is_running() { 0.05 } else { 3.0 };
        if let Ok(mut sim) = self.sim.lock() {
            let _ = sim.set_target_position(id, target);
            sim.step(step_seconds);
        }
    }

    fn snapshots(&self) -> Vec<FeetechServoSnapshot> {
        if let Some(robotdreams) = &self.robotdreams {
            return robotdreams
                .lock()
                .ok()
                .and_then(|mut dreams| {
                    dreams.advance_seconds(0.02);
                    dreams
                        .first_virtual_bus_id()
                        .map(str::to_string)
                        .and_then(|bus_id| dreams.servo_snapshots(&bus_id))
                })
                .unwrap_or_default();
        }
        self.sim
            .lock()
            .map(|mut sim| {
                sim.step(0.02);
                sim.servo_snapshots()
            })
            .unwrap_or_default()
    }

    fn recent_events(&self) -> Vec<TimedFeetechBusEvent> {
        self.events
            .lock()
            .map(|events| events.iter().cloned().collect())
            .unwrap_or_default()
    }

    fn start(&mut self, servo_count: u8) -> Result<String, String> {
        self.stop();
        if self.robotdreams.is_none() {
            self.set_servo_count(servo_count);
        }

        let mut port = VirtualUartPort::new()
            .map_err(|err| format!("Failed to create virtual bus device: {err}"))?;
        let path = port.slave_path().to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_in_thread = Arc::clone(&stop);
        let sim = Arc::clone(&self.sim);
        let robotdreams = self.robotdreams.clone();
        let events = Arc::clone(&self.events);
        let next_event_sequence = Arc::clone(&self.next_event_sequence);

        let join = thread::spawn(move || {
            let mut buffer = Vec::new();
            let mut last_step = Instant::now();

            while !stop_in_thread.load(Ordering::Relaxed) {
                let now = Instant::now();
                let dt = (now - last_step).as_secs_f32();
                last_step = now;
                if let Some(robotdreams) = &robotdreams {
                    if let Ok(mut dreams) = robotdreams.lock() {
                        dreams.advance_seconds(dt.max(0.001));
                    }
                } else if let Ok(mut sim) = sim.lock() {
                    sim.step(dt.max(0.001));
                }

                let mut incoming = port.read_port(512);
                if incoming.is_empty() {
                    thread::sleep(Duration::from_millis(2));
                    continue;
                }

                buffer.append(&mut incoming);
                for frame in extract_servo_frames(&mut buffer) {
                    let (response, event) = if let Some(robotdreams) = &robotdreams {
                        robotdreams
                            .lock()
                            .map(|mut dreams| {
                                let bus_id = dreams
                                    .first_virtual_bus_id()
                                    .unwrap_or_default()
                                    .to_string();
                                dreams.handle_virtual_bus_frame_with_event(&bus_id, &frame)
                            })
                            .unwrap_or_else(|_| {
                                (
                                    Err(ProtocolError::Malformed),
                                    FeetechBusEvent {
                                        instruction: "invalid".to_string(),
                                        id: None,
                                        broadcast: false,
                                        address: None,
                                        length: None,
                                        ids: Vec::new(),
                                        writes: Vec::new(),
                                        data: Vec::new(),
                                        target_position: None,
                                        raw: frame.clone(),
                                        error: Some("RobotDreams lock unavailable".to_string()),
                                    },
                                )
                            })
                    } else {
                        sim.lock()
                            .map(|mut sim| sim.handle_frame_with_event(&frame))
                            .unwrap_or_else(|_| {
                                (
                                    Err(ProtocolError::Malformed),
                                    FeetechBusEvent {
                                        instruction: "invalid".to_string(),
                                        id: None,
                                        broadcast: false,
                                        address: None,
                                        length: None,
                                        ids: Vec::new(),
                                        writes: Vec::new(),
                                        data: Vec::new(),
                                        target_position: None,
                                        raw: frame.clone(),
                                        error: Some("bus lock unavailable".to_string()),
                                    },
                                )
                            })
                    };
                    append_recent_bus_event(&events, &next_event_sequence, event);
                    let response = response.ok().flatten();
                    if let Some(response) = response {
                        let _ = port.write_port(&response);
                    }
                }
            }
        });

        self.stop = Some(stop);
        self.join = Some(join);
        self.path = Some(path.clone());
        self.status = "Running".to_string();
        self.configured = true;
        Ok(path)
    }

    fn stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            stop.store(true, Ordering::Relaxed);
        }
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
        self.path = None;
        self.status = "Stopped".to_string();
    }
}

#[cfg(unix)]
impl Drop for WorkbenchVirtualBus {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(not(unix))]
impl WorkbenchVirtualBus {
    fn new() -> Self {
        Self {
            sim: Arc::new(Mutex::new(FeetechBusSim::new())),
            robotdreams: None,
            events: Arc::new(Mutex::new(VecDeque::new())),
            next_event_sequence: Arc::new(AtomicU64::new(1)),
            status: "Virtual bus devices are only available on Unix".to_string(),
            configured: false,
        }
    }

    fn new_with_robotdreams(robotdreams: Arc<Mutex<RobotDreams>>) -> Self {
        Self {
            robotdreams: Some(robotdreams),
            ..Self::new()
        }
    }

    fn is_running(&self) -> bool {
        false
    }

    fn path(&self) -> Option<&str> {
        None
    }

    fn configure_from_hardware(&mut self, hardware_runtime: &HardwareRuntime) {
        if self.robotdreams.is_some() {
            self.configured = true;
            return;
        }
        if self.configured {
            return;
        }
        self.reset_from_hardware(hardware_runtime);
    }

    fn reset_from_hardware(&mut self, hardware_runtime: &HardwareRuntime) {
        if self.robotdreams.is_some() {
            self.configured = true;
            return;
        }
        if let Ok(mut sim) = self.sim.lock() {
            sim.set_servo_count(max_servo_count(hardware_runtime));
            for bus in &hardware_runtime.buses {
                for device in &bus.devices {
                    let HardwareDeviceRuntime::Servo(servo) = device else {
                        continue;
                    };
                    if let Ok(servo_id) = u8::try_from(servo.id) {
                        let _ = sim.set_target_position(servo_id, servo.zero_offset);
                    }
                }
            }
            sim.step(3.0);
            self.configured = true;
        }
    }

    fn set_target_position(&self, id: u8, target: i16) {
        if let Some(robotdreams) = &self.robotdreams {
            if let Ok(mut dreams) = robotdreams.lock()
                && let Some(bus_id) = dreams.first_virtual_bus_id().map(str::to_string)
            {
                dreams.set_virtual_servo_target(&bus_id, id, target);
                dreams.advance_seconds(3.0);
            }
            return;
        }
        if let Ok(mut sim) = self.sim.lock() {
            let _ = sim.set_target_position(id, target);
            sim.step(3.0);
        }
    }

    fn snapshots(&self) -> Vec<FeetechServoSnapshot> {
        if let Some(robotdreams) = &self.robotdreams {
            return robotdreams
                .lock()
                .ok()
                .and_then(|mut dreams| {
                    dreams.advance_seconds(0.02);
                    dreams
                        .first_virtual_bus_id()
                        .map(str::to_string)
                        .and_then(|bus_id| dreams.servo_snapshots(&bus_id))
                })
                .unwrap_or_default();
        }
        self.sim
            .lock()
            .map(|mut sim| {
                sim.step(0.02);
                sim.servo_snapshots()
            })
            .unwrap_or_default()
    }

    fn recent_events(&self) -> Vec<TimedFeetechBusEvent> {
        self.events
            .lock()
            .map(|events| events.iter().cloned().collect())
            .unwrap_or_default()
    }

    fn start(&mut self, _servo_count: u8) -> Result<String, String> {
        Err(self.status.clone())
    }

    fn stop(&mut self) {}
}

#[derive(Clone)]
pub(crate) struct WorkbenchVirtualBusHandle {
    inner: Arc<Mutex<WorkbenchVirtualBus>>,
}

impl WorkbenchVirtualBusHandle {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(WorkbenchVirtualBus::new())),
        }
    }

    pub(crate) fn new_with_robotdreams(robotdreams: Arc<Mutex<RobotDreams>>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(WorkbenchVirtualBus::new_with_robotdreams(
                robotdreams,
            ))),
        }
    }

    fn with_bus<T>(&self, fallback: T, f: impl FnOnce(&mut WorkbenchVirtualBus) -> T) -> T {
        self.inner
            .lock()
            .map(|mut bus| f(&mut bus))
            .unwrap_or(fallback)
    }

    pub(crate) fn is_running(&self) -> bool {
        self.with_bus(false, |bus| bus.is_running())
    }

    pub(crate) fn path(&self) -> Option<String> {
        self.with_bus(None, |bus| bus.path().map(str::to_string))
    }

    pub(crate) fn configure_from_hardware(&self, hardware_runtime: &HardwareRuntime) {
        self.with_bus((), |bus| bus.configure_from_hardware(hardware_runtime));
    }

    pub(crate) fn reset_from_hardware(&self, hardware_runtime: &HardwareRuntime) {
        self.with_bus((), |bus| bus.reset_from_hardware(hardware_runtime));
    }

    pub(crate) fn set_target_position(&self, id: u8, target: i16) {
        self.with_bus((), |bus| bus.set_target_position(id, target));
    }

    pub(crate) fn snapshots(&self) -> Vec<FeetechServoSnapshot> {
        self.with_bus(Vec::new(), |bus| bus.snapshots())
    }

    pub(crate) fn recent_events(&self) -> Vec<TimedFeetechBusEvent> {
        self.with_bus(Vec::new(), |bus| bus.recent_events())
    }

    #[cfg(test)]
    pub(crate) fn is_robotdreams_backed(&self) -> bool {
        self.with_bus(false, |bus| bus.robotdreams.is_some())
    }

    pub(crate) fn start(&self, servo_count: u8) -> Result<String, String> {
        self.with_bus(Err("Virtual bus lock is unavailable".to_string()), |bus| {
            bus.start(servo_count)
        })
    }

    pub(crate) fn stop(&self) {
        self.with_bus((), |bus| bus.stop());
    }
}
