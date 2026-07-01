use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::ast::{Vec3, Volume, VolumeShape};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Pose {
    pub(crate) position: Vec3,
    #[serde(default)]
    pub(crate) rotation: Vec3,
}

impl Pose {
    pub(crate) fn new(position: Vec3) -> Self {
        Self {
            position,
            rotation: [0.0, 0.0, 0.0],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Attachment {
    pub(crate) frame: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ObjectState {
    pub(crate) pose: Pose,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) attached_to: Option<Attachment>,
}

impl ObjectState {
    pub(crate) fn new(pose: Pose) -> Self {
        Self {
            pose,
            attached_to: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PressureSensorState {
    pub(crate) pressed: bool,
    pub(crate) pressure: f32,
}

impl PressureSensorState {
    pub(crate) fn new(pressed: bool, pressure: f32) -> Self {
        Self { pressed, pressure }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ScenarioWorld {
    #[serde(default)]
    pub(crate) elapsed_seconds: f32,
    #[serde(default)]
    pub(crate) frames: HashMap<String, Pose>,
    #[serde(default)]
    pub(crate) objects: HashMap<String, ObjectState>,
    #[serde(default)]
    pub(crate) servos: HashMap<u8, i16>,
    #[serde(default)]
    pub(crate) pressure_sensors: HashMap<String, PressureSensorState>,
}

impl ScenarioWorld {
    pub(crate) fn frame_pose(&self, frame: &str) -> Option<Pose> {
        self.frames.get(frame).copied()
    }

    pub(crate) fn object_pose(&self, object: &str) -> Option<Pose> {
        self.objects.get(object).map(|object| object.pose)
    }

    pub(crate) fn object_attached_to(&self, object: &str) -> Option<&str> {
        self.objects
            .get(object)
            .and_then(|object| object.attached_to.as_ref())
            .map(|attachment| attachment.frame.as_str())
    }

    pub(crate) fn set_frame_pose(&mut self, frame: impl Into<String>, pose: Pose) {
        self.frames.insert(frame.into(), pose);
    }

    pub(crate) fn set_object_pose(&mut self, object: impl Into<String>, pose: Pose) {
        let object = object.into();
        self.objects
            .entry(object)
            .and_modify(|state| state.pose = pose)
            .or_insert_with(|| ObjectState::new(pose));
    }

    pub(crate) fn set_servo_position(&mut self, id: u8, position: i16) {
        self.servos.insert(id, position);
    }

    pub(crate) fn pressure_sensor(&self, sensor: &str) -> Option<PressureSensorState> {
        self.pressure_sensors.get(sensor).copied()
    }

    pub(crate) fn set_pressure_sensor(
        &mut self,
        sensor: impl Into<String>,
        state: PressureSensorState,
    ) {
        self.pressure_sensors.insert(sensor.into(), state);
    }

    pub(crate) fn attach_object(
        &mut self,
        object: &str,
        frame: impl Into<String>,
    ) -> Result<(), String> {
        let Some(object_state) = self.objects.get_mut(object) else {
            return Err(format!("unknown object {object}"));
        };

        object_state.attached_to = Some(Attachment {
            frame: frame.into(),
        });
        Ok(())
    }

    pub(crate) fn detach_object(&mut self, object: &str) -> Result<(), String> {
        let Some(object_state) = self.objects.get_mut(object) else {
            return Err(format!("unknown object {object}"));
        };

        object_state.attached_to = None;
        Ok(())
    }

    pub(crate) fn sync_attached_objects(&mut self) {
        let frame_poses = self.frames.clone();
        for object in self.objects.values_mut() {
            let Some(attachment) = object.attached_to.as_ref() else {
                continue;
            };
            if let Some(frame_pose) = frame_poses.get(&attachment.frame) {
                object.pose = *frame_pose;
            }
        }
    }
}

pub(crate) fn distance(a: Vec3, b: Vec3) -> f32 {
    let dx = a[0] - b[0];
    let dy = a[1] - b[1];
    let dz = a[2] - b[2];
    (dx * dx + dy * dy + dz * dz).sqrt()
}

pub(crate) fn volume_center(volume: &Volume) -> Vec3 {
    match volume.shape {
        VolumeShape::Box { center, .. } => center,
    }
}

pub(crate) fn volume_contains(volume: &Volume, point: Vec3) -> bool {
    match volume.shape {
        VolumeShape::Box { center, size } => {
            point[0] >= center[0] - size[0] * 0.5
                && point[0] <= center[0] + size[0] * 0.5
                && point[1] >= center[1] - size[1] * 0.5
                && point[1] <= center[1] + size[1] * 0.5
                && point[2] >= center[2] - size[2] * 0.5
                && point[2] <= center[2] + size[2] * 0.5
        }
    }
}
