use serde::{Deserialize, Serialize};

pub(crate) type Vec3 = [f32; 3];

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Scenario {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) robot_id: String,
    #[serde(default)]
    pub(crate) frames: Vec<FrameRef>,
    #[serde(default)]
    pub(crate) volumes: Vec<Volume>,
    #[serde(default)]
    pub(crate) steps: Vec<ScenarioStep>,
    pub(crate) success: Condition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FrameRef {
    pub(crate) id: String,
    pub(crate) robot_id: String,
    pub(crate) link: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ScenarioStep {
    pub(crate) id: String,
    pub(crate) when: Condition,
    #[serde(default)]
    pub(crate) actions: Vec<Action>,
    #[serde(default = "default_once")]
    pub(crate) once: bool,
}

fn default_once() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub(crate) enum Condition {
    Always,
    All {
        conditions: Vec<Condition>,
    },
    Any {
        conditions: Vec<Condition>,
    },
    Not {
        condition: Box<Condition>,
    },
    FrameNearObject {
        frame: String,
        object: String,
        #[serde(alias = "distanceM")]
        distance_m: f32,
    },
    FrameInsideVolume {
        frame: String,
        volume: String,
    },
    ObjectInsideVolume {
        object: String,
        volume: String,
    },
    ObjectAttached {
        object: String,
    },
    ObjectDetached {
        object: String,
    },
    ServoAtOrPast {
        id: u8,
        position: i16,
    },
    ServoAtOrBefore {
        id: u8,
        position: i16,
    },
    ElapsedSeconds {
        seconds: f32,
    },
    PressureSensorPressed {
        sensor: String,
    },
    PressureSensorAtOrAbove {
        sensor: String,
        pressure: f32,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub(crate) enum Action {
    AttachObject {
        object: String,
        frame: String,
    },
    DetachObject {
        object: String,
    },
    MoveObjectToVolumeCenter {
        object: String,
        volume: String,
    },
    SetObjectPose {
        object: String,
        position: Vec3,
        #[serde(default)]
        rotation: Vec3,
    },
    MarkComplete,
    MarkFailed {
        reason: String,
    },
    SetPressureSensor {
        sensor: String,
        pressed: bool,
        pressure: f32,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Volume {
    pub(crate) id: String,
    pub(crate) shape: VolumeShape,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub(crate) enum VolumeShape {
    Box { center: Vec3, size: Vec3 },
}
