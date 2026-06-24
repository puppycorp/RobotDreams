use std::path::PathBuf;

use wgui::wui::runtime::WuiValue;
use wgui::{WguiModel, wgui_controller};

use crate::{
    ROBOT_SCENE_CONTROLLER_ENTRY, URDF_BASE_SLIDER_BASE_ID, URDF_JOINT_SLIDER_BASE_ID,
    UrdfViewerState, apply_urdf_slider_change, reload_urdf_state, robot_scene_props,
    urdf_joint_slider_range, urdf_slider_value_to_units, urdf_value_to_joint_units,
};

#[derive(Debug, Clone, WguiModel)]
pub(crate) struct UrdfSliderModel {
    index: u32,
    label: String,
    min: i32,
    max: i32,
    value: i32,
    display_value: String,
}

#[derive(Debug, Clone, WguiModel)]
pub(crate) struct UrdfViewModel {
    robot_scene_name: String,
    robot_scene_entry: String,
    robot_scene_props: WuiValue,
    file_label: String,
    status: String,
    has_robot: bool,
    loaded_summary: String,
    no_movable_joints: bool,
    base_controls: Vec<UrdfSliderModel>,
    joint_controls: Vec<UrdfSliderModel>,
}

pub(crate) struct UrdfViewController {
    state: UrdfViewerState,
}

fn serde_json_to_wui_value(value: &serde_json::Value) -> WuiValue {
    match value {
        serde_json::Value::Null => WuiValue::Null,
        serde_json::Value::Bool(value) => WuiValue::Bool(*value),
        serde_json::Value::Number(value) => WuiValue::Number(value.as_f64().unwrap_or(0.0)),
        serde_json::Value::String(value) => WuiValue::String(value.clone()),
        serde_json::Value::Array(values) => {
            WuiValue::List(values.iter().map(serde_json_to_wui_value).collect())
        }
        serde_json::Value::Object(values) => WuiValue::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), serde_json_to_wui_value(value)))
                .collect(),
        ),
    }
}

#[wgui_controller]
impl UrdfViewController {
    pub(crate) fn new(urdf_path: Option<PathBuf>) -> Self {
        let mut state = UrdfViewerState::new(urdf_path);
        reload_urdf_state(&mut state);
        Self { state }
    }

    pub(crate) fn state(&self) -> UrdfViewModel {
        let base_translation = [
            urdf_slider_value_to_units(self.state.base_translation_values[0]),
            urdf_slider_value_to_units(self.state.base_translation_values[1]),
            urdf_slider_value_to_units(self.state.base_translation_values[2]),
        ];
        let base_rotation = [
            urdf_slider_value_to_units(self.state.base_rotation_values[0]),
            urdf_slider_value_to_units(self.state.base_rotation_values[1]),
            urdf_slider_value_to_units(self.state.base_rotation_values[2]),
        ];

        let file_label = self
            .state
            .urdf_path
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| {
                "(auto-discovery failed: place a .urdf in ./models, ./examples, or ./model)"
                    .to_string()
            });

        let base_labels = [
            "Base X position",
            "Base Y position",
            "Base Z position",
            "Base roll (X)",
            "Base pitch (Y)",
            "Base yaw (Z)",
        ];
        let mut base_controls = Vec::new();
        for (index, label) in base_labels.iter().enumerate() {
            let value = if index < 3 {
                self.state.base_translation_values[index]
            } else {
                self.state.base_rotation_values[index - 3]
            };
            let (min, max) = if index < 3 {
                (
                    crate::URDF_BASE_POSITION_RANGE,
                    crate::URDF_BASE_POSITION_RANGE,
                )
            } else {
                (crate::URDF_BASE_ROTATION_MIN, crate::URDF_BASE_ROTATION_MAX)
            };
            base_controls.push(UrdfSliderModel {
                index: index as u32,
                label: (*label).to_string(),
                min: if index < 3 { -min } else { min },
                max,
                value,
                display_value: format!("{:.4}", urdf_slider_value_to_units(value)),
            });
        }

        let mut loaded_summary = String::new();
        let mut no_movable_joints = false;
        let mut joint_controls = Vec::new();
        if let Some(robot) = self.state.robot.as_ref() {
            loaded_summary = format!(
                "Loaded links: {}, joints: {}",
                robot.links.len(),
                robot.joints.len()
            );
            no_movable_joints = robot.movable_joint_indices.is_empty();
            for (slider_slot, joint_index) in robot.movable_joint_indices.iter().enumerate() {
                let joint = &robot.joints[*joint_index];
                let value = self
                    .state
                    .joint_values
                    .get(*joint_index)
                    .copied()
                    .unwrap_or(0);
                let (min, max) = urdf_joint_slider_range(joint);
                joint_controls.push(UrdfSliderModel {
                    index: slider_slot as u32,
                    label: joint.name.clone(),
                    min,
                    max,
                    value,
                    display_value: format!("{:.4}", urdf_value_to_joint_units(joint, value)),
                });
            }
        }

        UrdfViewModel {
            robot_scene_name: "robot-scene".to_string(),
            robot_scene_entry: ROBOT_SCENE_CONTROLLER_ENTRY.to_string(),
            robot_scene_props: serde_json_to_wui_value(&robot_scene_props(
                &self.state,
                base_translation,
                base_rotation,
            )),
            file_label,
            status: self.state.status.clone(),
            has_robot: self.state.robot.is_some(),
            loaded_summary,
            no_movable_joints,
            base_controls,
            joint_controls,
        }
    }

    pub(crate) fn title(&self) -> String {
        "Robot Dreams URDF viewer".to_string()
    }

    pub(crate) fn reload_urdf(&mut self) {
        reload_urdf_state(&mut self.state);
    }

    pub(crate) fn set_base_control(&mut self, arg: u32, value: i32) {
        let _ = apply_urdf_slider_change(&mut self.state, URDF_BASE_SLIDER_BASE_ID + arg, value);
    }

    pub(crate) fn set_joint_control(&mut self, arg: u32, value: i32) {
        let _ = apply_urdf_slider_change(&mut self.state, URDF_JOINT_SLIDER_BASE_ID + arg, value);
    }
}
