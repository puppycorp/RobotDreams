use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use feetech_servo::servo::sim::FeetechServoSnapshot;
use wgui::{CustomComponentController, CustomComponentCtx};

use crate::hardware_runtime::{
    HardwareRuntime, apply_servo_snapshots_to_hardware, apply_servo_snapshots_to_urdf,
    hardware_runtime_from_project,
};
use crate::scene_transform::{
    add_vec3, project_robot_base_rotation, project_robot_base_translation,
};
use crate::virtual_bus::WorkbenchVirtualBusHandle;
use crate::{
    ProjectConfig, UrdfViewerState, project_config_from_manifest, reload_urdf_state,
    robot_scene_props_with_static_scene, urdf_slider_value_to_units,
};

pub(crate) struct RobotSceneComponent {
    virtual_bus: WorkbenchVirtualBusHandle,
    urdf_state: UrdfViewerState,
    project_config: Option<ProjectConfig>,
    project_config_modified: Option<SystemTime>,
    hardware_runtime: HardwareRuntime,
}

impl RobotSceneComponent {
    pub(crate) fn new(
        virtual_bus: WorkbenchVirtualBusHandle,
        urdf_path: Option<PathBuf>,
        project_config: Option<ProjectConfig>,
    ) -> Self {
        let mut urdf_state = UrdfViewerState::new(urdf_path);
        reload_urdf_state(&mut urdf_state);
        let project_config_modified = project_manifest_modified(project_config.as_ref());
        let hardware_runtime = hardware_runtime_from_project(project_config.as_ref(), &urdf_state);
        virtual_bus.configure_from_hardware(&hardware_runtime);
        Self {
            virtual_bus,
            urdf_state,
            project_config,
            project_config_modified,
            hardware_runtime,
        }
    }

    fn reload_project_config_if_changed(&mut self) {
        let Some(manifest_path) = self
            .project_config
            .as_ref()
            .map(|project| project.manifest_path.clone())
        else {
            return;
        };
        let modified = manifest_path
            .metadata()
            .ok()
            .and_then(|metadata| metadata.modified().ok());
        if modified == self.project_config_modified {
            return;
        }
        let Some(project_config) = project_config_from_manifest(&manifest_path) else {
            return;
        };
        self.project_config = Some(project_config);
        self.project_config_modified = modified;
        self.hardware_runtime =
            hardware_runtime_from_project(self.project_config.as_ref(), &self.urdf_state);
        self.virtual_bus
            .configure_from_hardware(&self.hardware_runtime);
    }

    fn robot_scene_props_for_snapshots(
        &self,
        snapshots: &[FeetechServoSnapshot],
    ) -> serde_json::Value {
        let mut live_hardware_runtime = self.hardware_runtime.clone();
        apply_servo_snapshots_to_hardware(&mut live_hardware_runtime, snapshots);

        let mut live_urdf_state = self.urdf_state.clone();
        apply_servo_snapshots_to_urdf(&mut live_urdf_state, &live_hardware_runtime, snapshots);

        let live_base_translation = [
            urdf_slider_value_to_units(live_urdf_state.base_translation_values[0]),
            urdf_slider_value_to_units(live_urdf_state.base_translation_values[1]),
            urdf_slider_value_to_units(live_urdf_state.base_translation_values[2]),
        ];
        let live_base_rotation = [
            urdf_slider_value_to_units(live_urdf_state.base_rotation_values[0]),
            urdf_slider_value_to_units(live_urdf_state.base_rotation_values[1]),
            urdf_slider_value_to_units(live_urdf_state.base_rotation_values[2]),
        ];
        let project_config = self.project_config.as_ref();
        let base_translation = add_vec3(
            project_robot_base_translation(project_config),
            live_base_translation,
        );
        let base_rotation = add_vec3(
            project_robot_base_rotation(project_config),
            live_base_rotation,
        );

        robot_scene_props_with_static_scene(
            &live_urdf_state,
            project_config,
            base_translation,
            base_rotation,
            false,
        )
    }
}

pub(crate) fn servo_snapshots_json(snapshots: &[FeetechServoSnapshot]) -> serde_json::Value {
    serde_json::Value::Array(
        snapshots
            .iter()
            .map(|snapshot| {
                serde_json::json!({
                    "id": snapshot.id,
                    "mode": snapshot.mode,
                    "torqueEnabled": snapshot.torque_enabled,
                    "moving": snapshot.moving,
                    "targetPosition": snapshot.target_position,
                    "presentPosition": snapshot.present_position,
                    "presentSpeed": snapshot.present_speed,
                    "presentLoad": snapshot.present_load,
                    "currentRaw": snapshot.current_raw,
                    "temperatureC": snapshot.temperature_c,
                    "voltageTenths": snapshot.voltage_tenths,
                })
            })
            .collect(),
    )
}

fn project_manifest_modified(project_config: Option<&ProjectConfig>) -> Option<SystemTime> {
    project_config
        .and_then(|project| project.manifest_path.metadata().ok())
        .and_then(|metadata| metadata.modified().ok())
}

#[async_trait::async_trait]
impl CustomComponentController for RobotSceneComponent {
    async fn process(&mut self, ctx: CustomComponentCtx) -> anyhow::Result<()> {
        loop {
            self.reload_project_config_if_changed();
            if self.virtual_bus.is_running() {
                let snapshots = self.virtual_bus.snapshots();
                ctx.send_data(
                    "servoSnapshots",
                    serde_json::json!({
                        "snapshots": servo_snapshots_json(&snapshots),
                        "robotSceneProps": self.robot_scene_props_for_snapshots(&snapshots),
                    }),
                )
                .await?;
            }

            tokio::time::sleep(Duration::from_millis(33)).await;
        }
    }
}
