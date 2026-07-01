use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use super::ast::{Action, Condition, Scenario, Volume};
use super::world::{Pose, ScenarioWorld, distance, volume_center, volume_contains};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum ScenarioStatus {
    Running,
    Complete,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ScenarioStepEvent {
    pub(crate) step_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ScenarioTickReport {
    pub(crate) status: ScenarioStatus,
    #[serde(default)]
    pub(crate) events: Vec<ScenarioStepEvent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) failure_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct ScenarioRuntime {
    scenario: Scenario,
    triggered_steps: HashSet<String>,
    status: ScenarioStatus,
    failure_reason: Option<String>,
}

impl ScenarioRuntime {
    pub(crate) fn new(scenario: Scenario) -> Self {
        Self {
            scenario,
            triggered_steps: HashSet::new(),
            status: ScenarioStatus::Running,
            failure_reason: None,
        }
    }

    pub(crate) fn scenario(&self) -> &Scenario {
        &self.scenario
    }

    pub(crate) fn status(&self) -> &ScenarioStatus {
        &self.status
    }

    pub(crate) fn triggered_steps(&self) -> &HashSet<String> {
        &self.triggered_steps
    }

    pub(crate) fn tick(
        &mut self,
        world: &mut ScenarioWorld,
        dt_seconds: f32,
    ) -> ScenarioTickReport {
        if self.status != ScenarioStatus::Running {
            return self.report(Vec::new());
        }

        if dt_seconds > 0.0 {
            world.elapsed_seconds += dt_seconds;
        }
        world.sync_attached_objects();

        let mut events = Vec::new();
        let steps = self.scenario.steps.clone();
        for step in steps {
            if step.once && self.triggered_steps.contains(&step.id) {
                continue;
            }

            if !self.evaluate_condition(&step.when, world) {
                continue;
            }

            for action in &step.actions {
                if let Err(reason) = self.apply_action(action, world) {
                    self.status = ScenarioStatus::Failed;
                    self.failure_reason = Some(reason);
                    return self.report(events);
                }
            }

            if step.once {
                self.triggered_steps.insert(step.id.clone());
            }
            events.push(ScenarioStepEvent { step_id: step.id });
            world.sync_attached_objects();

            if self.status != ScenarioStatus::Running {
                return self.report(events);
            }
        }

        if self.evaluate_condition(&self.scenario.success, world) {
            self.status = ScenarioStatus::Complete;
        }

        self.report(events)
    }

    fn report(&self, events: Vec<ScenarioStepEvent>) -> ScenarioTickReport {
        ScenarioTickReport {
            status: self.status.clone(),
            events,
            failure_reason: self.failure_reason.clone(),
        }
    }

    fn evaluate_condition(&self, condition: &Condition, world: &ScenarioWorld) -> bool {
        match condition {
            Condition::Always => true,
            Condition::All { conditions } => conditions
                .iter()
                .all(|condition| self.evaluate_condition(condition, world)),
            Condition::Any { conditions } => conditions
                .iter()
                .any(|condition| self.evaluate_condition(condition, world)),
            Condition::Not { condition } => !self.evaluate_condition(condition, world),
            Condition::FrameNearObject {
                frame,
                object,
                distance_m,
            } => {
                let Some(frame_pose) = world.frame_pose(frame) else {
                    return false;
                };
                let Some(object_pose) = world.object_pose(object) else {
                    return false;
                };
                distance(frame_pose.position, object_pose.position) <= *distance_m
            }
            Condition::FrameInsideVolume { frame, volume } => {
                let Some(frame_pose) = world.frame_pose(frame) else {
                    return false;
                };
                self.volume(volume)
                    .map(|volume| volume_contains(volume, frame_pose.position))
                    .unwrap_or(false)
            }
            Condition::ObjectInsideVolume { object, volume } => {
                let Some(object_pose) = world.object_pose(object) else {
                    return false;
                };
                self.volume(volume)
                    .map(|volume| volume_contains(volume, object_pose.position))
                    .unwrap_or(false)
            }
            Condition::ObjectAttached { object } => world.object_attached_to(object).is_some(),
            Condition::ObjectDetached { object } => world
                .objects
                .get(object)
                .map(|object| object.attached_to.is_none())
                .unwrap_or(false),
            Condition::ServoAtOrPast { id, position } => world
                .servos
                .get(id)
                .map(|actual| actual >= position)
                .unwrap_or(false),
            Condition::ServoAtOrBefore { id, position } => world
                .servos
                .get(id)
                .map(|actual| actual <= position)
                .unwrap_or(false),
            Condition::ElapsedSeconds { seconds } => world.elapsed_seconds >= *seconds,
        }
    }

    fn apply_action(&mut self, action: &Action, world: &mut ScenarioWorld) -> Result<(), String> {
        match action {
            Action::AttachObject { object, frame } => world.attach_object(object, frame.clone()),
            Action::DetachObject { object } => world.detach_object(object),
            Action::MoveObjectToVolumeCenter { object, volume } => {
                let Some(volume) = self.volume(volume) else {
                    return Err(format!("unknown volume {volume}"));
                };
                world.set_object_pose(object.clone(), Pose::new(volume_center(volume)));
                Ok(())
            }
            Action::SetObjectPose {
                object,
                position,
                rotation,
            } => {
                world.set_object_pose(
                    object.clone(),
                    Pose {
                        position: *position,
                        rotation: *rotation,
                    },
                );
                Ok(())
            }
            Action::MarkComplete => {
                self.status = ScenarioStatus::Complete;
                Ok(())
            }
            Action::MarkFailed { reason } => {
                self.status = ScenarioStatus::Failed;
                self.failure_reason = Some(reason.clone());
                Ok(())
            }
        }
    }

    fn volume(&self, id: &str) -> Option<&Volume> {
        self.scenario.volumes.iter().find(|volume| volume.id == id)
    }
}
