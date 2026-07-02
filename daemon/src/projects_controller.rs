use std::sync::Arc;

use tokio::sync::Mutex as AsyncMutex;
use wgui::{WguiModel, wgui_controller};

use crate::{DaemonState, ProjectSession};

#[derive(Debug, Clone, WguiModel)]
pub(crate) struct ProjectCardModel {
    id: String,
    name: String,
    kind: String,
    path: String,
    status: String,
    badge: String,
    accent: String,
    url: String,
}

#[derive(Debug, Clone, WguiModel)]
pub(crate) struct ProjectsModel {
    projects: Vec<ProjectCardModel>,
    has_projects: bool,
    has_no_projects: bool,
    empty_message: String,
}

pub(crate) struct ProjectsController {
    daemon_state: Arc<AsyncMutex<DaemonState>>,
}

fn project_badge(project: &ProjectSession) -> String {
    project
        .project_config
        .as_ref()
        .map(|config| {
            config
                .name
                .split_whitespace()
                .filter_map(|part| part.chars().next())
                .take(3)
                .collect::<String>()
                .to_uppercase()
        })
        .filter(|badge| !badge.is_empty())
        .unwrap_or_else(|| "PRJ".to_string())
}

fn project_card(project: &ProjectSession) -> ProjectCardModel {
    let status = project
        .default_simulation()
        .map(|simulation| simulation.simulation_runtime.status().as_str().to_string())
        .unwrap_or_else(|_| "unavailable".to_string());
    ProjectCardModel {
        id: project.project_id.clone(),
        name: project
            .project_config
            .as_ref()
            .map(|config| config.name.clone())
            .unwrap_or_else(|| project.project_id.clone()),
        kind: "RobotDreams project".to_string(),
        path: project.source_path.display().to_string(),
        status,
        badge: project_badge(project),
        accent: "#1c6ea4".to_string(),
        url: project.workbench_url.clone(),
    }
}

#[wgui_controller(template = "projects_controller")]
impl ProjectsController {
    pub(crate) fn new(daemon_state: Arc<AsyncMutex<DaemonState>>) -> Self {
        Self { daemon_state }
    }

    pub(crate) fn state(&self) -> ProjectsModel {
        let Ok(state) = self.daemon_state.try_lock() else {
            return ProjectsModel {
                projects: Vec::new(),
                has_projects: false,
                has_no_projects: true,
                empty_message: "Project state is busy; refresh to retry.".to_string(),
            };
        };

        let mut projects = state
            .projects
            .values()
            .map(project_card)
            .collect::<Vec<_>>();
        projects.sort_by(|left, right| left.id.cmp(&right.id));
        let has_projects = !projects.is_empty();
        ProjectsModel {
            projects,
            has_projects,
            has_no_projects: !has_projects,
            empty_message: if has_projects {
                String::new()
            } else {
                "No projects are open. Start RobotDreams with a project path or open one through the daemon CLI.".to_string()
            },
        }
    }

    pub(crate) fn title(&self) -> String {
        "RobotDreams Projects".to_string()
    }
}
