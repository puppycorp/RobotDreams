use wgui::{WguiModel, wgui_controller};

#[derive(Debug, Clone, WguiModel)]
pub(crate) struct ProjectCardModel {
    name: String,
    kind: String,
    path: String,
    updated: String,
    status: String,
    badge: String,
    accent: String,
}

#[derive(Debug, Clone, WguiModel)]
pub(crate) struct ProjectActivityModel {
    time: String,
    title: String,
    detail: String,
}

#[derive(Debug, Clone, WguiModel)]
pub(crate) struct ProjectsModel {
    projects: Vec<ProjectCardModel>,
    activity: Vec<ProjectActivityModel>,
}

pub(crate) struct ProjectsController;

fn project_card(
    name: &str,
    kind: &str,
    path: &str,
    updated: &str,
    status: &str,
    badge: &str,
    accent: &str,
) -> ProjectCardModel {
    ProjectCardModel {
        name: name.to_string(),
        kind: kind.to_string(),
        path: path.to_string(),
        updated: updated.to_string(),
        status: status.to_string(),
        badge: badge.to_string(),
        accent: accent.to_string(),
    }
}

fn project_activity(time: &str, title: &str, detail: &str) -> ProjectActivityModel {
    ProjectActivityModel {
        time: time.to_string(),
        title: title.to_string(),
        detail: detail.to_string(),
    }
}

fn projects() -> Vec<ProjectCardModel> {
    vec![
        project_card(
            "SmartFactory",
            "Robot simulation",
            "examples/puppyarm/project.json",
            "Today",
            "Ready",
            "SF",
            "#1c6ea4",
        ),
        project_card(
            "PuppyArm Bench",
            "Model harness",
            "examples/puppyarm/model/robotdreams.json",
            "Today",
            "Demo",
            "ARM",
            "#3e7f65",
        ),
    ]
}

fn recent_activity() -> Vec<ProjectActivityModel> {
    vec![
        project_activity(
            "12:46",
            "PuppyArm model loaded",
            "45 links and 44 joints resolved from the demo URDF.",
        ),
        project_activity(
            "12:45",
            "Hardware bus resolved",
            "ST3215 devices mapped from the project hardware config.",
        ),
        project_activity(
            "12:44",
            "Workbench layout updated",
            "Scene, assets, telemetry, and inspector panels restored.",
        ),
    ]
}

#[wgui_controller(template = "projects_controller")]
impl ProjectsController {
    pub(crate) fn new() -> Self {
        Self
    }

    pub(crate) fn state(&self) -> ProjectsModel {
        ProjectsModel {
            projects: projects(),
            activity: recent_activity(),
        }
    }

    pub(crate) fn title(&self) -> String {
        "RobotDreams Projects".to_string()
    }
}
