use crate::ProjectConfig;

pub(crate) fn add_vec3(a: [f32; 3], b: [f32; 3]) -> [f32; 3] {
    [a[0] + b[0], a[1] + b[1], a[2] + b[2]]
}

pub(crate) fn project_robot_base_translation(project_config: Option<&ProjectConfig>) -> [f32; 3] {
    project_config
        .and_then(|project| project.robots.first())
        .map(|robot| robot.base_translation)
        .unwrap_or([0.0, 0.0, 0.0])
}

pub(crate) fn project_robot_base_rotation(project_config: Option<&ProjectConfig>) -> [f32; 3] {
    project_config
        .and_then(|project| project.robots.first())
        .map(|robot| robot.base_rotation)
        .unwrap_or([0.0, 0.0, 0.0])
}
