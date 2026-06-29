use std::error::Error;
use std::path::Path;

use urdf_rs::Robot;

#[derive(Debug)]
pub struct UrdfModel {
    pub robot: Robot,
}

impl UrdfModel {
    pub fn link_names(&self) -> Vec<&str> {
        self.robot
            .links
            .iter()
            .map(|link| link.name.as_str())
            .collect()
    }

    pub fn joint_names(&self) -> Vec<&str> {
        self.robot
            .joints
            .iter()
            .map(|joint| joint.name.as_str())
            .collect()
    }
}

pub fn load_urdf(path: impl AsRef<Path>) -> Result<UrdfModel, Box<dyn Error>> {
    let robot = urdf_rs::read_file(path)?;
    Ok(UrdfModel { robot })
}
