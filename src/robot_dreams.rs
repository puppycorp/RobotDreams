use std::error::Error;
use std::path::Path;

use crate::physics::PhysicsWorld;
use crate::urdf::{load_urdf, UrdfModel};

#[derive(Debug)]
pub struct RobotDreams {
    physics: PhysicsWorld,
    models: Vec<UrdfModel>,
    dt: f32,
}

impl RobotDreams {
    pub fn new() -> Self {
        Self {
            physics: PhysicsWorld::new(),
            models: Vec::new(),
            dt: 1.0 / 200.0,
        }
    }

    pub fn set_time_step(&mut self, dt: f32) {
        if dt > 0.0 {
            self.dt = dt;
            self.physics.set_time_step(dt);
        }
    }

    pub fn add_urdf(&mut self, path: impl AsRef<Path>) -> Result<usize, Box<dyn Error>> {
        let model = load_urdf(path)?;
        self.models.push(model);
        Ok(self.models.len() - 1)
    }

    pub fn step(&mut self, steps: usize) {
        for _ in 0..steps {
            self.physics.step();
        }
    }

    pub fn models(&self) -> &[UrdfModel] {
        &self.models
    }

    pub fn physics_mut(&mut self) -> &mut PhysicsWorld {
        &mut self.physics
    }
}

impl Default for RobotDreams {
    fn default() -> Self {
        Self::new()
    }
}
