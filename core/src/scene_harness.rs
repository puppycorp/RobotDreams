use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::path::Path;

use urdf_rs::{Joint, JointType, Pose, Robot};

#[derive(Clone, Debug)]
pub struct UrdfSceneHarness {
    robot: Robot,
    joint_values_rad: HashMap<String, f64>,
}

impl UrdfSceneHarness {
    pub fn from_urdf_path(path: impl AsRef<Path>) -> Result<Self, Box<dyn Error>> {
        let robot = urdf_rs::read_file(path)?;
        Ok(Self::new(robot))
    }

    pub fn new(robot: Robot) -> Self {
        Self {
            robot,
            joint_values_rad: HashMap::new(),
        }
    }

    pub fn robot(&self) -> &Robot {
        &self.robot
    }

    pub fn has_joint(&self, name: &str) -> bool {
        self.robot.joints.iter().any(|joint| joint.name == name)
    }

    pub fn has_link(&self, name: &str) -> bool {
        self.robot.links.iter().any(|link| link.name == name)
    }

    pub fn set_joint_angle(&mut self, name: impl Into<String>, radians: f64) {
        self.joint_values_rad.insert(name.into(), radians);
    }

    pub fn joint_angle(&self, name: &str) -> f64 {
        self.joint_values_rad.get(name).copied().unwrap_or_default()
    }

    pub fn set_joint_angles<I, N>(&mut self, angles: I)
    where
        I: IntoIterator<Item = (N, f64)>,
        N: Into<String>,
    {
        for (name, radians) in angles {
            self.set_joint_angle(name, radians);
        }
    }

    pub fn link_origin_world(&self, link_name: &str) -> Option<[f64; 3]> {
        self.link_pose_world(link_name)
            .map(|transform| transform.translation)
    }

    pub fn link_pose_world(&self, link_name: &str) -> Option<LinkPose> {
        let root = self.root_link_name()?;
        self.link_transform_from(&root, Transform::identity(), link_name)
            .map(LinkPose::from)
    }

    pub fn link_point_world(&self, link_name: &str, point: [f64; 3]) -> Option<[f64; 3]> {
        let root = self.root_link_name()?;
        self.link_transform_from(&root, Transform::identity(), link_name)
            .map(|transform| transform.transform_point(point))
    }

    fn root_link_name(&self) -> Option<String> {
        let children: HashSet<&str> = self
            .robot
            .joints
            .iter()
            .map(|joint| joint.child.link.as_str())
            .collect();

        self.robot
            .links
            .iter()
            .find(|link| !children.contains(link.name.as_str()))
            .map(|link| link.name.clone())
    }

    fn link_transform_from(
        &self,
        current_link: &str,
        current_transform: Transform,
        target_link: &str,
    ) -> Option<Transform> {
        if current_link == target_link {
            return Some(current_transform);
        }

        for joint in self
            .robot
            .joints
            .iter()
            .filter(|joint| joint.parent.link == current_link)
        {
            let child_transform = current_transform.then(joint_transform(
                joint,
                self.joint_values_rad
                    .get(&joint.name)
                    .copied()
                    .unwrap_or_default(),
            ));
            if let Some(transform) =
                self.link_transform_from(&joint.child.link, child_transform, target_link)
            {
                return Some(transform);
            }
        }

        None
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LinkPose {
    pub translation: [f64; 3],
    pub rotation: [[f64; 3]; 3],
}

impl From<Transform> for LinkPose {
    fn from(transform: Transform) -> Self {
        Self {
            translation: transform.translation,
            rotation: transform.rotation,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Transform {
    rotation: [[f64; 3]; 3],
    translation: [f64; 3],
}

impl Transform {
    fn identity() -> Self {
        Self {
            rotation: [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]],
            translation: [0.0, 0.0, 0.0],
        }
    }

    fn from_parts(translation: [f64; 3], rotation: [[f64; 3]; 3]) -> Self {
        Self {
            rotation,
            translation,
        }
    }

    fn then(self, next: Self) -> Self {
        Self {
            rotation: mat_mul(self.rotation, next.rotation),
            translation: vec_add(self.transform_vector(next.translation), self.translation),
        }
    }

    fn transform_point(self, point: [f64; 3]) -> [f64; 3] {
        vec_add(self.transform_vector(point), self.translation)
    }

    fn transform_vector(self, vector: [f64; 3]) -> [f64; 3] {
        [
            self.rotation[0][0] * vector[0]
                + self.rotation[0][1] * vector[1]
                + self.rotation[0][2] * vector[2],
            self.rotation[1][0] * vector[0]
                + self.rotation[1][1] * vector[1]
                + self.rotation[1][2] * vector[2],
            self.rotation[2][0] * vector[0]
                + self.rotation[2][1] * vector[1]
                + self.rotation[2][2] * vector[2],
        ]
    }
}

fn joint_transform(joint: &Joint, radians: f64) -> Transform {
    let origin = pose_transform(&joint.origin);
    let motion = match joint.joint_type {
        JointType::Revolute | JointType::Continuous => {
            Transform::from_parts([0.0, 0.0, 0.0], axis_angle(*joint.axis.xyz, radians))
        }
        JointType::Prismatic => {
            let axis = normalize(*joint.axis.xyz);
            Transform::from_parts(
                [axis[0] * radians, axis[1] * radians, axis[2] * radians],
                Transform::identity().rotation,
            )
        }
        _ => Transform::identity(),
    };

    origin.then(motion)
}

fn pose_transform(pose: &Pose) -> Transform {
    Transform::from_parts(*pose.xyz, rpy_rotation(*pose.rpy))
}

fn rpy_rotation(rpy: [f64; 3]) -> [[f64; 3]; 3] {
    let (roll, pitch, yaw) = (rpy[0], rpy[1], rpy[2]);
    mat_mul(mat_mul(rot_z(yaw), rot_y(pitch)), rot_x(roll))
}

fn axis_angle(axis: [f64; 3], angle: f64) -> [[f64; 3]; 3] {
    let axis = normalize(axis);
    let (x, y, z) = (axis[0], axis[1], axis[2]);
    let cos = angle.cos();
    let sin = angle.sin();
    let one_minus_cos = 1.0 - cos;

    [
        [
            cos + x * x * one_minus_cos,
            x * y * one_minus_cos - z * sin,
            x * z * one_minus_cos + y * sin,
        ],
        [
            y * x * one_minus_cos + z * sin,
            cos + y * y * one_minus_cos,
            y * z * one_minus_cos - x * sin,
        ],
        [
            z * x * one_minus_cos - y * sin,
            z * y * one_minus_cos + x * sin,
            cos + z * z * one_minus_cos,
        ],
    ]
}

fn normalize(axis: [f64; 3]) -> [f64; 3] {
    let norm = (axis[0] * axis[0] + axis[1] * axis[1] + axis[2] * axis[2]).sqrt();
    if norm <= f64::EPSILON {
        [0.0, 0.0, 1.0]
    } else {
        [axis[0] / norm, axis[1] / norm, axis[2] / norm]
    }
}

fn rot_x(angle: f64) -> [[f64; 3]; 3] {
    let cos = angle.cos();
    let sin = angle.sin();
    [[1.0, 0.0, 0.0], [0.0, cos, -sin], [0.0, sin, cos]]
}

fn rot_y(angle: f64) -> [[f64; 3]; 3] {
    let cos = angle.cos();
    let sin = angle.sin();
    [[cos, 0.0, sin], [0.0, 1.0, 0.0], [-sin, 0.0, cos]]
}

fn rot_z(angle: f64) -> [[f64; 3]; 3] {
    let cos = angle.cos();
    let sin = angle.sin();
    [[cos, -sin, 0.0], [sin, cos, 0.0], [0.0, 0.0, 1.0]]
}

fn mat_mul(left: [[f64; 3]; 3], right: [[f64; 3]; 3]) -> [[f64; 3]; 3] {
    let mut result = [[0.0; 3]; 3];
    for row in 0..3 {
        for col in 0..3 {
            result[row][col] = left[row][0] * right[0][col]
                + left[row][1] * right[1][col]
                + left[row][2] * right[2][col];
        }
    }
    result
}

fn vec_add(left: [f64; 3], right: [f64; 3]) -> [f64; 3] {
    [left[0] + right[0], left[1] + right[1], left[2] + right[2]]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_close(left: f64, right: f64) {
        assert!(
            (left - right).abs() < 1.0e-9,
            "expected {left} close to {right}"
        );
    }

    #[test]
    fn axis_angle_rotates_around_z() {
        let transform = Transform::from_parts(
            [0.0, 0.0, 0.0],
            axis_angle([0.0, 0.0, 1.0], std::f64::consts::FRAC_PI_2),
        );
        let point = transform.transform_point([1.0, 0.0, 0.0]);
        assert_close(point[0], 0.0);
        assert_close(point[1], 1.0);
        assert_close(point[2], 0.0);
    }
}
