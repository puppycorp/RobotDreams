//! Minimal geometric IK for a hobby robot arm:
//! - Base yaw (q0) about +Z
//! - Shoulder pitch (q1) in the yaw plane
//! - Elbow pitch (q2) in the same plane
//! - Optional wrist pitch (q3) to keep tool angle
//!
//! Input: desired end-effector point (x,y,z) in the base frame.
//! Output: joint angles in radians.

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Vec3 {
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct ArmGeom {
    /// Shoulder -> elbow length (same units as x,y,z)
    pub l1: f64,
    /// Elbow -> wrist/TCP pivot length (same units as x,y,z)
    pub l2: f64,
    /// Horizontal offset from yaw axis to shoulder pivot, in the yaw plane
    pub shoulder_r: f64,
    /// Vertical offset from base origin to shoulder pivot
    pub shoulder_z: f64,
    /// If Some(phi), compute q3 such that (q1 + q2 + q3) ~= phi.
    /// (phi is a pitch angle in radians in the arm plane.)
    pub wrist_pitch: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct IKSolution {
    pub q0_yaw: f64,
    pub q1_shoulder: f64,
    pub q2_elbow: f64,
    pub q3_wrist: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IKError {
    /// Target is outside the reachable workspace.
    Unreachable,
    /// Numerical issue (degenerate geometry, NaNs, etc.).
    Numerical,
}

#[inline]
fn clamp(x: f64, lo: f64, hi: f64) -> f64 {
    if x < lo {
        lo
    } else if x > hi {
        hi
    } else {
        x
    }
}

/// Elbow-up geometric IK.
///
/// - `target`: desired point (x,y,z) in base frame.
/// - `geom`: arm geometry.
/// - `elbow_up_sign`: usually +1.0 for the elbow-up branch.
///   If your arm bends the opposite way, use -1.0.
///
/// Returns joint angles in radians.
pub fn ik_elbow_up(target: Vec3, geom: ArmGeom, elbow_up_sign: f64) -> Result<IKSolution, IKError> {
    if !elbow_up_sign.is_finite() || elbow_up_sign.abs() < 1e-12 {
        return Err(IKError::Numerical);
    }

    // 1) Base yaw
    let q0 = target.y.atan2(target.x);

    // 2) Reduce to planar problem in the yaw plane
    let r_world = (target.x * target.x + target.y * target.y).sqrt();
    let r = r_world - geom.shoulder_r;
    let z = target.z - geom.shoulder_z;

    let l1 = geom.l1;
    let l2 = geom.l2;
    if !l1.is_finite() || !l2.is_finite() || l1 <= 0.0 || l2 <= 0.0 {
        return Err(IKError::Numerical);
    }

    // 3) Two-link IK in the plane
    let denom = 2.0 * l1 * l2;
    if denom.abs() < 1e-12 {
        return Err(IKError::Numerical);
    }

    let rr = r * r;
    let zz = z * z;

    let mut d = (rr + zz - l1 * l1 - l2 * l2) / denom; // d = cos(q2)

    // Reject if clearly unreachable, otherwise clamp small numerical drift.
    if !d.is_finite() {
        return Err(IKError::Numerical);
    }
    if d > 1.0 + 1e-9 || d < -1.0 - 1e-9 {
        return Err(IKError::Unreachable);
    }
    d = clamp(d, -1.0, 1.0);

    let sin_sq = 1.0 - d * d;
    if sin_sq < -1e-12 {
        return Err(IKError::Numerical);
    }

    // Elbow-up vs elbow-down branch selection:
    // sin(q2) is +/- sqrt(1-d^2). We pick sign via elbow_up_sign.
    let sin_q2 = elbow_up_sign.signum() * sin_sq.max(0.0).sqrt();
    let q2 = sin_q2.atan2(d);

    // q1 = atan2(z,r) - atan2(l2*sin(q2), l1 + l2*cos(q2))
    let k1 = l1 + l2 * d;
    let k2 = l2 * sin_q2;
    let q1 = z.atan2(r) - k2.atan2(k1);

    let q3 = geom.wrist_pitch.map(|phi| phi - (q1 + q2));

    Ok(IKSolution {
        q0_yaw: q0,
        q1_shoulder: q1,
        q2_elbow: q2,
        q3_wrist: q3,
    })
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct JointCalib {
    /// Tick value at angle_rad = 0
    pub offset_ticks: i32,
    /// Conversion factor
    pub ticks_per_rad: f64,
    /// +1.0 or -1.0 (servo direction)
    pub sign: f64,
    /// Min safe command
    pub tick_min: i32,
    /// Max safe command
    pub tick_max: i32,
}

/// Convert an angle to servo ticks with clamping.
pub fn angle_to_ticks(angle_rad: f64, c: JointCalib) -> i32 {
    let raw = (c.offset_ticks as f64) + c.sign * c.ticks_per_rad * angle_rad;
    let mut t = raw.round() as i32;
    if t < c.tick_min {
        t = c.tick_min;
    }
    if t > c.tick_max {
        t = c.tick_max;
    }
    t
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx(a: f64, b: f64, tol: f64) -> bool {
        (a - b).abs() <= tol
    }

    /// Forward kinematics matching the IK geometry.
    /// Returns the end point of the 2-link chain (wrist/TCP pivot) in base frame.
    fn fk(q0: f64, q1: f64, q2: f64, geom: ArmGeom) -> Vec3 {
        // In the yaw plane (shoulder frame):
        let r_link = geom.l1 * q1.cos() + geom.l2 * (q1 + q2).cos();
        let z_link = geom.l1 * q1.sin() + geom.l2 * (q1 + q2).sin();

        // Convert to base frame
        let r_world = geom.shoulder_r + r_link;
        let x = r_world * q0.cos();
        let y = r_world * q0.sin();
        let z = geom.shoulder_z + z_link;

        Vec3 { x, y, z }
    }

    #[test]
    fn roundtrip_fk_ik_elbow_up() {
        let geom = ArmGeom {
            l1: 1.0,
            l2: 0.7,
            shoulder_r: 0.2,
            shoulder_z: 0.1,
            wrist_pitch: None,
        };

        let q0 = 0.6;
        let q1 = 0.4;
        let q2 = 0.9; // positive branch

        let target = fk(q0, q1, q2, geom);
        let sol = ik_elbow_up(target, geom, 1.0).expect("IK should succeed");

        // FK of IK solution should match target
        let p = fk(sol.q0_yaw, sol.q1_shoulder, sol.q2_elbow, geom);
        let tol = 1e-9;
        assert!(
            approx(p.x, target.x, tol),
            "x mismatch: {:?} vs {:?}",
            p,
            target
        );
        assert!(
            approx(p.y, target.y, tol),
            "y mismatch: {:?} vs {:?}",
            p,
            target
        );
        assert!(
            approx(p.z, target.z, tol),
            "z mismatch: {:?} vs {:?}",
            p,
            target
        );

        // Also check the elbow sign matches our intended branch
        assert!(sol.q2_elbow > 0.0);
    }

    #[test]
    fn unreachable_target_returns_error() {
        let geom = ArmGeom {
            l1: 1.0,
            l2: 1.0,
            shoulder_r: 0.0,
            shoulder_z: 0.0,
            wrist_pitch: None,
        };

        let target = Vec3 {
            x: 3.0,
            y: 0.0,
            z: 0.0,
        };

        let err = ik_elbow_up(target, geom, 1.0).unwrap_err();
        assert_eq!(err, IKError::Unreachable);
    }

    #[test]
    fn max_reach_edge_case_is_solvable() {
        let geom = ArmGeom {
            l1: 1.0,
            l2: 1.0,
            shoulder_r: 0.0,
            shoulder_z: 0.0,
            wrist_pitch: None,
        };

        // Exactly at max reach (straight arm)
        let target = Vec3 {
            x: 2.0,
            y: 0.0,
            z: 0.0,
        };

        let sol = ik_elbow_up(target, geom, 1.0).expect("should be reachable at max reach");
        assert!(approx(sol.q2_elbow, 0.0, 1e-12));
        assert!(approx(sol.q1_shoulder, 0.0, 1e-12));

        let p = fk(sol.q0_yaw, sol.q1_shoulder, sol.q2_elbow, geom);
        assert!(approx(p.x, target.x, 1e-9));
        assert!(approx(p.y, target.y, 1e-9));
        assert!(approx(p.z, target.z, 1e-9));
    }

    #[test]
    fn elbow_branch_sign_flips_q2() {
        let geom = ArmGeom {
            l1: 1.0,
            l2: 0.8,
            shoulder_r: 0.0,
            shoulder_z: 0.0,
            wrist_pitch: None,
        };

        let q0 = 0.25;
        let q1 = 0.35;
        let q2 = 0.75;
        let target = fk(q0, q1, q2, geom);

        let sol_up = ik_elbow_up(target, geom, 1.0).unwrap();
        let sol_down = ik_elbow_up(target, geom, -1.0).unwrap();

        // Both should reach the same point
        let p_up = fk(sol_up.q0_yaw, sol_up.q1_shoulder, sol_up.q2_elbow, geom);
        let p_down = fk(
            sol_down.q0_yaw,
            sol_down.q1_shoulder,
            sol_down.q2_elbow,
            geom,
        );
        let tol = 1e-9;
        assert!(
            approx(p_up.x, target.x, tol)
                && approx(p_up.y, target.y, tol)
                && approx(p_up.z, target.z, tol)
        );
        assert!(
            approx(p_down.x, target.x, tol)
                && approx(p_down.y, target.y, tol)
                && approx(p_down.z, target.z, tol)
        );

        // And the elbow angle should be opposite (roughly)
        assert!(approx(sol_up.q2_elbow, -sol_down.q2_elbow, 1e-12));
    }

    #[test]
    fn wrist_pitch_is_computed_when_requested() {
        use std::f64::consts::FRAC_PI_2;

        let geom = ArmGeom {
            l1: 1.0,
            l2: 0.7,
            shoulder_r: 0.1,
            shoulder_z: 0.2,
            wrist_pitch: Some(-FRAC_PI_2), // e.g., tool points "down" in plane
        };

        let q0 = 0.1;
        let q1 = 0.2;
        let q2 = 0.4;
        let target = fk(q0, q1, q2, geom);

        let sol = ik_elbow_up(target, geom, 1.0).unwrap();
        let q3 = sol.q3_wrist.expect("wrist_pitch should produce q3");

        // Check q1+q2+q3 == phi
        let phi = geom.wrist_pitch.unwrap();
        assert!(approx(sol.q1_shoulder + sol.q2_elbow + q3, phi, 1e-12));
    }

    #[test]
    fn angle_to_ticks_clamps_and_rounds() {
        let c = JointCalib {
            offset_ticks: 1500,
            ticks_per_rad: 1000.0,
            sign: 1.0,
            tick_min: 1000,
            tick_max: 2000,
        };

        // 0 rad -> offset
        assert_eq!(angle_to_ticks(0.0, c), 1500);

        // 0.25 rad -> 1750
        assert_eq!(angle_to_ticks(0.25, c), 1750);

        // Big positive -> clamp
        assert_eq!(angle_to_ticks(1.0, c), 2000);

        // Big negative -> clamp
        assert_eq!(angle_to_ticks(-1.0, c), 1000);

        // Rounding check: 0.0006 rad => 1500.6 -> rounds to 1501
        assert_eq!(angle_to_ticks(0.0006, c), 1501);
    }
}
