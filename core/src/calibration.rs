//! Reproducible, evidence-carrying calibration artifacts.
//!
//! This module deliberately has no defaults for measured values. A calibration
//! record is either backed by provenance and observed traces or validation
//! reports why it cannot support a physics profile.

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::project::ProjectVehiclePhysicsConfig;

pub const CALIBRATION_FORMAT: &str = "robotdreams.calibration.v1";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MeasurementProvenance {
    pub measured_at: String,
    pub operator: String,
    pub method: String,
    pub source: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VehicleMeasurement {
    pub mass_kg: f32,
    pub center_of_mass_m: [f32; 3],
    pub wheel_radius_m: f32,
    pub gear_ratio: f32,
    pub motor_stall_torque_nm: f32,
    pub motor_no_load_rpm: f32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ServoMeasurement {
    pub servo_id: u8,
    pub max_speed_ticks_per_sec: f32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DriveTraceSample {
    pub time_sec: f64,
    pub left_command: f32,
    pub right_command: f32,
    pub observed_linear_mps: f32,
    pub observed_yaw_rps: f32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ServoTraceSample {
    pub time_sec: f64,
    pub servo_id: u8,
    pub target_ticks: i32,
    pub observed_present_ticks: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_load: Option<i16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_current_raw: Option<u16>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CalibrationRecord {
    pub format: String,
    pub hardware_revision: String,
    pub provenance: MeasurementProvenance,
    pub vehicle: VehicleMeasurement,
    pub servos: Vec<ServoMeasurement>,
    pub drive_trace: Vec<DriveTraceSample>,
    pub servo_trace: Vec<ServoTraceSample>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CalibrationValidationReport {
    pub valid: bool,
    pub max_theoretical_wheel_speed_mps: f32,
    pub max_observed_drive_speed_mps: f32,
    pub drive_samples: usize,
    pub servo_samples: usize,
    pub issues: Vec<String>,
}

/// An explicit, reviewable result of applying a measured calibration record to
/// an authored vehicle profile. The source profile is preserved so callers
/// cannot silently lose the pre-calibration assumptions or measurement source.
#[derive(Clone, Debug, PartialEq)]
pub struct CalibratedVehicleProfile {
    pub source_profile: ProjectVehiclePhysicsConfig,
    pub vehicle: ProjectVehiclePhysicsConfig,
    pub hardware_revision: String,
    pub provenance: MeasurementProvenance,
    pub servo_measurements: Vec<ServoMeasurement>,
    pub servo_trace_evidence: Vec<ServoTraceSample>,
}

impl CalibrationRecord {
    pub fn to_json_pretty(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

pub fn load_calibration(path: impl AsRef<Path>) -> Result<CalibrationRecord, String> {
    let path = path.as_ref();
    let source = std::fs::read_to_string(path)
        .map_err(|error| format!("could not read calibration '{}': {error}", path.display()))?;
    serde_json::from_str(&source)
        .map_err(|error| format!("invalid calibration '{}': {error}", path.display()))
}

pub fn validate_calibration(record: &CalibrationRecord) -> CalibrationValidationReport {
    let mut report = CalibrationValidationReport {
        max_theoretical_wheel_speed_mps: record.vehicle.motor_no_load_rpm * std::f32::consts::TAU
            / 60.0
            * record.vehicle.wheel_radius_m
            / record.vehicle.gear_ratio,
        drive_samples: record.drive_trace.len(),
        servo_samples: record.servo_trace.len(),
        ..CalibrationValidationReport::default()
    };
    if record.format != CALIBRATION_FORMAT {
        report.issues.push(format!(
            "format must be '{CALIBRATION_FORMAT}', got '{}'",
            record.format
        ));
    }
    for (field, value) in [
        ("hardwareRevision", record.hardware_revision.as_str()),
        (
            "provenance.measuredAt",
            record.provenance.measured_at.as_str(),
        ),
        ("provenance.operator", record.provenance.operator.as_str()),
        ("provenance.method", record.provenance.method.as_str()),
        ("provenance.source", record.provenance.source.as_str()),
    ] {
        if value.trim().is_empty() {
            report.issues.push(format!("missing {field}"));
        }
    }
    if !record.vehicle.mass_kg.is_finite() || record.vehicle.mass_kg <= 0.0 {
        report
            .issues
            .push("massKg must be finite and positive".to_string());
    }
    if !record
        .vehicle
        .center_of_mass_m
        .iter()
        .all(|value| value.is_finite())
    {
        report
            .issues
            .push("centerOfMass must be finite".to_string());
    }
    for (field, value) in [
        ("wheelRadiusM", record.vehicle.wheel_radius_m),
        ("gearRatio", record.vehicle.gear_ratio),
        ("motorStallTorqueNm", record.vehicle.motor_stall_torque_nm),
        ("motorNoLoadRpm", record.vehicle.motor_no_load_rpm),
    ] {
        if !value.is_finite() || value <= 0.0 {
            report
                .issues
                .push(format!("{field} must be finite and positive"));
        }
    }
    validate_drive_trace(record, &mut report);
    validate_servo_trace(record, &mut report);
    report.valid = report.issues.is_empty();
    report
}

/// Applies only measured vehicle/motor fields after validating the complete
/// evidence record. Damping, traction, brake, steering, collision, and other
/// authored profile settings remain unchanged because the record does not
/// measure them. Servo measurements accompany the result as evidence; they do
/// not pretend to make arm bodies dynamically coupled.
pub fn apply_calibration_to_vehicle_profile(
    source_profile: &ProjectVehiclePhysicsConfig,
    record: &CalibrationRecord,
) -> Result<CalibratedVehicleProfile, String> {
    let report = validate_calibration(record);
    if !report.valid {
        return Err(format!(
            "refusing to apply invalid calibration: {}",
            report.issues.join("; ")
        ));
    }
    let mut vehicle = source_profile.clone();
    vehicle.mass_kg = record.vehicle.mass_kg;
    vehicle.center_of_mass_m = record.vehicle.center_of_mass_m;
    vehicle.motor.wheel_radius_m = record.vehicle.wheel_radius_m;
    vehicle.motor.gear_ratio = record.vehicle.gear_ratio;
    vehicle.motor.stall_torque_nm = record.vehicle.motor_stall_torque_nm;
    vehicle.motor.no_load_rpm = record.vehicle.motor_no_load_rpm;
    Ok(CalibratedVehicleProfile {
        source_profile: source_profile.clone(),
        vehicle,
        hardware_revision: record.hardware_revision.clone(),
        provenance: record.provenance.clone(),
        servo_measurements: record.servos.clone(),
        servo_trace_evidence: record.servo_trace.clone(),
    })
}

fn validate_drive_trace(record: &CalibrationRecord, report: &mut CalibrationValidationReport) {
    if record.drive_trace.is_empty() {
        report
            .issues
            .push("missing observed drive trace".to_string());
        return;
    }
    let mut previous_time = None;
    for sample in &record.drive_trace {
        if !sample.time_sec.is_finite()
            || !sample.left_command.is_finite()
            || !sample.right_command.is_finite()
            || !sample.observed_linear_mps.is_finite()
            || !sample.observed_yaw_rps.is_finite()
        {
            report
                .issues
                .push("drive trace has non-finite sample".to_string());
            continue;
        }
        if sample.left_command.abs() > 1.0 || sample.right_command.abs() > 1.0 {
            report
                .issues
                .push("drive command must be normalized to [-1, 1]".to_string());
        }
        if previous_time.is_some_and(|previous| sample.time_sec <= previous) {
            report
                .issues
                .push("drive trace timestamps must increase".to_string());
        }
        previous_time = Some(sample.time_sec);
        report.max_observed_drive_speed_mps = report
            .max_observed_drive_speed_mps
            .max(sample.observed_linear_mps.abs());
    }
    if report.max_observed_drive_speed_mps > report.max_theoretical_wheel_speed_mps * 1.1 {
        report.issues.push(format!(
            "observed drive speed {:.3} m/s exceeds no-load model {:.3} m/s by >10%",
            report.max_observed_drive_speed_mps, report.max_theoretical_wheel_speed_mps
        ));
    }
}

fn validate_servo_trace(record: &CalibrationRecord, report: &mut CalibrationValidationReport) {
    if record.servo_trace.is_empty() {
        report
            .issues
            .push("missing observed servo trace".to_string());
        return;
    }
    let speeds = record
        .servos
        .iter()
        .map(|servo| (servo.servo_id, servo.max_speed_ticks_per_sec))
        .collect::<BTreeMap<_, _>>();
    let mut previous = BTreeMap::<u8, &ServoTraceSample>::new();
    for sample in &record.servo_trace {
        if !sample.time_sec.is_finite() {
            report
                .issues
                .push("servo trace has non-finite timestamp".to_string());
            continue;
        }
        if sample
            .observed_load
            .is_some_and(|load| !(-1000..=1000).contains(&load))
        {
            report.issues.push(format!(
                "servo {} observed load is outside signed actuator range",
                sample.servo_id
            ));
        }
        if sample
            .observed_current_raw
            .is_some_and(|current| current > 4095)
        {
            report.issues.push(format!(
                "servo {} observed current is outside raw actuator range",
                sample.servo_id
            ));
        }
        let Some(max_speed) = speeds.get(&sample.servo_id) else {
            report.issues.push(format!(
                "servo trace references unmeasured servo {}",
                sample.servo_id
            ));
            continue;
        };
        if !max_speed.is_finite() || *max_speed <= 0.0 {
            report.issues.push(format!(
                "servo {} has invalid measured max speed",
                sample.servo_id
            ));
        }
        if let Some(previous) = previous.get(&sample.servo_id) {
            let dt = sample.time_sec - previous.time_sec;
            if dt <= 0.0 {
                report.issues.push(format!(
                    "servo {} timestamps must increase",
                    sample.servo_id
                ));
            } else if ((sample.observed_present_ticks - previous.observed_present_ticks).abs()
                as f64
                / dt)
                > f64::from(*max_speed) * 1.1
            {
                report.issues.push(format!(
                    "servo {} observed position changes faster than measured speed",
                    sample.servo_id
                ));
            }
        }
        previous.insert(sample.servo_id, sample);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::project::{
        ProjectSceneColliderGeometry, ProjectVehicleColliderConfig, ProjectVehicleMotorConfig,
        ProjectVehiclePhysicsConfig,
    };

    fn measured_record() -> CalibrationRecord {
        CalibrationRecord {
            format: CALIBRATION_FORMAT.to_string(),
            hardware_revision: "puppybot-prototype-a".to_string(),
            provenance: MeasurementProvenance {
                measured_at: "2026-07-20T12:00:00Z".to_string(),
                operator: "tester".to_string(),
                method: "scale, calipers, tachometer, and servo log".to_string(),
                source: "lab notebook calibration-run-17".to_string(),
            },
            vehicle: VehicleMeasurement {
                mass_kg: 2.4,
                center_of_mass_m: [0.0, 0.0, 0.08],
                wheel_radius_m: 0.04,
                gear_ratio: 1.0,
                motor_stall_torque_nm: 0.45,
                motor_no_load_rpm: 120.0,
            },
            servos: vec![ServoMeasurement {
                servo_id: 1,
                max_speed_ticks_per_sec: 1000.0,
            }],
            drive_trace: vec![
                DriveTraceSample {
                    time_sec: 0.0,
                    left_command: 0.0,
                    right_command: 0.0,
                    observed_linear_mps: 0.0,
                    observed_yaw_rps: 0.0,
                },
                DriveTraceSample {
                    time_sec: 1.0,
                    left_command: 0.5,
                    right_command: 0.5,
                    observed_linear_mps: 0.15,
                    observed_yaw_rps: 0.0,
                },
            ],
            servo_trace: vec![
                ServoTraceSample {
                    time_sec: 0.0,
                    servo_id: 1,
                    target_ticks: 2200,
                    observed_present_ticks: 2048,
                    observed_load: None,
                    observed_current_raw: None,
                },
                ServoTraceSample {
                    time_sec: 0.2,
                    servo_id: 1,
                    target_ticks: 2200,
                    observed_present_ticks: 2200,
                    observed_load: Some(120),
                    observed_current_raw: Some(230),
                },
            ],
        }
    }

    fn authored_vehicle_profile() -> ProjectVehiclePhysicsConfig {
        ProjectVehiclePhysicsConfig {
            mass_kg: 8.0,
            center_of_mass_m: [0.0; 3],
            linear_damping: 0.25,
            angular_damping: 1.0,
            wheelbase_m: 0.22,
            track_width_m: 0.18,
            max_wheel_speed_mps: 0.4,
            max_drive_force_n: 18.0,
            lateral_grip_n_per_mps: 45.0,
            steering_response_deg_per_sec: 240.0,
            motor: ProjectVehicleMotorConfig {
                wheel_radius_m: 0.03,
                gear_ratio: 2.0,
                stall_torque_nm: 0.2,
                no_load_rpm: 80.0,
                brake_torque_nm: 0.25,
                rolling_resistance_n: 0.4,
            },
            colliders: vec![ProjectVehicleColliderConfig {
                geometry: ProjectSceneColliderGeometry::Box {
                    size: [0.3, 0.2, 0.1],
                },
                offset: [0.0; 3],
                rotation: [0.0; 3],
            }],
            collision_profile: Some("reviewed.json".to_string()),
        }
    }

    #[test]
    fn validates_measured_provenance_and_observed_traces() {
        assert!(validate_calibration(&measured_record()).valid);
    }

    #[test]
    fn rejects_unproven_or_physically_inconsistent_trace_data() {
        let mut record = measured_record();
        record.provenance.source.clear();
        record.drive_trace[1].observed_linear_mps = 10.0;
        record.servo_trace[1].observed_present_ticks = 4095;
        record.servo_trace[1].observed_current_raw = Some(5000);
        let report = validate_calibration(&record);
        assert!(!report.valid);
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("missing provenance.source"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("exceeds no-load"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("faster than measured speed"))
        );
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("observed current"))
        );
    }

    #[test]
    fn invalid_calibration_cannot_change_an_authored_profile() {
        let source = authored_vehicle_profile();
        let mut invalid = measured_record();
        invalid.provenance.source.clear();
        assert!(apply_calibration_to_vehicle_profile(&source, &invalid).is_err());
        assert_eq!(source.mass_kg, 8.0);
        assert_eq!(source.motor.wheel_radius_m, 0.03);
    }

    #[test]
    fn valid_calibration_maps_measured_fields_and_preserves_provenance() {
        let source = authored_vehicle_profile();
        let applied = apply_calibration_to_vehicle_profile(&source, &measured_record()).unwrap();
        assert_eq!(applied.servo_trace_evidence[1].observed_load, Some(120));
        assert_eq!(applied.source_profile, source);
        assert_eq!(applied.vehicle.mass_kg, 2.4);
        assert_eq!(applied.vehicle.center_of_mass_m, [0.0, 0.0, 0.08]);
        assert_eq!(applied.vehicle.motor.wheel_radius_m, 0.04);
        assert_eq!(applied.vehicle.motor.stall_torque_nm, 0.45);
        assert_eq!(applied.vehicle.linear_damping, 0.25);
        assert_eq!(
            applied.vehicle.collision_profile.as_deref(),
            Some("reviewed.json")
        );
        assert_eq!(applied.hardware_revision, "puppybot-prototype-a");
        assert_eq!(applied.provenance.source, "lab notebook calibration-run-17");
        assert_eq!(
            applied.servo_measurements[0].max_speed_ticks_per_sec,
            1000.0
        );
    }
}
