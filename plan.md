# Hardware Bus Project Model Plan

## Goal

Represent robot mechanics, hardware buses, simulated devices, and their mappings as separate concepts.

The robot model owns links, joints, meshes, and limits. A bus owns transport, protocol, devices, commands, and telemetry. A mapping connects a hardware device to a simulation object when needed.

This keeps the scene tree honest and prepares the app for both virtual devices and real hardware.

Board direction for PuppyBot prototype work on 2026-07-01: use RobotDreams to host a deterministic ball-to-bin scenario that proves PuppyBot arm control, virtual bus integration, simple perception, and explicit success/failure checks. Do not treat a visual arm motion demo as manipulation unless the scene contains a tracked ball object, a grasp or attach rule, and a bin containment assertion.

## Scene Tree

Use separate top-level groups:

```text
Robots
  PuppyBot
    Links
    Joints

Hardware
  Main Serial Bus
    Servo 1
    Servo 2
    Servo 3
    IO Board 20
    IMU 30

Sensors
  Joint States
  Camera_1
  Lidar_1
```

Do not put the servo bus under the robot. The bus is hardware/control infrastructure, not robot geometry.

Do not keep a second `Sensors > Servo Bus` row. The bus can expose telemetry, but the bus itself belongs under `Hardware`.

## Ownership Rules

- `Robot > Joints` describes the mechanical simulation model.
- `Hardware > Bus > Device` describes physical or virtual bus devices.
- Device-to-robot relationships are references, not containment.
- A servo can drive a robot joint.
- An IMU can be mounted on a robot link.
- An IO board may have no robot geometry mapping.
- The same project format should work for virtual simulation and real serial hardware.

## Project File Shape

Store bus/device definitions and mappings in the project file, not the URDF.

The URDF remains mechanical. The project file describes runtime hardware and simulation wiring.

Draft JSON:

```json
{
  "format": "robotdreams.project.v1",
  "name": "PuppyBot Bin And Ball",
  "modelProfile": "examples/assets/robots/puppybot/robotdreams.json",
  "robots": [
    {
      "id": "puppybot",
      "name": "PuppyBot",
      "model": {
        "type": "urdf",
        "path": "examples/assets/robots/puppybot/final2/urdf/final2.urdf"
      }
    }
  ],
  "hardware": {
    "buses": [
      {
        "id": "main_bus",
        "name": "Main Serial Bus",
        "transport": {
          "type": "virtual",
          "baud": 1000000
        },
        "protocol": "feetech",
        "devices": [
          {
            "type": "servo",
            "id": 1,
            "name": "Base Servo",
            "profile": "ST3215",
            "drives": {
              "robot": "puppybot",
              "joint": "revolute_2_1"
            },
            "calibration": {
              "zeroOffset": 2048,
              "direction": 1
            }
          },
          {
            "type": "servo",
            "id": 2,
            "name": "Shoulder Servo",
            "profile": "ST3215",
            "drives": {
              "robot": "puppybot",
              "joint": "revolute_1_2"
            },
            "calibration": {
              "zeroOffset": 2048,
              "direction": -1
            }
          },
          {
            "type": "io_board",
            "id": 20,
            "name": "Tool IO",
            "profile": "custom_gpio"
          },
          {
            "type": "imu",
            "id": 30,
            "name": "Wrist IMU",
            "profile": "custom_imu",
            "mountedOn": {
              "robot": "puppybot",
              "link": "wrist_link"
            }
          }
        ]
      }
    ]
  }
}
```

## Runtime Resolution

On project load:

```text
project file
  -> robots[] load URDF models
  -> hardware.buses[] create bus drivers or simulators
  -> devices[] create typed simulated or real device states
  -> drives / mountedOn mappings connect devices to simulation targets
```

For servo-driven joints:

```text
slider or controller command
  -> target joint
  -> mapping lookup
  -> servo command
  -> bus driver or simulator
  -> servo telemetry snapshot
  -> backend joint state
  -> backend dynamic transforms
  -> frontend cached Three.js objects
```

The backend remains authoritative for joint state and transforms.

## Device Model

Every bus device should share a small base shape:

```text
type
id
name
profile
status
telemetry
mapping fields depending on type
```

Servo-specific fields:

```text
drives.robot
drives.joint
calibration.zeroOffset
calibration.direction
limits
targetPosition
presentPosition
torque
temperature
voltage
```

IMU-specific fields:

```text
mountedOn.robot
mountedOn.link
orientation
angularVelocity
linearAcceleration
```

IO-board-specific fields:

```text
digitalInputs
digitalOutputs
analogInputs
```

## Backend Types

Add project/runtime types roughly like:

```rust
struct ProjectConfig {
    format: String,
    name: String,
    robots: Vec<ProjectRobotConfig>,
    hardware: HardwareConfig,
}

struct HardwareConfig {
    buses: Vec<BusConfig>,
}

struct BusConfig {
    id: String,
    name: String,
    transport: BusTransportConfig,
    protocol: String,
    devices: Vec<DeviceConfig>,
}

enum DeviceConfig {
    Servo(ServoDeviceConfig),
    Imu(ImuDeviceConfig),
    IoBoard(IoBoardDeviceConfig),
}
```

Runtime state should be separate from config:

```text
ProjectConfig: loaded from disk
HardwareRuntime: bus drivers, simulators, snapshots
SimulationState: robot joint state, physics state, transforms
```

## UI Plan

Scene rows:

```text
SCENE_SECTION_ROBOTS
SCENE_SECTION_HARDWARE
SCENE_SECTION_SENSORS

SCENE_ROW_ROBOT_BASE
SCENE_ROW_LINK_BASE
SCENE_ROW_JOINT_BASE
SCENE_ROW_BUS_BASE
SCENE_ROW_DEVICE_BASE
```

Selected inspector behavior:

- Selecting a robot shows URDF/model summary.
- Selecting a joint shows mechanical joint data and mapped device, if any.
- Selecting a bus shows protocol, transport, baud, device count, and connection status.
- Selecting a servo shows id, profile, mapped joint, target, present position, and calibration.
- Selecting an IMU shows mounted link and live telemetry.
- Selecting an IO board shows channels and live values.

## Implementation Steps

1. Move `Virtual ServoBus` out of `Robots` into a new `Hardware` section.
2. Remove `Sensors > Servo Bus`.
3. Add bus child rows for devices.
4. Start with inferred devices from movable URDF joints:
   - servo id `1` maps to movable joint slot `0`
   - servo id `2` maps to movable joint slot `1`
   - continue by slot
5. Add project-file parsing for explicit `hardware.buses[].devices[]`.
6. Use explicit project mappings when available and inferred mappings only as fallback.
7. Route joint slider commands through the mapping:
   - joint -> servo device -> bus simulator/driver
8. Feed servo telemetry back into backend joint state.
9. Keep backend-generated dynamic transforms as the only source of rendered joint motion.
10. Extend device types beyond servos after the bus/device abstraction is stable.

## Migration Rule

Existing URDF-only launches should still work.

When no project hardware config exists:

```text
create one virtual Feetech bus
create one ST3215 servo per movable URDF joint
map servo ids by movable joint order
```

This gives the current demo behavior while making the project file the long-term source of truth.
