#!/usr/bin/env python3

import argparse
import importlib
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterable, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SDK_ROOTS = [
    REPO_ROOT / "third_party",
    REPO_ROOT / "workdir" / "STServo_Python" / "stservo-env",
]


def assert_true(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def log(message: str) -> None:
    print(f"[sdk-e2e] {message}", flush=True)


def wait_for_vbus_path(process: subprocess.Popen[str], timeout_s: float) -> str:
    deadline = time.time() + timeout_s
    lines: list[str] = []
    while time.time() < deadline:
        line = process.stdout.readline() if process.stdout is not None else ""
        if line:
            clean = line.rstrip()
            lines.append(clean)
            print(f"[robot_dreams] {clean}", flush=True)
            m = re.search(r"Slave device:\s*(\S+)", clean)
            if m:
                return m.group(1)
            continue

        if process.poll() is not None:
            raise RuntimeError(
                "robot_dreams exited before publishing virtual bus path.\n"
                + "\n".join(lines[-20:])
            )
        time.sleep(0.05)

    raise TimeoutError(
        "Timed out waiting for robot_dreams virtual bus path.\n"
        + "\n".join(lines[-20:])
    )


def terminate_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=3)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=3)


def resolve_sdk_root(explicit_root: str | None) -> Path:
    if explicit_root:
        root = Path(explicit_root)
        if not root.is_absolute():
            root = REPO_ROOT / root
        return root

    for root in DEFAULT_SDK_ROOTS:
        if root.exists():
            return root

    return DEFAULT_SDK_ROOTS[0]


def load_sdk(module_name: str, sdk_root: Path):
    if str(sdk_root) not in sys.path:
        sys.path.insert(0, str(sdk_root))
    return importlib.import_module(module_name)


def make_port_handler(base_port_handler):
    import serial

    class PtySafePortHandler(base_port_handler):
        def setupPort(self, cflag_baud):
            if self.is_open:
                self.closePort()

            self.ser = serial.Serial(
                port=self.port_name,
                baudrate=self.baudrate,
                bytesize=serial.EIGHTBITS,
                timeout=0,
            )

            try:
                self.ser.setRTS(False)
            except Exception:
                pass
            try:
                self.ser.setDTR(False)
            except Exception:
                pass

            self.is_open = True
            self.ser.reset_input_buffer()
            self.tx_time_per_byte = (1000.0 / self.baudrate) * 10.0
            return True

    return PtySafePortHandler


def run_sdk_suite(
    module_name: str,
    sdk_root: Path,
    robot_bin: str,
    servo_ids: Sequence[int],
    baud: int,
) -> None:
    log(f"Running suite for module `{module_name}`")
    sdk = load_sdk(module_name, sdk_root)

    for attr in (
        "PortHandler",
        "GroupSyncRead",
        "scscl",
        "COMM_SUCCESS",
        "SCSCL_GOAL_POSITION_L",
        "SCSCL_GOAL_TIME_L",
        "SCSCL_LOCK",
        "SCSCL_PRESENT_POSITION_L",
        "SCSCL_MOVING",
    ):
        assert_true(hasattr(sdk, attr), f"{module_name} missing attribute `{attr}`")

    PortHandler = make_port_handler(sdk.PortHandler)
    GroupSyncRead = sdk.GroupSyncRead
    Scscl = sdk.scscl
    COMM_SUCCESS = sdk.COMM_SUCCESS

    goal_addr = sdk.SCSCL_GOAL_POSITION_L
    goal_time_addr = sdk.SCSCL_GOAL_TIME_L
    lock_addr = sdk.SCSCL_LOCK
    present_pos_addr = sdk.SCSCL_PRESENT_POSITION_L
    moving_addr = sdk.SCSCL_MOVING

    process = subprocess.Popen(
        [
            robot_bin,
            "vbus",
            "--first-servo-id",
            str(min(servo_ids)),
            "--last-servo-id",
            str(max(servo_ids)),
            "--step-seconds",
            "0.005",
            "--idle-sleep-ms",
            "1",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    port = None
    try:
        slave_path = wait_for_vbus_path(process, timeout_s=10.0)
        log(f"Virtual bus path is {slave_path}")

        port = PortHandler(slave_path)
        selected_baud = baud
        try:
            port.baudrate = selected_baud
            assert_true(port.openPort(), f"openPort() failed at baud {selected_baud}")
        except Exception as err:
            if selected_baud != 115200:
                log(
                    f"openPort at baud {selected_baud} failed ({err}); retrying at 115200 for PTY compatibility"
                )
                selected_baud = 115200
                port.baudrate = selected_baud
                assert_true(port.openPort(), "openPort() fallback at 115200 failed")
            else:
                raise
        log(f"Connected SDK port at baud {selected_baud}")

        client = Scscl(port)

        def check_result(label: str, result: int, error: int = 0) -> None:
            assert_true(
                result == COMM_SUCCESS,
                f"{label}: result={result}, expected COMM_SUCCESS",
            )
            assert_true(error == 0, f"{label}: error={error}, expected 0")

        def read_u16(sid: int, address: int) -> int:
            value, result, error = client.read2ByteTxRx(sid, address)
            check_result(f"read2ByteTxRx(id={sid}, addr={address})", result, error)
            return value

        def read_u8(sid: int, address: int) -> int:
            value, result, error = client.read1ByteTxRx(sid, address)
            check_result(f"read1ByteTxRx(id={sid}, addr={address})", result, error)
            return value

        for sid in servo_ids:
            model, result, error = client.ping(sid)
            check_result(f"ping({sid})", result, error)
            log(f"ping({sid}) ok, model={model}")

        result, error = client.WritePos(servo_ids[0], 3000, 0, 400)
        check_result("WritePos", result, error)
        assert_true(
            read_u16(servo_ids[0], goal_addr) == 3000,
            "WritePos did not update GOAL_POSITION register",
        )

        reg_id = servo_ids[1]
        initial_goal = read_u16(reg_id, goal_addr)
        result, error = client.RegWritePos(reg_id, 2500, 0, 400)
        check_result("RegWritePos", result, error)
        assert_true(
            read_u16(reg_id, goal_addr) == initial_goal,
            "RegWritePos applied before RegAction",
        )
        result = client.RegAction()
        assert_true(result == COMM_SUCCESS, f"RegAction: result={result}")
        time.sleep(0.05)
        assert_true(
            read_u16(reg_id, goal_addr) == 2500,
            "RegAction did not apply queued goal position",
        )

        assert_true(client.SyncWritePos(servo_ids[2], 1800, 0, 400), "SyncWritePos addParam id3 failed")
        assert_true(client.SyncWritePos(servo_ids[3], 2200, 0, 400), "SyncWritePos addParam id4 failed")
        result = client.groupSyncWrite.txPacket()
        assert_true(result == COMM_SUCCESS, f"groupSyncWrite.txPacket: result={result}")
        client.groupSyncWrite.clearParam()
        time.sleep(0.05)
        assert_true(
            read_u16(servo_ids[2], goal_addr) == 1800,
            "SyncWritePos did not update servo 3 goal",
        )
        assert_true(
            read_u16(servo_ids[3], goal_addr) == 2200,
            "SyncWritePos did not update servo 4 goal",
        )

        sync_read = GroupSyncRead(client, present_pos_addr, 4)
        for sid in servo_ids:
            assert_true(sync_read.addParam(sid), f"GroupSyncRead addParam({sid}) failed")

        result = sync_read.txRxPacket()
        assert_true(result == COMM_SUCCESS, f"GroupSyncRead txRxPacket: result={result}")

        for sid in servo_ids:
            available, last_error = sync_read.isAvailable(sid, present_pos_addr, 4)
            assert_true(available, f"GroupSyncRead data unavailable for servo {sid}")
            assert_true(last_error == 0, f"GroupSyncRead reported servo error for {sid}: {last_error}")
            _position = sync_read.getData(sid, present_pos_addr, 2)
            _speed = sync_read.getData(sid, present_pos_addr + 2, 2)

        for sid in servo_ids:
            _, _, result, error = client.ReadPosSpeed(sid)
            check_result(f"ReadPosSpeed({sid})", result, error)
            _, result, error = client.ReadMoving(sid)
            check_result(f"ReadMoving({sid})", result, error)
            _ = read_u8(sid, moving_addr)

        result, error = client.WritePWM(servo_ids[0], 33)
        check_result("WritePWM", result, error)
        assert_true(
            read_u16(servo_ids[0], goal_time_addr) == 33,
            "WritePWM did not update GOAL_TIME register",
        )

        result, error = client.LockEprom(servo_ids[0])
        check_result("LockEprom", result, error)
        assert_true(read_u8(servo_ids[0], lock_addr) == 1, "LockEprom did not set lock=1")
        result, error = client.unLockEprom(servo_ids[0])
        check_result("unLockEprom", result, error)
        assert_true(read_u8(servo_ids[0], lock_addr) == 0, "unLockEprom did not set lock=0")

        log(f"Suite passed for `{module_name}`")

    finally:
        if port is not None and getattr(port, "is_open", False):
            port.closePort()
        terminate_process(process)


def parse_servo_ids(values: Iterable[str]) -> list[int]:
    ids = [int(v) for v in values]
    assert_true(ids, "At least one --servo-id is required")
    assert_true(all(1 <= sid <= 252 for sid in ids), "Servo IDs must be in [1, 252]")
    return ids


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run Python SDK e2e tests against robot_dreams virtual bus"
    )
    parser.add_argument(
        "--robot-bin",
        default=os.environ.get("ROBOT_DREAMS_BIN", str(REPO_ROOT / "target" / "release" / "robot_dreams")),
        help="Path to robot_dreams binary",
    )
    parser.add_argument(
        "--sdk-modules",
        default=os.environ.get("SDK_MODULES", "scservo_sdk"),
        help="Comma-separated SDK module names under --sdk-root",
    )
    parser.add_argument(
        "--sdk-root",
        default=os.environ.get("SDK_ROOT"),
        help="Path that contains SDK module directories (defaults to tracked third_party)",
    )
    parser.add_argument(
        "--baud",
        type=int,
        default=int(os.environ.get("SDK_BAUD", "1000000")),
        help="Baudrate passed to SDK PortHandler",
    )
    parser.add_argument(
        "--servo-ids",
        nargs="+",
        default=os.environ.get("SDK_SERVO_IDS", "1,2,3,4").split(","),
        help="Servo IDs to use in tests",
    )
    args = parser.parse_args()

    robot_bin_path = Path(args.robot_bin)
    assert_true(robot_bin_path.exists(), f"robot binary not found: {robot_bin_path}")
    sdk_root = resolve_sdk_root(args.sdk_root)
    assert_true(sdk_root.exists(), f"sdk root not found: {sdk_root}")
    log(f"Using SDK root: {sdk_root}")

    servo_ids = parse_servo_ids(args.servo_ids)
    assert_true(
        len(servo_ids) >= 4,
        "Need at least 4 servo IDs for full sync write/read coverage",
    )

    modules = [m.strip() for m in args.sdk_modules.split(",") if m.strip()]
    assert_true(modules, "No SDK module names provided")
    for module_name in modules:
        module_path = sdk_root / module_name
        assert_true(
            module_path.exists(),
            f"module `{module_name}` not found at {module_path}",
        )

    started = time.time()
    for module_name in modules:
        run_sdk_suite(module_name, sdk_root, str(robot_bin_path), servo_ids, args.baud)

    elapsed = time.time() - started
    log(f"All SDK suites passed in {elapsed:.2f}s")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        log(f"FAILED: {exc}")
        raise
