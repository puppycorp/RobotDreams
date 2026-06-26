#!/usr/bin/env python3

import sys
import time

import serial

from scservo_sdk import *

serial.Serial.setRTS = lambda self, level=True: None
serial.Serial.setDTR = lambda self, level=True: None

SCRATCH_1 = 0x70
SCRATCH_2 = 0x72
SCRATCH_4 = 0x74


def require_success(name, result, error=0):
    if result != COMM_SUCCESS or error != 0:
        raise AssertionError(f"{name} failed: result={result} error={error}")


def require_equal(name, actual, expected):
    if actual != expected:
        raise AssertionError(f"{name}: expected {expected!r}, got {actual!r}")


def require_true(name, value):
    if not value:
        raise AssertionError(f"{name}: expected truthy value, got {value!r}")


def check_protocol_handler(sts):
    model, result, error = sts.ping(1)
    require_success("ping", result, error)
    require_equal("ping model", model, 0)

    result, error = sts.write1ByteTxRx(1, SMS_STS_TORQUE_ENABLE, 0)
    require_success("write1ByteTxRx", result, error)
    value, result, error = sts.read1ByteTxRx(1, SMS_STS_TORQUE_ENABLE)
    require_success("read1ByteTxRx", result, error)
    require_equal("read1ByteTxRx value", value, 0)

    result = sts.write1ByteTxOnly(BROADCAST_ID, SMS_STS_TORQUE_ENABLE, 1)
    require_success("write1ByteTxOnly", result)
    value, result, error = sts.read1ByteTxRx(1, SMS_STS_TORQUE_ENABLE)
    require_success("read after write1ByteTxOnly", result, error)
    require_equal("write1ByteTxOnly value", value, 1)

    result, error = sts.writeTxRx(1, SCRATCH_1, 1, [12])
    require_success("writeTxRx", result, error)
    data, result, error = sts.readTxRx(1, SCRATCH_1, 1)
    require_success("readTxRx", result, error)
    require_equal("readTxRx data", data, [12])

    result = sts.writeTxOnly(BROADCAST_ID, SCRATCH_1, 1, [13])
    require_success("writeTxOnly", result)
    data, result, error = sts.readTxRx(1, SCRATCH_1, 1)
    require_success("read after writeTxOnly", result, error)
    require_equal("writeTxOnly data", data, [13])

    result = sts.read1ByteTx(1, SCRATCH_1)
    require_success("read1ByteTx", result)
    value, result, error = sts.read1ByteRx(1)
    require_success("read1ByteRx", result, error)
    require_equal("read1ByteRx value", value, 13)

    result, error = sts.write2ByteTxRx(1, SCRATCH_2, 1234)
    require_success("write2ByteTxRx", result, error)
    value, result, error = sts.read2ByteTxRx(1, SCRATCH_2)
    require_success("read2ByteTxRx", result, error)
    require_equal("read2ByteTxRx value", value, 1234)

    result = sts.write2ByteTxOnly(BROADCAST_ID, SCRATCH_2, 222)
    require_success("write2ByteTxOnly", result)
    value, result, error = sts.read2ByteTxRx(1, SCRATCH_2)
    require_success("read after write2ByteTxOnly", result, error)
    require_equal("write2ByteTxOnly value", value, 222)

    result = sts.read2ByteTx(1, SCRATCH_2)
    require_success("read2ByteTx", result)
    value, result, error = sts.read2ByteRx(1)
    require_success("read2ByteRx", result, error)
    require_equal("read2ByteRx value", value, 222)

    result, error = sts.write4ByteTxRx(1, SCRATCH_4, 0x01020304)
    require_success("write4ByteTxRx", result, error)
    value, result, error = sts.read4ByteTxRx(1, SCRATCH_4)
    require_success("read4ByteTxRx", result, error)
    require_equal("read4ByteTxRx value", value, 0x01020304)

    result = sts.write4ByteTxOnly(BROADCAST_ID, SCRATCH_4, 0x05060708)
    require_success("write4ByteTxOnly", result)
    value, result, error = sts.read4ByteTxRx(1, SCRATCH_4)
    require_success("read after write4ByteTxOnly", result, error)
    require_equal("write4ByteTxOnly value", value, 0x05060708)

    result = sts.read4ByteTx(1, SCRATCH_4)
    require_success("read4ByteTx", result)
    value, result, error = sts.read4ByteRx(1)
    require_success("read4ByteRx", result, error)
    require_equal("read4ByteRx value", value, 0x05060708)

    result = sts.regWriteTxOnly(BROADCAST_ID, SMS_STS_ACC, 1, [9])
    require_success("regWriteTxOnly", result)
    value, result, error = sts.read1ByteTxRx(1, SMS_STS_ACC)
    require_success("read before action", result, error)
    require_equal("regWriteTxOnly before action", value, 0)
    result = sts.action(BROADCAST_ID)
    require_success("action after regWriteTxOnly", result)
    value, result, error = sts.read1ByteTxRx(1, SMS_STS_ACC)
    require_success("read after action", result, error)
    require_equal("regWriteTxOnly after action", value, 9)

    result, error = sts.regWriteTxRx(1, SMS_STS_ACC, 1, [11])
    require_success("regWriteTxRx", result, error)
    value, result, error = sts.read1ByteTxRx(1, SMS_STS_ACC)
    require_success("read before second action", result, error)
    require_equal("regWriteTxRx before action", value, 9)
    result = sts.action(BROADCAST_ID)
    require_success("action after regWriteTxRx", result)
    value, result, error = sts.read1ByteTxRx(1, SMS_STS_ACC)
    require_success("read after second action", result, error)
    require_equal("regWriteTxRx after action", value, 11)

    result = sts.readTx(1, SCRATCH_2, 2)
    require_success("readTx", result)
    data, result, error = sts.readRx(1, 2)
    require_success("readRx", result, error)
    require_equal("readRx length", len(data), 2)

    result = sts.syncWriteTxOnly(SCRATCH_2, 2, [1, 0x78, 0x56, 2, 0x34, 0x12], 6)
    require_success("syncWriteTxOnly", result)
    value, result, error = sts.read2ByteTxRx(1, SCRATCH_2)
    require_success("read sync write id 1", result, error)
    require_equal("syncWriteTxOnly id 1 value", value, 0x5678)
    value, result, error = sts.read2ByteTxRx(2, SCRATCH_2)
    require_success("read sync write id 2", result, error)
    require_equal("syncWriteTxOnly id 2 value", value, 0x1234)

    result = sts.syncReadTx(SCRATCH_2, 2, [1, 2], 2)
    require_success("syncReadTx", result)
    result, rxpacket = sts.syncReadRx(2, 2)
    require_success("syncReadRx", result)
    require_true("syncReadRx packet length", len(rxpacket) >= 16)


def check_sts(sts):
    result, error = sts.WritePosEx(1, 1500, 300, 20)
    require_success("WritePosEx", result, error)

    pos, result, error = sts.ReadPos(1)
    require_success("ReadPos", result, error)
    require_true("ReadPos after WritePosEx", pos >= 0)

    speed, result, error = sts.ReadSpeed(1)
    require_success("ReadSpeed", result, error)
    require_true("ReadSpeed type", isinstance(speed, int))

    pos, speed, result, error = sts.ReadPosSpeed(1)
    require_success("ReadPosSpeed", result, error)
    require_true("ReadPosSpeed values", isinstance(pos, int) and isinstance(speed, int))

    moving, result, error = sts.ReadMoving(1)
    require_success("ReadMoving", result, error)
    require_true("ReadMoving value", moving in (0, 1))

    require_true("SyncWritePosEx 1", sts.SyncWritePosEx(1, 1600, 400, 12))
    require_true("SyncWritePosEx 2", sts.SyncWritePosEx(2, 1700, 500, 13))
    result = sts.groupSyncWrite.txPacket()
    require_success("STS group sync write txPacket", result)

    group = GroupSyncRead(sts, SMS_STS_PRESENT_POSITION_L, 4)
    require_true("GroupSyncRead add 1", group.addParam(1))
    require_true("GroupSyncRead add 2", group.addParam(2))
    result = group.txRxPacket()
    require_success("GroupSyncRead txRxPacket", result)
    available, error = group.isAvailable(1, SMS_STS_PRESENT_POSITION_L, 2)
    require_equal("GroupSyncRead availability error", error, 0)
    require_true("GroupSyncRead available", available)
    require_true("GroupSyncRead data", isinstance(group.getData(1, SMS_STS_PRESENT_POSITION_L, 2), int))

    result, error = sts.RegWritePosEx(1, 1800, 600, 14)
    require_success("RegWritePosEx", result, error)
    result = sts.RegAction()
    require_success("RegAction", result)

    result, error = sts.WheelMode(1)
    require_success("WheelMode", result, error)
    result, error = sts.WriteSpec(1, 500, 10)
    require_success("WriteSpec positive", result, error)
    time.sleep(0.08)
    speed, result, error = sts.ReadSpeed(1)
    require_success("ReadSpeed after WriteSpec positive", result, error)
    require_true("wheel speed positive", speed > 0)

    result, error = sts.WriteSpec(1, -350, 10)
    require_success("WriteSpec negative", result, error)
    time.sleep(0.12)
    speed, result, error = sts.ReadSpeed(1)
    require_success("ReadSpeed after WriteSpec negative", result, error)
    require_true("wheel speed negative", speed < 0)

    result, error = sts.LockEprom(1)
    require_success("LockEprom", result, error)
    result, error = sts.unLockEprom(1)
    require_success("unLockEprom", result, error)


def check_scscl(scs):
    result, error = scs.WritePos(2, 1111, 222, 333)
    require_success("SCSCL WritePos", result, error)

    pos, result, error = scs.ReadPos(2)
    require_success("SCSCL ReadPos", result, error)
    require_true("SCSCL ReadPos value", isinstance(pos, int))

    speed, result, error = scs.ReadSpeed(2)
    require_success("SCSCL ReadSpeed", result, error)
    require_true("SCSCL ReadSpeed value", isinstance(speed, int))

    pos, speed, result, error = scs.ReadPosSpeed(2)
    require_success("SCSCL ReadPosSpeed", result, error)
    require_true("SCSCL ReadPosSpeed values", isinstance(pos, int) and isinstance(speed, int))

    moving, result, error = scs.ReadMoving(2)
    require_success("SCSCL ReadMoving", result, error)
    require_true("SCSCL ReadMoving value", moving in (0, 1))

    require_true("SCSCL SyncWritePos", scs.SyncWritePos(2, 1200, 250, 350))
    result = scs.groupSyncWrite.txPacket()
    require_success("SCSCL group sync write txPacket", result)

    result, error = scs.RegWritePos(2, 1300, 260, 360)
    require_success("SCSCL RegWritePos", result, error)
    result = scs.RegAction()
    require_success("SCSCL RegAction", result)

    result, error = scs.PWMMode(2)
    require_success("SCSCL PWMMode", result, error)
    result, error = scs.WritePWM(2, -123)
    require_success("SCSCL WritePWM", result, error)

    result, error = scs.LockEprom(2)
    require_success("SCSCL LockEprom", result, error)
    result, error = scs.unLockEprom(2)
    require_success("SCSCL unLockEprom", result, error)


def main():
    if len(sys.argv) != 2:
        raise SystemExit("usage: python_sdk_compat.py /dev/pts/N")

    port = PortHandler(sys.argv[1])
    if not port.openPort():
        raise AssertionError("openPort failed")
    if not port.setBaudRate(1000000):
        raise AssertionError("setBaudRate failed")

    try:
        sts = sms_sts(port)
        scs = scscl(port)

        check_protocol_handler(sts)
        check_sts(sts)
        check_scscl(scs)
    finally:
        port.closePort()


if __name__ == "__main__":
    main()
