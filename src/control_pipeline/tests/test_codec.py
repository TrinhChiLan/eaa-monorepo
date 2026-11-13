"""Unit tests for B-03 v1.0 binary codec."""

from __future__ import annotations

import struct
import pytest

from control_pipeline import codec
from control_pipeline.schemas.bridge import McuState


def test_crc_check_vector():
    """Verify standard test vector: crc16("123456789") == 0x29B1."""
    assert codec.crc16_ccitt_false(b"123456789") == 0x29B1


def test_newer_seq_basic():
    assert codec.newer_seq(1, 0) is True
    assert codec.newer_seq(0, 1) is False
    assert codec.newer_seq(100, 99) is True
    assert codec.newer_seq(99, 100) is False


def test_newer_seq_wraparound():
    """Sequence wraps at 65536. 0 is newer than 65535."""
    assert codec.newer_seq(0, 65535) is True
    assert codec.newer_seq(1, 65530) is True
    assert codec.newer_seq(65535, 0) is False


def test_encode_decode_heartbeat():
    hb = codec.encode_heartbeat(seq=1, ack=0, tick_ms=1000, mpu_state=1)
    assert hb[0] == codec.MAGIC
    assert hb[1] == codec.MT_HEARTBEAT

    dec = codec.decode_frame(hb)
    assert dec is not None
    msg_type, seq, ack, tick_ms, flags, payload = dec
    assert msg_type == codec.MT_HEARTBEAT
    assert seq == 1
    assert ack == 0
    assert tick_ms == 1000
    assert len(payload) == 1
    assert payload[0] == 1


def test_encode_decode_run():
    run = codec.encode_run(seq=2, ack=1, tick_ms=2000, cmd_id=7, ttl_ms=500, kind=0, setpoint_mms=120)
    assert run[0] == codec.MAGIC
    assert run[1] == codec.MT_RUN

    dec = codec.decode_frame(run)
    assert dec is not None
    msg_type, seq, ack, tick_ms, flags, payload = dec
    assert msg_type == codec.MT_RUN
    assert seq == 2
    assert ack == 1
    cmd_id, ttl, kind, sp = struct.unpack_from("<HHBH", payload)
    assert cmd_id == 7
    assert ttl == 500
    assert kind == 0
    assert sp == 120


def test_spec_vector_telemetry():
    """Verify exact 27-byte test vector from Section 9 of B-03 spec."""
    raw = bytes.fromhex(
        "aa 90 28 00 02 00 88 13 00 00 37 0d 02 00 78 00 76 00 52 03 56 01 80 76 00 17 6f"
    )
    dec = codec.decode_frame(raw)
    assert dec is not None
    tf = codec.decode_telemetry(*dec)
    assert tf is not None
    assert tf.mcu_state == McuState.RUNNING
    assert tf.setpoint_mms == 120
    assert tf.speed_mms == 118
    assert tf.current_ma == 850
    assert tf.temp_c10 == 342  # 34.2 °C
    assert tf.flags == 0x37


def test_bad_magic_rejected():
    raw = bytearray(codec.encode_heartbeat(seq=1, ack=0, tick_ms=1000, mpu_state=1))
    raw[0] = 0x55  # corrupt magic
    assert codec.decode_frame(bytes(raw)) is None


def test_bad_crc_rejected():
    raw = bytearray(codec.encode_heartbeat(seq=1, ack=0, tick_ms=1000, mpu_state=1))
    raw[-1] ^= 0xFF  # corrupt CRC
    assert codec.decode_frame(bytes(raw)) is None


def test_oversized_payload_rejected():
    with pytest.raises(ValueError):
        codec.encode_frame(
            msg_type=0x10,
            seq=1,
            ack=0,
            tick_ms=1000,
            flags=0,
            payload=b"\x00" * 45,  # max is 40
        )
