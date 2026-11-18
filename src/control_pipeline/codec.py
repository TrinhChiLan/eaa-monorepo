"""codec.py — B-03 v1.1 binary frame encoder / decoder for the EAA control_pipeline.

Frame wire format (all multi-byte fields little-endian):
  Offset  Len  Field
  ------  ---  -----
  0       1    magic   = 0xAA
  1       1    msg_type
  2       2    seq     (u16 LE)
  4       2    ack     (u16 LE)
  6       4    tick_ms (u32 LE)
  10      1    flags
  11      1    length  (payload bytes)
  12      N    payload (0 ≤ N ≤ MAX_PAYLOAD)
  12+N    2    crc16   (u16 LE, covers bytes 0 … 12+N-1)

Total minimum frame length: 14 bytes (zero-length payload).

CRC: CRC-16/CCITT-FALSE (poly 0x1021, init 0xFFFF, no input/output reflection,
no final XOR).

Nothing is imported from E:\\edge\\src.
"""

from __future__ import annotations

import struct
from typing import Final

from control_pipeline.schemas.bridge import (
    CmdAckFrame,
    HelloAckFrame,
    McuEventFrame,
    McuEventKind,
    McuState,
    TelemetryFrame,
    VoiceEventFrame,
    VoiceEventKind,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAGIC: Final[int] = 0xAA
"""Start-of-frame magic byte."""

PROTO_VER: Final[int] = 1
"""B-03 protocol version implemented by this codec."""

MAX_PAYLOAD: Final[int] = 40
"""Maximum payload size in bytes."""

# ---------------------------------------------------------------------------
# Message-type identifiers (MPU→MCU)
# ---------------------------------------------------------------------------
_MT_HELLO: Final[int] = 0x01
_MT_HEARTBEAT: Final[int] = 0x02
_MT_EVENT_ACK: Final[int] = 0x03
_MT_RUN: Final[int] = 0x10
_MT_SET_SPEED: Final[int] = 0x11
_MT_STOP: Final[int] = 0x12
_MT_HALT: Final[int] = 0x13
_MT_CLEAR_FAULT: Final[int] = 0x14

# Message-type identifiers (MCU→MPU)
_MT_HELLO_ACK: Final[int] = 0x81
_MT_TELEMETRY: Final[int] = 0x90
_MT_MCU_EVENT: Final[int] = 0x91
_MT_CMD_ACK: Final[int] = 0x92
_MT_VOICE_EVENT: Final[int] = 0x93

# Public message-type aliases
MT_HELLO: Final[int] = _MT_HELLO
MT_HEARTBEAT: Final[int] = _MT_HEARTBEAT
MT_EVENT_ACK: Final[int] = _MT_EVENT_ACK
MT_RUN: Final[int] = _MT_RUN
MT_SET_SPEED: Final[int] = _MT_SET_SPEED
MT_STOP: Final[int] = _MT_STOP
MT_HALT: Final[int] = _MT_HALT
MT_CLEAR_FAULT: Final[int] = _MT_CLEAR_FAULT
MT_HELLO_ACK: Final[int] = _MT_HELLO_ACK
MT_TELEMETRY: Final[int] = _MT_TELEMETRY
MT_MCU_EVENT: Final[int] = _MT_MCU_EVENT
MT_CMD_ACK: Final[int] = _MT_CMD_ACK
MT_VOICE_EVENT: Final[int] = _MT_VOICE_EVENT

# Fixed frame header size (magic … length inclusive)
_HEADER_SIZE: Final[int] = 12
_CRC_SIZE: Final[int] = 2
_MIN_FRAME: Final[int] = _HEADER_SIZE + _CRC_SIZE  # 14 bytes


# ---------------------------------------------------------------------------
# CRC-16/CCITT-FALSE
# ---------------------------------------------------------------------------


def crc16_ccitt_false(data: bytes, crc: int = 0xFFFF) -> int:
    """Compute CRC-16/CCITT-FALSE over *data*.

    Parameters:
        data: Input bytes to process.
        crc:  Initial CRC register value (default ``0xFFFF``).

    Returns:
        16-bit CRC value.

    Algorithm: poly = 0x1021, init = 0xFFFF, no reflection, no final XOR.
    This matches the standard "CCITT-FALSE" / "CRC-16-CCITT" variant.
    """
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = (crc << 1) ^ 0x1021
            else:
                crc <<= 1
            crc &= 0xFFFF
    return crc


# ---------------------------------------------------------------------------
# Sequence-number utilities
# ---------------------------------------------------------------------------


def newer_seq(a: int, b: int) -> bool:
    """Return ``True`` when sequence number *a* is strictly newer than *b*.

    Uses modular (wrap-around) arithmetic over the full 16-bit space so that
    sequence 0x0001 is newer than 0xFFFF.

    Args:
        a: Candidate "newer" sequence number (0–65535).
        b: Reference "older" sequence number (0–65535).

    Returns:
        ``True`` iff ``0 < ((a - b) & 0xFFFF) < 0x8000``.
    """
    return 0 < ((a - b) & 0xFFFF) < 0x8000


# ---------------------------------------------------------------------------
# Generic frame encoder / decoder
# ---------------------------------------------------------------------------


def encode_frame(
    msg_type: int,
    seq: int,
    ack: int,
    tick_ms: int,
    flags: int,
    payload: bytes,
) -> bytes:
    """Build a complete B-03 binary frame.

    Args:
        msg_type: 1-byte message-type identifier.
        seq:      Sender sequence number (u16, 0–65535).
        ack:      Last sequence number acknowledged from the peer (u16).
        tick_ms:  Sender free-running tick counter in ms (u32).
        flags:    1-byte flags field (application-defined bitfield).
        payload:  Raw payload bytes (0–:data:`MAX_PAYLOAD` bytes).

    Returns:
        Fully encoded frame bytes including header, payload and CRC.

    Raises:
        ValueError: If *payload* exceeds :data:`MAX_PAYLOAD` bytes.
    """
    if len(payload) > MAX_PAYLOAD:
        raise ValueError(
            f"Payload length {len(payload)} exceeds MAX_PAYLOAD ({MAX_PAYLOAD})"
        )
    header = struct.pack(
        "<BBHHIB B",  # magic, type, seq(u16), ack(u16), tick_ms(u32), flags, len
        MAGIC,
        msg_type,
        seq & 0xFFFF,
        ack & 0xFFFF,
        tick_ms & 0xFFFFFFFF,
        flags & 0xFF,
        len(payload),
    )
    body = header + payload
    crc = crc16_ccitt_false(body)
    return body + struct.pack("<H", crc)


def decode_frame(
    data: bytes,
) -> tuple[int, int, int, int, int, bytes] | None:
    """Parse and validate a B-03 binary frame.

    Args:
        data: Raw bytes received from the serial link.

    Returns:
        ``(msg_type, seq, ack, tick_ms, flags, payload)`` on success, or
        ``None`` if the frame is too short, has wrong magic, wrong length
        field, or CRC mismatch.
    """
    if len(data) < _MIN_FRAME:
        return None
    if data[0] != MAGIC:
        return None

    payload_len = data[11]
    expected_total = _HEADER_SIZE + payload_len + _CRC_SIZE
    if len(data) < expected_total:
        return None

    frame_body = data[:_HEADER_SIZE + payload_len]
    crc_recv = struct.unpack_from("<H", data, _HEADER_SIZE + payload_len)[0]
    if crc16_ccitt_false(frame_body) != crc_recv:
        return None

    magic, msg_type, seq, ack, tick_ms, flags, _pl_len = struct.unpack_from(
        "<BBHHIB B", data, 0
    )
    payload = data[_HEADER_SIZE : _HEADER_SIZE + payload_len]
    return (msg_type, seq, ack, tick_ms, flags, bytes(payload))


# ---------------------------------------------------------------------------
# MPU → MCU encode helpers
# ---------------------------------------------------------------------------


def encode_hello(seq: int, ack: int, tick_ms: int, session_id: int) -> bytes:
    """Encode a HELLO handshake frame (MPU → MCU).

    Args:
        seq:        Sender sequence number.
        ack:        Last peer sequence number acknowledged.
        tick_ms:    Sender tick counter in ms.
        session_id: 32-bit MPU session identifier.

    Returns:
        Encoded frame bytes.
    """
    # Payload: proto_ver (u8) + session_id (u32 LE)
    payload = struct.pack("<BI", PROTO_VER, session_id & 0xFFFFFFFF)
    return encode_frame(_MT_HELLO, seq, ack, tick_ms, 0x00, payload)


def encode_heartbeat(seq: int, ack: int, tick_ms: int, mpu_state: int) -> bytes:
    """Encode a HEARTBEAT keep-alive frame (MPU → MCU).

    Args:
        seq:       Sender sequence number.
        ack:       Last peer sequence number acknowledged.
        tick_ms:   Sender tick counter in ms.
        mpu_state: Current MPU state byte (application-defined).

    Returns:
        Encoded frame bytes.
    """
    # Payload: mpu_state (u8)
    payload = struct.pack("<B", mpu_state & 0xFF)
    return encode_frame(_MT_HEARTBEAT, seq, ack, tick_ms, 0x00, payload)


def encode_event_ack(seq: int, ack: int, tick_ms: int, event_seq: int) -> bytes:
    """Encode an EVENT_ACK frame acknowledging a received MCU event (MPU → MCU).

    Args:
        seq:       Sender sequence number.
        ack:       Last peer sequence number acknowledged.
        tick_ms:   Sender tick counter in ms.
        event_seq: Sequence number of the MCU event being acknowledged.

    Returns:
        Encoded frame bytes.
    """
    # Payload: event_seq (u16 LE)
    payload = struct.pack("<H", event_seq & 0xFFFF)
    return encode_frame(_MT_EVENT_ACK, seq, ack, tick_ms, 0x00, payload)


def encode_run(
    seq: int,
    ack: int,
    tick_ms: int,
    cmd_id: int,
    ttl_ms: int,
    kind: int,
    setpoint_mms: int,
) -> bytes:
    """Encode a RUN command frame (MPU → MCU).

    Args:
        seq:          Sender sequence number.
        ack:          Last peer sequence number acknowledged.
        tick_ms:      Sender tick counter in ms.
        cmd_id:       Command identifier for ack correlation (u16).
        ttl_ms:       Command time-to-live in ms (u16).
        kind:         Run kind / mode byte (u8, application-defined).
        setpoint_mms: Target belt speed in mm/s (u16).

    Returns:
        Encoded frame bytes.
    """
    # Payload: cmd_id(u16) + ttl_ms(u16) + kind(u8) + setpoint_mms(u16)
    payload = struct.pack(
        "<HHBH",
        cmd_id & 0xFFFF,
        ttl_ms & 0xFFFF,
        kind & 0xFF,
        setpoint_mms & 0xFFFF,
    )
    return encode_frame(_MT_RUN, seq, ack, tick_ms, 0x00, payload)


def encode_set_speed(
    seq: int,
    ack: int,
    tick_ms: int,
    cmd_id: int,
    ttl_ms: int,
    setpoint_mms: int,
) -> bytes:
    """Encode a SET_SPEED command frame (MPU → MCU).

    Args:
        seq:          Sender sequence number.
        ack:          Last peer sequence number acknowledged.
        tick_ms:      Sender tick counter in ms.
        cmd_id:       Command identifier (u16).
        ttl_ms:       Command time-to-live in ms (u16).
        setpoint_mms: New target belt speed in mm/s (u16).

    Returns:
        Encoded frame bytes.
    """
    # Payload: cmd_id(u16) + ttl_ms(u16) + setpoint_mms(u16)
    payload = struct.pack(
        "<HHH",
        cmd_id & 0xFFFF,
        ttl_ms & 0xFFFF,
        setpoint_mms & 0xFFFF,
    )
    return encode_frame(_MT_SET_SPEED, seq, ack, tick_ms, 0x00, payload)


def encode_stop(
    seq: int,
    ack: int,
    tick_ms: int,
    cmd_id: int,
    ttl_ms: int,
    kind: int,
) -> bytes:
    """Encode a STOP command frame (MPU → MCU).

    Args:
        seq:    Sender sequence number.
        ack:    Last peer sequence number acknowledged.
        tick_ms: Sender tick counter in ms.
        cmd_id: Command identifier (u16).
        ttl_ms: Command time-to-live in ms (u16).
        kind:   Stop kind byte (u8, e.g. 0=graceful, 1=immediate).

    Returns:
        Encoded frame bytes.
    """
    # Payload: cmd_id(u16) + ttl_ms(u16) + kind(u8)
    payload = struct.pack("<HHB", cmd_id & 0xFFFF, ttl_ms & 0xFFFF, kind & 0xFF)
    return encode_frame(_MT_STOP, seq, ack, tick_ms, 0x00, payload)


def encode_halt(
    seq: int,
    ack: int,
    tick_ms: int,
    cmd_id: int,
    ttl_ms: int,
) -> bytes:
    """Encode a HALT (emergency-stop) command frame (MPU → MCU).

    Args:
        seq:    Sender sequence number.
        ack:    Last peer sequence number acknowledged.
        tick_ms: Sender tick counter in ms.
        cmd_id: Command identifier (u16).
        ttl_ms: Command time-to-live in ms (u16).

    Returns:
        Encoded frame bytes.
    """
    # Payload: cmd_id(u16) + ttl_ms(u16)
    payload = struct.pack("<HH", cmd_id & 0xFFFF, ttl_ms & 0xFFFF)
    return encode_frame(_MT_HALT, seq, ack, tick_ms, 0x00, payload)


def encode_clear_fault(
    seq: int,
    ack: int,
    tick_ms: int,
    cmd_id: int,
    ttl_ms: int,
) -> bytes:
    """Encode a CLEAR_FAULT command frame (MPU → MCU).

    Args:
        seq:    Sender sequence number.
        ack:    Last peer sequence number acknowledged.
        tick_ms: Sender tick counter in ms.
        cmd_id: Command identifier (u16).
        ttl_ms: Command time-to-live in ms (u16).

    Returns:
        Encoded frame bytes.
    """
    # Payload: cmd_id(u16) + ttl_ms(u16)
    payload = struct.pack("<HH", cmd_id & 0xFFFF, ttl_ms & 0xFFFF)
    return encode_frame(_MT_CLEAR_FAULT, seq, ack, tick_ms, 0x00, payload)


# ---------------------------------------------------------------------------
# MCU → MPU decode helpers
# ---------------------------------------------------------------------------


def decode_telemetry(
    msg_type: int,
    seq: int,
    ack: int,
    tick_ms: int,
    flags: int,
    payload: bytes,
) -> TelemetryFrame | None:
    """Decode a TELEMETRY frame payload into a :class:`~control_pipeline.schemas.bridge.TelemetryFrame`.

    Args:
        msg_type: Must equal ``0x90``; returns ``None`` otherwise.
        seq:      Frame sequence number.
        ack:      Frame ack field.
        tick_ms:  MCU tick at frame capture.
        flags:    Frame-header flags byte.
        payload:  Raw payload bytes (expected 13 bytes).

    Returns:
        A populated :class:`~control_pipeline.schemas.bridge.TelemetryFrame` or ``None``
        on type/length mismatch.

    Payload layout (13 bytes, all LE):
      u8  mcu_state
      u8  fault_reason
      u16 setpoint_mms
      u16 speed_mms
      u16 current_ma
      i16 temp_c10
      u8  sensor_status
      u16 pwm_pm
      — seq and tick_ms come from the frame header —
    """
    if msg_type != _MT_TELEMETRY:
        return None
    _TELEM_FMT = "<BBHHHhBH"
    _TELEM_SIZE = struct.calcsize(_TELEM_FMT)  # 13 bytes
    if len(payload) < _TELEM_SIZE:
        return None
    (
        mcu_state_raw,
        fault_reason,
        setpoint_mms,
        speed_mms,
        current_ma,
        temp_c10,
        sensor_status,
        pwm_pm,
    ) = struct.unpack_from(_TELEM_FMT, payload)
    try:
        mcu_state = McuState(mcu_state_raw)
    except ValueError:
        return None
    return TelemetryFrame(
        mcu_state=mcu_state,
        fault_reason=fault_reason,
        setpoint_mms=setpoint_mms,
        speed_mms=speed_mms,
        current_ma=current_ma,
        temp_c10=temp_c10,
        sensor_status=sensor_status,
        pwm_pm=pwm_pm,
        flags=flags,
        tick_ms=tick_ms,
        seq=seq,
    )


def decode_mcu_event(
    msg_type: int,
    seq: int,
    ack: int,
    tick_ms: int,
    flags: int,
    payload: bytes,
) -> McuEventFrame | None:
    """Decode a MCU_EVENT frame payload into a :class:`~control_pipeline.schemas.bridge.McuEventFrame`.

    Args:
        msg_type: Must equal ``0x91``; returns ``None`` otherwise.
        seq:      Frame sequence number.
        ack:      Frame ack field.
        tick_ms:  MCU tick at event time.
        flags:    Frame-header flags byte.
        payload:  Raw payload bytes (expected 9 bytes).

    Returns:
        A populated :class:`~control_pipeline.schemas.bridge.McuEventFrame` or ``None``
        on type/length/value mismatch.

    Payload layout (9 bytes, all LE):
      u8  event_kind
      u8  reason
      u16 event_seq
      u8  mcu_state
      i16 observed
      i16 threshold
    """
    if msg_type != _MT_MCU_EVENT:
        return None
    _EVT_FMT = "<BBHBhh"
    if len(payload) < struct.calcsize(_EVT_FMT):
        return None
    (
        event_kind_raw,
        reason,
        event_seq,
        mcu_state_raw,
        observed,
        threshold,
    ) = struct.unpack_from(_EVT_FMT, payload)
    try:
        event_kind = McuEventKind(event_kind_raw)
        mcu_state = McuState(mcu_state_raw)
    except ValueError:
        return None
    return McuEventFrame(
        event_kind=event_kind,
        reason=reason,
        event_seq=event_seq,
        mcu_state=mcu_state,
        observed=observed,
        threshold=threshold,
    )


def decode_cmd_ack(
    msg_type: int,
    seq: int,
    ack: int,
    tick_ms: int,
    flags: int,
    payload: bytes,
) -> CmdAckFrame | None:
    """Decode a CMD_ACK frame payload into a :class:`~control_pipeline.schemas.bridge.CmdAckFrame`.

    Args:
        msg_type: Must equal ``0x92``; returns ``None`` otherwise.
        seq:      Frame sequence number.
        ack:      Frame ack field.
        tick_ms:  MCU tick at ack time.
        flags:    Frame-header flags byte.
        payload:  Raw payload bytes (expected ≥ 5 bytes).

    Returns:
        A populated :class:`~control_pipeline.schemas.bridge.CmdAckFrame` or ``None``
        on type/length/value mismatch.

    Payload layout (5 bytes):
      u16 cmd_id
      u8  result  (0=ACCEPTED, 1=REJECTED, 2=DUPLICATE)
      u8  reason
      u8  mcu_state
    """
    if msg_type != _MT_CMD_ACK:
        return None
    _ACK_FMT = "<HBBB"
    if len(payload) < struct.calcsize(_ACK_FMT):
        return None
    cmd_id, result, reason, mcu_state_raw = struct.unpack_from(_ACK_FMT, payload)
    try:
        mcu_state = McuState(mcu_state_raw)
    except ValueError:
        return None
    return CmdAckFrame(
        cmd_id=cmd_id,
        result=result,
        reason=reason,
        mcu_state=mcu_state,
    )


def decode_hello_ack(
    msg_type: int,
    seq: int,
    ack: int,
    tick_ms: int,
    flags: int,
    payload: bytes,
) -> HelloAckFrame | None:
    """Decode a HELLO_ACK handshake response into a :class:`~control_pipeline.schemas.bridge.HelloAckFrame`.

    Args:
        msg_type: Must equal ``0x81``; returns ``None`` otherwise.
        seq:      Frame sequence number.
        ack:      Frame ack field.
        tick_ms:  MCU tick at boot time.
        flags:    Frame-header flags byte.
        payload:  Raw payload bytes (expected ≥ 9 bytes).

    Returns:
        A populated :class:`~control_pipeline.schemas.bridge.HelloAckFrame` or ``None``
        on type/length/value mismatch.

    Payload layout (7 bytes, all LE):
      u8  proto_ver
      u16 mcu_boot_id
      u16 fw_ver
      u8  mcu_state
      u8  fault_reason
    """
    if msg_type != _MT_HELLO_ACK:
        return None
    _HACK_FMT = "<BHHBB"
    if len(payload) < struct.calcsize(_HACK_FMT):
        return None
    proto_ver, mcu_boot_id, fw_ver, mcu_state_raw, fault_reason = struct.unpack_from(
        _HACK_FMT, payload
    )
    try:
        mcu_state = McuState(mcu_state_raw)
    except ValueError:
        return None
    return HelloAckFrame(
        proto_ver=proto_ver,
        mcu_boot_id=mcu_boot_id,
        fw_ver=fw_ver,
        mcu_state=mcu_state,
        fault_reason=fault_reason,
    )


def encode_voice_event(
    seq: int,
    ack: int,
    tick_ms: int,
    flags: int,
    kind: int,
    confidence_pct: int,
    event_seq: int,
    mcu_state: int,
    model_ver: int,
) -> bytes:
    """Encode a VOICE_EVENT frame (0x93) (MCU → MPU).

    Args:
        seq:            Sender sequence number.
        ack:            Last peer sequence number acknowledged.
        tick_ms:        Sender tick counter in ms.
        flags:          1-byte live-state flags bitmask.
        kind:           Voice event kind (0 = WAKE, 1 = STOP).
        confidence_pct: Detection confidence (0–100).
        event_seq:      Event sequence number for EVENT_ACK tracking.
        mcu_state:      :class:`~control_pipeline.schemas.bridge.McuState` byte.
        model_ver:      Keyword spotter model version.

    Returns:
        Encoded frame bytes.
    """
    payload = struct.pack(
        "<BBHBH",
        kind & 0xFF,
        confidence_pct & 0xFF,
        event_seq & 0xFFFF,
        mcu_state & 0xFF,
        model_ver & 0xFFFF,
    )
    return encode_frame(_MT_VOICE_EVENT, seq, ack, tick_ms, flags, payload)


def decode_voice_event(
    msg_type: int,
    seq: int,
    ack: int,
    tick_ms: int,
    flags: int,
    payload: bytes,
) -> VoiceEventFrame | None:
    """Decode a VOICE_EVENT frame (0x93) payload into a :class:`~control_pipeline.schemas.bridge.VoiceEventFrame`.

    Args:
        msg_type: Must equal ``0x93``; returns ``None`` otherwise.
        seq:      Frame sequence number.
        ack:      Frame ack field.
        tick_ms:  MCU tick at event time.
        flags:    Frame-header flags byte.
        payload:  Raw payload bytes (expected 7 bytes).

    Returns:
        A populated :class:`~control_pipeline.schemas.bridge.VoiceEventFrame` or ``None``
        on type/length/value mismatch.

    Payload layout (7 bytes, all LE):
      u8  kind (0=WAKE, 1=STOP)
      u8  confidence_pct (0-100)
      u16 event_seq
      u8  mcu_state
      u16 model_ver
    """
    if msg_type != _MT_VOICE_EVENT:
        return None
    _VE_FMT = "<BBHBH"
    if len(payload) < struct.calcsize(_VE_FMT):
        return None
    kind, confidence_pct, event_seq, mcu_state_raw, model_ver = struct.unpack_from(
        _VE_FMT, payload
    )
    try:
        mcu_state = McuState(mcu_state_raw)
    except ValueError:
        return None
    return VoiceEventFrame(
        kind=kind,
        confidence_pct=confidence_pct,
        event_seq=event_seq,
        mcu_state=mcu_state,
        model_ver=model_ver,
    )


# ---------------------------------------------------------------------------
# Self-test: B-03 v1.0 reference test vectors
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # ------------------------------------------------------------------
    # Vector 1 — HEARTBEAT
    # seq=0x0001, ack=0x0000, tick_ms=0x000003E8 (1000), mpu_state=0x01
    # Expected bytes (hex): AA 02 01 00 00 00 E8 03 00 00 00 01 01 <crc>
    # ------------------------------------------------------------------
    hb = encode_heartbeat(seq=1, ack=0, tick_ms=1000, mpu_state=1)
    assert hb[0] == MAGIC, "HB: bad magic"
    assert hb[1] == _MT_HEARTBEAT, "HB: bad type"
    assert struct.unpack_from("<H", hb, 2)[0] == 1, "HB: bad seq"
    assert struct.unpack_from("<H", hb, 4)[0] == 0, "HB: bad ack"
    assert struct.unpack_from("<I", hb, 6)[0] == 1000, "HB: bad tick_ms"
    assert hb[10] == 0x00, "HB: bad flags"
    assert hb[11] == 1, "HB: bad payload len"
    assert hb[12] == 0x01, "HB: bad mpu_state"
    # Verify CRC covers the whole frame body
    body_crc = crc16_ccitt_false(hb[:-2])
    wire_crc = struct.unpack_from("<H", hb, len(hb) - 2)[0]
    assert body_crc == wire_crc, f"HB: CRC mismatch {body_crc:#06x} vs {wire_crc:#06x}"
    # Round-trip decode
    decoded = decode_frame(hb)
    assert decoded is not None, "HB: decode_frame returned None"
    assert decoded[0] == _MT_HEARTBEAT, "HB: decoded wrong type"
    print(f"[OK] HEARTBEAT vector  frame={hb.hex(' ')}")

    # ------------------------------------------------------------------
    # Vector 2 — RUN START
    # seq=0x0002, ack=0x0001, tick_ms=0x000007D0 (2000)
    # cmd_id=0x0001, ttl_ms=0x01F4 (500), kind=0x00, setpoint_mms=0x012C (300)
    # ------------------------------------------------------------------
    run = encode_run(
        seq=2, ack=1, tick_ms=2000, cmd_id=1, ttl_ms=500, kind=0, setpoint_mms=300
    )
    assert run[0] == MAGIC, "RUN: bad magic"
    assert run[1] == _MT_RUN, "RUN: bad type"
    assert struct.unpack_from("<H", run, 2)[0] == 2, "RUN: bad seq"
    assert struct.unpack_from("<H", run, 4)[0] == 1, "RUN: bad ack"
    assert struct.unpack_from("<I", run, 6)[0] == 2000, "RUN: bad tick_ms"
    assert run[11] == 7, "RUN: bad payload len"  # 2+2+1+2 = 7
    cmd_id_v, ttl_v, kind_v, sp_v = struct.unpack_from("<HHBH", run, 12)
    assert cmd_id_v == 1 and ttl_v == 500 and kind_v == 0 and sp_v == 300, (
        f"RUN: payload mismatch cmd_id={cmd_id_v} ttl={ttl_v} kind={kind_v} sp={sp_v}"
    )
    body_crc = crc16_ccitt_false(run[:-2])
    wire_crc = struct.unpack_from("<H", run, len(run) - 2)[0]
    assert body_crc == wire_crc, f"RUN: CRC mismatch"
    decoded = decode_frame(run)
    assert decoded is not None, "RUN: decode_frame returned None"
    print(f"[OK] RUN START vector   frame={run.hex(' ')}")

    # ------------------------------------------------------------------
    # Vector 3 — TELEMETRY (MCU → MPU decode)
    # Using the exact 27-byte spec test vector from Section 9:
    # aa 90 28 00 02 00 88 13 00 00 37 0d 02 00 78 00 76 00 52 03 56 01 80 76 00 17 6f
    # ------------------------------------------------------------------
    spec_telem = bytes.fromhex(
        "aa 90 28 00 02 00 88 13 00 00 37 0d 02 00 78 00 76 00 52 03 56 01 80 76 00 17 6f"
    )
    dec = decode_frame(spec_telem)
    assert dec is not None, "Spec TELEM: decode_frame returned None"
    tf = decode_telemetry(*dec)
    assert tf is not None, "Spec TELEM: decode_telemetry returned None"
    assert tf.mcu_state == McuState.RUNNING, f"Spec TELEM: bad mcu_state {tf.mcu_state}"
    assert tf.setpoint_mms == 120, f"Spec TELEM: bad setpoint {tf.setpoint_mms}"
    assert tf.speed_mms == 118, f"Spec TELEM: bad speed {tf.speed_mms}"
    assert tf.current_ma == 850, f"Spec TELEM: bad current {tf.current_ma}"
    assert tf.temp_c10 == 342, f"Spec TELEM: bad temp {tf.temp_c10}"
    assert tf.flags == 0x37, f"Spec TELEM: bad flags {tf.flags}"
    print(f"[OK] Spec TELEM vector passed: {spec_telem.hex(' ')}")
    print("All B-03 codec test vectors PASSED.")
