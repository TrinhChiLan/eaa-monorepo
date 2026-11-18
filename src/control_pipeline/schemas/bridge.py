"""bridge.py — McuBridgeWorker schemas for the EAA control_pipeline.

Defines MCU state/event enumerations, link-state tracking, and all
Pydantic frame models that map 1-to-1 with the B-03 v1.0 binary wire format.
Nothing is imported from E:\\edge\\src.
"""

from __future__ import annotations

import enum

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class McuState(enum.IntEnum):
    """MCU macro-state reported in every telemetry and ack frame."""

    BOOT = 0
    """MCU is initialising; no motion permitted."""

    STOPPED = 1
    """Belt is stopped; ready to receive RUN commands."""

    RUNNING = 2
    """Belt is running at the commanded setpoint."""

    SAFE_STOP = 3
    """MCU executed an autonomous safe-stop due to a local fault."""


class McuEventKind(enum.IntEnum):
    """Discriminator for unsolicited event frames from the MCU."""

    SAFE_STOP = 0x01
    """MCU performed an autonomous safe-stop."""

    MOTOR_OVERLOAD = 0x02
    """Motor overcurrent or overload condition detected."""

    JAM_DETECTED = 0x03
    """Belt jam detected via current signature or encoder."""

    HEARTBEAT_TIMEOUT = 0x04
    """MPU heartbeat watchdog expired on the MCU."""

    HARDWARE_ESTOP = 0x05
    """Hardware e-stop circuit was opened."""

    ESTOP_RELEASED = 0x06
    """Hardware e-stop circuit was closed again."""

    OVERTEMP = 0x07
    """Motor or driver over-temperature condition."""

    SENSOR_LOSS = 0x08
    """One or more required sensors became unresponsive."""

    FAULT_CLEARED = 0x09
    """Previously latched fault has been cleared."""

    STATE_CHANGED = 0x0A
    """MCU transitioned between :class:`McuState` values."""

    BOOT = 0x0B
    """MCU completed its boot sequence."""

    VOICE_WAKE = 0x0C
    """MCU keyword spotter detected wake phrase."""

    VOICE_STOP = 0x0D
    """MCU keyword spotter detected spoken stop command."""


class LinkState(str, enum.Enum):
    """Serial-link state maintained by the bridge worker."""

    DOWN = "down"
    """No connection; HELLO not yet sent or no response received."""

    SYNC = "sync"
    """HELLO sent; awaiting HELLO_ACK from MCU."""

    UP = "up"
    """Link fully established; telemetry frames are flowing."""

    STALE = "stale"
    """Link was UP but no frame received within the stale threshold."""

    LOST = "lost"
    """Stale timeout expired; link is considered broken."""


# ---------------------------------------------------------------------------
# Frame models (MCU → MPU)
# ---------------------------------------------------------------------------


class TelemetryFrame(BaseModel):
    """Decoded periodic telemetry frame transmitted by the MCU.

    Attributes:
        mcu_state:      Current :class:`McuState` reported by the MCU.
        fault_reason:   Active fault reason code (0 = none).
        setpoint_mms:   Commanded belt speed setpoint in mm/s.
        speed_mms:      Measured belt speed in mm/s.
        current_ma:     Motor phase current in mA.
        temp_c10:       Raw temperature × 10 (°C × 10).
                        ``0x7FFF`` = sensor not installed;
                        ``0x7FFE`` = sensor fault.
        sensor_status:  Raw sensor bitmask (bit definitions per B-03 §4.3).
        pwm_pm:         PWM duty cycle in per-mille (0–1000).
        flags:          Frame-header flags byte.
        tick_ms:        MCU free-running tick counter at frame capture (ms).
        seq:            Frame sequence number (wraps at 0xFFFF).
    """

    model_config = ConfigDict(frozen=True)

    mcu_state: McuState
    fault_reason: int
    setpoint_mms: int
    speed_mms: int
    current_ma: int
    temp_c10: int
    sensor_status: int
    pwm_pm: int
    flags: int
    tick_ms: int
    seq: int


class McuEventFrame(BaseModel):
    """Decoded unsolicited event frame from the MCU.

    Attributes:
        event_kind:  :class:`McuEventKind` discriminator.
        reason:      Sub-reason code (see B-03 §5.2).
        event_seq:   Event sequence number used for ack correlation.
        mcu_state:   :class:`McuState` at the time of the event.
        observed:    Signed observed measurement (units depend on event kind).
        threshold:   Signed threshold that was exceeded (same units).
    """

    model_config = ConfigDict(frozen=True)

    event_kind: McuEventKind
    reason: int
    event_seq: int
    mcu_state: McuState
    observed: int
    threshold: int


class CmdAckFrame(BaseModel):
    """Decoded command-acknowledgement frame from the MCU.

    Attributes:
        cmd_id:     Echoed command ID from the originating MPU command.
        result:     Outcome: ``0`` = ACCEPTED, ``1`` = REJECTED, ``2`` = DUPLICATE.
        reason:     Reason code qualifying *result* (see :class:`~control_pipeline.schemas.arbiter.ReasonCode`).
        mcu_state:  :class:`McuState` at the time of the ack.
    """

    model_config = ConfigDict(frozen=True)

    cmd_id: int
    result: int
    reason: int
    mcu_state: McuState


class HelloAckFrame(BaseModel):
    """Decoded HELLO_ACK handshake response from the MCU.

    Attributes:
        proto_ver:    Protocol version the MCU supports.
        mcu_boot_id:  Random 16-bit boot identifier (changes on each MCU reset).
        fw_ver:       Firmware version word.
        mcu_state:    :class:`McuState` at boot time.
        fault_reason: Any pre-existing fault code (0 = none).
    """

    model_config = ConfigDict(frozen=True)

    proto_ver: int
    mcu_boot_id: int
    fw_ver: int
    mcu_state: McuState
    fault_reason: int


class VoiceEventKind(enum.IntEnum):
    """Kind discriminator for 0x93 VOICE_EVENT frames."""

    WAKE = 0
    """Wake phrase detected; command window open."""

    STOP = 1
    """Spoken stop detected; belt stopped autonomously by MCU."""


class VoiceEventFrame(BaseModel):
    """Decoded VOICE_EVENT frame (0x93) from the MCU front-end keyword spotter.

    Attributes:
        kind:            0 = WAKE, 1 = STOP.
        confidence_pct:  Detection confidence percentage (0-100).
        event_seq:       Event sequence number for EVENT_ACK correlation.
        mcu_state:       :class:`McuState` at the time of the event.
        model_ver:       Keyword spotter model version.
    """

    model_config = ConfigDict(frozen=True)

    kind: int
    confidence_pct: int
    event_seq: int
    mcu_state: McuState
    model_ver: int


# ---------------------------------------------------------------------------
# Mutable bridge health / diagnostics
# ---------------------------------------------------------------------------


class BridgeHealth(BaseModel):
    """Mutable diagnostics record maintained by the bridge worker.

    Unlike the frame models this object is updated in-place as the worker
    processes frames; it is **not** frozen.

    Attributes:
        link_state:            Current :class:`LinkState`.
        last_valid_frame_at:   ``time.monotonic()`` of last good frame, or ``None``.
        hello_sent_at:         ``time.monotonic()`` when HELLO was last sent, or ``None``.
        session_id:            MPU-side session identifier sent in every HELLO.
        mcu_boot_id:           Boot identifier echoed in HELLO_ACK (``None`` pre-sync).
        fw_ver:                Firmware version from HELLO_ACK (``None`` pre-sync).
        telemetry_frames_rx:   Count of valid telemetry frames received.
        bad_crc:               Count of frames discarded for CRC mismatch.
        bad_magic:             Count of frames discarded for wrong magic byte.
        out_of_seq:            Count of out-of-sequence frames detected.
        duplicate_frames:      Count of duplicate-sequence frames dropped.
        event_acks_sent:       Count of MCU event acks transmitted.
    """

    link_state: LinkState
    last_valid_frame_at: float | None = None
    hello_sent_at: float | None = None
    session_id: int
    mcu_boot_id: int | None = None
    fw_ver: int | None = None
    telemetry_frames_rx: int = 0
    bad_crc: int = 0
    bad_magic: int = 0
    out_of_seq: int = 0
    duplicate_frames: int = 0
    event_acks_sent: int = 0
