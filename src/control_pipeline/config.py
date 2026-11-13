"""config.py — Configuration settings for the EAA orchestrator block."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class OrchestratorSettings:
    """Runtime configuration for FSMEngine, SafetyInvariantArbiter, and McuBridgeWorker."""

    # FSM settings
    anti_flap_lockout_s: float = 5.0
    initial_state: str = "idle"

    # Arbiter invariants and thresholds
    max_current_ma: float = 2500.0
    max_temp_celsius: float = 80.0
    min_speed_mms: int = 50
    max_speed_mms: int = 500
    min_stt_confidence: float = 0.80
    max_intent_age_ms: int = 2000

    # Bridge timing and transport
    heartbeat_interval_s: float = 0.1
    hello_retry_interval_s: float = 0.5
    stale_threshold_ms: float = 250.0
    lost_threshold_ms: float = 500.0
    default_ttl_ms: int = 500

    # Transport profile ("mock" | "pipe" | "socket")
    transport_profile: str = "mock"
    socket_path: str = "/var/run/arduino-router.sock"
    uart_device: str = "/dev/ttyHS1"
    uart_baud: int = 115200

    # General
    log_level: str = "INFO"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> OrchestratorSettings:
        valid_keys = {f for f in cls.__dataclass_fields__}
        filtered = {k: v for k, v in data.items() if k in valid_keys}
        return cls(**filtered)

    @classmethod
    def from_env(cls) -> OrchestratorSettings:
        """Load settings from environment variables prefixed with EAA_."""
        prefix = "EAA_"
        kwargs: dict[str, Any] = {}

        if val := os.getenv(f"{prefix}ANTI_FLAP_LOCKOUT_S"):
            kwargs["anti_flap_lockout_s"] = float(val)
        if val := os.getenv(f"{prefix}INITIAL_STATE"):
            kwargs["initial_state"] = val
        if val := os.getenv(f"{prefix}MAX_CURRENT_MA"):
            kwargs["max_current_ma"] = float(val)
        if val := os.getenv(f"{prefix}MAX_TEMP_CELSIUS"):
            kwargs["max_temp_celsius"] = float(val)
        if val := os.getenv(f"{prefix}MIN_SPEED_MMS"):
            kwargs["min_speed_mms"] = int(val)
        if val := os.getenv(f"{prefix}MAX_SPEED_MMS"):
            kwargs["max_speed_mms"] = int(val)
        if val := os.getenv(f"{prefix}MIN_STT_CONFIDENCE"):
            kwargs["min_stt_confidence"] = float(val)
        if val := os.getenv(f"{prefix}MAX_INTENT_AGE_MS"):
            kwargs["max_intent_age_ms"] = int(val)
        if val := os.getenv(f"{prefix}HEARTBEAT_INTERVAL_S"):
            kwargs["heartbeat_interval_s"] = float(val)
        if val := os.getenv(f"{prefix}HELLO_RETRY_INTERVAL_S"):
            kwargs["hello_retry_interval_s"] = float(val)
        if val := os.getenv(f"{prefix}STALE_THRESHOLD_MS"):
            kwargs["stale_threshold_ms"] = float(val)
        if val := os.getenv(f"{prefix}LOST_THRESHOLD_MS"):
            kwargs["lost_threshold_ms"] = float(val)
        if val := os.getenv(f"{prefix}DEFAULT_TTL_MS"):
            kwargs["default_ttl_ms"] = int(val)
        if val := os.getenv(f"{prefix}TRANSPORT_PROFILE"):
            kwargs["transport_profile"] = val
        if val := os.getenv(f"{prefix}SOCKET_PATH"):
            kwargs["socket_path"] = val
        if val := os.getenv(f"{prefix}UART_DEVICE"):
            kwargs["uart_device"] = val
        if val := os.getenv(f"{prefix}UART_BAUD"):
            kwargs["uart_baud"] = int(val)
        if val := os.getenv(f"{prefix}LOG_LEVEL"):
            kwargs["log_level"] = val

        return cls(**kwargs)

    @classmethod
    def from_yaml(cls, path: str | Path) -> OrchestratorSettings:
        """Load settings from a YAML file (requires PyYAML)."""
        try:
            import yaml
        except ImportError:
            raise RuntimeError("PyYAML is required to load configuration from YAML files.")

        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return cls.from_dict(data)
