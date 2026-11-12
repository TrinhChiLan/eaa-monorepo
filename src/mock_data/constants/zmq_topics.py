"""ZeroMQ topic names and default IPC endpoints.

The router is the subscriber/puller:
  * CV and sensor: router SUB **connects** to a producer PUB.
  * STT and UI:    router PULL **binds**, producers PUSH-connect to it.
"""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

TOPIC_CV_FRAME = "cv.frame"
TOPIC_STT_INTENT = "stt.intent"
TOPIC_SENSOR_SNAPSHOT = "sensor.snapshot"
TOPIC_UI_COMMAND = "ui.command"

ALL_TOPICS = (
    TOPIC_CV_FRAME,
    TOPIC_STT_INTENT,
    TOPIC_SENSOR_SNAPSHOT,
    TOPIC_UI_COMMAND,
)

# SUB sockets only ask for the prefixes they understand.
CV_SUBSCRIPTION_PREFIX = "cv."
SENSOR_SUBSCRIPTION_PREFIX = "sensor."


def default_ipc_dir() -> Path:
    raw = os.environ.get("EAA_IPC_DIR")
    if raw:
        return Path(raw).expanduser()
    return Path(tempfile.gettempdir()) / "eaa"


def _endpoint(ipc_dir: Path, name: str) -> str:
    return f"ipc://{ipc_dir / name}"


@dataclass(frozen=True, slots=True)
class RouterEndpoints:
    cv_sub: str
    sensor_sub: str
    stt_pull: str
    ui_pull: str

    @classmethod
    def default(cls, ipc_dir: str | Path | None = None) -> RouterEndpoints:
        directory = Path(ipc_dir).expanduser() if ipc_dir is not None else default_ipc_dir()
        return cls(
            cv_sub=_endpoint(directory, "cv.pub"),
            sensor_sub=_endpoint(directory, "sensor.pub"),
            stt_pull=_endpoint(directory, "stt.pull"),
            ui_pull=_endpoint(directory, "ui.pull"),
        )

    @property
    def bind_endpoints(self) -> tuple[str, ...]:
        """Endpoints the router binds (PULL)."""
        return (self.stt_pull, self.ui_pull)

    @property
    def connect_endpoints(self) -> tuple[str, ...]:
        """Endpoints the router connects to (SUB)."""
        return (self.cv_sub, self.sensor_sub)


__all__ = [
    "ALL_TOPICS",
    "CV_SUBSCRIPTION_PREFIX",
    "RouterEndpoints",
    "SENSOR_SUBSCRIPTION_PREFIX",
    "TOPIC_CV_FRAME",
    "TOPIC_SENSOR_SNAPSHOT",
    "TOPIC_STT_INTENT",
    "TOPIC_UI_COMMAND",
    "default_ipc_dir",
]
