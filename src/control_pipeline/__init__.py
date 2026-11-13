"""control_pipeline — EAA conveyor control: FSMEngine, SafetyInvariantArbiter, McuBridgeWorker."""

from __future__ import annotations

import sys
from pathlib import Path

_pkg_dir = str(Path(__file__).resolve().parent)
if _pkg_dir not in sys.path:
    sys.path.insert(0, _pkg_dir)
