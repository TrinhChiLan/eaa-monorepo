"""event_pipeline — Ingress routing, telemetry ring buffer, and persistence pipeline."""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure event_pipeline directory is on sys.path so its internal modules resolve bare imports
_pkg_dir = str(Path(__file__).resolve().parent)
if _pkg_dir not in sys.path:
    sys.path.insert(0, _pkg_dir)
