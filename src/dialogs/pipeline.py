from __future__ import annotations

"""Backward-compatible facade for legacy pipeline imports."""

from .infrastructure.reporting import _build_accuracy_heatmap_data, _heatmap_zone
from .infrastructure.scan_runner import load_messages_for_range
from .interfaces.report import build_report
from .interfaces.scan import run_scan

__all__ = [
    "_build_accuracy_heatmap_data",
    "_heatmap_zone",
    "build_report",
    "load_messages_for_range",
    "run_scan",
]
