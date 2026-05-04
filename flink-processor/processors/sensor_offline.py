"""
Pipeline 5 — Sensor Offline Detector
======================================
Spec reference: 06-flink-processor.md §8

Tracks the last time each bin sent a telemetry event.  When a bin has
not been heard from for OFFLINE_THRESHOLD_MINUTES the processor yields
an offline alert dict and clears that bin's timer so subsequent ticks
do not re-raise the same alert.

Because we are running outside the PyFlink runtime (plain Python /
kafka-python consumer loop), the "processing-time timer" from the spec
is emulated with a wall-clock dict keyed by bin_id.  The semantics are
identical: on every incoming event the bin's last-seen timestamp is
refreshed; on every periodic tick() call, bins whose last-seen timestamp
is older than the threshold are declared offline.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional


logger = logging.getLogger(__name__)


class SensorOfflineDetector:
    """
    Stateful detector that tracks the most-recent telemetry event per bin.

    Usage pattern (mirrors what job_sensor_offline.py does):

        detector = SensorOfflineDetector()

        # On every incoming bin telemetry event:
        detector.record_heartbeat(bin_id, event_timestamp)

        # On every polling interval (e.g. every 60 s):
        for alert in detector.tick():
            # write to DB + publish to Kafka
    """

    OFFLINE_THRESHOLD_MINUTES: int = 30

    def __init__(self, offline_threshold_minutes: int = OFFLINE_THRESHOLD_MINUTES) -> None:
        self.offline_threshold = timedelta(minutes=offline_threshold_minutes)
        # bin_id → (last_seen_at, alert_already_sent)
        self._state: Dict[str, Dict[str, Any]] = {}

    # ── Public API ─────────────────────────────────────────────────────────────

    def record_heartbeat(self, bin_id: str, event_timestamp: Any) -> None:
        """
        Called on every incoming bin telemetry message.
        Refreshes the last-seen timestamp for this bin and clears any
        previously sent offline alert so the bin can alert again if it
        goes offline in the future.
        """
        ts = self._coerce_timestamp(event_timestamp)
        if ts is None:
            logger.warning("record_heartbeat: could not parse timestamp for bin %s — skipping", bin_id)
            return

        self._state[bin_id] = {
            "last_seen_at": ts,
            "alert_sent": False,
        }
        logger.debug("Heartbeat recorded: bin=%s last_seen=%s", bin_id, ts.isoformat())

    def tick(self, now: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Evaluates which bins have been silent for longer than the threshold.

        Call this on a regular interval (e.g. once per minute) from the
        job loop.

        Returns a list of alert dicts — one per newly-offline bin.  Empty
        list means all bins are reporting normally.
        """
        if now is None:
            now = datetime.now(tz=timezone.utc)

        alerts: List[Dict[str, Any]] = []

        for bin_id, state in self._state.items():
            last_seen: datetime = state["last_seen_at"]
            silence_duration = now - last_seen

            if silence_duration >= self.offline_threshold and not state["alert_sent"]:
                state["alert_sent"] = True
                alert = self._build_alert(bin_id, last_seen, int(silence_duration.total_seconds()))
                alerts.append(alert)
                logger.info(
                    "SENSOR_OFFLINE: bin=%s last_seen=%s silence=%.0fs",
                    bin_id,
                    last_seen.isoformat(),
                    silence_duration.total_seconds(),
                )

        return alerts

    def tracked_bins(self) -> List[str]:
        """Returns the list of bin_ids currently tracked by the detector."""
        return list(self._state.keys())

    # ── Internals ──────────────────────────────────────────────────────────────

    @staticmethod
    def _build_alert(bin_id: str, last_seen_at: datetime, silence_seconds: int) -> Dict[str, Any]:
        return {
            "bin_id": bin_id,
            "anomaly_type": "SENSOR_OFFLINE",
            "last_seen_at": last_seen_at.isoformat().replace("+00:00", "Z"),
            "silence_seconds": silence_seconds,
            "detected_at": datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z"),
        }

    @staticmethod
    def _coerce_timestamp(value: Any) -> Optional[datetime]:
        if isinstance(value, datetime):
            return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        if isinstance(value, str):
            try:
                ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
                return ts if ts.tzinfo is not None else ts.replace(tzinfo=timezone.utc)
            except ValueError:
                return None
        if isinstance(value, (int, float)):
            # Treat as milliseconds epoch (Kafka-style)
            try:
                return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)
            except (OSError, OverflowError, ValueError):
                return None
        return None
