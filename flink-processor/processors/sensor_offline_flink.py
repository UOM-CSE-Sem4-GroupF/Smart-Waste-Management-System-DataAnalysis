import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from pyflink.common import Types
from pyflink.common.state import ValueStateDescriptor
from pyflink.datastream import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.functions import Collector

logger = logging.getLogger(__name__)


class SensorOfflineFlinkProcessor(KeyedProcessFunction):
    """
    KeyedProcessFunction that detects when a bin hasn't sent a heartbeat for a threshold duration.
    
    Logic:
    1. On each event (heartbeat):
       - Record the event's timestamp as 'last_seen'.
       - Clear 'alert_sent' flag.
       - Register a processing-time timer for 'now + threshold'.
    2. On timer:
       - Check if 'now - last_seen >= threshold'.
       - If yes and alert hasn't been sent, emit an offline alert.
       - Set 'alert_sent' flag to True to avoid duplicate alerts.
    """

    def __init__(self, offline_threshold_minutes: int = 30):
        self.offline_threshold_ms = offline_threshold_minutes * 60 * 1000
        self.last_seen_state = None
        self.alert_sent_state = None

    def open(self, runtime_context: RuntimeContext):
        # last_seen_state: stores the event timestamp (ms epoch)
        last_seen_desc = ValueStateDescriptor("last_seen_state", Types.LONG())
        self.last_seen_state = runtime_context.get_state(last_seen_desc)

        # alert_sent_state: boolean flag to prevent duplicate alerts
        alert_sent_desc = ValueStateDescriptor("alert_sent_state", Types.BOOLEAN())
        self.alert_sent_state = runtime_context.get_state(alert_sent_desc)

    def process_element(self, value: str, ctx: KeyedProcessFunction.Context, out: Collector):
        try:
            event = json.loads(value)
            # In the telemetry stream, bin_id is usually in the top level or payload
            # The job.py (Pipeline 1) usually emits the spec format.
            # But this pipeline listens to waste.bin.telemetry (Pipeline 1 input).
            # The telemetry format has bin_id at the top level.
            bin_id = event.get("bin_id")
            
            # Use event timestamp if available, otherwise processing time
            # Note: Pipeline 1 inputs have 'timestamp' as epoch ms or ISO string.
            raw_ts = event.get("timestamp")
            event_ts_ms = self._coerce_to_ms(raw_ts) or ctx.timer_service().current_processing_time()

            if not bin_id:
                return

            self.last_seen_state.update(event_ts_ms)
            self.alert_sent_state.update(False)

            # Register a timer to check for timeout
            # We register for event_ts + threshold to be precise about the silence duration
            # but using processing time timer for the trigger.
            ctx.timer_service().register_processing_time_timer(
                ctx.timer_service().current_processing_time() + self.offline_threshold_ms
            )
            
            # We don't emit anything on every heartbeat, but we could emit a "Recovery" event here 
            # if alert_sent was previously True.
            
        except Exception:
            logger.exception("SensorOfflineFlinkProcessor.process_element failed")

    def on_timer(self, timestamp: int, ctx: KeyedProcessFunction.OnTimerContext, out: Collector):
        last_seen_ms = self.last_seen_state.value()
        alert_sent = self.alert_sent_state.value()

        if last_seen_ms is None:
            return

        now_ms = ctx.timer_service().current_processing_time()
        silence_ms = now_ms - last_seen_ms

        if silence_ms >= self.offline_threshold_ms and not alert_sent:
            # Declare offline
            bin_id = ctx.get_current_key()
            alert = self._build_alert(bin_id, last_seen_ms, silence_ms // 1000)
            
            self.alert_sent_state.update(True)
            out.collect(json.dumps(alert))
            
            logger.info("SENSOR_OFFLINE detected for bin_id=%s (silent for %ds)", bin_id, silence_ms // 1000)

    @staticmethod
    def _coerce_to_ms(value: Any) -> Optional[int]:
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            try:
                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                return int(dt.timestamp() * 1000)
            except ValueError:
                return None
        return None

    @staticmethod
    def _build_alert(bin_id: str, last_seen_ms: int, silence_seconds: int) -> Dict[str, Any]:
        last_seen_dt = datetime.fromtimestamp(last_seen_ms / 1000.0, tz=timezone.utc)
        now_dt = datetime.now(tz=timezone.utc)
        return {
            "bin_id": bin_id,
            "anomaly_type": "SENSOR_OFFLINE",
            "last_seen_at": last_seen_dt.isoformat().replace("+00:00", "Z"),
            "silence_seconds": int(silence_seconds),
            "detected_at": now_dt.isoformat().replace("+00:00", "Z"),
        }
