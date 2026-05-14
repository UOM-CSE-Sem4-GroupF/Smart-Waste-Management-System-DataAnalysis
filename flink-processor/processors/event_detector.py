"""
Event-Based Anomaly Detection for Smart Waste Management System
Detects unpredictable filling patterns due to special events like festivals, concerts, etc.
"""
import logging
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from typing import Dict, List, Any, Optional
from decimal import Decimal

logger = logging.getLogger(__name__)


class EventDetector:
    """
    Real-time event-based anomaly detection in Flink
    Detects: chaotic demand patterns, rapid multi-bin surges, forecast deviations
    """
    
    # Thresholds
    RAPID_SURGE_BIN_COUNT = 5  # 5+ bins
    RAPID_SURGE_FILL_INCREASE_PCT = 20  # >20% increase
    RAPID_SURGE_WINDOW_MINUTES = 30
    CHAOTIC_DEMAND_STD_COUNT = 3.0  # 3 standard deviations
    CHAOTIC_DEMAND_DURATION_HOURS = 2
    CHAOTIC_DEMAND_CONFIDENCE_MIN = 0.7
    
    def __init__(self):
        """Initialize event detector with in-memory state"""
        self.zone_reading_history = defaultdict(list)  # zone_id -> [readings]
        self.zone_forecast_state = {}  # zone_id -> {forecast_data}
        self.rapid_surge_tracking = defaultdict(list)  # zone_id -> [surge_events]
        
        # Event type probability mapping
        self.event_type_probabilities = {
            "FESTIVAL": 0.40,
            "ACCIDENT": 0.30,
            "CLEANING": 0.20,
            "OTHER": 0.10,
        }
    
    def detect_event_anomalies(
        self,
        event: Dict[str, Any],
        zone_id: int,
        zone_forecast: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Run event-based anomaly detection
        Returns: {
            event_anomalies: [
                {type, severity, evidence, confidence}
            ]
        }
        """
        anomalies = []
        
        # 1. Rapid Multi-Bin Surge Detection
        surge_anomaly = self._detect_rapid_surge(event, zone_id)
        if surge_anomaly:
            anomalies.append(surge_anomaly)
        
        # 2. Chaotic Demand Pattern Detection (on historical data + forecast)
        # This is typically triggered on windowed aggregations, not per-event
        # But we track state here for windowing
        if zone_id is not None:
            self._track_zone_readings(zone_id, event)
        
        return {
            "event_anomalies": anomalies,
            "is_event_anomaly": len(anomalies) > 0,
        }
    
    def detect_chaotic_demand_pattern(
        self,
        zone_id: int,
        zone_waste_kg: float,
        forecasted_waste_kg: Optional[float],
        timestamp: datetime,
    ) -> Optional[Dict[str, Any]]:
        """
        Detect chaotic demand patterns using forecast deviation
        Called on windowed aggregations (e.g., 10-minute windows)
        """
        if forecasted_waste_kg is None:
            return None
        
        # Calculate deviation in standard deviations
        # For now, use simple heuristic: if actual > forecast by 30%, flag it
        deviation_pct = ((zone_waste_kg - forecasted_waste_kg) / forecasted_waste_kg) * 100
        
        # Conservative: flag if >30% above forecast
        if deviation_pct > 30:
            confidence = min(1.0, (deviation_pct - 30) / 50)  # Scale 30-80% to 0-1
            
            # Estimate event type probabilities
            estimated_event_type = self._estimate_event_type(deviation_pct)
            
            return {
                "type": "CHAOTIC_DEMAND",
                "severity": "MEDIUM" if deviation_pct < 50 else "HIGH",
                "waste_volume_actual_kg": zone_waste_kg,
                "waste_volume_forecasted_kg": forecasted_waste_kg,
                "deviation_pct": deviation_pct,
                "deviation_std_count": deviation_pct / 10,  # Rough estimate
                "confidence_score": confidence,
                "estimated_event_type": estimated_event_type,
                "evidence": {
                    "zone_id": zone_id,
                    "actual_kg": zone_waste_kg,
                    "forecasted_kg": forecasted_waste_kg,
                    "timestamp": timestamp.isoformat() if timestamp else None,
                },
            }
        
        return None
    
    def _detect_rapid_surge(
        self,
        event: Dict[str, Any],
        zone_id: int,
    ) -> Optional[Dict[str, Any]]:
        """
        Detect rapid multi-bin surge patterns
        5+ bins in same zone all fill >20% within 30 minutes
        """
        bin_id = event.get("bin_id")
        fill_level = event.get("fill_level_pct")
        timestamp = event.get("timestamp")
        
        if not bin_id or fill_level is None or not timestamp:
            return None
        
        # Track surge for this zone
        surge_key = f"{zone_id}_{timestamp[:13]}"  # Group by zone and 1-hour window
        
        if surge_key not in self.rapid_surge_tracking[zone_id]:
            self.rapid_surge_tracking[zone_id].append([])
        
        current_surge = self.rapid_surge_tracking[zone_id][-1]
        
        # Add reading if it's a significant fill level
        if fill_level > 50:  # Only track high fills
            current_surge.append({
                "bin_id": bin_id,
                "fill_level": fill_level,
                "timestamp": timestamp,
            })
        
        # Check if we have 5+ bins with high fill in this surge
        if len(current_surge) >= self.RAPID_SURGE_BIN_COUNT:
            # Verify that fills are recent and within time window
            timestamps_in_surge = [s["timestamp"] for s in current_surge]
            
            try:
                time_diff = self._calc_time_diff_minutes(timestamps_in_surge[0], timestamps_in_surge[-1])
                
                if time_diff <= self.RAPID_SURGE_WINDOW_MINUTES:
                    return {
                        "type": "RAPID_SURGE",
                        "severity": "MEDIUM",
                        "bins_surged_count": len(current_surge),
                        "time_window_minutes": time_diff,
                        "estimated_event_type": "FESTIVAL/MARKET/EVENT",
                        "confidence_score": min(1.0, len(current_surge) / 10),
                        "evidence": {
                            "zone_id": zone_id,
                            "affected_bins": [s["bin_id"] for s in current_surge],
                            "fill_levels": [s["fill_level"] for s in current_surge],
                            "latest_timestamp": timestamps_in_surge[-1],
                        },
                    }
            except:
                pass
        
        return None
    
    def _track_zone_readings(self, zone_id: int, event: Dict[str, Any]) -> None:
        """
        Track zone-level readings for chaotic demand detection
        """
        fill_level = event.get("fill_level_pct")
        estimated_weight = event.get("estimated_weight_kg")
        timestamp = event.get("timestamp")
        
        if fill_level is not None:
            self.zone_reading_history[zone_id].append({
                "fill_level": fill_level,
                "estimated_weight": estimated_weight,
                "timestamp": timestamp,
            })
        
        # Keep only last 1000 readings per zone
        if len(self.zone_reading_history[zone_id]) > 1000:
            self.zone_reading_history[zone_id] = self.zone_reading_history[zone_id][-1000:]
    
    @staticmethod
    def _estimate_event_type(deviation_pct: float) -> str:
        """
        Estimate event type based on deviation percentage
        Very heuristic; should be improved with actual model
        """
        if deviation_pct > 100:
            return "FESTIVAL"  # Major event
        elif deviation_pct > 60:
            return "CONCERT/SPORTS"
        elif deviation_pct > 30:
            return "MARKET_DAY/CLEANING"
        else:
            return "UNKNOWN"
    
    @staticmethod
    def _calc_time_diff_minutes(timestamp1: str, timestamp2: str) -> float:
        """
        Calculate time difference between two ISO timestamps in minutes
        """
        try:
            t1 = datetime.fromisoformat(timestamp1.replace('Z', '+00:00')) if isinstance(timestamp1, str) else timestamp1
            t2 = datetime.fromisoformat(timestamp2.replace('Z', '+00:00')) if isinstance(timestamp2, str) else timestamp2
            
            diff_sec = abs((t2 - t1).total_seconds())
            return diff_sec / 60
        except:
            return 0


class EventAnomalyCorrelation:
    """
    Helper class to correlate detected event anomalies with anomaly events
    and determine if an event-driven anomaly should trigger alerts
    """
    
    @staticmethod
    def correlate_event_to_anomalies(
        event_id: int,
        anomalies: List[Dict[str, Any]],
        zone_id: int,
    ) -> Dict[str, Any]:
        """
        Correlate a detected event to anomalies in the same zone
        Returns correlation metadata
        """
        related_anomaly_types = defaultdict(int)
        severity_scores = []
        
        for anomaly in anomalies:
            if anomaly.get("zone_id") == zone_id:
                anom_type = anomaly.get("anomaly_type")
                if anom_type:
                    related_anomaly_types[anom_type] += 1
                
                severity = anomaly.get("severity")
                if severity:
                    severity_scores.append(EventAnomalyCorrelation._severity_to_score(severity))
        
        avg_severity_score = sum(severity_scores) / len(severity_scores) if severity_scores else 0
        
        return {
            "event_id": event_id,
            "zone_id": zone_id,
            "related_anomalies_count": len(anomalies),
            "anomaly_type_distribution": dict(related_anomaly_types),
            "avg_severity_score": avg_severity_score,
            "confidence_escalation": min(1.0, len(anomalies) / 10),  # More anomalies = higher confidence
        }
    
    @staticmethod
    def _severity_to_score(severity: str) -> float:
        """Convert severity string to numeric score"""
        return {
            "LOW": 1.0,
            "MEDIUM": 2.0,
            "HIGH": 3.0,
            "CRITICAL": 4.0,
        }.get(severity, 0.0)
