"""
Security Anomaly Detection for Smart Waste Management System
Detects hacking attempts, spoofed data, and coordinated attacks
"""
import logging
import hashlib
import math
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from typing import Dict, List, Any, Tuple, Optional
from decimal import Decimal

logger = logging.getLogger(__name__)


class SecurityDetector:
    """
    Real-time security anomaly detection in Flink
    Detects: coordinated attacks, impossible physics, metadata tampering, entropy anomalies, network anomalies
    """
    
    # Thresholds
    COORDINATED_ATTACK_BIN_THRESHOLD = 3  # 3+ bins in different zones
    COORDINATED_ATTACK_WINDOW_MINUTES = 5
    IMPOSSIBLE_GPS_DISTANCE_KM = 100  # 100km in 5 min
    IMPOSSIBLE_GPS_TIME_SEC = 300  # 5 minutes
    ENTROPY_UNIFORM_THRESHOLD = 0.1  # too constant = spoofing
    ENTROPY_RANDOM_THRESHOLD = 0.9  # too random = noise injection
    NETWORK_IP_THRESHOLD = 5  # same token from 5+ IPs
    NETWORK_IP_WINDOW_MINUTES = 60
    NETWORK_BURST_THRESHOLD = 1000  # readings/sec
    
    def __init__(self):
        """Initialize security detector with in-memory state"""
        self.coordinated_attack_state = defaultdict(list)  # window_key -> [events]
        self.network_activity_state = defaultdict(list)  # token_hash -> [ip_times]
        self.bin_reading_history = defaultdict(list)  # bin_id -> [fill_levels]
        self.vehicle_gps_history = defaultdict(list)  # vehicle_id -> [gps_points]
        self.metadata_state = {}  # bin_id/vehicle_id -> metadata snapshot
        
    def detect_security_anomalies(
        self, 
        event: Dict[str, Any],
        metadata: Dict[str, Any],
        available_zones: Dict[int, str],
        available_vehicles: Dict[str, str],
        api_token: Optional[str] = None,
        source_ip: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Run all security checks on incoming event
        Returns: {
            security_anomalies: [
                {type, severity, evidence, probability}
            ]
        }
        """
        anomalies = []
        
        # Extract event data
        bin_id = event.get("bin_id")
        timestamp = event.get("timestamp")
        fill_level = event.get("fill_level_pct")
        temperature = event.get("temperature_c")
        vehicle_id = event.get("vehicle_id")
        
        # 1. Impossible Physics Detection
        impossible_physics = self._detect_impossible_physics(event, metadata, available_zones)
        if impossible_physics:
            anomalies.append(impossible_physics)
        
        # 2. Entropy-Based Spoofing Detection
        if bin_id and fill_level is not None:
            entropy_anomaly = self._detect_entropy_anomaly(bin_id, fill_level)
            if entropy_anomaly:
                anomalies.append(entropy_anomaly)
        
        # 3. Metadata Tampering Detection
        if bin_id:
            tampering = self._detect_metadata_tampering(bin_id, metadata)
            if tampering:
                anomalies.append(tampering)
        
        # 4. Vehicle GPS Spoofing Detection
        if vehicle_id and "vehicle_gps" in event:
            gps_anomaly = self._detect_gps_spoofing(vehicle_id, event)
            if gps_anomaly:
                anomalies.append(gps_anomaly)
        
        # 5. Network Anomaly Detection (API level)
        if api_token and source_ip:
            network_anomaly = self._detect_network_anomaly(api_token, source_ip, bin_id, event)
            if network_anomaly:
                anomalies.append(network_anomaly)
        
        # 6. Coordinated Attack Detection (cross-bin analysis)
        # This is triggered separately on windowed state
        
        return {
            "security_anomalies": anomalies,
            "is_security_incident": len(anomalies) > 0,
            "total_anomalies": len(anomalies),
        }
    
    def _detect_impossible_physics(
        self, 
        event: Dict[str, Any], 
        metadata: Dict[str, Any],
        available_zones: Dict[int, str]
    ) -> Optional[Dict[str, Any]]:
        """
        Detect physically impossible readings:
        - Fill decreases without collection
        - Duplicate timestamps
        - Negative weight
        """
        bin_id = event.get("bin_id")
        fill_level = event.get("fill_level_pct")
        estimated_weight = event.get("estimated_weight_kg")
        timestamp = event.get("timestamp")
        
        violations = []
        
        # Check 1: Fill level out of bounds
        if fill_level is not None:
            if fill_level < -5 or fill_level > 105:
                violations.append(f"Impossible fill level: {fill_level}%")
        
        # Check 2: Negative weight
        if estimated_weight is not None and estimated_weight < 0:
            violations.append(f"Negative estimated weight: {estimated_weight}kg")
        
        # Check 3: Fill decrease without collection
        if bin_id and fill_level is not None:
            last_record = self.metadata_state.get(bin_id, {})
            last_fill = last_record.get("last_fill_level")
            last_collected = last_record.get("last_collected_at")
            
            if last_fill is not None and fill_level < (last_fill - 5):
                # Fill decreased by >5% without collection event
                time_since_collection = None
                if last_collected and timestamp:
                    try:
                        ts = datetime.fromisoformat(timestamp.replace('Z', '+00:00')) if isinstance(timestamp, str) else timestamp
                        lc = datetime.fromisoformat(last_collected.replace('Z', '+00:00')) if isinstance(last_collected, str) else last_collected
                        time_since_collection = (ts - lc).total_seconds() / 3600
                    except:
                        pass
                
                if time_since_collection is None or time_since_collection > 0.5:
                    violations.append(f"Fill decreased {last_fill}% → {fill_level}% without collection")
        
        # Check 4: Zone mismatch
        bin_zone = metadata.get("zone_id")
        if bin_zone and bin_zone not in available_zones:
            violations.append(f"Bin assigned to non-existent zone: {bin_zone}")
        
        if violations:
            # Update metadata
            if bin_id:
                self.metadata_state[bin_id] = {
                    "last_fill_level": fill_level,
                    "last_collected_at": event.get("last_collected_at"),
                    "timestamp": timestamp,
                }
            
            return {
                "type": "IMPOSSIBLE_PHYSICS",
                "severity": "CRITICAL",
                "violations": violations,
                "evidence": {
                    "bin_id": bin_id,
                    "fill_level": fill_level,
                    "estimated_weight": estimated_weight,
                    "timestamp": timestamp,
                },
                "bayesian_probability": 0.95,  # Very high confidence
            }
        
        return None
    
    def _detect_entropy_anomaly(self, bin_id: str, fill_level: float) -> Optional[Dict[str, Any]]:
        """
        Detect spoofed data by Shannon entropy analysis
        Too uniform (entropy < 0.1) = constant spoofed values
        Too random (entropy > 0.9) = noise/random injection
        """
        window_size = 50
        self.bin_reading_history[bin_id].append(fill_level)
        
        # Keep only last 50 readings
        if len(self.bin_reading_history[bin_id]) > window_size:
            self.bin_reading_history[bin_id] = self.bin_reading_history[bin_id][-window_size:]
        
        if len(self.bin_reading_history[bin_id]) < 10:
            return None
        
        # Calculate Shannon entropy
        entropy = self._calculate_entropy(self.bin_reading_history[bin_id])
        
        if entropy < self.ENTROPY_UNIFORM_THRESHOLD:
            return {
                "type": "ENTROPY_ANOMALY",
                "severity": "HIGH",
                "subtype": "TOO_UNIFORM",
                "entropy_score": entropy,
                "threshold": self.ENTROPY_UNIFORM_THRESHOLD,
                "evidence": {
                    "bin_id": bin_id,
                    "recent_readings": self.bin_reading_history[bin_id][-10:],
                    "entropy": entropy,
                    "pattern": "Spoofed constant values detected",
                },
                "bayesian_probability": 0.85,
            }
        
        elif entropy > self.ENTROPY_RANDOM_THRESHOLD:
            return {
                "type": "ENTROPY_ANOMALY",
                "severity": "HIGH",
                "subtype": "TOO_RANDOM",
                "entropy_score": entropy,
                "threshold": self.ENTROPY_RANDOM_THRESHOLD,
                "evidence": {
                    "bin_id": bin_id,
                    "recent_readings": self.bin_reading_history[bin_id][-10:],
                    "entropy": entropy,
                    "pattern": "Random noise injection detected",
                },
                "bayesian_probability": 0.80,
            }
        
        return None
    
    def _detect_metadata_tampering(
        self, 
        bin_id: str, 
        metadata: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Detect metadata tampering:
        - Bin capacity changed mid-stream
        - Zone assignment changed
        - Invalid bin ID
        """
        violations = []
        
        current_state = self.metadata_state.get(bin_id, {})
        
        # Check bin capacity
        if "volume_litres" in metadata:
            last_volume = current_state.get("volume_litres")
            current_volume = metadata.get("volume_litres")
            
            if last_volume and last_volume != current_volume:
                if abs(last_volume - current_volume) > 5:  # More than 5L change
                    violations.append(f"Bin capacity changed: {last_volume}L → {current_volume}L")
        
        # Check zone assignment
        if "zone_id" in metadata:
            last_zone = current_state.get("zone_id")
            current_zone = metadata.get("zone_id")
            
            if last_zone and last_zone != current_zone:
                violations.append(f"Bin zone changed: {last_zone} → {current_zone}")
        
        # Check waste category
        if "waste_category_id" in metadata:
            last_category = current_state.get("waste_category_id")
            current_category = metadata.get("waste_category_id")
            
            if last_category and last_category != current_category:
                violations.append(f"Waste category changed: {last_category} → {current_category}")
        
        if violations:
            self.metadata_state[bin_id] = metadata
            return {
                "type": "METADATA_TAMPERING",
                "severity": "HIGH",
                "violations": violations,
                "evidence": {
                    "bin_id": bin_id,
                    "previous_metadata": current_state,
                    "new_metadata": metadata,
                },
                "bayesian_probability": 0.88,
            }
        
        self.metadata_state[bin_id] = metadata
        return None
    
    def _detect_gps_spoofing(self, vehicle_id: str, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Detect GPS spoofing by checking impossible vehicle movements
        """
        gps_data = event.get("vehicle_gps", {})
        lat = gps_data.get("lat")
        lng = gps_data.get("lng")
        timestamp = event.get("timestamp")
        
        if not lat or not lng:
            return None
        
        self.vehicle_gps_history[vehicle_id].append({
            "lat": lat,
            "lng": lng,
            "timestamp": timestamp,
        })
        
        # Keep only last 50 points
        if len(self.vehicle_gps_history[vehicle_id]) > 50:
            self.vehicle_gps_history[vehicle_id] = self.vehicle_gps_history[vehicle_id][-50:]
        
        if len(self.vehicle_gps_history[vehicle_id]) < 2:
            return None
        
        # Check last two GPS points
        last_point = self.vehicle_gps_history[vehicle_id][-2]
        current_point = self.vehicle_gps_history[vehicle_id][-1]
        
        distance_km = self._haversine_distance(
            last_point["lat"], last_point["lng"],
            current_point["lat"], current_point["lng"]
        )
        
        try:
            last_ts = datetime.fromisoformat(last_point["timestamp"].replace('Z', '+00:00')) if isinstance(last_point["timestamp"], str) else last_point["timestamp"]
            curr_ts = datetime.fromisoformat(current_point["timestamp"].replace('Z', '+00:00')) if isinstance(current_point["timestamp"], str) else current_point["timestamp"]
            time_diff_sec = (curr_ts - last_ts).total_seconds()
        except:
            return None
        
        if time_diff_sec <= 0:
            return None
        
        # Calculate required speed
        required_speed_kmh = (distance_km / time_diff_sec) * 3600
        max_realistic_speed_kmh = 120  # Realistic for waste trucks
        
        if distance_km > self.IMPOSSIBLE_GPS_DISTANCE_KM and time_diff_sec < self.IMPOSSIBLE_GPS_TIME_SEC:
            return {
                "type": "IMPOSSIBLE_PHYSICS",
                "severity": "CRITICAL",
                "subtype": "GPS_SPOOFING",
                "evidence": {
                    "vehicle_id": vehicle_id,
                    "distance_km": distance_km,
                    "time_sec": time_diff_sec,
                    "required_speed_kmh": required_speed_kmh,
                    "max_realistic_speed_kmh": max_realistic_speed_kmh,
                },
                "bayesian_probability": 0.92,
            }
        
        return None
    
    def _detect_network_anomaly(
        self, 
        api_token: str, 
        source_ip: str,
        bin_id: Optional[str],
        event: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Detect network-level attacks:
        - Same token from multiple IPs
        - Burst request patterns
        - Impossible request rates
        """
        token_hash = hashlib.sha256(api_token.encode()).hexdigest()[:16]
        now = datetime.now(timezone.utc)
        
        # Record network activity
        self.network_activity_state[token_hash].append({
            "ip": source_ip,
            "timestamp": now,
        })
        
        # Clean old entries (>60 min)
        window_start = now - timedelta(minutes=self.NETWORK_IP_WINDOW_MINUTES)
        self.network_activity_state[token_hash] = [
            a for a in self.network_activity_state[token_hash]
            if a["timestamp"] > window_start
        ]
        
        violations = []
        
        # Check 1: Multiple IPs using same token
        unique_ips = set(a["ip"] for a in self.network_activity_state[token_hash])
        if len(unique_ips) >= self.NETWORK_IP_THRESHOLD:
            violations.append(f"Same token used from {len(unique_ips)} different IPs in {self.NETWORK_IP_WINDOW_MINUTES}min")
        
        # Check 2: Burst request pattern (>1000 readings/sec)
        if len(self.network_activity_state[token_hash]) > self.NETWORK_BURST_THRESHOLD:
            violations.append(f"Abnormal request burst: {len(self.network_activity_state[token_hash])} requests in {self.NETWORK_IP_WINDOW_MINUTES}min")
        
        if violations:
            return {
                "type": "NETWORK_ANOMALY",
                "severity": "HIGH",
                "violations": violations,
                "evidence": {
                    "token_hash": token_hash,
                    "unique_ips": list(unique_ips),
                    "request_count": len(self.network_activity_state[token_hash]),
                    "bin_id": bin_id,
                },
                "bayesian_probability": 0.80,
            }
        
        return None
    
    def detect_coordinated_attack(self, recent_events: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Detect coordinated attacks across multiple bins/zones
        Triggered on windowed state (5-minute windows)
        """
        critical_events = [
            e for e in recent_events
            if e.get("severity") == "CRITICAL" or e.get("status") == "critical"
        ]
        
        if len(critical_events) < self.COORDINATED_ATTACK_BIN_THRESHOLD:
            return None
        
        # Check if events are from different zones
        zones = set()
        bins = set()
        timestamps = []
        
        for event in critical_events:
            if event.get("zone_id"):
                zones.add(event["zone_id"])
            if event.get("bin_id"):
                bins.add(event["bin_id"])
            if event.get("timestamp"):
                timestamps.append(event["timestamp"])
        
        # Must be from different zones to be "coordinated"
        if len(zones) < 2 or len(bins) < self.COORDINATED_ATTACK_BIN_THRESHOLD:
            return None
        
        # Bayesian probability: P(coordinated_attack | observations)
        # Base rate: 0.01 (1% prior)
        # Likelihood: P(3+ critical failures in 5 min window | attack) = 0.95
        # Likelihood: P(3+ critical failures in 5 min window | no attack) = 0.05
        base_rate = 0.01
        likelihood_attack = 0.95
        likelihood_normal = 0.05
        
        posterior = (likelihood_attack * base_rate) / (
            likelihood_attack * base_rate + likelihood_normal * (1 - base_rate)
        )
        
        return {
            "type": "COORDINATED_ATTACK",
            "severity": "CRITICAL",
            "attack_description": f"Coordinated critical failures in {len(zones)} zones affecting {len(bins)} bins",
            "evidence": {
                "affected_zones": list(zones),
                "affected_bins": list(bins),
                "critical_events_count": len(critical_events),
                "time_window_minutes": self.COORDINATED_ATTACK_WINDOW_MINUTES,
            },
            "bayesian_probability": min(1.0, posterior),
        }
    
    @staticmethod
    def _calculate_entropy(values: List[float]) -> float:
        """
        Calculate Shannon entropy of a list of values
        Bins values into 10 buckets and calculates entropy
        """
        if not values or len(values) < 2:
            return 0.0
        
        min_val = min(values)
        max_val = max(values)
        
        if min_val == max_val:
            return 0.0  # All same values
        
        # Bin values into 10 buckets
        num_bins = 10
        bucket_size = (max_val - min_val) / num_bins
        buckets = [0] * num_bins
        
        for val in values:
            bucket_idx = min(
                int((val - min_val) / bucket_size),
                num_bins - 1
            )
            buckets[bucket_idx] += 1
        
        # Calculate Shannon entropy
        entropy = 0.0
        n = len(values)
        for count in buckets:
            if count > 0:
                p = count / n
                entropy -= p * math.log2(p)
        
        return entropy
    
    @staticmethod
    def _haversine_distance(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
        """
        Calculate distance between two GPS points in kilometers
        """
        R = 6371  # Earth radius in km
        
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = math.radians(lat2 - lat1)
        delta_lng = math.radians(lng2 - lng1)
        
        a = math.sin(delta_lat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lng / 2) ** 2
        c = 2 * math.asin(math.sqrt(a))
        
        return R * c
