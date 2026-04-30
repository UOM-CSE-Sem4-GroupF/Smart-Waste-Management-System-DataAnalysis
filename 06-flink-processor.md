# Technical Specification — Flink Stream Processor
**Owner:** F2  
**Repo:** group-f-data/flink-processor  
**Version:** 1.0  
**Stack:** Python 3.11 · Apache Flink (PyFlink) · InfluxDB client · Psycopg2

---

## 1. Purpose

The Flink stream processor is the real-time intelligence engine of the system. It consumes raw sensor data and GPS streams from Kafka, applies stateful processing, and publishes enriched events back to Kafka and into the databases.

It is the boundary between raw IoT data and business-meaningful information.

---

## 2. Context in the system

```
waste.bin.telemetry ──► Flink ──► waste.bin.processed
                             ──► InfluxDB bin_readings_raw
                             ──► InfluxDB bin_readings_processed
                             ──► PostgreSQL bin_current_state (upsert)

waste.vehicle.location ──► Flink ──► waste.vehicle.deviation
                                ──► InfluxDB vehicle_positions

waste.bin.processed ──► Flink ──► waste.zone.statistics
                             ──► PostgreSQL zone_snapshots
                             ──► InfluxDB zone_statistics
```

---

## 3. Flink deployment

```
Job manager:   1 pod — coordinates job execution
Task manager:  2-3 pods — execute the actual processing
               Each task manager has 4 task slots

Checkpointing: every 30 seconds to persistent volume
               Enables exactly-once processing after crash recovery

State backend: RocksDB (persisted to disk — survives pod restart)
```

---

## 4. Pipeline 1 — Bin telemetry processor

**Source:** `waste.bin.telemetry`  
**Parallelism:** keyed by `bin_id`

### 4.1 Raw writer sink

Every message is written to InfluxDB immediately, before any processing.

```python
class RawBinWriter(SinkFunction):

    def invoke(self, event: dict, context):
        self.influx.write(
            measurement='bin_readings_raw',
            tags={
                'bin_id': event['bin_id'],
                'zone_id': str(self.get_zone_id(event['bin_id'])),
                'waste_category': self.get_waste_category(event['bin_id'])
            },
            fields={
                'fill_level_pct':     event['fill_level_pct'],
                'battery_level_pct':  event['battery_level_pct'],
                'signal_strength_dbm': event['signal_strength_dbm'],
                'temperature_c':      event.get('temperature_c', 0.0)
            },
            time=event['timestamp']
        )
```

### 4.2 Enrichment and classification

```python
class BinTelemetryProcessor(KeyedProcessFunction):
    """
    Keyed by bin_id — all events for BIN-047 go to same instance.
    State persisted in RocksDB.
    """

    def open(self, runtime_context):
        # State: previous readings for fill rate calculation
        self.previous_level = runtime_context.get_state(
            ValueStateDescriptor('previous_level', Types.FLOAT())
        )
        self.previous_timestamp = runtime_context.get_state(
            ValueStateDescriptor('previous_timestamp', Types.LONG())
        )
        self.last_alert_time = runtime_context.get_state(
            ValueStateDescriptor('last_alert_time', Types.LONG())
        )

        # Load bin metadata from PostgreSQL (cached, refreshed hourly)
        self.bin_metadata_cache = BinMetadataCache()

    def process_element(self, event: dict, ctx):

        bin_id = event['bin_id']
        fill_level = event['fill_level_pct']
        event_time_ms = parse_timestamp_ms(event['timestamp'])

        # ── LOAD BIN METADATA ──────────────────────────────────
        metadata = self.bin_metadata_cache.get(bin_id)
        if not metadata:
            # Bin not in registry — skip, log warning
            logger.warning(f'Unknown bin: {bin_id}')
            return

        # ── CALCULATE ESTIMATED WEIGHT ─────────────────────────
        estimated_weight_kg = (
            (fill_level / 100.0)
            * metadata.volume_litres
            * metadata.avg_kg_per_litre
        )
        estimated_weight_kg = round(estimated_weight_kg, 2)

        # ── CALCULATE FILL RATE ────────────────────────────────
        prev_level = self.previous_level.value()
        prev_ts = self.previous_timestamp.value()
        fill_rate = None
        predicted_full_at = None

        if prev_level is not None and prev_ts is not None:
            time_diff_hours = (event_time_ms - prev_ts) / 3_600_000

            if time_diff_hours > 0:
                fill_rate = (fill_level - prev_level) / time_diff_hours
                fill_rate = round(fill_rate, 3)

                if fill_rate > 0:
                    hours_until_full = (100.0 - fill_level) / fill_rate
                    predicted_full_at = (
                        event_time_ms + int(hours_until_full * 3_600_000)
                    )

        # Update state for next reading
        self.previous_level.update(fill_level)
        self.previous_timestamp.update(event_time_ms)

        # ── URGENCY CLASSIFICATION ─────────────────────────────
        status, urgency_score = classify_urgency(fill_level, fill_rate)

        # ── ANOMALY DETECTION ──────────────────────────────────
        anomaly = detect_anomaly(
            fill_level, fill_rate, event['battery_level_pct'],
            event_time_ms, self.last_alert_time.value()
        )

        if anomaly:
            self.last_alert_time.update(event_time_ms)

        # ── EMIT PROCESSED EVENT ───────────────────────────────
        processed = {
            'version': '1.0',
            'source_service': 'flink-processor',
            'timestamp': event['timestamp'],
            'payload': {
                'bin_id':                   bin_id,
                'fill_level_pct':           fill_level,
                'status':                   status,
                'urgency_score':            urgency_score,
                'estimated_weight_kg':      estimated_weight_kg,
                'fill_rate_pct_per_hour':   fill_rate,
                'predicted_full_at':        ms_to_iso(predicted_full_at),
                'battery_level_pct':        event['battery_level_pct'],
                'signal_strength_dbm':      event['signal_strength_dbm'],
                'zone_id':                  metadata.zone_id,
                'waste_category':           metadata.waste_category,
                'cluster_id':               metadata.cluster_id,
                'anomaly':                  anomaly
            }
        }

        yield processed  # goes to all sinks


def classify_urgency(fill_level: float, fill_rate: float | None) -> tuple:
    """
    Returns (status, urgency_score).
    Fill rate modifier: rapid filling increases score.
    """
    # Base score from fill level
    if fill_level >= 90:
        base_score = 85 + int((fill_level - 90) * 1.5)
        status = 'critical'
    elif fill_level >= 75:
        base_score = 60 + int((fill_level - 75) * 1.67)
        status = 'urgent'
    elif fill_level >= 50:
        base_score = 30 + int((fill_level - 50) * 1.2)
        status = 'monitor'
    else:
        base_score = int(fill_level * 0.6)
        status = 'normal'

    # Fill rate modifiers
    score = base_score
    if fill_rate is not None:
        if fill_rate > 15:  score += 15   # very rapid filling
        elif fill_rate > 10: score += 10  # rapid filling
        elif fill_rate > 5:  score += 5   # above average

    score = min(score, 100)

    # Re-classify status after modifier
    if score >= 85:   status = 'critical'
    elif score >= 60: status = 'urgent'
    elif score >= 30: status = 'monitor'
    else:             status = 'normal'

    return status, score


def detect_anomaly(fill_level, fill_rate, battery, event_time_ms, last_alert_ms):
    """
    Returns anomaly dict or None.
    Throttle: same bin cannot raise same anomaly twice within 30 minutes.
    """
    cooldown_ms = 30 * 60 * 1000
    if last_alert_ms and (event_time_ms - last_alert_ms) < cooldown_ms:
        return None

    if fill_rate is not None:
        if fill_rate > 15:
            return { 'type': 'RAPID_FILLING', 'fill_rate': fill_rate }
        if fill_rate < -5:
            return { 'type': 'POSSIBLE_TAMPERING', 'fill_rate': fill_rate }

    if battery < 10:
        return { 'type': 'LOW_BATTERY', 'battery_pct': battery }

    return None
```

### 4.3 Sinks for Pipeline 1

```python
# Sink 1: InfluxDB processed readings
class ProcessedBinWriter(SinkFunction):
    def invoke(self, event, context):
        payload = event['payload']
        self.influx.write(
            measurement='bin_readings_processed',
            tags={
                'bin_id': payload['bin_id'],
                'zone_id': str(payload['zone_id']),
                'waste_category': payload['waste_category'],
                'status': payload['status']
            },
            fields={
                'fill_level_pct':          payload['fill_level_pct'],
                'urgency_score':           payload['urgency_score'],
                'estimated_weight_kg':     payload['estimated_weight_kg'],
                'fill_rate_pct_per_hour':  payload['fill_rate_pct_per_hour'] or 0.0,
                'predicted_full_hours':    self.hours_until_full(payload)
            },
            time=event['timestamp']
        )


# Sink 2: PostgreSQL bin_current_state (UPSERT)
class BinCurrentStateWriter(SinkFunction):
    def invoke(self, event, context):
        payload = event['payload']
        self.db.execute("""
            INSERT INTO f2.bin_current_state (
                bin_id, fill_level_pct, battery_level_pct,
                signal_strength_dbm, temperature_c,
                estimated_weight_kg, fill_rate_pct_per_hour,
                predicted_full_at, status, urgency_score,
                zone_id, waste_category_id, volume_litres,
                last_reading_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, NOW()
            )
            ON CONFLICT (bin_id) DO UPDATE SET
                fill_level_pct          = EXCLUDED.fill_level_pct,
                battery_level_pct       = EXCLUDED.battery_level_pct,
                signal_strength_dbm     = EXCLUDED.signal_strength_dbm,
                estimated_weight_kg     = EXCLUDED.estimated_weight_kg,
                fill_rate_pct_per_hour  = EXCLUDED.fill_rate_pct_per_hour,
                predicted_full_at       = EXCLUDED.predicted_full_at,
                status                  = EXCLUDED.status,
                urgency_score           = EXCLUDED.urgency_score,
                last_reading_at         = EXCLUDED.last_reading_at,
                updated_at              = NOW()
        """, [
            payload['bin_id'],
            payload['fill_level_pct'],
            payload['battery_level_pct'],
            payload['signal_strength_dbm'],
            payload.get('temperature_c'),
            payload['estimated_weight_kg'],
            payload['fill_rate_pct_per_hour'],
            payload['predicted_full_at'],
            payload['status'],
            payload['urgency_score'],
            payload['zone_id'],
            self.get_category_id(payload['waste_category']),
            self.get_volume(payload['bin_id']),
            event['timestamp']
        ])


# Sink 3: Kafka waste.bin.processed
# (standard KafkaSink — publishes enriched event)
```

---

## 5. Pipeline 2 — Zone aggregation

**Source:** `waste.bin.processed` (same topic as bin enrichment output)  
**Window:** Sliding — 10 minutes, slide every 2 minutes  
**Key:** `zone_id`

```python
class ZoneAggregator(AggregateFunction):

    def create_accumulator(self):
        return {
            'zone_id': None,
            'total_fill': 0.0,
            'total_weight': 0.0,
            'urgent_count': 0,
            'critical_count': 0,
            'total_bins': 0,
            'category_totals': {}   # category → {count, fill_sum, weight_sum}
        }

    def add(self, event: dict, acc: dict) -> dict:
        payload = event['payload']
        category = payload['waste_category']

        acc['zone_id'] = payload['zone_id']
        acc['total_fill'] += payload['fill_level_pct']
        acc['total_weight'] += payload['estimated_weight_kg']
        acc['total_bins'] += 1

        if payload['urgency_score'] > 85:
            acc['critical_count'] += 1
        elif payload['urgency_score'] > 60:
            acc['urgent_count'] += 1

        if category not in acc['category_totals']:
            acc['category_totals'][category] = {
                'count': 0, 'fill_sum': 0.0, 'weight_sum': 0.0
            }
        acc['category_totals'][category]['count'] += 1
        acc['category_totals'][category]['fill_sum'] += payload['fill_level_pct']
        acc['category_totals'][category]['weight_sum'] += payload['estimated_weight_kg']

        return acc

    def get_result(self, acc: dict) -> dict:
        n = max(acc['total_bins'], 1)
        category_breakdown = {
            cat: {
                'count': v['count'],
                'avg_fill': round(v['fill_sum'] / v['count'], 2),
                'total_kg': round(v['weight_sum'], 2)
            }
            for cat, v in acc['category_totals'].items()
        }
        dominant = max(
            acc['category_totals'],
            key=lambda c: acc['category_totals'][c]['weight_sum'],
            default=None
        )

        return {
            'version': '1.0',
            'source_service': 'flink-processor',
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'payload': {
                'zone_id':                  acc['zone_id'],
                'avg_fill_level_pct':       round(acc['total_fill'] / n, 2),
                'urgent_bin_count':         acc['urgent_count'],
                'critical_bin_count':       acc['critical_count'],
                'total_bins':               acc['total_bins'],
                'total_estimated_weight_kg': round(acc['total_weight'], 2),
                'dominant_waste_category':  dominant,
                'category_breakdown':       category_breakdown,
                'window_minutes':           10
            }
        }
```

**Sinks for Pipeline 2:**

```python
# Sink 1: PostgreSQL zone_snapshots
# Sink 2: InfluxDB zone_statistics
# Sink 3: Kafka waste.zone.statistics
```

---

## 6. Pipeline 3 — Vehicle deviation detector

**Source:** `waste.vehicle.location`  
**Key:** `vehicle_id`

```python
class VehicleDeviationDetector(KeyedProcessFunction):

    def open(self, runtime_context):
        # Sliding window of last 5 minutes of deviations
        self.deviation_history = runtime_context.get_list_state(
            ListStateDescriptor('deviation_history', Types.PICKLED_BYTEARRAY())
        )
        self.planned_route = runtime_context.get_state(
            ValueStateDescriptor('planned_route', Types.PICKLED_BYTEARRAY())
        )

    def process_element(self, event: dict, ctx):
        payload = event['payload']
        vehicle_id = payload['vehicle_id']
        job_id = payload.get('job_id')

        if not job_id:
            return  # vehicle not on active job — skip

        # Load planned route if not cached
        route = self.planned_route.value()
        if not route:
            route = load_route_from_db(job_id)
            if route:
                self.planned_route.update(route)
            else:
                return  # no route found — skip

        # Find nearest point on planned route
        deviation_m = calculate_deviation(
            payload['lat'], payload['lng'], route['waypoints']
        )

        # Update deviation history (keep last 5 minutes)
        now_ms = parse_timestamp_ms(event['timestamp'])
        history = list(self.deviation_history.get())
        history.append({'metres': deviation_m, 'time_ms': now_ms})

        # Remove entries older than 5 minutes
        cutoff_ms = now_ms - (5 * 60 * 1000)
        history = [h for h in history if h['time_ms'] > cutoff_ms]
        self.deviation_history.update(history)

        # Alert if consistently off route for > 2 minutes
        two_min_ago = now_ms - (2 * 60 * 1000)
        recent = [h for h in history if h['time_ms'] > two_min_ago]

        if len(recent) >= 3 and all(h['metres'] > 500 for h in recent):
            duration_s = int((now_ms - recent[0]['time_ms']) / 1000)
            yield {
                'version': '1.0',
                'source_service': 'flink-processor',
                'timestamp': event['timestamp'],
                'payload': {
                    'vehicle_id':        vehicle_id,
                    'job_id':            job_id,
                    'deviation_metres':  int(deviation_m),
                    'duration_seconds':  duration_s,
                    'current_lat':       payload['lat'],
                    'current_lng':       payload['lng']
                }
            }
            # Clear history to prevent repeated alerts
            self.deviation_history.clear()
```

---

## 7. Pipeline 4 — Vehicle position historian

**Source:** `waste.vehicle.location`

Simple pass-through to InfluxDB. No processing.

```python
class VehiclePositionHistorian(SinkFunction):
    def invoke(self, event, context):
        payload = event['payload']
        self.influx.write(
            measurement='vehicle_positions',
            tags={
                'vehicle_id': payload['vehicle_id'],
                'driver_id':  payload.get('driver_id', 'unknown'),
                'job_id':     payload.get('job_id', 'none')
            },
            fields={
                'lat':              payload['lat'],
                'lng':              payload['lng'],
                'speed_kmh':        payload.get('speed_kmh', 0.0),
                'heading_degrees':  payload.get('heading_degrees', 0.0),
                'accuracy_m':       payload.get('accuracy_m', 0.0)
            },
            time=event['timestamp']
        )
```

---

## 8. Sensor offline detector

A separate Flink job using processing-time timers to detect bins that stop reporting.

```python
class SensorOfflineDetector(KeyedProcessFunction):

    OFFLINE_THRESHOLD_MS = 30 * 60 * 1000  # 30 minutes

    def open(self, runtime_context):
        self.last_seen = runtime_context.get_state(
            ValueStateDescriptor('last_seen', Types.LONG())
        )

    def process_element(self, event, ctx):
        now_ms = ctx.timestamp()
        self.last_seen.update(now_ms)

        # Register a timer to fire in 30 minutes
        ctx.timer_service().register_processing_time_timer(
            now_ms + self.OFFLINE_THRESHOLD_MS
        )

    def on_timer(self, timestamp, ctx):
        last = self.last_seen.value()

        if timestamp - last >= self.OFFLINE_THRESHOLD_MS:
            # Bin has not reported for 30 minutes
            bin_id = ctx.get_current_key()

            # Update bin_current_state to offline
            update_bin_status_db(bin_id, 'offline', 0)

            # Publish alert
            yield {
                'payload': {
                    'bin_id': bin_id,
                    'anomaly': { 'type': 'SENSOR_OFFLINE' }
                }
            }
```

---

## 9. Acceptance criteria

```
[ ] Pipeline 1: consumes waste.bin.telemetry
[ ] Pipeline 1: writes raw readings to InfluxDB immediately
[ ] Pipeline 1: calculates estimated_weight_kg using waste category metadata
[ ] Pipeline 1: calculates fill_rate from state (previous reading)
[ ] Pipeline 1: calculates predicted_full_at from fill rate
[ ] Pipeline 1: classifies urgency correctly (normal/monitor/urgent/critical)
[ ] Pipeline 1: detects RAPID_FILLING anomaly (fill_rate > 15%/hr)
[ ] Pipeline 1: detects POSSIBLE_TAMPERING (fill_rate < -5%/hr)
[ ] Pipeline 1: detects LOW_BATTERY (battery < 10%)
[ ] Pipeline 1: upserts bin_current_state in PostgreSQL
[ ] Pipeline 1: publishes to waste.bin.processed
[ ] Pipeline 2: aggregates zone stats in sliding 10-min window
[ ] Pipeline 2: publishes to waste.zone.statistics every 2 minutes
[ ] Pipeline 2: writes to PostgreSQL zone_snapshots
[ ] Pipeline 3: detects vehicle deviation > 500m for > 2 minutes
[ ] Pipeline 3: publishes to waste.vehicle.deviation
[ ] Pipeline 4: writes all GPS positions to InfluxDB
[ ] Recovery: after pod crash, restores state from checkpoint
[ ] Recovery: no events lost after recovery (exactly-once)
[ ] Performance: processes 100 events/second without lag growth
```