# Guide: Implementing Pipeline 1 (Bin Telemetry) using PyFlink

This guide outlines the steps to align the `flink-processor` with the `06-flink-processor.md` specification. The goal is to move from a standard Kafka consumer to a stateful Apache Flink stream processing job.

## 1. Architectural Shift
Instead of a manual `for message in consumer` loop, we will use Flink's **DataStream API**. This allows for:
- **Exactly-once processing** (via Checkpoints).
- **Stateful calculations** (storing previous fill levels in Flink's state backend).
- **Parallelism** (scaling processing by `bin_id`).

## 2. Step 1: Define the Stateful Processor
The core logic should be moved into a `KeyedProcessFunction`. This is necessary because Flink state is "keyed"—meaning Flink ensures that all data for `BIN-001` always goes to the same state instance.

### Target: `processors/bin_telemetry_flink.py`
```python
from pyflink.datastream import KeyedProcessFunction
from pyflink.common import Types
from pyflink.common.state import ValueStateDescriptor

class BinTelemetryProcessor(KeyedProcessFunction):
    def open(self, runtime_context):
        # 1. Initialize Managed State (survives restarts)
        self.prev_level = runtime_context.get_state(
            ValueStateDescriptor("prev_level", Types.FLOAT())
        )
        self.prev_ts = runtime_context.get_state(
            ValueStateDescriptor("prev_ts", Types.LONG())
        )
        self.last_alert_ts = runtime_context.get_state(
            ValueStateDescriptor("last_alert_ts", Types.LONG())
        )

    def process_element(self, event, ctx):
        # 2. Logic Flow
        # - Load Metadata (Postgres)
        # - Calculate Weight (fill * volume)
        # - Calculate Fill Rate (current - state.prev_level) / time_diff
        # - Classify Urgency (critical/urgent/monitor/normal)
        # - Detect Anomaly (Throttled by state.last_alert_ts)
        
        # 3. Update State
        self.prev_level.update(current_level)
        self.prev_ts.update(current_ts)
        
        yield processed_event
```

## 3. Step 2: Implement Flink Sinks
Flink uses specialized Sink functions to write data. You need three:

1.  **`PostgresUpsertSink`**: Handles the `INSERT ... ON CONFLICT UPDATE` logic for `bin_current_state`.
2.  **`InfluxDBSink`**: Specifically for time-series logging.
3.  **`KafkaOutputSink`**: To push results to `waste.bin.processed`.

## 4. Step 3: Wire the Job (`job_flink.py`)
This script initializes the environment and defines the "Graph".

```python
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    
    # 1. Source: Kafka
    ds = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source")
    
    # 2. Process: Keyed by bin_id
    processed_ds = ds.key_by(lambda x: x['bin_id']) \
                     .process(BinTelemetryProcessor())
    
    # 3. Sinks: Fan out
    processed_ds.add_sink(PostgresSink())
    processed_ds.add_sink(InfluxSink())
    processed_ds.add_sink(KafkaSink())
    
    env.execute("Bin Telemetry Pipeline")
```

## 5. Critical Implementation Details (From Spec)
- **Weight Calculation**: Ensure you are using the `volume_litres` and `avg_kg_per_litre` from the PostgreSQL metadata cache.
- **Anomaly Cooldown**: Use the `last_alert_time` state to ensure a bin doesn't spam "RAPID_FILLING" alerts more than once every 30 minutes.
- **Urgency Score**: Implement the exact scoring logic (Base score + fill rate modifier) defined in Section 4.2 of the Spec.

## 6. Validation Checklist
- [ ] Service starts using `pyflink-local` mode or a Flink Cluster.
- [ ] Checkpoints are visible in the logs (proving state is being persisted).
- [ ] `estimated_weight_kg` appears in the `bin_current_state` table.
- [ ] If the service is killed and restarted, the `fill_rate` is calculated correctly on the *first* message received (retrieved from state).
