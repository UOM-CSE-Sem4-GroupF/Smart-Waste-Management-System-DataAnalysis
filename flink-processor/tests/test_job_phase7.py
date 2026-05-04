import json
import logging

from job import _load_local_event_json, _read_local_events, build_arg_parser, process_single_event, run_local_mode


class DummySettings:
    def __init__(self, local_test_input_file: str):
        self.local_test_input_file = local_test_input_file


def test_process_single_event_success():
    logger = logging.getLogger("test")

    processor = type("Processor", (), {"process": lambda self, e: {"bin_id": "BIN-001"}})()
    influx_sink = type(
        "InfluxSink",
        (),
        {
            "write_raw_event": lambda self, e: None,
            "write_processed_event": lambda self, e: None,
        },
    )()
    postgres_sink = type("PostgresSink", (), {"upsert_bin_current_state": lambda self, e: None})()
    kafka_sink = type("KafkaSink", (), {"publish_processed": lambda self, e: None})()

    ok = process_single_event(
        raw_event={"payload": {"bin_id": "BIN-001"}},
        processor=processor,
        influx_sink=influx_sink,
        postgres_sink=postgres_sink,
        kafka_sink=kafka_sink,
        logger=logger,
    )

    assert ok is True


def test_process_single_event_returns_false_when_processor_drops():
    logger = logging.getLogger("test")

    processor = type("Processor", (), {"process": lambda self, e: None})()
    influx_sink = type(
        "InfluxSink",
        (),
        {
            "write_raw_event": lambda self, e: None,
            "write_processed_event": lambda self, e: None,
        },
    )()
    postgres_sink = type("PostgresSink", (), {"upsert_bin_current_state": lambda self, e: None})()
    kafka_sink = type("KafkaSink", (), {"publish_processed": lambda self, e: None})()

    ok = process_single_event(
        raw_event={"payload": {"bin_id": "BIN-001"}},
        processor=processor,
        influx_sink=influx_sink,
        postgres_sink=postgres_sink,
        kafka_sink=kafka_sink,
        logger=logger,
    )

    assert ok is False


def test_read_local_events_skips_invalid_rows(tmp_path):
    input_file = tmp_path / "events.jsonl"
    input_file.write_text(
        "\n".join(
            [
                json.dumps({"payload": {"bin_id": "BIN-001"}}),
                "not-json",
                json.dumps([1, 2, 3]),
                json.dumps({"payload": {"bin_id": "BIN-002"}}),
            ]
        ),
        encoding="utf-8",
    )

    logger = logging.getLogger("test")
    events = list(_read_local_events(str(input_file), logger))

    assert len(events) == 2
    assert events[0]["payload"]["bin_id"] == "BIN-001"
    assert events[1]["payload"]["bin_id"] == "BIN-002"


def test_run_local_mode_honors_max_messages(tmp_path):
    input_file = tmp_path / "events.jsonl"
    input_file.write_text(
        "\n".join(
            [
                json.dumps({"payload": {"bin_id": "BIN-001"}}),
                json.dumps({"payload": {"bin_id": "BIN-002"}}),
                json.dumps({"payload": {"bin_id": "BIN-003"}}),
            ]
        ),
        encoding="utf-8",
    )

    settings = DummySettings(str(input_file))
    logger = logging.getLogger("test")

    class Counter:
        def __init__(self):
            self.raw_calls = 0
            self.proc_calls = 0
            self.pg_calls = 0
            self.kafka_calls = 0

    c = Counter()

    class Processor:
        def process(self, event):
            _ = event
            c.proc_calls += 1
            return {"bin_id": "BIN-001"}

    class Influx:
        def write_raw_event(self, event):
            _ = event
            c.raw_calls += 1

        def write_processed_event(self, event):
            _ = event

    class Pg:
        def upsert_bin_current_state(self, event):
            _ = event
            c.pg_calls += 1

    class Kafka:
        def publish_processed(self, event):
            _ = event
            c.kafka_calls += 1

    run_local_mode(
        settings=settings,
        processor=Processor(),
        influx_sink=Influx(),
        postgres_sink=Pg(),
        kafka_sink=Kafka(),
        logger=logger,
        max_messages=2,
    )

    assert c.raw_calls == 2
    assert c.proc_calls == 2
    assert c.pg_calls == 2
    assert c.kafka_calls == 2


def test_load_local_event_json_honors_limit(tmp_path):
    input_file = tmp_path / "events.jsonl"
    input_file.write_text(
        "\n".join(
            [
                json.dumps({"payload": {"bin_id": "BIN-001"}}),
                json.dumps({"payload": {"bin_id": "BIN-002"}}),
                json.dumps({"payload": {"bin_id": "BIN-003"}}),
            ]
        ),
        encoding="utf-8",
    )

    logger = logging.getLogger("test")
    payloads = _load_local_event_json(str(input_file), logger, max_messages=2)

    assert len(payloads) == 2
    first = json.loads(payloads[0])
    assert first["payload"]["bin_id"] == "BIN-001"


def test_parser_supports_pyflink_local_mode():
    parser = build_arg_parser()
    args = parser.parse_args(["--mode", "pyflink-local", "--max-messages", "5"])

    assert args.mode == "pyflink-local"
    assert args.max_messages == 5
