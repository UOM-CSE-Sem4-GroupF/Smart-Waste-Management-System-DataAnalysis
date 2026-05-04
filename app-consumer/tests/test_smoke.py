import kafka_consumer


def test_consumer_module_contract() -> None:
    assert hasattr(kafka_consumer, "run_consumer")
    assert callable(kafka_consumer.run_consumer)
    assert kafka_consumer.TOPIC == "waste.bin.telemetry" or isinstance(kafka_consumer.TOPIC, str)
