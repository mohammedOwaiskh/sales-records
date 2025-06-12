import json
import pytest

from src.utils.kafkaconfig import (
    AutoOffsetReset,
    get_consumer_config,
    get_kafka_config,
    get_producer_config,
)


def test_kafka_config_isnotempty():
    kafka_config = get_kafka_config()
    print(kafka_config)
    assert kafka_config != {}


def test_producer_config():
    result = {
        "bootstrap.server": "<your bootstrap server>",
        "sasl.username": "<your api-key>",
        "sasl.password": "<your api-secret>",
        "sasl.mechanisms": "PLAIN",
        "security.protocol": "SASL_SSL",
        "acks": "all",
    }

    assert get_producer_config() == result


@pytest.mark.parametrize(
    "consumer_grp,offset_reset",
    [
        ("grp-01", AutoOffsetReset.EARLIEST),
        ("grp-02", AutoOffsetReset.NONE),
        ("grp-02", AutoOffsetReset.LATEST),
    ],
)
def test_consumer_config(consumer_grp, offset_reset):
    result = {
        "bootstrap.server": "<your bootstrap server>",
        "sasl.username": "<your api-key>",
        "sasl.password": "<your api-secret>",
        "sasl.mechanisms": "PLAIN",
        "security.protocol": "SASL_SSL",
        "group.id": consumer_grp,
        "auto.offset.reset": offset_reset.value,
    }

    assert get_consumer_config(consumer_grp, offset_reset) == result
