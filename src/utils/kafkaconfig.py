from configparser import ConfigParser
from functools import cache
import json
from enum import Enum
import os
import pprint


class AutoOffsetReset(Enum):
    LATEST = "latest"
    EARLIEST = "earliest"
    NONE = "none"


def get_configparser() -> ConfigParser:
    config = ConfigParser()
    current_directory = os.getcwd()
    # print(current_directory)
    config.read(os.path.join(current_directory, "config\\kafka.conf"))
    return config


@cache
def get_kafka_config() -> dict:
    config = get_configparser()
    return dict(config.items("DEFAULT"))


def get_producer_config() -> dict:
    return get_kafka_config() | {"acks": "all"}


def get_consumer_config(
    consumer_grp: str, offset_reset: AutoOffsetReset = AutoOffsetReset.EARLIEST
) -> dict:
    consumer_config = get_kafka_config()
    consumer_config.update(
        {"group.id": consumer_grp, "auto.offset.reset": offset_reset.value}
    )
    return consumer_config
