import json
import logging
from confluent_kafka import Producer, Message

from .logger import getLogger
from .kafkaconfig import get_producer_config

logger = getLogger(filename="kafka.log", name=__name__)


def create_producer(extra_config: dict = {}) -> Producer:
    return Producer(get_producer_config() | extra_config)


def acknowledge(err: Exception, msg: Message):
    if err is not None:
        logger.error(
            f"Error occured while producing msg, {msg.key().decode("utf-8")}, Exception: {err}"
        )
    else:
        logger.debug(
            f"Message {msg.key().decode("utf-8")} produced successfully to the topic {msg.topic()}"
        )


def produce_msg(
    topic: str,
    value: str | bytes,
    key=None,
    producer: Producer = None,
    callback=acknowledge,
    headers=None,
):
    if producer is None:
        producer = create_producer()
    producer.produce(topic, value, key, callback=callback)
    producer.flush(1)
