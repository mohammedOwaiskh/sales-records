from .kafkaconfig import AutoOffsetReset, get_consumer_config
from confluent_kafka import Consumer


def create_consumer(
    topics: list[str],
    consumer_grp: str,
    auto_offset_reset: AutoOffsetReset = AutoOffsetReset.EARLIEST,
):
    consumer = Consumer(get_consumer_config(consumer_grp, auto_offset_reset))
    consumer.subscribe(topics)
    return consumer
