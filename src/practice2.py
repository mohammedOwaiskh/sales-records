import json
import time
from confluent_kafka import Consumer, Producer

from utils.logger import getLogger
from utils.producer import acknowledge, create_producer, produce_msg
from utils.kafkaconfig import AutoOffsetReset
from utils.consumer import create_consumer

logger = getLogger("app.log", name=__name__)

CONSUMER_TOPIC = "test.topic.raw"
PRODUCER_TOPIC = "test.topic.threshold_costs"

TIMEOUT_SEC = 60

extra_producer_config = {
    "linger.ms": 100,  # Adding 100ms of delay before sending the batch
    "batch.num.messages": 10000,  # Creating a batch of 10k records
    "compression.type": "snappy",
    # "queue.buffering.max.messages": 100000,
    # "queue.buffering.max.kbytes": 1048576,  # 1 GB
}

producer = create_producer(extra_producer_config)

if __name__ == "__main__":

    consumer = create_consumer(
        [CONSUMER_TOPIC],
        "cg_0100",
        AutoOffsetReset.EARLIEST,
    )

    start_time = None

    count = 0

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if start_time is None:
                    start_time = time.time()
                # Check timeout and break after 60 sec of inactivity
                if time.time() - start_time < TIMEOUT_SEC:
                    logger.debug("Waiting for a new message....")
                else:
                    logger.info("Exiting consumer. Timeout occured...")
                    raise KeyboardInterrupt
            elif msg.error():
                start_time = None
                logger.error(f"Error Occured: {msg.error()}")
            else:
                start_time = None
                logger.debug("Message Recieved!")
                data = msg.value().decode("utf-8")
                json_data = json.loads(data)
                if float(json_data["Total Cost"]) > 304017.56:

                    key = json_data["Order ID"]
                    producer.produce(PRODUCER_TOPIC, data, key, callback=acknowledge)
                    count += 1
                    if count % 10000 == 0:
                        logger.debug(f"Producing batch number {int(count/10000)}")
                        producer.flush(0.1)

    except KeyboardInterrupt:
        logger.info("Stopped consuming")
    finally:
        producer.flush()
        consumer.close()
