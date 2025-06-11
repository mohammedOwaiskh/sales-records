import csv
import json
import os
from confluent_kafka import Producer

from utils.logger import getLogger
from utils.producer import acknowledge, create_producer, produce_msg

TOPIC = "test.topic.raw"

logger = getLogger(name=__name__)


# These config support opmitization for batch processing of 1.5M records
extra_producer_config = {
    "linger.ms": 100,  # Adding 100ms of delay before sending the batch
    "batch.num.messages": 10000,  # Creating a batch of 10k records
    "compression.type": "snappy",
    "queue.buffering.max.messages": 100000,
    "queue.buffering.max.kbytes": 1048576,  # 1 GB
}

producer = create_producer(extra_config=extra_producer_config)

if __name__ == "__main__":

    dataset_file = os.path.join(os.getcwd(), "data\\1500000_Sales_Records.csv")

    count = 0

    with open(dataset_file, "r", newline="", encoding="utf-8") as csvfile:

        logger.debug("Reading CSV file...")
        csv_data = csv.DictReader(csvfile)

        for row in csv_data:

            data = json.dumps(row)
            key = row["Order ID"]
            producer.produce(TOPIC, data, key, callback=acknowledge)
            count += 1
            if count % 10000 == 0:
                producer.flush(0.1)
        producer.flush()

    logger.info(f"Published {count} messages to the producer")
