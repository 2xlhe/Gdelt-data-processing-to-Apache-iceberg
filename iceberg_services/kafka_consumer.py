import json
import logging
import os
from kafka import KafkaConsumer
from parser.kafka_parser import get_files_from_kafka
from data_processor import read_dataset_csv, save_processed
from iceberg.iceberg import insert_to_iceberg

logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9093")

logger.debug(f"\nThe broker is {KAFKA_BROKER}")

YELLOW_COLOUR = "\033[93m"
GREEN_COLOUR = "\033[92m"
GREY_COLOUR = "\033[2m"
RESET = "\033[0m"

print(f"{GREEN_COLOUR}\nIniting Kafka Consumer{RESET}")
consumer = KafkaConsumer("gdelt_urls", bootstrap_servers=KAFKA_BROKER)

for msg in consumer:
    print(f"\n{YELLOW_COLOUR}> Starting Consuming: {RESET}")
    print(f"{GREY_COLOUR}> {msg}: {RESET}")
    try:
        msg_values = json.loads(msg.value)
        downloaded = get_files_from_kafka(*msg_values)
        processed = read_dataset_csv(downloaded)
        file_path = save_processed(processed)

        logger.info(f"\nSaved parquet in {file_path[0]}")

        logger.info(f"> Saved parquet in {file_path[0]}")
        for file in file_path:
            try:
                insert_to_iceberg(file)
            except Exception as e:
                logger.error(f" error inserting {file} to iceberg: {e.args}\n")
    except Exception as e:
        logger.error(f"Error processing message: {e}\n")
        continue
