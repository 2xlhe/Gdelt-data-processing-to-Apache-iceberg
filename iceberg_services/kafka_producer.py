import time
import json
import logging
import os
from kafka import KafkaProducer
from parser.gdelt_parser import DownloadGDeltUpdatedData

logger = logging.getLogger(__name__)
logging.basicConfig(encoding="utf-8", level=None)

INTERVAL_MINUTES = 0.1


KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
print(f"{KAFKA_BROKER=}")


GREEN_COLOUR = "\033[32m"
RESET = "\033[0m"


logger.debug(f"\nThe broker is {KAFKA_BROKER}")

print(f"{GREEN_COLOUR}\nIniting Kafka Producer{RESET}")
producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER])


def parse_and_send():
    files = DownloadGDeltUpdatedData().tables
    for f in files.items():
        data = json.dumps(f)
        print(f"> {data}")
        producer.send("gdelt_urls", data.encode("UTF-8"))
        print(f"{GREEN_COLOUR}Data sent to Kafka{RESET}")
    producer.flush()


if __name__ == "__main__":
    while True:
        parse_and_send()
        time.sleep(INTERVAL_MINUTES * 60)
