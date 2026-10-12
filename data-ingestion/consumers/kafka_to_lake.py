import json
import logging
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaConsumer

# Find the root directory of the project.
PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DIR = PROJECT_ROOT / "data-lake" / "raw"
BRONZE_DIR = PROJECT_ROOT / "data-lake" / "bronze"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "ecommerce-events"
KAFKA_GROUP_ID = "ecommerce-data-lake-consumer"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

running = True


def stop_consumer(signum, frame):
    """Request a graceful shutdown."""
    global running
    running = False
    logger.info("Shutdown requested. Finishing current work...")


def create_output_files():
    """Create timestamped JSON Lines files for raw and bronze data."""

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    raw_file = RAW_DIR / f"events_{timestamp}.jsonl"
    bronze_file = BRONZE_DIR / f"events_{timestamp}.jsonl"

    return raw_file, bronze_file


def main():
    """Consume Kafka events and save them to the local data lake."""

    signal.signal(signal.SIGINT, stop_consumer)
    signal.signal(signal.SIGTERM, stop_consumer)

    raw_file, bronze_file = create_output_files()

    logger.info("Starting Kafka-to-data-lake consumer")
    logger.info("Kafka topic: %s", KAFKA_TOPIC)
    logger.info("Raw output: %s", raw_file)
    logger.info("Bronze output: %s", bronze_file)

    consumer = None
    processed_count = 0

    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda value: json.loads(
                value.decode("utf-8")
            ),
            consumer_timeout_ms=1000,
        )

        logger.info("Connected to Kafka. Waiting for events...")

        while running:
            for message in consumer:
                if not running:
                    break

                event = message.value

                # Add ingestion metadata without changing the original event.
                bronze_event = {
                    **event,
                    "_ingestion_timestamp": datetime.now(
                        timezone.utc
                    ).isoformat(),
                    "_kafka_topic": message.topic,
                    "_kafka_partition": message.partition,
                    "_kafka_offset": message.offset,
                }

                # Write the original event to the raw layer.
                with raw_file.open("a", encoding="utf-8") as file:
                    file.write(
                        json.dumps(event, ensure_ascii=False) + "\n"
                    )

                # Write the enriched event to the bronze layer.
                with bronze_file.open("a", encoding="utf-8") as file:
                    file.write(
                        json.dumps(bronze_event, ensure_ascii=False) + "\n"
                    )

                # Commit this message's offset after writing the files.
                consumer.commit()

                processed_count += 1

                logger.info(
                    "Saved event %s | partition=%s | offset=%s",
                    event.get("event_type", "UNKNOWN"),
                    message.partition,
                    message.offset,
                )

            # The consumer timeout allows the program to check the stop flag.

    except Exception:
        logger.exception("Consumer stopped because of an error.")
        sys.exit_code = 1

    finally:
        if consumer is not None:
            consumer.close()

        logger.info("Consumer closed. Events processed: %s", processed_count)


if __name__ == "__main__":
    main()
