import json
import logging
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_DIR = PROJECT_ROOT / "data-lake" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data-lake" / "silver"
QUARANTINE_DIR = SILVER_DIR / "quarantine"

VALID_EVENT_TYPES = {
    "CUSTOMER_CREATED",
    "PRODUCT_VIEWED",
    "ADD_TO_CART",
    "ORDER_CREATED",
    "PAYMENT_COMPLETED",
}

REQUIRED_FIELDS = {
    "event_id": str,
    "event_type": str,
    "event_timestamp": str,
    "customer_id": str,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def validate_event(event):
    """Return a list of validation errors for one event."""
    errors = []

    if not isinstance(event, dict):
        return ["Event must be a JSON object"]

    for field, expected_type in REQUIRED_FIELDS.items():
        value = event.get(field)

        if not isinstance(value, expected_type) or not value.strip():
            errors.append(f"Missing or invalid field: {field}")

    event_type = event.get("event_type")

    if isinstance(event_type, str) and event_type not in VALID_EVENT_TYPES:
        errors.append(f"Unsupported event_type: {event_type}")

    timestamp = event.get("event_timestamp")

    if isinstance(timestamp, str) and timestamp.strip():
        try:
            datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError:
            errors.append("Invalid event_timestamp")

    if event_type == "PAYMENT_COMPLETED":
        amount = event.get("amount")

        if (
            isinstance(amount, bool)
            or not isinstance(amount, (int, float))
            or amount <= 0
        ):
            errors.append("Payment amount must be a positive number")

        if not isinstance(event.get("payment_id"), str) or not event["payment_id"].strip():
            errors.append("Missing or invalid payment_id")

        if not isinstance(event.get("order_id"), str) or not event["order_id"].strip():
            errors.append("Missing or invalid order_id")

    if event_type == "ORDER_CREATED":
        total = event.get("total_amount")

        if (
            isinstance(total, bool)
            or not isinstance(total, (int, float))
            or total <= 0
        ):
            errors.append("Order total_amount must be a positive number")

    return errors


def main():
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

    bronze_files = sorted(BRONZE_DIR.glob("*.jsonl"))

    if not bronze_files:
        logger.error("No Bronze JSONL files found in %s", BRONZE_DIR)
        return

    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    silver_file = SILVER_DIR / f"clean_events_{run_timestamp}.jsonl"
    quarantine_file = QUARANTINE_DIR / f"invalid_events_{run_timestamp}.jsonl"
    summary_file = SILVER_DIR / f"quality_summary_{run_timestamp}.json"

    seen_event_ids = set()
    counts = Counter()

    with (
        silver_file.open("w", encoding="utf-8") as silver_output,
        quarantine_file.open("w", encoding="utf-8") as quarantine_output,
    ):
        for bronze_file in bronze_files:
            logger.info("Processing Bronze file: %s", bronze_file.name)

            with bronze_file.open("r", encoding="utf-8") as input_file:
                for line_number, line in enumerate(input_file, start=1):
                    counts["processed"] += 1

                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError:
                        counts["invalid"] += 1
                        quarantine_output.write(
                            json.dumps({
                                "source_file": bronze_file.name,
                                "source_line": line_number,
                                "errors": ["Malformed JSON"],
                                "raw_record": line.rstrip("\n"),
                            }) + "\n"
                        )
                        continue

                    errors = validate_event(event)

                    if errors:
                        counts["invalid"] += 1
                        quarantine_output.write(
                            json.dumps({
                                "source_file": bronze_file.name,
                                "source_line": line_number,
                                "errors": errors,
                                "record": event,
                            }, ensure_ascii=False) + "\n"
                        )
                        continue

                    event_id = event["event_id"]

                    if event_id in seen_event_ids:
                        counts["duplicates"] += 1
                        quarantine_output.write(
                            json.dumps({
                                "source_file": bronze_file.name,
                                "source_line": line_number,
                                "errors": ["Duplicate event_id"],
                                "record": event,
                            }, ensure_ascii=False) + "\n"
                        )
                        continue

                    seen_event_ids.add(event_id)

                    # Add Silver processing metadata.
                    event["_silver_processed_at"] = datetime.now(
                        timezone.utc
                    ).isoformat()

                    silver_output.write(
                        json.dumps(event, ensure_ascii=False) + "\n"
                    )

                    counts["valid"] += 1
                    counts[f"event_type:{event['event_type']}"] += 1

    summary = {
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "bronze_files_processed": len(bronze_files),
        "processed_records": counts["processed"],
        "valid_records": counts["valid"],
        "invalid_records": counts["invalid"],
        "duplicate_records": counts["duplicates"],
        "valid_event_type_counts": {
            key.removeprefix("event_type:"): value
            for key, value in counts.items()
            if key.startswith("event_type:")
        },
    }

    with summary_file.open("w", encoding="utf-8") as output_file:
        json.dump(summary, output_file, indent=2)

    logger.info("Silver pipeline completed")
    logger.info("Processed: %s", counts["processed"])
    logger.info("Valid: %s", counts["valid"])
    logger.info("Invalid: %s", counts["invalid"])
    logger.info("Duplicates: %s", counts["duplicates"])
    logger.info("Silver output: %s", silver_file)
    logger.info("Quarantine output: %s", quarantine_file)
    logger.info("Quality summary: %s", summary_file)


if __name__ == "__main__":
    main()
