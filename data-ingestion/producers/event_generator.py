import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer

from config import EVENT_TYPES, PRODUCT_CATEGORIES, US_LOCATIONS

fake = Faker()


def generate_event():
    """Generate one e-commerce event with event-specific fields."""

    event_type = random.choice(EVENT_TYPES)
    city, state = random.choice(US_LOCATIONS)

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "city": city,
        "state": state,
    }

    if event_type == "CUSTOMER_CREATED":
        event.update({
            "customer_name": fake.name(),
            "customer_email": fake.email(),
        })

    elif event_type == "PRODUCT_VIEWED":
        event.update({
            "product_id": f"PROD-{random.randint(100, 999)}",
            "product_category": random.choice(PRODUCT_CATEGORIES),
        })

    elif event_type == "ADD_TO_CART":
        event.update({
            "product_id": f"PROD-{random.randint(100, 999)}",
            "product_category": random.choice(PRODUCT_CATEGORIES),
            "quantity": random.randint(1, 5),
        })

    elif event_type == "ORDER_CREATED":
        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(10, 1000), 2)

        event.update({
            "order_id": f"ORD-{uuid.uuid4().hex[:8].upper()}",
            "product_id": f"PROD-{random.randint(100, 999)}",
            "product_category": random.choice(PRODUCT_CATEGORIES),
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": round(quantity * unit_price, 2),
        })

    elif event_type == "PAYMENT_COMPLETED":
        event.update({
            "payment_id": f"PAY-{uuid.uuid4().hex[:8].upper()}",
            "order_id": f"ORD-{uuid.uuid4().hex[:8].upper()}",
            "payment_method": random.choice([
                "CREDIT_CARD",
                "DEBIT_CARD",
                "PAYPAL",
            ]),
            "payment_status": "SUCCESS",
            "amount": round(random.uniform(10, 5000), 2),
        })

    return event


def main():
    """Generate events and optionally publish them to Kafka."""

    parser = argparse.ArgumentParser(
        description="Generate sample e-commerce events."
    )
    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of events to generate. Use 0 for continuous generation.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Seconds to wait between events.",
    )
    parser.add_argument(
        "--kafka",
        action="store_true",
        help="Publish generated events to Kafka instead of printing them.",
    )
    parser.add_argument(
        "--topic",
        default="ecommerce-events",
        help="Kafka topic name.",
    )
    parser.add_argument(
        "--bootstrap-server",
        default="localhost:9092",
        help="Kafka bootstrap server address.",
    )

    args = parser.parse_args()

    if args.count < 0:
        parser.error("--count must be zero or greater.")

    if args.interval < 0:
        parser.error("--interval must be zero or greater.")

    producer = None

    if args.kafka:
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_server,
            value_serializer=lambda event: json.dumps(event).encode("utf-8"),
            acks="all",
            retries=5,
        )

    print("Starting e-commerce event generator...")
    print("Mode:", "Kafka" if args.kafka else "Terminal")
    print("Press Ctrl+C to stop.\n")

    generated = 0

    try:
        while args.count == 0 or generated < args.count:
            event = generate_event()

            if producer:
                metadata = producer.send(args.topic, value=event).get(
                    timeout=10
                )
                print(
                    f"Published {event['event_type']} | "
                    f"topic={metadata.topic} | "
                    f"partition={metadata.partition} | "
                    f"event_id={event['event_id']}",
                    flush=True,
                )
            else:
                print(json.dumps(event), flush=True)

            generated += 1

            if args.count == 0 or generated < args.count:
                time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\nGenerator stopped.", flush=True)

    finally:
        if producer:
            producer.flush()
            producer.close()

        print(f"Events generated: {generated}", flush=True)


if __name__ == "__main__":
    main()
