import json
import random
import uuid
from datetime import datetime, timezone

from faker import Faker

from config import EVENT_TYPES, PRODUCT_CATEGORIES, US_LOCATIONS


fake = Faker()


def generate_event():
    """Generate one realistic e-commerce event."""

    event_type = random.choice(EVENT_TYPES)
    city, state = random.choice(US_LOCATIONS)

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "product_id": f"PROD-{random.randint(100, 999)}",
        "product_category": random.choice(PRODUCT_CATEGORIES),
        "quantity": random.randint(1, 5),
        "price": round(random.uniform(10, 1000), 2),
        "customer_name": fake.name(),
        "customer_email": fake.email(),
        "city": city,
        "state": state,
    }

    return event


def main():
    """Generate and display sample events."""

    print("Starting e-commerce event generator...")
    print("Press Ctrl+C to stop.\n")

    for _ in range(10):
        event = generate_event()
        print(json.dumps(event, indent=2))


if __name__ == "__main__":
    main()
