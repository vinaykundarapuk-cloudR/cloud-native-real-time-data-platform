import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_DIR = PROJECT_ROOT / "data-lake" / "silver"
GOLD_DIR = PROJECT_ROOT / "data-lake" / "gold"


def read_silver_events():
    """Load cleaned events from Silver JSONL files."""

    events = []

    for file_path in sorted(SILVER_DIR.glob("clean_events_*.jsonl")):
        with file_path.open("r", encoding="utf-8") as file:
            for line in file:
                if line.strip():
                    events.append(json.loads(line))

    return events


def write_json(file_path, data):
    """Write an analytics dataset as formatted JSON."""

    with file_path.open("w", encoding="utf-8") as file:
        json.dump(data, file, indent=2, ensure_ascii=False)


def main():
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    events = read_silver_events()

    if not events:
        print("No Silver events found. Run the Silver pipeline first.")
        return

    run_timestamp = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%SZ"
    )

    # 1. Event analytics
    event_counts = Counter(
        event["event_type"] for event in events
    )

    event_analytics = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_events": len(events),
        "events_by_type": dict(sorted(event_counts.items())),
    }

    write_json(
        GOLD_DIR / f"event_analytics_{run_timestamp}.json",
        event_analytics,
    )

    # 2. Payment analytics
    payments = [
        event
        for event in events
        if event.get("event_type") == "PAYMENT_COMPLETED"
    ]

    payment_status_counts = Counter(
        event.get("payment_status", "UNKNOWN")
        for event in payments
    )

    successful_payments = [
        event
        for event in payments
        if event.get("payment_status") == "SUCCESS"
    ]

    failed_payments = [
        event
        for event in payments
        if event.get("payment_status") == "FAILED"
    ]

    payment_analytics = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "payment_events": len(payments),
        "successful_payments": len(successful_payments),
        "failed_payments": len(failed_payments),
        "payment_status_counts": dict(
            sorted(payment_status_counts.items())
        ),
        "successful_payment_amount": round(
            sum(event["amount"] for event in successful_payments), 2
        ),
        "failed_payment_amount": round(
            sum(event["amount"] for event in failed_payments), 2
        ),
    }

    write_json(
        GOLD_DIR / f"payment_analytics_{run_timestamp}.json",
        payment_analytics,
    )

    # 3. Order analytics
    orders = [
        event
        for event in events
        if event.get("event_type") == "ORDER_CREATED"
    ]

    order_analytics = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_orders": len(orders),
        "total_order_value": round(
            sum(event.get("total_amount", 0) for event in orders), 2
        ),
        "average_order_value": round(
            sum(event.get("total_amount", 0) for event in orders)
            / len(orders),
            2,
        ) if orders else 0,
    }

    write_json(
        GOLD_DIR / f"order_analytics_{run_timestamp}.json",
        order_analytics,
    )

    # 4. Event analytics by US state
    events_by_state = defaultdict(int)

    for event in events:
        state = event.get("state")

        if state:
            events_by_state[state] += 1

    geographic_analytics = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "events_by_state": dict(
            sorted(events_by_state.items())
        ),
    }

    write_json(
        GOLD_DIR / f"geographic_analytics_{run_timestamp}.json",
        geographic_analytics,
    )

    print("\nGold analytics pipeline completed successfully!")
    print(f"Silver events analyzed: {len(events)}")
    print(f"Order events: {len(orders)}")
    print(f"Payment events: {len(payments)}")
    print(f"Gold output directory: {GOLD_DIR}")


if __name__ == "__main__":
    main()
