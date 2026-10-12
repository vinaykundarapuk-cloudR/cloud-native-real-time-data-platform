import csv
import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

GOLD_DIR = PROJECT_ROOT / "data-lake" / "gold"
CSV_DIR = PROJECT_ROOT / "analytics" / "powerbi"


def latest_json(pattern):
    files = sorted(
        GOLD_DIR.glob(pattern),
        key=lambda path: path.stat().st_mtime,
    )

    if not files:
        raise FileNotFoundError(
            f"No files found for pattern: {pattern}"
        )

    with files[-1].open("r", encoding="utf-8") as file:
        return json.load(file)


def write_csv(path, headers, rows):
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        writer.writerows(rows)


def main():
    CSV_DIR.mkdir(parents=True, exist_ok=True)

    events = latest_json("event_analytics_*.json")
    payments = latest_json("payment_analytics_*.json")
    orders = latest_json("order_analytics_*.json")
    geography = latest_json("geographic_analytics_*.json")

    # Event counts
    write_csv(
        CSV_DIR / "event_analytics.csv",
        ["event_type", "event_count"],
        [
            [event_type, count]
            for event_type, count
            in events["events_by_type"].items()
        ],
    )

    # Payment summary
    write_csv(
        CSV_DIR / "payment_analytics.csv",
        ["metric", "value"],
        [
            ["payment_events", payments["payment_events"]],
            ["successful_payments", payments["successful_payments"]],
            ["failed_payments", payments["failed_payments"]],
            [
                "successful_payment_amount",
                payments["successful_payment_amount"],
            ],
            [
                "failed_payment_amount",
                payments["failed_payment_amount"],
            ],
        ],
    )

    # Order summary
    write_csv(
        CSV_DIR / "order_analytics.csv",
        ["metric", "value"],
        [
            ["total_orders", orders["total_orders"]],
            ["total_order_value", orders["total_order_value"]],
            ["average_order_value", orders["average_order_value"]],
        ],
    )

    # Geographic summary
    write_csv(
        CSV_DIR / "geographic_analytics.csv",
        ["state", "event_count"],
        [
            [state, count]
            for state, count
            in geography["events_by_state"].items()
        ],
    )

    print("Gold analytics exported to CSV successfully.")

    for path in sorted(CSV_DIR.glob("*.csv")):
        print(f"Created: {path}")


if __name__ == "__main__":
    main()
