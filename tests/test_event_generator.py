import sys
import unittest
from pathlib import Path

# Add the event producer directory to Python's import path.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PRODUCER_DIR = PROJECT_ROOT / "data-ingestion" / "producers"
sys.path.insert(0, str(PRODUCER_DIR))

from event_generator import generate_event


class TestEventGenerator(unittest.TestCase):

    def test_event_has_required_common_fields(self):
        event = generate_event()

        required_fields = {
            "event_id",
            "event_type",
            "event_timestamp",
            "customer_id",
            "city",
            "state",
        }

        self.assertTrue(required_fields.issubset(event.keys()))

    def test_event_id_is_unique(self):
        event_ids = {
            generate_event()["event_id"]
            for _ in range(100)
        }

        self.assertEqual(len(event_ids), 100)

    def test_customer_created_has_customer_details(self):
        event = generate_event()

        # Generate events until we get a CUSTOMER_CREATED event.
        for _ in range(500):
            if event["event_type"] == "CUSTOMER_CREATED":
                break
            event = generate_event()

        self.assertEqual(event["event_type"], "CUSTOMER_CREATED")
        self.assertIn("customer_name", event)
        self.assertIn("customer_email", event)

    def test_order_total_is_correct(self):
        # Generate events until we get an ORDER_CREATED event.
        for _ in range(500):
            event = generate_event()

            if event["event_type"] == "ORDER_CREATED":
                break
        else:
            self.fail("No ORDER_CREATED event generated in 500 attempts")

        expected_total = round(
            event["quantity"] * event["unit_price"], 2
        )

        self.assertEqual(event["total_amount"], expected_total)
        self.assertGreater(event["quantity"], 0)
        self.assertGreater(event["unit_price"], 0)

    def test_payment_has_required_fields(self):
        # Generate events until we get a PAYMENT_COMPLETED event.
        for _ in range(500):
            event = generate_event()

            if event["event_type"] == "PAYMENT_COMPLETED":
                break
        else:
            self.fail("No PAYMENT_COMPLETED event generated in 500 attempts")

        self.assertIn("payment_id", event)
        self.assertIn("order_id", event)
        self.assertIn("payment_method", event)
        self.assertEqual(event["payment_status"], "SUCCESS")


if __name__ == "__main__":
    unittest.main()