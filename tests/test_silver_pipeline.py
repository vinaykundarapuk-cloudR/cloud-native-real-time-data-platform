import importlib.util
import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

PIPELINE_PATH = (
    PROJECT_ROOT
    / "data-processing"
    / "silver"
    / "clean_events.py"
)

spec = importlib.util.spec_from_file_location(
    "clean_events",
    PIPELINE_PATH,
)

clean_events = importlib.util.module_from_spec(spec)
sys.modules["clean_events"] = clean_events
spec.loader.exec_module(clean_events)

validate_event = clean_events.validate_event


def make_valid_event():
    return {
        "event_id": "event-001",
        "event_type": "PAYMENT_COMPLETED",
        "event_timestamp": "2026-10-12T12:00:00+00:00",
        "customer_id": "CUST-001",
        "payment_id": "PAY-001",
        "order_id": "ORD-001",
        "payment_method": "CREDIT_CARD",
        "payment_status": "SUCCESS",
        "amount": 99.99,
    }


class TestSilverValidation(unittest.TestCase):

    def test_valid_payment_event(self):
        event = make_valid_event()
        self.assertEqual(validate_event(event), [])

    def test_missing_event_id(self):
        event = make_valid_event()
        del event["event_id"]

        errors = validate_event(event)

        self.assertTrue(
            any("event_id" in error for error in errors)
        )

    def test_unsupported_event_type(self):
        event = make_valid_event()
        event["event_type"] = "UNKNOWN_EVENT"

        errors = validate_event(event)

        self.assertTrue(
            any("Unsupported event_type" in error for error in errors)
        )

    def test_invalid_timestamp(self):
        event = make_valid_event()
        event["event_timestamp"] = "not-a-timestamp"

        errors = validate_event(event)

        self.assertIn("Invalid event_timestamp", errors)

    def test_negative_payment_amount(self):
        event = make_valid_event()
        event["amount"] = -10

        errors = validate_event(event)

        self.assertIn(
            "Payment amount must be a positive number",
            errors,
        )

    def test_missing_payment_id(self):
        event = make_valid_event()
        del event["payment_id"]

        errors = validate_event(event)

        self.assertIn("Missing or invalid payment_id", errors)

    def test_non_object_event(self):
        errors = validate_event("not a JSON object")

        self.assertIn("Event must be a JSON object", errors)


if __name__ == "__main__":
    unittest.main(verbosity=2)
