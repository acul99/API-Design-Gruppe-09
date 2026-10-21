"""Fachliche Grenzwerte und fehlerhafte externe Sensornachrichten."""

import hashlib
import json
import unittest
from datetime import datetime

from iot_analysis.models import alarm_for, encode_json, make_event, parse_sensor


class SensorContractTests(unittest.TestCase):
    def test_huge_json_integer_is_rejected_as_contract_error(self):
        payload = {"sensor_id": "temp-01", "timestamp": "2026-10-21T10:00:00Z",
                   "type": "temperature", "value": 10 ** 400}
        with self.assertRaises(ValueError):
            parse_sensor(encode_json(payload), "sensors.temperature.temp-01")

    def sensor(self, **changes):
        return {"sensor_id": "temperature-01", "timestamp": "2026-10-21T10:00:00Z",
                "type": "temperature", "value": 23.5, **changes}

    def parse(self, payload):
        return parse_sensor(encode_json(payload), f"sensors.{payload.get('type')}.{payload.get('sensor_id')}")

    def test_required_assignment_contract_needs_no_extra_id_field(self):
        payload = self.sensor()
        body = encode_json(payload)
        parsed = self.parse(payload)
        self.assertEqual(parsed["value"], 23.5)
        self.assertEqual(parsed["message_id"], hashlib.sha256(body).hexdigest())
        self.assertEqual(self.parse(payload)["message_id"], parsed["message_id"])

    def test_optional_contract_extensions_are_preserved(self):
        parsed = self.parse(self.sensor(message_id="m-1", location="Werk Süd"))
        self.assertEqual(parsed["message_id"], "m-1")
        self.assertEqual(parsed["location"], "Werk Süd")

    def test_invalid_json_encoding_and_non_objects_are_rejected(self):
        for body in (b"{bad", b"\xff", b"[]", b"null", b"42", b'"hello"'):
            with self.subTest(body=body), self.assertRaises(ValueError):
                parse_sensor(body, "sensors.temperature.temperature-01")

    def test_all_assignment_fields_are_required(self):
        for field in ("sensor_id", "timestamp", "type", "value"):
            payload = self.sensor()
            del payload[field]
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.parse(payload)

    def test_sensor_id_cannot_change_topic_segmentation(self):
        for sensor_id in ("", "room.1", "*", "#", "sensor 1", "x" * 65, None, 1):
            with self.subTest(sensor_id=sensor_id), self.assertRaises(ValueError):
                self.parse(self.sensor(sensor_id=sensor_id))

    def test_payload_cannot_claim_different_type_than_routing_key(self):
        with self.assertRaises(ValueError):
            parse_sensor(encode_json(self.sensor()), "sensors.humidity.temperature-01")

    def test_invalid_values_and_unknown_types_are_rejected(self):
        for value in (True, False, "23", None, [], {}):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.parse(self.sensor(value=value))
        with self.assertRaises(ValueError):
            self.parse(self.sensor(type="pressure"))

    def test_nonfinite_json_numbers_are_rejected(self):
        # json.loads akzeptiert NaN/Infinity; unser Nachrichtenvertrag nicht.
        for token in ("NaN", "Infinity", "-Infinity"):
            body = json.dumps(self.sensor()).replace("23.5", token).encode()
            with self.subTest(token=token), self.assertRaises(ValueError):
                parse_sensor(body, "sensors.temperature.temperature-01")

    def test_physical_ranges_have_inclusive_valid_boundaries(self):
        for kind, value in (("humidity", 0), ("humidity", 100), ("vibration", 0)):
            with self.subTest(kind=kind, value=value):
                self.assertEqual(self.parse(self.sensor(type=kind, value=value))["value"], value)
        for kind, value in (("humidity", -0.1), ("humidity", 100.1), ("vibration", -0.1)):
            with self.subTest(kind=kind, value=value), self.assertRaises(ValueError):
                self.parse(self.sensor(type=kind, value=value))

    def test_timestamp_requires_explicit_timezone(self):
        for timestamp in ("2026-10-21T10:00:00", "yesterday", None, 42, "2026-02-30T10:00:00Z"):
            with self.subTest(timestamp=timestamp), self.assertRaises(ValueError):
                self.parse(self.sensor(timestamp=timestamp))
        self.parse(self.sensor(timestamp="2026-10-21T12:00:00+02:00"))

    def test_optional_message_id_must_be_nonempty_string(self):
        for message_id in ("", None, 42):
            with self.subTest(message_id=message_id), self.assertRaises(ValueError):
                self.parse(self.sensor(message_id=message_id))

    def test_json_encoder_preserves_utf8_and_rejects_nonfinite_values(self):
        self.assertIn("Süd".encode(), encode_json({"site": "Süd"}))
        for value in (float("nan"), float("inf"), -float("inf")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                encode_json({"value": value})


class AlarmRuleTests(unittest.TestCase):
    def test_all_rule_boundaries(self):
        cases = (
            ("temperature", 80, None), ("temperature", 80.01, "WARNING"),
            ("temperature", 100, "WARNING"), ("temperature", 100.01, "CRITICAL"),
            ("humidity", 14.99, "CRITICAL"), ("humidity", 15, "WARNING"),
            ("humidity", 29.99, "WARNING"), ("humidity", 30, None),
            ("humidity", 70, None), ("humidity", 70.01, "WARNING"),
            ("humidity", 85, "WARNING"), ("humidity", 85.01, "CRITICAL"),
            ("vibration", 5, None), ("vibration", 5.01, "WARNING"),
            ("vibration", 10, "WARNING"), ("vibration", 10.01, "CRITICAL"),
        )
        for kind, value, level in cases:
            with self.subTest(kind=kind, value=value):
                alarm = alarm_for({"type": kind, "value": value, "message_id": "m1", "sensor_id": "s1"})
                self.assertEqual(alarm["level"] if alarm else None, level)
                if alarm:
                    self.assertEqual(alarm["priority"], 9 if level == "CRITICAL" else 3)

    def test_alarm_has_stable_identity_and_source_reference(self):
        sensor = {"type": "temperature", "value": 110, "message_id": "m1", "sensor_id": "s1"}
        first, second = alarm_for(sensor), alarm_for(sensor)
        self.assertEqual(first["alarm_id"], second["alarm_id"])
        self.assertEqual(first["message_id"], first["alarm_id"])
        self.assertEqual(first["sensor_message_id"], "m1")
        self.assertIsNotNone(datetime.fromisoformat(first["timestamp"]).tzinfo)

    def test_telemetry_events_are_individually_identifiable(self):
        first = make_event("sensor_processed", "environment", "m1", processing_ms=1.25)
        second = make_event("sensor_processed", "environment", "m1")
        self.assertNotEqual(first["event_id"], second["event_id"])
        self.assertEqual(first["message_id"], "m1")
        self.assertEqual(first["processing_ms"], 1.25)


if __name__ == "__main__":
    unittest.main()
