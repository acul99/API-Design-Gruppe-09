"""Fehlerhafte Nutzdaten ablehnen, ohne nachfolgende Messungen zu blockieren."""

from contextlib import redirect_stdout
from io import StringIO
import json
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import pika

import analysis
import common


class PayloadValidationTests(unittest.TestCase):
    def sensor(self, **changes):
        return {"sensor_id": "h1", "timestamp": "2026-10-26T12:00:00Z",
                "type": "humidity", "value": 90, "message_id": "measurement", **changes}

    def test_valid_ids_and_extra_fields_survive_forwarding(self):
        extra = {"site": "Halle 1", "samples": [1.5, {"active": True}], "note": None}
        for message_id in ("a" * 255, "ä" * 127 + "a"):
            with self.subTest(bytes=len(message_id.encode("utf-8"))):
                sensor = self.sensor(message_id=message_id, extra=extra)
                parsed = common.parse_sensor(json.dumps(sensor).encode(), "sensors.humidity.h1")
                self.assertEqual(parsed, sensor)
                channel = Mock()
                channel.basic_publish.side_effect = lambda **kwargs: kwargs["properties"].encode()
                common.publish(channel, "CRITICAL", parsed, priority=9)
                self.assertEqual(json.loads(channel.basic_publish.call_args.kwargs["body"]), sensor)

    def test_bad_payloads_are_rejected_and_the_next_message_is_processed(self):
        bad_bodies = [
            json.dumps(self.sensor(message_id=value)).encode()
            for value in ("a" * 256, "ä" * 128, "", None, 42, "\ud800")
        ]
        bad_bodies += [
            json.dumps(self.sensor(extra={"nested": [value]})).encode()
            for value in (float("nan"), float("inf"), float("-inf"))
        ]
        # A JSON number can overflow during decoding without using an Infinity token.
        bad_bodies.append(json.dumps(self.sensor(extra="overflow")).encode().replace(
            b'"overflow"', b'{"nested": [1e999]}'))
        bad_bodies.append(b"[" * 2000 + b"0" + b"]" * 2000)

        for body in bad_bodies:
            with self.subTest(body=body[:100]):
                connection, channel = Mock(), Mock()
                published = []

                def publish(**kwargs):
                    # Exercise Pika's real AMQP property encoder, including its shortstr limit.
                    kwargs["properties"].encode()
                    published.append((kwargs["routing_key"], json.loads(kwargs["body"])))

                channel.basic_publish.side_effect = publish
                channel.consume.return_value = iter([
                    (SimpleNamespace(delivery_tag=1, routing_key="sensors.humidity.h1", redelivered=False),
                     pika.BasicProperties(message_id="bad-delivery"), body),
                    (SimpleNamespace(delivery_tag=2, routing_key="sensors.humidity.h1", redelivered=False),
                     pika.BasicProperties(message_id="good-delivery"),
                     json.dumps(self.sensor(message_id="good", value=50)).encode()),
                ])

                with patch("analysis.open_channel", return_value=(connection, channel)), \
                     patch("analysis.time.monotonic", return_value=0), redirect_stdout(StringIO()):
                    analysis.main("environment")

                channel.basic_nack.assert_called_once_with(delivery_tag=1, requeue=False)
                channel.basic_ack.assert_called_once_with(delivery_tag=2)
                self.assertEqual([key for key, _ in published], ["monitor", "monitor"])
                self.assertEqual([(metric["status"], metric["message_id"]) for _, metric in published],
                                 [("failed", "bad-delivery"), ("processed", "good")])


if __name__ == "__main__":
    unittest.main()
