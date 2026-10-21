"""Transportgarantien ohne Netzwerk; echte Brokerprüfungen ergänzen diese Tests."""

import json
import unittest
from unittest.mock import Mock, call, patch

import pika

from iot_analysis.config import Settings
from iot_analysis.messaging import close_quietly, connect, publish_json, with_retry


class RetryTests(unittest.TestCase):
    def test_permanent_network_failure_has_exactly_five_retries(self):
        failure = OSError("network disconnected")
        operation = Mock(side_effect=failure)
        sleep, notify = Mock(), Mock()
        with self.assertRaises(OSError) as caught:
            with_retry(operation, sleep=sleep, on_retry=notify)
        self.assertIs(caught.exception, failure)
        self.assertEqual(operation.call_count, 6)
        self.assertEqual(sleep.call_args_list, [call(1), call(2), call(4), call(8), call(10)])
        self.assertEqual([item.args[:2] for item in notify.call_args_list],
                         [(1, 1), (2, 2), (3, 4), (4, 8), (5, 10)])

    def test_initial_success_needs_no_wait(self):
        operation, sleep = Mock(return_value="ok"), Mock()
        self.assertEqual(with_retry(operation, sleep=sleep), "ok")
        operation.assert_called_once_with()
        sleep.assert_not_called()

    def test_success_on_last_allowed_attempt_is_returned(self):
        operation = Mock(side_effect=[pika.exceptions.AMQPConnectionError()] * 5 + ["sent"])
        sleep = Mock()
        self.assertEqual(with_retry(operation, sleep=sleep, on_retry=Mock()), "sent")
        self.assertEqual(operation.call_count, 6)
        self.assertEqual(sleep.call_count, 5)

    def test_contract_and_routing_errors_are_not_retried(self):
        for error in (ValueError("bad payload"), pika.exceptions.UnroutableError([])):
            operation, sleep = Mock(side_effect=error), Mock()
            with self.subTest(error=type(error).__name__), self.assertRaises(type(error)):
                with_retry(operation, sleep=sleep)
            operation.assert_called_once_with()
            sleep.assert_not_called()


class PublicationTests(unittest.TestCase):
    def test_publication_requires_route_and_persists_utf8_json(self):
        channel = Mock()
        payload = {"message_id": "m1", "site": "Werk Süd", "value": 105.0}
        publish_json(channel, "sensors", "sensors.temperature.s1", payload, priority=9)
        channel.basic_publish.assert_called_once()
        sent = channel.basic_publish.call_args.kwargs
        self.assertEqual(sent["exchange"], "sensors")
        self.assertEqual(sent["routing_key"], "sensors.temperature.s1")
        self.assertTrue(sent["mandatory"])
        self.assertEqual(json.loads(sent["body"]), payload)
        self.assertEqual(sent["properties"].delivery_mode, 2)
        self.assertEqual(sent["properties"].content_type, "application/json")
        self.assertEqual(sent["properties"].content_encoding, "utf-8")
        self.assertEqual(sent["properties"].message_id, "m1")
        self.assertEqual(sent["properties"].priority, 9)

    def test_explicit_id_and_event_id_are_supported(self):
        channel = Mock()
        publish_json(channel, "telemetry", "", {"event_id": "e1"})
        self.assertEqual(channel.basic_publish.call_args.kwargs["properties"].message_id, "e1")
        publish_json(channel, "telemetry", "", {"message_id": "old"}, message_id="override")
        self.assertEqual(channel.basic_publish.call_args.kwargs["properties"].message_id, "override")

    def test_broker_failure_is_propagated_without_false_success(self):
        channel = Mock()
        channel.basic_publish.side_effect = pika.exceptions.UnroutableError([])
        with self.assertRaises(pika.exceptions.UnroutableError):
            publish_json(channel, "sensors", "missing", {"value": 1})

    @patch("iot_analysis.messaging.pika.BlockingConnection")
    def test_connection_has_bounded_timeouts_and_no_hidden_pika_retries(self, blocking_connection):
        settings = Settings()
        self.assertIs(connect(settings), blocking_connection.return_value)
        params = blocking_connection.call_args.args[0]
        self.assertEqual(params.connection_attempts, 1)
        self.assertGreater(params.heartbeat, 0)
        self.assertGreater(params.socket_timeout, 0)
        self.assertGreater(params.blocked_connection_timeout, 0)

    def test_cleanup_tolerates_connection_loss(self):
        close_quietly(None)
        connection = Mock(is_open=True)
        connection.close.side_effect = pika.exceptions.AMQPConnectionError()
        close_quietly(connection)
        connection.close.assert_called_once_with()
        closed = Mock(is_open=False)
        close_quietly(closed)
        closed.close.assert_not_called()


if __name__ == "__main__":
    unittest.main()
