"""Regressionstests für begrenzten Wiederanlauf und sichere Eingangs-ACKs."""

from types import SimpleNamespace
import unittest
from unittest.mock import Mock, call, patch

import pika

from iot_analysis.config import Settings
from iot_analysis.consumers import _with_reconnect, run_analysis
from iot_analysis.models import encode_json


def one_delivery(body, *, redelivered=False):
    channel = Mock()
    method = SimpleNamespace(delivery_tag=1, redelivered=redelivered,
                             routing_key="sensors.humidity.humidity-01")
    properties = SimpleNamespace(message_id="m1", headers=None)
    channel.consume.return_value = iter([(method, properties, body)])
    connection = Mock(is_open=True)
    connection.channel.return_value = channel
    return connection, channel


class ReconnectLimitTests(unittest.TestCase):
    @patch("iot_analysis.consumers.time.sleep")
    @patch("iot_analysis.consumers.close_quietly")
    @patch("iot_analysis.consumers.retry_connect")
    def test_exhausted_connect_retry_is_not_multiplied_by_session_retries(self, connect, close, sleep):
        failure = pika.exceptions.AMQPConnectionError()
        connect.side_effect = failure
        session = Mock()
        with self.assertRaises(pika.exceptions.AMQPConnectionError) as caught:
            _with_reconnect(Settings(), "environment", session)
        self.assertIs(caught.exception, failure)
        connect.assert_called_once_with(Settings())
        session.assert_not_called()
        close.assert_not_called()
        sleep.assert_not_called()

    @patch("iot_analysis.consumers.emit_log")
    @patch("iot_analysis.consumers.time.sleep")
    @patch("iot_analysis.consumers.close_quietly")
    @patch("iot_analysis.consumers.retry_connect")
    def test_connect_exhaustion_after_session_loss_also_propagates(self, connect, close, sleep, _log):
        first = Mock()
        connect.side_effect = [first, pika.exceptions.AMQPConnectionError()]
        session = Mock(side_effect=pika.exceptions.StreamLostError("disconnected"))
        with self.assertRaises(pika.exceptions.AMQPConnectionError):
            _with_reconnect(Settings(), "environment", session)
        self.assertEqual(connect.call_count, 2)
        session.assert_called_once_with(first)
        close.assert_called_once_with(first)
        sleep.assert_called_once_with(1)

    @patch("iot_analysis.consumers.emit_log")
    @patch("iot_analysis.consumers.time.sleep")
    @patch("iot_analysis.consumers.close_quietly")
    @patch("iot_analysis.consumers.retry_connect")
    def test_repeated_session_failures_have_only_five_restarts(self, connect, close, sleep, _log):
        connections = [Mock() for _ in range(6)]
        connect.side_effect = connections
        session = Mock(side_effect=pika.exceptions.NackError([]))
        with self.assertRaises(pika.exceptions.NackError):
            _with_reconnect(Settings(), "environment", session)
        self.assertEqual(connect.call_count, 6)
        self.assertEqual(session.call_count, 6)
        self.assertEqual(close.call_args_list, [call(connection) for connection in connections])
        self.assertEqual(sleep.call_args_list, [call(1), call(2), call(4), call(8), call(10)])


class DeliverySafetyTests(unittest.TestCase):
    def valid_humidity(self):
        return encode_json({"sensor_id": "humidity-01", "timestamp": "2026-10-21T10:00:00Z",
                            "type": "humidity", "value": 50})

    @patch("iot_analysis.consumers.emit_log")
    @patch("iot_analysis.consumers.time.sleep")
    @patch("iot_analysis.consumers.declare_topology")
    def test_publish_nack_preserves_input_for_redelivery_instead_of_rejecting_it(self, _declare, sleep, _log):
        first_connection, first_channel = one_delivery(self.valid_humidity())
        second_connection, second_channel = one_delivery(self.valid_humidity(), redelivered=True)

        def publication(channel, *args, **kwargs):
            # Weder vor fehlgeschlagener noch vor erfolgreicher Veröffentlichung
            # darf die Eingangsmessung bereits bestätigt oder abgelehnt sein.
            channel.basic_ack.assert_not_called()
            channel.basic_nack.assert_not_called()
            if channel is first_channel:
                raise pika.exceptions.NackError([])

        with (
            patch("iot_analysis.consumers.retry_connect", side_effect=[first_connection, second_connection]) as connect,
            patch("iot_analysis.consumers.publish_json", side_effect=publication) as publish,
        ):
            run_analysis(Settings(), "environment", max_messages=1)
        self.assertEqual(connect.call_count, 2)
        self.assertEqual(publish.call_count, 2)
        first_channel.basic_ack.assert_not_called()
        first_channel.basic_nack.assert_not_called()
        second_channel.basic_ack.assert_called_once_with(delivery_tag=1)
        second_channel.basic_nack.assert_not_called()
        first_connection.close.assert_called_once_with()
        second_connection.close.assert_called_once_with()
        sleep.assert_called_once_with(1)

    @patch("iot_analysis.consumers.emit_log")
    @patch("iot_analysis.consumers.declare_topology")
    def test_invalid_input_is_rejected_only_after_failure_telemetry(self, _declare, _log):
        connection, channel = one_delivery(b"{bad json")

        def publication(_channel, _exchange, _routing_key, payload, **kwargs):
            channel.basic_nack.assert_not_called()
            self.assertEqual(payload["kind"], "processing_failed")

        with (
            patch("iot_analysis.consumers.retry_connect", return_value=connection),
            patch("iot_analysis.consumers.publish_json", side_effect=publication) as publish,
        ):
            run_analysis(Settings(), "environment", max_messages=1)
        publish.assert_called_once()
        channel.basic_ack.assert_not_called()
        channel.basic_nack.assert_called_once_with(delivery_tag=1, requeue=False)
        connection.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
