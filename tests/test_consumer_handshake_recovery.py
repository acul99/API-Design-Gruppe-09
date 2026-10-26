"""Pikas vermutete Handshake-Fehler koennen durch Transportabbrueche entstehen."""

from contextlib import redirect_stdout
from io import StringIO
import json
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import pika
from pika.adapters.select_connection import SelectConnection

import alarm
import common


class ConsumerHandshakeRecoveryTests(unittest.TestCase):
    stages = (
        (SelectConnection.CONNECTION_PROTOCOL, pika.exceptions.IncompatibleProtocolError),
        (SelectConnection.CONNECTION_START, pika.exceptions.ProbableAuthenticationError),
        (SelectConnection.CONNECTION_TUNE, pika.exceptions.ProbableAccessDeniedError),
    )

    def pika_handshake_error(self, state):
        # Use the pinned Pika implementation's real error classification. Only
        # transport cleanup is mocked, so this needs no broker or network socket.
        connection = SelectConnection.__new__(SelectConnection)
        connection._opened = False
        connection._error = None
        connection.connection_state = state
        connection.params = SimpleNamespace(blocked_connection_timeout=None)
        connection._channels = {}
        connection._remove_heartbeat = Mock()
        connection._remove_callbacks = Mock()
        connection._set_connection_state = Mock()
        connection._init_connection_state = Mock()
        connection.callbacks = Mock()
        transport_error = pika.exceptions.StreamLostError("connection reset during handshake")

        with patch("pika.connection.LOGGER"):
            connection._on_stream_terminated(transport_error)

        connection.callbacks.process.assert_called_once()
        return connection.callbacks.process.call_args.args[-1], transport_error

    def test_transport_loss_becomes_a_heuristic_handshake_error(self):
        for state, expected_type in self.stages:
            with self.subTest(state=state):
                error, transport_error = self.pika_handshake_error(state)
                self.assertIs(type(error), expected_type)
                # Pika exposes the original cause only as text, not a typed cause.
                self.assertEqual(error.args, (repr(transport_error),))

    def test_consumer_reconnects_and_processes_alarm_after_interrupted_handshake(self):
        for state, expected_type in self.stages:
            with self.subTest(error=expected_type.__name__):
                error, _ = self.pika_handshake_error(state)
                connection, channel = Mock(is_open=True), Mock()
                message = json.dumps({"level": "WARNING", "sensor_id": "t1", "value": 90}).encode()

                def deliveries():
                    yield (SimpleNamespace(delivery_tag=7),
                           pika.BasicProperties(message_id="after-reconnect", priority=3), message)
                    # Stop the permanent service after successful processing.
                    raise KeyboardInterrupt

                channel.consume.return_value = deliveries()
                with patch("alarm.open_channel", side_effect=[error, (connection, channel)]) as opened, \
                     patch("common.time.monotonic", return_value=0), \
                     patch("common.time.sleep") as sleep, redirect_stdout(StringIO()):
                    with self.assertRaises(KeyboardInterrupt):
                        common.run_consumer(alarm.main)

                self.assertEqual(opened.call_count, 2)
                sleep.assert_called_once_with(1)
                channel.basic_qos.assert_called_once_with(prefetch_count=1)
                channel.basic_ack.assert_called_once_with(delivery_tag=7)
                channel.basic_nack.assert_not_called()
                connection.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
