"""Consumer bleiben bei Transportfehlern aktiv und bestaetigen erst nach Erfolg."""

from contextlib import redirect_stdout
from io import StringIO
import json
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, call, patch

import pika
from pika.adapters import blocking_connection

import alarm
import analysis
import common
import monitor


def delivery_then_shutdown(delivery):
    """Nach erfolgreicher Verarbeitung beendet Strg+C den dauerhaften Dienst."""
    yield delivery
    raise KeyboardInterrupt


class ConsumerRetryTests(unittest.TestCase):
    def test_normal_session_end_restarts_worker_with_same_arguments(self):
        worker = Mock(side_effect=[None, KeyboardInterrupt()])
        with patch("common.time.monotonic", return_value=0), \
             patch("common.time.sleep") as sleep, redirect_stdout(StringIO()):
            with self.assertRaises(KeyboardInterrupt):
                common.run_consumer(worker, "environment", 0)
        self.assertEqual(worker.call_args_list, [call("environment", 0), call("environment", 0)])
        sleep.assert_called_once_with(1)

    def test_transport_and_broker_restart_errors_are_retried(self):
        errors = (
            pika.exceptions.AMQPConnectionError("offline"),
            pika.exceptions.StreamLostError("connection reset"),
            pika.exceptions.AMQPHeartbeatTimeout("heartbeat missed"),
            pika.exceptions.ConnectionBlockedTimeout("broker blocked"),
            pika.exceptions.ProbableAuthenticationError("transport lost during authentication"),
            pika.exceptions.ProbableAccessDeniedError("transport lost while opening vhost"),
            pika.exceptions.IncompatibleProtocolError("transport lost during protocol negotiation"),
            pika.exceptions.ConnectionClosedByBroker(320, "broker restarting"),
            pika.exceptions.ConnectionClosedByBroker(541, "internal broker error"),
            pika.exceptions.ChannelClosedByBroker(404, "queue disappeared"),
            pika.exceptions.ChannelClosedByBroker(320, "channel forced closed"),
            pika.exceptions.ChannelClosedByBroker(541, "internal broker error"),
            pika.exceptions.UnroutableError([]),
            OSError("socket unavailable"),
        )
        for error in errors:
            with self.subTest(error=type(error).__name__), \
                 patch("common.time.monotonic", return_value=0), \
                 patch("common.time.sleep") as sleep, redirect_stdout(StringIO()):
                worker = Mock(side_effect=[error, KeyboardInterrupt()])
                with self.assertRaises(KeyboardInterrupt):
                    common.run_consumer(worker)
                self.assertEqual(worker.call_count, 2)
                sleep.assert_called_once_with(1)

    def test_long_outage_keeps_retrying_with_capped_backoff(self):
        worker = Mock(side_effect=[pika.exceptions.AMQPConnectionError("offline")
                                   for _ in range(8)] + [KeyboardInterrupt()])
        with patch("common.time.monotonic", return_value=0), \
             patch("common.time.sleep") as sleep, redirect_stdout(StringIO()):
            with self.assertRaises(KeyboardInterrupt):
                common.run_consumer(worker)
        self.assertEqual(worker.call_count, 9)
        self.assertEqual(sleep.call_args_list, [call(n) for n in (1, 2, 4, 8, 10, 10, 10, 10)])

    def test_stable_session_resets_backoff(self):
        clock = [0]
        session_lengths = iter((0, 0, 60))

        def worker():
            length = next(session_lengths, None)
            if length is None:
                raise KeyboardInterrupt
            clock[0] += length
            raise pika.exceptions.StreamLostError("connection reset")

        def advance(seconds):
            clock[0] += seconds

        with patch("common.time.monotonic", side_effect=lambda: clock[0]), \
             patch("common.time.sleep", side_effect=advance) as sleep, redirect_stdout(StringIO()):
            with self.assertRaises(KeyboardInterrupt):
                common.run_consumer(worker)
        self.assertEqual(sleep.call_args_list, [call(1), call(2), call(1)])

    def test_configuration_programming_and_shutdown_errors_propagate(self):
        errors = (
            pika.exceptions.AuthenticationError("PLAIN"),
            pika.exceptions.ConnectionClosedByBroker(403, "access refused"),
            pika.exceptions.ChannelClosedByBroker(403, "access refused"),
            pika.exceptions.ChannelClosedByBroker(405, "queue locked"),
            pika.exceptions.ChannelClosedByBroker(406, "queue arguments mismatch"),
            pika.exceptions.ConnectionWrongStateError("already closed"),
            pika.exceptions.ChannelWrongStateError("already closed"),
            pika.exceptions.ConnectionClosedByClient(200, "client shutdown"),
            pika.exceptions.NoFreeChannels(),
            ValueError("programming error"),
            KeyboardInterrupt(),
        )
        for error in errors:
            with self.subTest(error=type(error).__name__), \
                 patch("common.time.sleep") as sleep, redirect_stdout(StringIO()):
                worker = Mock(side_effect=[error, AssertionError("Dauerhafter Fehler wurde wiederholt")])
                with self.assertRaises(type(error)) as raised:
                    common.run_consumer(worker)
                self.assertIs(raised.exception, error)
                worker.assert_called_once_with()
                sleep.assert_not_called()


class ConsumerSessionRecoveryTests(unittest.TestCase):
    def test_analysis_does_not_ack_when_metric_publication_loses_connection(self):
        self.check_analysis_publication_recovery(pika.exceptions.StreamLostError("publish interrupted"))

    def test_analysis_does_not_ack_when_monitor_queue_disappears(self):
        self.check_analysis_publication_recovery(pika.exceptions.UnroutableError([]))

    def check_analysis_publication_recovery(self, error):
        first_connection, second_connection = Mock(is_open=True), Mock(is_open=True)
        first_channel, second_channel = Mock(), Mock()
        method = SimpleNamespace(delivery_tag=42, routing_key="sensors.humidity.h1", redelivered=True)
        properties = SimpleNamespace(message_id="measurement-1")
        body = json.dumps({"message_id": "measurement-1", "sensor_id": "h1",
                           "timestamp": "2026-10-26T12:00:00Z", "type": "humidity", "value": 50}).encode()
        for channel in (first_channel, second_channel):
            channel.consume.return_value = delivery_then_shutdown((method, properties, body))
        first_channel.basic_publish.side_effect = error

        with patch("analysis.open_channel", side_effect=[(first_connection, first_channel),
                                                         (second_connection, second_channel)]), \
             patch("common.time.monotonic", return_value=0), \
             patch("common.time.sleep") as sleep, redirect_stdout(StringIO()):
            with self.assertRaises(KeyboardInterrupt):
                common.run_consumer(analysis.main, "environment")

        first_channel.basic_ack.assert_not_called()
        first_channel.basic_nack.assert_not_called()
        second_channel.basic_publish.assert_called_once()
        second_channel.basic_ack.assert_called_once_with(delivery_tag=42)
        first_connection.close.assert_called_once_with()
        second_connection.close.assert_called_once_with()
        sleep.assert_called_once_with(1)

    def test_alarm_and_monitor_resume_consumption_after_stream_loss(self):
        cases = (
            (alarm, {"level": "WARNING", "sensor_id": "temp-01", "value": 90}),
            (monitor, {"status": "processed", "sensor_type": "temperature", "elapsed_ms": 2,
                       "alarm": True}),
        )
        for module, payload in cases:
            with self.subTest(consumer=module.__name__):
                first_connection, second_connection = Mock(is_open=True), Mock(is_open=True)
                first_channel, second_channel = Mock(), Mock()
                first_channel.consume.side_effect = pika.exceptions.StreamLostError("connection reset")
                method = SimpleNamespace(delivery_tag=42)
                properties = SimpleNamespace(message_id="measurement-1", priority=3)
                second_channel.consume.return_value = delivery_then_shutdown(
                    (method, properties, json.dumps(payload).encode()))
                with patch.object(module, "open_channel", side_effect=[(first_connection, first_channel),
                                                                        (second_connection, second_channel)]), \
                     patch("common.time.monotonic", return_value=0), \
                     patch("common.time.sleep") as sleep, redirect_stdout(StringIO()):
                    with self.assertRaises(KeyboardInterrupt):
                        common.run_consumer(module.main)
                first_channel.basic_ack.assert_not_called()
                first_channel.basic_nack.assert_not_called()
                second_channel.basic_ack.assert_called_once_with(delivery_tag=42)
                first_connection.close.assert_called_once_with()
                second_connection.close.assert_called_once_with()
                sleep.assert_called_once_with(1)

    def test_real_pika_broker_cancellation_restarts_each_consumer(self):
        cases = (
            (analysis, common.ENV_QUEUE, ("environment",),
             {"message_id": "measurement-1", "sensor_id": "h1", "type": "humidity", "value": 50,
              "timestamp": "2026-10-26T12:00:00Z"}),
            (alarm, common.ALARM_QUEUE, (), {"level": "WARNING", "sensor_id": "temp-01", "value": 90}),
            (monitor, common.MONITOR_QUEUE, (),
             {"status": "processed", "sensor_type": "humidity", "elapsed_ms": 2, "alarm": False}),
        )
        for module, queue, args, payload in cases:
            with self.subTest(consumer=module.__name__):
                first_connection, second_connection = Mock(is_open=True), Mock(is_open=True)
                # Pika 1.3.2 beendet consume() bei Basic.Cancel ohne Exception.
                # Der echte Generator prueft diese Luecke besser als ein Mock.
                first_channel = blocking_connection.BlockingChannel(Mock(), first_connection)
                generator = blocking_connection._QueueConsumerGeneratorInfo((queue, False, False), "consumer-1")
                frame = pika.frame.Method(1, pika.spec.Basic.Cancel(consumer_tag="consumer-1"))
                generator.pending_events.append(blocking_connection._ConsumerCancellationEvt(frame))
                first_channel._queue_consumer_generator = generator
                second_channel = Mock()
                method = SimpleNamespace(delivery_tag=42, routing_key="sensors.humidity.h1", redelivered=True)
                properties = SimpleNamespace(message_id="measurement-1", priority=3)
                second_channel.consume.return_value = delivery_then_shutdown(
                    (method, properties, json.dumps(payload).encode()))

                with patch.object(module, "open_channel", side_effect=[(first_connection, first_channel),
                                                                        (second_connection, second_channel)]) as connect, \
                     patch.object(first_channel, "basic_qos"), \
                     patch("common.time.monotonic", return_value=0), \
                     patch("common.time.sleep") as sleep, redirect_stdout(StringIO()):
                    with self.assertRaises(KeyboardInterrupt):
                        common.run_consumer(module.main, *args)

                self.assertEqual(connect.call_count, 2)
                self.assertIsNone(first_channel._queue_consumer_generator)
                first_connection.close.assert_called_once_with()
                second_connection.close.assert_called_once_with()
                second_channel.basic_ack.assert_called_once_with(delivery_tag=42)
                sleep.assert_called_once_with(1)


if __name__ == "__main__":
    unittest.main()
