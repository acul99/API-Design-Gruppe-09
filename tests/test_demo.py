"""Sichtbare Demos bewahren Belege und halten Lieferungen vor dem ACK offen."""

from contextlib import nullcontext, redirect_stdout
import importlib.util
from io import StringIO
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, call, patch

import pika

import analysis
import demo


class DemoWaitTests(unittest.TestCase):
    def test_crash_wait_allows_delayed_heartbeat_detection(self):
        elapsed = [0.0]
        evidence = object()

        def advance(seconds):
            elapsed[0] += seconds

        with patch("demo.time.monotonic", side_effect=lambda: elapsed[0]), \
             patch("demo.time.sleep", side_effect=advance):
            result = demo.wait_for(lambda: evidence if elapsed[0] >= 60 else None, timeout=95)

        self.assertIs(result, evidence)
        self.assertGreaterEqual(elapsed[0], 60)
        self.assertLess(elapsed[0], 61)

    def test_default_wait_keeps_descriptive_fifteen_second_timeout(self):
        elapsed = [0.0]

        def advance(seconds):
            elapsed[0] += seconds

        with patch("demo.time.monotonic", side_effect=lambda: elapsed[0]), \
             patch("demo.time.sleep", side_effect=advance):
            with self.assertRaisesRegex(RuntimeError, "innerhalb von 15s"):
                demo.wait_for(lambda: None)

        self.assertGreaterEqual(elapsed[0], 15)
        self.assertLess(elapsed[0], 16)


class EvidenceInspectionTests(unittest.TestCase):
    def setUp(self):
        self.connection, self.channel = Mock(), Mock(is_open=True)
        self.connection.channel.return_value = self.channel

    def test_finds_current_run_after_old_evidence_and_retains_every_delivery(self):
        self.channel.queue_declare.return_value.method.message_count = 3
        self.channel.basic_get.side_effect = [
            (SimpleNamespace(delivery_tag=1), pika.BasicProperties(message_id="old-run"), b"old"),
            (SimpleNamespace(delivery_tag=2), pika.BasicProperties(message_id="current-run"), b"current"),
        ]
        predicate = lambda properties, body: body if properties.message_id == "current-run" else None

        result = demo.read_evidence(self.connection, "demo.dlq", predicate)

        self.assertEqual(result, b"current")
        self.channel.queue_declare.assert_called_once_with(queue="demo.dlq", passive=True)
        self.assertEqual(self.channel.basic_get.call_args_list,
                         [call("demo.dlq", auto_ack=False)] * 2)
        self.channel.basic_ack.assert_not_called()
        self.channel.basic_nack.assert_not_called()
        self.channel.queue_purge.assert_not_called()
        self.channel.close.assert_called_once_with()
        self.connection.close.assert_not_called()

    def test_scan_is_bounded_by_the_queue_snapshot(self):
        self.channel.queue_declare.return_value.method.message_count = 2
        self.channel.basic_get.return_value = (SimpleNamespace(delivery_tag=1),
                                               pika.BasicProperties(message_id="old"), b"old")

        self.assertIsNone(demo.read_evidence(self.connection, "demo.monitor", lambda *_: None))

        self.assertEqual(self.channel.basic_get.call_count, 2)
        self.channel.close.assert_called_once_with()
        self.channel.basic_ack.assert_not_called()

    def test_predicate_failure_still_requeues_inspected_evidence(self):
        self.channel.queue_declare.return_value.method.message_count = 1
        self.channel.basic_get.return_value = (SimpleNamespace(delivery_tag=1), pika.BasicProperties(), b"bad")

        with self.assertRaisesRegex(ValueError, "invalid evidence"):
            demo.read_evidence(self.connection, "demo.dlq", Mock(side_effect=ValueError("invalid evidence")))

        self.channel.close.assert_called_once_with()
        self.channel.basic_ack.assert_not_called()
        self.channel.basic_nack.assert_not_called()

    def test_concurrently_emptied_queue_ends_the_scan(self):
        self.channel.queue_declare.return_value.method.message_count = 2
        self.channel.basic_get.return_value = (None, None, None)
        predicate = Mock()

        self.assertIsNone(demo.read_evidence(self.connection, "demo.monitor", predicate))

        predicate.assert_not_called()
        self.channel.basic_get.assert_called_once_with("demo.monitor", auto_ack=False)
        self.channel.close.assert_called_once_with()


class DemoReceiveGateTests(unittest.TestCase):
    def setUp(self):
        self.connection, self.channel = Mock(is_open=True), Mock()
        body = json.dumps({"sensor_id": "humidity-demo", "type": "humidity", "value": 50,
                           "timestamp": "2026-10-26T12:00:00Z", "message_id": "gate-test"}).encode()

        def deliveries():
            yield (SimpleNamespace(delivery_tag=7, routing_key="sensors.humidity.humidity-demo", redelivered=False),
                   pika.BasicProperties(message_id="gate-test"), body)
            raise KeyboardInterrupt

        self.channel.consume.return_value = deliveries()

    def test_gate_serves_heartbeats_and_defers_processing_until_released(self):
        def heartbeat_sleep(seconds):
            self.assertEqual(seconds, 0.1)
            self.channel.basic_ack.assert_not_called()
            self.channel.basic_publish.assert_not_called()

        self.connection.sleep.side_effect = heartbeat_sleep
        with patch("analysis.open_channel", return_value=(self.connection, self.channel)), \
             patch("pathlib.Path.exists", side_effect=[True, True, False]), \
             redirect_stdout(StringIO()):
            with self.assertRaises(KeyboardInterrupt):
                analysis.main("environment", wait_file="receive.gate")

        self.assertEqual(self.connection.sleep.call_args_list, [call(0.1), call(0.1)])
        self.channel.basic_publish.assert_called_once()
        self.channel.basic_ack.assert_called_once_with(delivery_tag=7)
        self.connection.close.assert_called_once_with()

    def test_interrupted_gate_leaves_source_delivery_unacknowledged(self):
        self.connection.sleep.side_effect = KeyboardInterrupt
        with patch("analysis.open_channel", return_value=(self.connection, self.channel)), \
             patch("pathlib.Path.exists", return_value=True), redirect_stdout(StringIO()):
            with self.assertRaises(KeyboardInterrupt):
                analysis.main("environment", wait_file="receive.gate")

        self.channel.basic_ack.assert_not_called()
        self.channel.basic_nack.assert_not_called()
        self.channel.basic_publish.assert_not_called()
        self.connection.close.assert_called_once_with()


class DemoTopologyTests(unittest.TestCase):
    def queue_arguments(self, ttl):
        spec = importlib.util.spec_from_file_location("demo_test_common", Path(__file__).resolve().parents[1] / "common.py")
        module = importlib.util.module_from_spec(spec)
        with patch.dict(os.environ, {"IOT_NAMESPACE": "demo_test_topology"}):
            if ttl is None:
                os.environ.pop("IOT_MESSAGE_TTL_MS", None)
            else:
                os.environ["IOT_MESSAGE_TTL_MS"] = ttl
            spec.loader.exec_module(module)
            channel = Mock()
            module.declare_topology(channel)
        return module, {item.kwargs["queue"]: item.kwargs["arguments"]
                        for item in channel.queue_declare.call_args_list}

    def test_normal_application_keeps_sixty_second_ttl(self):
        module, queues = self.queue_arguments(None)
        for queue in module.QUEUES[:-1]:
            self.assertEqual(queues[queue]["x-message-ttl"], 60000)
        self.assertNotIn("x-message-ttl", queues[module.DLQ])

    def test_demo_retains_messages_without_changing_dead_lettering_or_priority(self):
        module, queues = self.queue_arguments("none")
        for queue, arguments in queues.items():
            self.assertNotIn("x-message-ttl", arguments)
            self.assertEqual(arguments["x-queue-type"], "classic")
            if queue != module.DLQ:
                self.assertEqual(arguments["x-dead-letter-exchange"], module.EVENT_EXCHANGE)
                self.assertEqual(arguments["x-dead-letter-routing-key"], "dead")
        self.assertEqual(queues[module.ALARM_QUEUE]["x-max-priority"], 9)


class DemoLifecycleTests(unittest.TestCase):
    def broker(self):
        connection, channel = Mock(is_open=True), Mock()
        channel.queue_declare.return_value.method = SimpleNamespace(consumer_count=0, message_count=0)
        mq = SimpleNamespace(open_channel=Mock(return_value=(connection, channel)), close=Mock(),
                             ENV_QUEUE="iot.demo.invalid.environment", SENSOR_EXCHANGE="iot.demo.invalid.sensors")
        return mq, connection, channel

    def test_interrupt_at_first_checkpoint_keeps_queued_sample_and_topology(self):
        mq, connection, channel = self.broker()
        observed_settings = []

        def connect():
            observed_settings.append((os.environ["IOT_NAMESPACE"], os.environ["IOT_MESSAGE_TTL_MS"]))
            return connection, channel

        mq.open_channel.side_effect = connect
        with patch.dict(os.environ, {"IOT_NAMESPACE": "normal-application"}), \
             patch.dict(sys.modules, {"common": mq}), patch("builtins.input", side_effect=KeyboardInterrupt), \
             patch("demo.tempfile.TemporaryDirectory", return_value=nullcontext("unused-demo-logs")), \
             patch("demo.subprocess.Popen") as launch, redirect_stdout(StringIO()):
            os.environ.pop("IOT_DEMO_NAMESPACE", None)
            with self.assertRaises(KeyboardInterrupt):
                demo.main("invalid", automatic=False)

        self.assertEqual(observed_settings, [("iot.demo.invalid", "none")])
        launch.assert_not_called()
        channel.basic_publish.assert_called_once()
        self.assertTrue(channel.basic_publish.call_args.kwargs["properties"].message_id.startswith("invalid-demo-"))
        channel.queue_delete.assert_not_called()
        channel.exchange_delete.assert_not_called()
        channel.queue_purge.assert_not_called()
        channel.basic_ack.assert_not_called()
        mq.close.assert_called_with(connection)

    def test_occupied_source_is_not_purged_or_consumed(self):
        for consumer_count, message_count in ((1, 0), (0, 1)):
            with self.subTest(consumers=consumer_count, messages=message_count):
                mq, _, channel = self.broker()
                channel.queue_declare.return_value.method = SimpleNamespace(
                    consumer_count=consumer_count, message_count=message_count)
                with patch.dict(os.environ, {"IOT_DEMO_NAMESPACE": "demo-test"}), \
                     patch.dict(sys.modules, {"common": mq}), patch("demo.subprocess.Popen") as launch, \
                     patch("demo.tempfile.TemporaryDirectory", return_value=nullcontext("unused-demo-logs")), \
                     redirect_stdout(StringIO()):
                    with self.assertRaisesRegex(RuntimeError, "belegt"):
                        demo.main("invalid", automatic=True)
                launch.assert_not_called()
                channel.basic_publish.assert_not_called()
                channel.basic_get.assert_not_called()
                channel.queue_delete.assert_not_called()
                channel.exchange_delete.assert_not_called()
                channel.queue_purge.assert_not_called()


if __name__ == "__main__":
    unittest.main()
