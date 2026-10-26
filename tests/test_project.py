"""Kleine Tests fuer die fachlichen Regeln und die wichtigsten AMQP-Einstellungen."""

from datetime import datetime, timezone
from contextlib import redirect_stdout
from io import StringIO
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import unittest
from unittest.mock import Mock, call, patch
from uuid import uuid4

import pika

import common
from analysis import alarm_level, summarize
from producer import send_with_retry
import start


class StarterTests(unittest.TestCase):
    def test_offline_consumers_stay_alive_until_producer_finishes_retries(self):
        output, processes = StringIO(), []
        real_popen = subprocess.Popen
        # Run the real entrypoints with a deterministic outage and short test delays.
        child_code = """
import runpy, sys, time
import common
def offline():
    raise common.pika.exceptions.AMQPConnectionError('simulated outage')
common.open_channel = offline
real_sleep = time.sleep
time.sleep = lambda seconds: real_sleep(min(seconds, 0.01))
sys.argv = sys.argv[1:]
runpy.run_path(sys.argv[0], run_name='__main__')
"""

        def launch_child(command, **kwargs):
            process = real_popen([*command[:4], "-c", child_code, *command[4:]], **kwargs)
            process.terminate = Mock(wraps=process.terminate)
            processes.append(process)
            return process

        with patch("start.subprocess.Popen", side_effect=launch_child), redirect_stdout(output):
            with self.assertRaises(RuntimeError):
                start.main()

        self.assertEqual(len(processes), 5)
        for retry in range(1, 6):
            self.assertIn(f"Wiederholung {retry}/5", output.getvalue())
        for consumer in processes[:4]:
            consumer.terminate.assert_called_once()
        processes[-1].terminate.assert_not_called()
        self.assertNotEqual(processes[-1].returncode, 0)

    def test_child_output_and_errors_reach_parent_console(self):
        output = StringIO()
        real_popen, real_sleep = subprocess.Popen, time.sleep
        code = "import sys,time; print('CHILD-OUT',flush=True); print('CHILD-ERR',file=sys.stderr,flush=True); time.sleep(30)"

        def launch_child(command, **kwargs):
            return real_popen([sys.executable, "-u", "-X", "utf8", "-c", code], **kwargs)

        def stop_after_output(_):
            deadline = time.monotonic() + 3
            while output.getvalue().count("CHILD-OUT") < 5 or output.getvalue().count("CHILD-ERR") < 5:
                if time.monotonic() >= deadline:
                    self.fail("Ausgaben der fuenf Kindprozesse erreichen die Elternkonsole nicht")
                real_sleep(0.01)
            raise KeyboardInterrupt

        with patch("start.subprocess.Popen", side_effect=launch_child), \
             patch("start.time.sleep", side_effect=stop_after_output), redirect_stdout(output):
            start.main()
        self.assertEqual(output.getvalue().count("CHILD-OUT"), 5)
        self.assertEqual(output.getvalue().count("CHILD-ERR"), 5)


class ProjectTests(unittest.TestCase):
    def test_sensor_contract_and_invalid_values(self):
        sensor = {"sensor_id": "t1", "timestamp": datetime.now(timezone.utc).isoformat(),
                  "type": "temperature", "value": 25}
        self.assertEqual(common.parse_sensor(json.dumps(sensor).encode(), "sensors.temperature.t1")["value"], 25)
        for change in ({"value": True}, {"value": float("inf")}, {"value": 10 ** 400},
                       {"timestamp": "2026-10-26"}, {"type": "unknown"}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                common.parse_sensor(json.dumps({**sensor, **change}).encode(), "sensors.temperature.t1")

    def test_alarm_boundaries(self):
        for kind, value, expected in (("temperature", 80, None), ("temperature", 81, "WARNING"),
                                     ("temperature", 101, "CRITICAL"), ("humidity", 50, None),
                                     ("humidity", 29, "WARNING"), ("humidity", 14, "CRITICAL"),
                                     ("vibration", 5, None), ("vibration", 6, "WARNING"),
                                     ("vibration", 11, "CRITICAL")):
            with self.subTest(kind=kind, value=value):
                self.assertEqual(alarm_level({"type": kind, "value": value}), expected)

    def test_temperature_average(self):
        self.assertEqual(summarize([20, 30, 40]), {"count": 3, "average": 30, "min": 20, "max": 40})
        self.assertEqual(summarize([1e308, 1e308])["average"], 1e308)

    def test_topology_ttl_dead_letters_priority_and_routing(self):
        channel = Mock()
        common.declare_topology(channel)
        queues = {c.kwargs["queue"]: c.kwargs["arguments"] for c in channel.queue_declare.call_args_list}
        for queue in common.QUEUES[:-1]:
            self.assertEqual(queues[queue]["x-message-ttl"], 60000)
            self.assertEqual(queues[queue]["x-dead-letter-exchange"], common.EVENT_EXCHANGE)
        self.assertNotIn("x-message-ttl", queues[common.DLQ])
        self.assertEqual(queues[common.ALARM_QUEUE]["x-max-priority"], 9)
        bindings = {(c.kwargs["queue"], c.kwargs["routing_key"]) for c in channel.queue_bind.call_args_list}
        self.assertIn((common.ENV_QUEUE, "sensors.temperature.*"), bindings)
        self.assertIn((common.MACHINE_QUEUE, "sensors.vibration.*"), bindings)
        self.assertNotIn((common.MACHINE_QUEUE, "sensors.temperature.*"), bindings)

    def test_publication_is_persistent_and_mandatory(self):
        channel = Mock()
        common.publish(channel, "CRITICAL", {"message_id": "m1"}, priority=9)
        args = channel.basic_publish.call_args.kwargs
        self.assertTrue(args["mandatory"])
        self.assertEqual(args["properties"].delivery_mode, 2)
        self.assertEqual(args["properties"].priority, 9)

    @patch("producer.time.sleep")
    @patch("producer.open_channel", side_effect=OSError("offline"))
    def test_producer_stops_after_five_retries(self, connect, sleep):
        with self.assertRaises(OSError):
            send_with_retry(None, None, {"message_id": "same-id"})
        self.assertEqual(connect.call_count, 6)
        self.assertEqual(sleep.call_args_list, [call(1), call(2), call(4), call(8), call(10)])


@unittest.skipUnless(os.getenv("RUN_RABBITMQ_TESTS") == "1", "Optional: RUN_RABBITMQ_TESTS=1 und laufender Broker")
class BrokerTests(unittest.TestCase):
    def test_invalid_message_and_crash(self):
        root = Path(__file__).resolve().parents[1]
        base = "demo_test_" + uuid4().hex
        normal_queue = f"{base}.normal.dlq"
        environment = {**os.environ, "IOT_NAMESPACE": f"{base}.normal", "IOT_DEMO_NAMESPACE": base}
        parameters = pika.URLParameters(os.getenv("AMQP_URL", "amqp://guest:guest@localhost:5672/%2F"))
        parameters.socket_timeout = 3
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.confirm_delivery()
        queues = [normal_queue]
        exchanges = []

        def declare_demo(prefix):
            for exchange, kind in ((f"{prefix}.sensors", "topic"), (f"{prefix}.events", "direct")):
                channel.exchange_declare(exchange=exchange, exchange_type=kind, durable=True)
                if exchange not in exchanges:
                    exchanges.append(exchange)
            for suffix in ("environment", "machinery", "alarms", "monitor", "dlq"):
                queue = f"{prefix}.{suffix}"
                arguments = {"x-queue-type": "classic"}
                if suffix != "dlq":
                    arguments.update({"x-dead-letter-exchange": f"{prefix}.events",
                                      "x-dead-letter-routing-key": "dead"})
                if suffix == "alarms":
                    arguments["x-max-priority"] = 9
                # A declaration with matching arguments also proves that the
                # retained demo queues have no TTL that could expire the evidence.
                channel.queue_declare(queue=queue, durable=True, arguments=arguments)
                if queue not in queues:
                    queues.append(queue)

        def drain_owned_test_queue(queue):
            records = []
            while True:
                method, properties, body = channel.basic_get(queue=queue, auto_ack=True)
                if method is None:
                    return records
                records.append((properties, body))

        def assert_source_idle(queue):
            deadline = time.monotonic() + 5
            while True:
                state = channel.queue_declare(queue=queue, passive=True).method
                if state.consumer_count == 0 and state.message_count == 0:
                    return
                if time.monotonic() >= deadline:
                    self.fail(f"Demoquelle nicht leer/ohne Consumer: {queue}")
                connection.sleep(0.05)

        try:
            channel.queue_declare(queue=normal_queue, durable=True, arguments={"x-queue-type": "classic"})
            channel.basic_publish(exchange="", routing_key=normal_queue, body=b"normal-data-untouched", mandatory=True)
            for scenario in ("invalid", "crash"):
                with self.subTest(scenario=scenario):
                    prefix = f"{base}.{scenario}"
                    declare_demo(prefix)
                    # Earlier evidence must survive, and must never count as
                    # proof that this run has processed its new measurement.
                    old_id = f"old-{scenario}"
                    channel.basic_publish(exchange="", routing_key=f"{prefix}.dlq", body=b"older-evidence",
                        properties=pika.BasicProperties(message_id=old_id, delivery_mode=2), mandatory=True)
                    old_metric = {"status": "processed", "message_id": old_id, "sensor_type": "humidity",
                                  "elapsed_ms": 0, "alarm": False, "redelivered": False}
                    channel.basic_publish(exchange="", routing_key=f"{prefix}.monitor",
                        body=json.dumps(old_metric).encode(),
                        properties=pika.BasicProperties(message_id=old_id, delivery_mode=2), mandatory=True)

                    for run in (1, 2):
                        result = subprocess.run([sys.executable, "-u", "-X", "utf8", "demo.py", scenario, "--auto"],
                            cwd=root, env=environment, check=True, timeout=150, capture_output=True,
                            text=True, encoding="utf-8", stdin=subprocess.DEVNULL)
                        self.assertIn("OK:", result.stdout)
                        declare_demo(prefix)
                        assert_source_idle(f"{prefix}.environment")
                        self.assertEqual(channel.queue_declare(queue=f"{prefix}.dlq", passive=True).method.message_count,
                                         1 + (run if scenario == "invalid" else 0))
                        self.assertEqual(channel.queue_declare(queue=f"{prefix}.monitor", passive=True).method.message_count,
                                         1 + 2 * run)
                        self.assertEqual(channel.queue_declare(queue=normal_queue, passive=True).method.message_count, 1)

                    dead_letters = drain_owned_test_queue(f"{prefix}.dlq")
                    self.assertIn((old_id, b"older-evidence"),
                                  [(properties.message_id, body) for properties, body in dead_letters])
                    new_dead_letters = [(properties, body) for properties, body in dead_letters
                                        if properties.message_id != old_id]
                    metrics = [json.loads(body) for _, body in drain_owned_test_queue(f"{prefix}.monitor")]
                    self.assertIn(old_metric, metrics)
                    new_metrics = [metric for metric in metrics if metric["message_id"] != old_id]
                    self.assertEqual(len({metric["message_id"] for metric in new_metrics}), 4)
                    followups = [metric for metric in new_metrics if metric["message_id"].startswith("after-demo-")]
                    self.assertEqual(len(followups), 2)
                    self.assertTrue(all(metric["status"] == "processed" for metric in followups))
                    if scenario == "invalid":
                        self.assertEqual(len(new_dead_letters), 2)
                        rejected_ids = set()
                        for properties, body in new_dead_letters:
                            self.assertTrue(properties.message_id.startswith("invalid-demo-"))
                            self.assertEqual(properties.headers["x-death"][0]["reason"], "rejected")
                            self.assertEqual(body, b"{kaputtes JSON")
                            rejected_ids.add(properties.message_id)
                        self.assertEqual(len(rejected_ids), 2)
                        self.assertEqual({metric["message_id"] for metric in new_metrics if metric["status"] == "failed"},
                                         rejected_ids)
                    else:
                        self.assertEqual(new_dead_letters, [])
                        redelivered = [metric for metric in new_metrics if metric["message_id"].startswith("crash-demo-")]
                        self.assertEqual(len(redelivered), 2)
                        self.assertTrue(all(metric["redelivered"] is True and metric["status"] == "processed"
                                            for metric in redelivered))
            self.assertEqual(drain_owned_test_queue(normal_queue)[0][1], b"normal-data-untouched")
        finally:
            # These names include this test's UUID. User-visible demo queues and
            # the application's ordinary namespace are never removed by tests.
            for queue in queues:
                cleanup = connection.channel()
                try:
                    cleanup.queue_delete(queue=queue)
                finally:
                    if cleanup.is_open:
                        cleanup.close()
            for exchange in exchanges:
                cleanup = connection.channel()
                try:
                    cleanup.exchange_delete(exchange=exchange)
                finally:
                    if cleanup.is_open:
                        cleanup.close()
            connection.close()


if __name__ == "__main__":
    unittest.main()
