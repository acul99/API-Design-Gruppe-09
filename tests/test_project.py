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
        for scenario in ("invalid", "crash"):
            with self.subTest(scenario=scenario):
                subprocess.run([sys.executable, "demo.py", scenario], cwd=root, check=True, timeout=30)


if __name__ == "__main__":
    unittest.main()
