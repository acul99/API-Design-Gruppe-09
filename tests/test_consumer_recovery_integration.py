"""Optionale echte Brokerpruefung fuer den Wiederanlauf nach Queue-Loeschung."""

import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
from uuid import uuid4

import pika


@unittest.skipUnless(os.getenv("RUN_RABBITMQ_TESTS") == "1", "Optional: RUN_RABBITMQ_TESTS=1 und laufender Broker")
class ConsumerRecoveryBrokerTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(__file__).resolve().parents[1]
        self.namespace = "recovery_test_" + uuid4().hex
        self.environment = {**os.environ, "IOT_NAMESPACE": self.namespace}
        spec = importlib.util.spec_from_file_location("recovery_test_common", self.root / "common.py")
        self.mq = importlib.util.module_from_spec(spec)
        with patch.dict(os.environ, {"IOT_NAMESPACE": self.namespace}):
            spec.loader.exec_module(self.mq)
        self.connection, self.channel = self.mq.open_channel()
        self.folder = tempfile.TemporaryDirectory()
        self.process = self.stream = None

    def tearDown(self):
        if self.process is not None:
            if self.process.poll() is None:
                self.process.terminate()
            self.process.wait(timeout=5)
        if self.stream is not None:
            self.stream.close()
        try:
            for queue in self.mq.QUEUES:
                self.channel.queue_delete(queue)
            for exchange in (self.mq.SENSOR_EXCHANGE, self.mq.EVENT_EXCHANGE):
                self.channel.exchange_delete(exchange)
        finally:
            self.mq.close(self.connection)
            self.folder.cleanup()

    def wait_for(self, condition, require_alive=True):
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            result = condition()
            if result:
                return result
            if require_alive and self.process is not None and self.process.poll() is not None:
                self.fail("Consumer wurde beendet:\n" + self.log.read_text(encoding="utf-8"))
            self.connection.sleep(0.05)
        self.fail("Consumer wurde nicht rechtzeitig bereit:\n" + self.log.read_text(encoding="utf-8"))

    def check_recovery(self, command, queue, send_valid):
        self.log = Path(self.folder.name) / "consumer.log"
        self.stream = self.log.open("w", encoding="utf-8")
        self.process = subprocess.Popen([sys.executable, "-u", "-X", "utf8", *command],
            cwd=self.root, env=self.environment, stdout=self.stream, stderr=subprocess.STDOUT,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
        original_pid = self.process.pid
        self.wait_for(lambda: "BEREIT" in self.log.read_text(encoding="utf-8"))
        self.wait_for(lambda: self.channel.queue_declare(queue=queue, passive=True).method.consumer_count == 1)

        # Ausschliesslich die eigene Testqueue loeschen. RabbitMQ sendet Basic.Cancel.
        self.channel.queue_delete(queue=queue)
        self.wait_for(lambda: self.log.read_text(encoding="utf-8").count("BEREIT") >= 2)
        self.wait_for(lambda: self.channel.queue_declare(queue=queue, passive=True).method.consumer_count == 1)
        send_valid()

        # Die nachfolgende fehlerhafte Nachricht dient als Verarbeitungsbarriere:
        # ihr NACK belegt, dass der Consumer den vorherigen gueltigen Wert bereits ACKte.
        sentinel_id = "barrier-" + uuid4().hex
        self.channel.basic_publish(exchange="", routing_key=queue, body=b"{invalid JSON", mandatory=True,
            properties=pika.BasicProperties(message_id=sentinel_id, delivery_mode=2, priority=3))

        def rejected_barrier():
            method, properties, _ = self.channel.basic_get(queue=self.mq.DLQ, auto_ack=False)
            if method is None:
                return False
            self.channel.basic_ack(delivery_tag=method.delivery_tag)
            self.assertEqual(properties.message_id, sentinel_id)
            self.assertEqual(properties.headers["x-death"][0]["reason"], "rejected")
            return True

        self.wait_for(rejected_barrier)
        self.assertIsNone(self.process.poll())
        self.assertEqual(self.process.pid, original_pid)
        self.process.terminate()
        self.process.wait(timeout=5)
        self.wait_for(lambda: self.channel.queue_declare(queue=queue, passive=True).method.consumer_count == 0,
                      require_alive=False)
        self.assertEqual(self.channel.queue_declare(queue=queue, passive=True).method.message_count, 0)

    def test_environment_restarts_after_queue_deletion(self):
        sensor = {"sensor_id": "humidity-test", "type": "humidity", "value": 50,
                  "timestamp": "2026-10-26T12:00:00Z", "message_id": "humidity-valid"}
        self.check_recovery(("analysis.py", "environment"), self.mq.ENV_QUEUE,
                            lambda: self.mq.send_sensor(self.channel, sensor))

    def test_machinery_restarts_after_queue_deletion(self):
        sensor = {"sensor_id": "vibration-test", "type": "vibration", "value": 1,
                  "timestamp": "2026-10-26T12:00:00Z", "message_id": "vibration-valid"}
        self.check_recovery(("analysis.py", "machinery"), self.mq.MACHINE_QUEUE,
                            lambda: self.mq.send_sensor(self.channel, sensor))

    def test_alarm_restarts_after_queue_deletion(self):
        payload = {"level": "WARNING", "sensor_id": "temperature-test", "value": 90,
                   "message_id": "alarm-valid"}
        self.check_recovery(("alarm.py",), self.mq.ALARM_QUEUE,
                            lambda: self.mq.publish(self.channel, "WARNING", payload, priority=3))

    def test_monitor_restarts_after_queue_deletion(self):
        self.check_recovery(("monitor.py",), self.mq.MONITOR_QUEUE,
                            lambda: self.mq.emit_metric(self.channel, "processed", "metric-valid", "humidity"))


if __name__ == "__main__":
    unittest.main()
