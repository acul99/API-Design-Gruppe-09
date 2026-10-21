"""Auch den regulaeren Producer als echtes Programm am Broker pruefen."""

import os
import unittest

from iot_analysis.config import Settings
from iot_analysis.demo import isolated_lab
from iot_analysis.models import parse_sensor
from iot_analysis.producer import run_producer


@unittest.skipUnless(os.getenv("RUN_RABBITMQ_TESTS") == "1", "RUN_RABBITMQ_TESTS=1 erforderlich")
class ProducerBrokerTests(unittest.TestCase):
    def test_all_three_types_are_persisted_and_routed_to_distinct_queues(self):
        with isolated_lab(Settings.from_env()) as lab:
            run_producer(lab.settings, count=6, interval=0, seed=42)
            self.assertEqual(lab.ready_count(lab.topology.environment_queue), 4)
            self.assertEqual(lab.ready_count(lab.topology.machinery_queue), 2)
            types = []
            for queue, expected in ((lab.topology.environment_queue, {"temperature", "humidity"}),
                                    (lab.topology.machinery_queue, {"vibration"})):
                while True:
                    method, properties, body = lab.channel.basic_get(queue, auto_ack=False)
                    if method is None:
                        break
                    sensor = parse_sensor(body, method.routing_key)
                    self.assertIn(sensor["type"], expected)
                    self.assertEqual(properties.delivery_mode, 2)
                    self.assertEqual(properties.message_id, sensor["message_id"])
                    types.append(sensor["type"])
                    lab.channel.basic_ack(method.delivery_tag)
            self.assertEqual({kind: types.count(kind) for kind in set(types)},
                             {"temperature": 2, "humidity": 2, "vibration": 2})
