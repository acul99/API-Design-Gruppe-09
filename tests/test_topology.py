"""Brokerkonfiguration als überprüfbarer Nachrichtenvertrag."""

import unittest
from unittest.mock import Mock

from iot_analysis.config import Settings
from iot_analysis.topology import declare_topology


class TopologyTests(unittest.TestCase):
    def setUp(self):
        self.channel = Mock()
        self.topology = declare_topology(self.channel, Settings(namespace="isolated_test"))
        self.declarations = {c.kwargs["queue"]: c.kwargs for c in self.channel.queue_declare.call_args_list}
        self.bindings = [c.kwargs for c in self.channel.queue_bind.call_args_list]

    def test_distinct_exchange_types_match_message_purposes(self):
        declared = {c.kwargs["exchange"]: c.kwargs for c in self.channel.exchange_declare.call_args_list}
        expected = {self.topology.sensors: "topic", self.topology.alarms: "direct",
                    self.topology.telemetry: "fanout", self.topology.dlx: "direct"}
        self.assertEqual({name: data["exchange_type"] for name, data in declared.items()}, expected)
        self.assertTrue(all(data["durable"] for data in declared.values()))

    def test_main_queues_expire_after_sixty_seconds_and_dead_letter(self):
        main_queues = (self.topology.environment_queue, self.topology.machinery_queue,
                       self.topology.alarm_queue, self.topology.monitoring_queue)
        for queue in main_queues:
            with self.subTest(queue=queue):
                declared = self.declarations[queue]
                self.assertTrue(declared["durable"])
                args = declared["arguments"]
                self.assertEqual(args["x-message-ttl"], 60_000)
                self.assertEqual(args["x-dead-letter-exchange"], self.topology.dlx)
                self.assertEqual(args["x-dead-letter-routing-key"], "dead")

    def test_dlq_keeps_evidence_without_ttl_or_dead_letter_cycle(self):
        args = self.declarations[self.topology.dlq]["arguments"]
        self.assertTrue(self.declarations[self.topology.dlq]["durable"])
        self.assertNotIn("x-message-ttl", args)
        self.assertNotIn("x-dead-letter-exchange", args)
        self.assertIn({"queue": self.topology.dlq, "exchange": self.topology.dlx,
                       "routing_key": "dead"}, self.bindings)

    def test_sensor_selection_is_encoded_in_broker_bindings(self):
        sensor_bindings = {(b["queue"], b["routing_key"]) for b in self.bindings
                           if b["exchange"] == self.topology.sensors}
        self.assertEqual(sensor_bindings, {
            (self.topology.environment_queue, "sensors.temperature.*"),
            (self.topology.environment_queue, "sensors.humidity.*"),
            (self.topology.machinery_queue, "sensors.vibration.*"),
        })

    def test_alarm_queue_supports_high_priority_and_both_levels(self):
        args = self.declarations[self.topology.alarm_queue]["arguments"]
        self.assertEqual(args["x-queue-type"], "classic")
        self.assertGreaterEqual(args["x-max-priority"], 9)
        alarm_bindings = {b["routing_key"] for b in self.bindings if b["queue"] == self.topology.alarm_queue}
        self.assertEqual(alarm_bindings, {"WARNING", "CRITICAL"})

    def test_telemetry_is_delivered_to_separate_monitoring_queue(self):
        self.assertIn({"queue": self.topology.monitoring_queue, "exchange": self.topology.telemetry},
                      self.bindings)

    def test_namespace_isolated_and_topic_metacharacters_are_rejected(self):
        self.assertTrue(all(name.startswith("isolated_test.") for name in
                            (*self.topology.queues, *self.topology.exchanges)))
        for namespace in ("", "production.#", "*", "space here", "x" * 65):
            with self.subTest(namespace=namespace), self.assertRaises(ValueError):
                Settings(namespace=namespace)


if __name__ == "__main__":
    unittest.main()
