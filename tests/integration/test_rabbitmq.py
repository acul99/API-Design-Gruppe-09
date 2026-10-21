"""Echte Broker- und Prozess-Tests; explizit mit RUN_RABBITMQ_TESTS=1 aktivieren."""

import os
import unittest

from iot_analysis.config import Settings
from iot_analysis.demo import (isolated_lab, scenario_crash, scenario_end_to_end,
                               scenario_invalid, scenario_priority, scenario_processing, scenario_ttl)


@unittest.skipUnless(os.getenv("RUN_RABBITMQ_TESTS") == "1", "RUN_RABBITMQ_TESTS=1 und laufender RabbitMQ erforderlich")
class RabbitMQIntegrationTests(unittest.TestCase):
    def run_scenario(self, function, **kwargs):
        with isolated_lab(Settings.from_env()) as lab:
            return function(lab, **kwargs)

    def test_end_to_end_with_real_window_and_monitoring(self):
        self.run_scenario(scenario_end_to_end)

    def test_invalid_messages_rejected_to_dlq_and_recovery(self):
        self.run_scenario(scenario_invalid)

    def test_bounded_processing_retry_and_recovery(self):
        self.run_scenario(scenario_processing)

    def test_process_crash_before_ack_redelivers(self):
        self.run_scenario(scenario_crash)

    def test_priority_is_applied_by_broker(self):
        self.run_scenario(scenario_priority)

    def test_expiration_dead_letters(self):
        self.run_scenario(scenario_ttl)

    @unittest.skipUnless(os.getenv("RUN_FULL_TTL_TEST") == "1", "RUN_FULL_TTL_TEST=1 fuer echten 60s Queue-TTL-Test")
    def test_actual_sixty_second_queue_ttl(self):
        self.run_scenario(scenario_ttl, full_ttl=True)


if __name__ == "__main__":
    unittest.main()
