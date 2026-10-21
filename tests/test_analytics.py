"""Empfangszeitfenster und Kennzahlen aus externen Monitoring-Ereignissen."""

import unittest

from iot_analysis.analytics import MonitoringStats, TemperatureWindow


class TemperatureWindowTests(unittest.TestCase):
    def test_average_of_large_finite_values_stays_finite(self):
        window = TemperatureWindow(started_at=0)
        window.add(1e308, "a")
        window.add(1e308, "b")
        self.assertEqual(window.summary()["average"], 1e308)

    def test_window_closes_at_ten_seconds_even_without_new_measurement(self):
        window = TemperatureWindow(started_at=100)
        window.add(20, "delivery-1")
        window.add(40, "delivery-2")
        self.assertFalse(window.due(109.999))
        self.assertTrue(window.due(110))
        self.assertEqual(window.summary(), {"count": 2, "average": 30, "min": 20,
                                            "max": 40, "window_seconds": 10.0})
        self.assertEqual(window.pending, ["delivery-1", "delivery-2"])

    def test_advance_starts_contiguous_window_and_clears_pending_tags(self):
        window = TemperatureWindow(started_at=100)
        window.add(12, 7)
        window.advance(110.25)
        self.assertEqual(window.started_at, 110)
        self.assertIsNone(window.summary())
        self.assertEqual(window.pending, [])
        self.assertFalse(window.due(119.999))
        self.assertTrue(window.due(120))

    def test_idle_time_skips_empty_windows_without_drift(self):
        window = TemperatureWindow(started_at=100)
        window.advance(137)
        self.assertEqual(window.started_at, 130)
        self.assertFalse(window.due(139.999))
        self.assertTrue(window.due(140))

    def test_empty_window_does_not_invent_a_zero_average(self):
        self.assertIsNone(TemperatureWindow(started_at=0).summary())

    def test_nonpositive_window_is_invalid(self):
        for seconds in (0, -1):
            with self.subTest(seconds=seconds), self.assertRaises(ValueError):
                TemperatureWindow(seconds=seconds)


class MonitoringStatsTests(unittest.TestCase):
    def event(self, event_id="e1", **changes):
        return {"event_id": event_id, "kind": "sensor_processed", "sensor_type": "temperature",
                "processing_ms": 2.0, "alarm": False, **changes}

    def test_counts_sensor_types_alarms_failures_and_processing_average(self):
        stats = MonitoringStats()
        stats.observe(self.event("e1", alarm=True, processing_ms=2))
        stats.observe(self.event("e2", sensor_type="vibration", processing_ms=4))
        stats.observe({"event_id": "e3", "kind": "processing_failed"})
        # Die Alarmverarbeitung zählt dieselbe Anomalie nicht erneut.
        stats.observe({"event_id": "e4", "kind": "alarm_processed"})
        self.assertEqual(stats.snapshot(), {"processed": 2, "per_type": {"temperature": 1, "vibration": 1},
                                            "anomalies": 1, "failures": 1, "avg_processing_ms": 3.0})

    def test_redelivered_event_is_not_counted_twice(self):
        stats = MonitoringStats()
        event = self.event()
        self.assertTrue(stats.observe(event))
        self.assertFalse(stats.observe(event))
        self.assertEqual(stats.snapshot()["processed"], 1)

    def test_deduplication_memory_is_bounded(self):
        stats = MonitoringStats(dedup_limit=2)
        for event_id in ("e1", "e2", "e3"):
            stats.observe(self.event(event_id))
        self.assertFalse(stats.observe(self.event("e2")))
        self.assertTrue(stats.observe(self.event("e1")))
        self.assertEqual(stats.snapshot()["processed"], 4)

    def test_malformed_event_does_not_pollute_counters_or_deduplication(self):
        stats = MonitoringStats()
        malformed = (
            {}, self.event(event_id=""), self.event(event_id=42),
            self.event(kind=None), self.event(sensor_type=""),
            self.event(processing_ms=-1), self.event(processing_ms=True),
            self.event(processing_ms="2"),
        )
        for event in malformed:
            with self.subTest(event=event), self.assertRaises(ValueError):
                stats.observe(event)
        self.assertEqual(stats.snapshot()["processed"], 0)
        self.assertTrue(stats.observe(self.event()))

    def test_monitoring_duration_must_be_finite(self):
        for duration in (float("nan"), float("inf"), -float("inf")):
            with self.subTest(duration=duration), self.assertRaises(ValueError):
                MonitoringStats().observe(self.event(processing_ms=duration))

    def test_empty_monitoring_has_defined_zero_average(self):
        stats = MonitoringStats()
        self.assertEqual(stats.snapshot()["avg_processing_ms"], 0.0)
        snapshot = stats.snapshot()
        snapshot["per_type"]["temperature"] = 5
        self.assertEqual(stats.snapshot()["per_type"], {})


if __name__ == "__main__":
    unittest.main()
