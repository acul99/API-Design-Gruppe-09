"""Sensorverträge und Wiederanlauf des Producers ohne Netzwerkzugriffe."""

from datetime import datetime
import random
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, call, patch

import pika

from iot_analysis.config import Settings
from iot_analysis.messaging import with_retry
from iot_analysis.models import SENSOR_TYPES, encode_json, parse_sensor
from iot_analysis.producer import ReliablePublisher, generate_sensor, run_producer


def connection_with(channel):
    connection = Mock(is_open=True, is_closed=False)
    connection.channel.return_value = channel
    return connection


class SensorGenerationTests(unittest.TestCase):
    def test_each_generated_sensor_satisfies_the_public_contract(self):
        rng = random.Random(42)
        identities = set()
        for index, kind in enumerate(SENSOR_TYPES):
            with self.subTest(kind=kind):
                sensor = generate_sensor(kind, index, rng)
                routing_key = f"sensors.{kind}.{sensor['sensor_id']}"
                self.assertEqual(parse_sensor(encode_json(sensor), routing_key), sensor)
                self.assertEqual(sensor["type"], kind)
                self.assertIsNotNone(datetime.fromisoformat(sensor["timestamp"]).utcoffset())
                identities.add(sensor["sensor_id"])
        self.assertEqual(len(identities), 3)

    def test_seed_reproduces_measurements_but_new_messages_keep_unique_ids(self):
        first = generate_sensor("temperature", 0, random.Random(42))
        second = generate_sensor("temperature", 0, random.Random(42))
        self.assertEqual(first["value"], second["value"])
        self.assertNotEqual(first["message_id"], second["message_id"])

    @patch("iot_analysis.producer.emit_log")
    @patch("iot_analysis.producer.ReliablePublisher")
    def test_finite_run_visits_all_three_types_and_closes_connection(self, publisher_class, _log):
        publisher = publisher_class.return_value
        run_producer(Settings(), count=3, interval=0.5)
        sensors = [entry.args[0] for entry in publisher.send.call_args_list]
        self.assertEqual([sensor["type"] for sensor in sensors], list(SENSOR_TYPES))
        self.assertEqual(len({sensor["message_id"] for sensor in sensors}), 3)
        self.assertEqual(publisher.pause.call_args_list, [call(0.5), call(0.5)])
        publisher.close.assert_called_once_with()

    @patch("iot_analysis.producer.ReliablePublisher")
    def test_invalid_run_parameters_do_not_open_a_connection(self, publisher_class):
        for changes in ({"count": -1}, {"interval": -0.1}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                run_producer(Settings(), **changes)
        publisher_class.assert_not_called()


class ReliablePublisherTests(unittest.TestCase):
    def setUp(self):
        self.sensor = generate_sensor("temperature", 0, random.Random(42))
        self.settings = Settings()
        self.topology = SimpleNamespace(sensors="iotlab.sensors")

    def no_wait_retry(self, sleep):
        # with_retry bindet time.sleep als Defaultargument. Den echten
        # Backoffalgorithmus mit injizierter Uhr ausführen, nicht wegmocken.
        return lambda operation: with_retry(operation, sleep=sleep, on_retry=Mock())

    def test_network_failure_reconnects_and_republishes_same_message_identity(self):
        first_channel, second_channel = Mock(is_closed=False), Mock(is_closed=False)
        first_connection = connection_with(first_channel)
        second_connection = connection_with(second_channel)
        sleep = Mock()
        with (
            patch("iot_analysis.producer.connect", side_effect=[first_connection, second_connection]) as connect,
            patch("iot_analysis.producer.declare_topology", return_value=self.topology),
            patch("iot_analysis.producer.publish_json", side_effect=[pika.exceptions.StreamLostError("network"), None]) as publish,
            patch("iot_analysis.producer.with_retry", side_effect=self.no_wait_retry(sleep)),
        ):
            publisher = ReliablePublisher(self.settings)
            publisher.send(self.sensor)
        self.assertEqual(connect.call_count, 2)
        sleep.assert_called_once_with(1)
        first_connection.close.assert_called_once_with()
        first_channel.confirm_delivery.assert_called_once_with()
        second_channel.confirm_delivery.assert_called_once_with()
        self.assertIs(publisher.connection, second_connection)
        for entry in publish.call_args_list:
            self.assertIs(entry.args[3], self.sensor)
            self.assertEqual(entry.kwargs["message_id"], self.sensor["message_id"])
            self.assertEqual(entry.args[2], "sensors.temperature.temperature-01")

    def test_permanent_outage_stops_after_five_retries(self):
        sleep = Mock()
        with (
            patch("iot_analysis.producer.connect", side_effect=pika.exceptions.AMQPConnectionError()) as connect,
            patch("iot_analysis.producer.with_retry", side_effect=self.no_wait_retry(sleep)),
        ):
            publisher = ReliablePublisher(self.settings)
            with self.assertRaises(pika.exceptions.AMQPConnectionError):
                publisher.send(self.sensor)
        self.assertEqual(connect.call_count, 6)
        self.assertEqual(sleep.call_args_list, [call(1), call(2), call(4), call(8), call(10)])
        self.assertIsNone(publisher.connection)

    def test_loss_during_heartbeat_pause_can_recover_on_next_measurement(self):
        lost = connection_with(Mock(is_closed=False))
        lost.sleep.side_effect = pika.exceptions.StreamLostError("between measurements")
        recovered_channel = Mock(is_closed=False)
        recovered = connection_with(recovered_channel)
        sleep = Mock()
        publisher = ReliablePublisher(self.settings)
        publisher.connection, publisher.channel = lost, lost.channel.return_value
        with (
            patch("iot_analysis.producer.emit_log"),
            patch("iot_analysis.producer.connect", side_effect=[pika.exceptions.AMQPConnectionError(), recovered]),
            patch("iot_analysis.producer.declare_topology", return_value=self.topology),
            patch("iot_analysis.producer.publish_json") as publish,
            patch("iot_analysis.producer.with_retry", side_effect=self.no_wait_retry(sleep)),
        ):
            publisher.pause(0.5)
            self.assertIsNone(publisher.connection)
            self.assertIsNone(publisher.channel)
            publisher.send(self.sensor)
        lost.close.assert_called_once_with()
        sleep.assert_called_once_with(1)
        publish.assert_called_once()
        self.assertIs(publisher.connection, recovered)

    def test_unroutable_message_is_not_blindly_retried(self):
        channel = Mock(is_closed=False)
        connection = connection_with(channel)
        sleep = Mock()
        with (
            patch("iot_analysis.producer.connect", return_value=connection),
            patch("iot_analysis.producer.declare_topology", return_value=self.topology),
            patch("iot_analysis.producer.publish_json", side_effect=pika.exceptions.UnroutableError([])) as publish,
            patch("iot_analysis.producer.with_retry", side_effect=self.no_wait_retry(sleep)),
        ):
            publisher = ReliablePublisher(self.settings)
            with self.assertRaises(pika.exceptions.UnroutableError):
                publisher.send(self.sensor)
        publish.assert_called_once()
        sleep.assert_not_called()
        connection.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
