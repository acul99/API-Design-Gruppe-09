"""Drei simulierte Sensoren; jede Messung wird bestaetigt veroeffentlicht."""

import random
import time
from uuid import uuid4

from .messaging import (RETRYABLE_EXCEPTIONS, close_quietly, connect,
                        emit_log, publish_json, with_retry)
from .models import SENSOR_TYPES, utc_now
from .topology import declare_topology


def generate_sensor(kind, index, rng):
    ranges = {"temperature": (20, 115), "humidity": (10, 95), "vibration": (0, 13)}
    return {"message_id": str(uuid4()), "sensor_id": f"{kind}-01", "timestamp": utc_now(),
            "type": kind, "value": round(rng.uniform(*ranges[kind]), 2)}


class ReliablePublisher:
    def __init__(self, settings):
        self.settings, self.connection, self.channel, self.topology = settings, None, None, None

    def _ready(self):
        if self.connection is None or self.connection.is_closed or self.channel is None or self.channel.is_closed:
            self.connection = connect(self.settings)
            self.channel = self.connection.channel()
            self.topology = declare_topology(self.channel, self.settings)
            self.channel.confirm_delivery()

    def send(self, sensor):
        def operation():
            try:
                self._ready()
                publish_json(self.channel, self.topology.sensors,
                             f"sensors.{sensor['type']}.{sensor['sensor_id']}", sensor,
                             message_id=sensor["message_id"])
            except Exception:
                self.close()
                raise
        # Dasselbe Objekt und dieselbe ID werden bei unklarer Bestaetigung erneut gesendet.
        with_retry(operation)

    def pause(self, seconds):
        if self.connection and self.connection.is_open:
            try:
                self.connection.sleep(seconds)
            except RETRYABLE_EXCEPTIONS as exc:
                # Ein Abbruch kann auch bei Heartbeats zwischen zwei Messungen
                # auffallen. Die naechste send()-Operation verbindet mit Backoff.
                emit_log("producer_connection_lost", error=type(exc).__name__)
                self.close()
        else:
            time.sleep(seconds)

    def close(self):
        close_quietly(self.connection)
        self.connection = self.channel = None


def run_producer(settings, *, count=0, interval=1.0, seed=42):
    if count < 0 or interval < 0:
        raise ValueError("count und interval duerfen nicht negativ sein")
    rng, publisher, sent = random.Random(seed), ReliablePublisher(settings), 0
    try:
        while count == 0 or sent < count:
            sensor = generate_sensor(SENSOR_TYPES[sent % len(SENSOR_TYPES)], sent, rng)
            publisher.send(sensor)
            sent += 1
            emit_log("sensor_sent", **sensor)
            if count == 0 or sent < count:
                publisher.pause(interval)
    finally:
        publisher.close()
