"""Gemeinsame RabbitMQ-Konfiguration und Nachrichtenfunktionen."""

from datetime import datetime
import hashlib
import json
import math
import os
import re
import time

import pika

PREFIX = os.getenv("IOT_NAMESPACE", "iot")
SENSOR_EXCHANGE = f"{PREFIX}.sensors"   # Topic: verteilt nach Sensortyp.
EVENT_EXCHANGE = f"{PREFIX}.events"    # Direct: Alarme, Monitoring und DLQ.
ENV_QUEUE = f"{PREFIX}.environment"
MACHINE_QUEUE = f"{PREFIX}.machinery"
ALARM_QUEUE = f"{PREFIX}.alarms"
MONITOR_QUEUE = f"{PREFIX}.monitor"
DLQ = f"{PREFIX}.dlq"
QUEUES = (ENV_QUEUE, MACHINE_QUEUE, ALARM_QUEUE, MONITOR_QUEUE, DLQ)
PUBLISH_ERRORS = (pika.exceptions.AMQPError, OSError)


def declare_topology(channel):
    channel.exchange_declare(exchange=SENSOR_EXCHANGE, exchange_type="topic", durable=True)
    channel.exchange_declare(exchange=EVENT_EXCHANGE, exchange_type="direct", durable=True)
    for queue in QUEUES:
        arguments = {"x-queue-type": "classic"}
        if queue != DLQ:
            arguments.update({"x-message-ttl": 60000,
                              "x-dead-letter-exchange": EVENT_EXCHANGE,
                              "x-dead-letter-routing-key": "dead"})
        if queue == ALARM_QUEUE:
            arguments["x-max-priority"] = 9
        channel.queue_declare(queue=queue, durable=True, arguments=arguments)
    for sensor_type in ("temperature", "humidity"):
        channel.queue_bind(queue=ENV_QUEUE, exchange=SENSOR_EXCHANGE,
                           routing_key=f"sensors.{sensor_type}.*")
    channel.queue_bind(queue=MACHINE_QUEUE, exchange=SENSOR_EXCHANGE,
                       routing_key="sensors.vibration.*")
    for level in ("WARNING", "CRITICAL"):
        channel.queue_bind(queue=ALARM_QUEUE, exchange=EVENT_EXCHANGE, routing_key=level)
    channel.queue_bind(queue=MONITOR_QUEUE, exchange=EVENT_EXCHANGE, routing_key="monitor")
    channel.queue_bind(queue=DLQ, exchange=EVENT_EXCHANGE, routing_key="dead")


def open_channel():
    parameters = pika.URLParameters(os.getenv("AMQP_URL", "amqp://guest:guest@localhost:5672/%2F"))
    parameters.connection_attempts = 1
    parameters.socket_timeout = 3
    parameters.blocked_connection_timeout = 10
    parameters.heartbeat = 30
    connection = pika.BlockingConnection(parameters)
    try:
        channel = connection.channel()
        declare_topology(channel)
        channel.confirm_delivery()
        return connection, channel
    except Exception:
        close(connection)
        raise


def run_consumer(worker, *args):
    """Verbindungsabbrueche mit Backoff behandeln, dauerhafte Fehler weitergeben."""
    delay = 1
    while True:
        started = time.monotonic()
        try:
            return worker(*args)
        except (pika.exceptions.AMQPConnectionError, OSError) as error:
            recoverable = (
                type(error) is pika.exceptions.AMQPConnectionError
                or isinstance(error, (OSError, pika.exceptions.StreamLostError,
                                      pika.exceptions.AMQPHeartbeatTimeout,
                                      pika.exceptions.ConnectionBlockedTimeout))
                or (isinstance(error, pika.exceptions.ConnectionClosedByBroker)
                    and error.reply_code in (320, 541))
            )
            if not recoverable:
                raise
            # Nach einer stabilen Sitzung beginnt eine neue Backoff-Folge.
            if time.monotonic() - started >= 60:
                delay = 1
            print(f"Consumer-Verbindung unterbrochen: neuer Versuch in {delay}s ({error})", flush=True)
            time.sleep(delay)
            delay = min(delay * 2, 10)


def publish(channel, routing_key, payload, priority=0, exchange=EVENT_EXCHANGE):
    channel.basic_publish(
        exchange=exchange, routing_key=routing_key,
        body=json.dumps(payload, allow_nan=False).encode("utf-8"), mandatory=True,
        properties=pika.BasicProperties(content_type="application/json", delivery_mode=2,
                                        message_id=payload.get("message_id"), priority=priority))


def send_sensor(channel, sensor):
    publish(channel, f"sensors.{sensor['type']}.{sensor['sensor_id']}", sensor,
            exchange=SENSOR_EXCHANGE)


def emit_metric(channel, status, message_id, sensor_type="unknown", alarm=False,
                elapsed_ms=0, redelivered=False):
    publish(channel, "monitor", {"status": status, "message_id": message_id,
            "sensor_type": sensor_type, "alarm": alarm, "elapsed_ms": elapsed_ms,
            "redelivered": redelivered})


def parse_sensor(body, routing_key):
    """Pflichtfelder pruefen, bevor eine Messung verarbeitet wird."""
    try:
        sensor = json.loads(body)
        # Auch Zusatzfelder werden in Alarmen weitergeleitet und muessen JSON bleiben.
        json.dumps(sensor, allow_nan=False)
    except RecursionError as error:
        raise ValueError("JSON ist zu tief verschachtelt") from error
    if not isinstance(sensor, dict):
        raise ValueError("Ein JSON-Objekt ist erforderlich")
    sensor_id, kind, value = sensor.get("sensor_id"), sensor.get("type"), sensor.get("value")
    if not isinstance(sensor_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", sensor_id):
        raise ValueError("Ungueltige sensor_id")
    if kind not in ("temperature", "humidity", "vibration"):
        raise ValueError("Unbekannter Sensortyp")
    try:
        valid = isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
    except OverflowError:
        valid = False
    if not valid or (kind == "humidity" and not 0 <= value <= 100) or (kind == "vibration" and value < 0):
        raise ValueError("Ungueltiger Messwert")
    stamp = datetime.fromisoformat(sensor["timestamp"].replace("Z", "+00:00"))
    if stamp.utcoffset() is None:
        raise ValueError("timestamp braucht eine Zeitzone")
    if routing_key != f"sensors.{kind}.{sensor_id}":
        raise ValueError("Routing Key passt nicht zur Messung")
    sensor.setdefault("message_id", hashlib.sha256(body).hexdigest())
    message_id = sensor["message_id"]
    if not isinstance(message_id, str) or not message_id or len(message_id.encode("utf-8")) > 255:
        raise ValueError("Ungueltige message_id")
    return sensor


def close(connection):
    if connection is not None and connection.is_open:
        try:
            connection.close()
        except PUBLISH_ERRORS:
            pass
