"""Analyse, Alarmverarbeitung und Monitoring mit manuellen RabbitMQ-ACKs.

Keine fachlich fehlerhafte Nachricht wird mit requeue=True zurückgeschickt.
Ein echter Verbindungsabbruch lässt unbestätigte Lieferungen dagegen beim Broker;
nach dem Wiederverbinden werden sie erneut zugestellt (mindestens einmal).
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import time
from typing import Any, Callable

import pika

from .analytics import MonitoringStats, TemperatureWindow
from .config import Settings
from .messaging import (
    RETRYABLE_EXCEPTIONS,
    close_quietly,
    emit_log,
    publish_json,
    retry_connect,
)
from .models import alarm_for, make_event, parse_sensor
from .topology import Topology, declare_topology


# Ein fehlgeschlagener ausgehender Publish ist kein fachlicher Fehler der
# Eingangsnachricht. Sie bleibt unbestätigt und wird nach Wiederanlauf zugestellt.
SESSION_EXCEPTIONS = RETRYABLE_EXCEPTIONS + (
    pika.exceptions.NackError,
    pika.exceptions.UnroutableError,
)


@dataclass
class PendingDelivery:
    tag: int
    sensor: dict[str, Any]
    event: dict[str, Any]
    redelivered: bool


def _message_id(body: bytes, properties: Any) -> str:
    return getattr(properties, "message_id", None) or hashlib.sha256(body).hexdigest()


def _telemetry(channel: Any, topology: Topology, event: dict[str, Any]) -> None:
    publish_json(channel, topology.telemetry, "", event, message_id=event["event_id"])


def _reject(
    channel: Any,
    topology: Topology,
    service: str,
    tag: int,
    message_id: str,
    error: Exception,
    *,
    sensor_type: str = "unknown",
    attempts: int = 1,
) -> None:
    # Erst das Monitoring veröffentlichen, dann die Originalnachricht ablehnen.
    # Ein Transportfehler lässt das Original somit unbestätigt.
    event = make_event(
        "processing_failed", service, message_id,
        sensor_type=sensor_type, error=str(error), attempts=attempts,
    )
    _telemetry(channel, topology, event)
    channel.basic_nack(delivery_tag=tag, requeue=False)
    emit_log("rejected", service=service, id=message_id, error=str(error), attempts=attempts, requeue=False)


def _with_reconnect(settings: Settings, service: str, session: Callable[[Any], None]) -> None:
    """Verbindet neu; jede Verbindungsphase verwendet den begrenzten Backoff."""
    failures = 0
    while True:
        # Ausgeschoepfte Verbindungsversuche nicht nochmals durch die
        # Session-Schleife vervielfachen: retry_connect hat sein eigenes Limit.
        connection = retry_connect(settings)
        try:
            session(connection)
            return
        except KeyboardInterrupt:
            emit_log("stopped", service=service)
            return
        except SESSION_EXCEPTIONS as error:
            failures += 1
            if failures > 5:
                raise
            delay = min(2 ** (failures - 1), 10)
            emit_log("consumer_reconnect", service=service, retry=failures, delay_seconds=delay, error=str(error))
        finally:
            close_quietly(connection)
        time.sleep(delay)


def run_analysis(
    settings: Settings,
    kind: str,
    *,
    max_messages: int = 0,
    processing_delay: float = 0,
) -> None:
    """Analysiert Umwelt- oder Maschinendaten aus getrennten Topic-Bindings."""
    if kind not in {"environment", "machinery"}:
        raise ValueError("Analyse muss environment oder machinery sein.")
    if max_messages < 0 or processing_delay < 0:
        raise ValueError("max_messages und processing_delay dürfen nicht negativ sein.")
    topology = Topology(settings.namespace)
    queue = topology.environment_queue if kind == "environment" else topology.machinery_queue
    acknowledged = 0

    def session(connection: Any) -> None:
        nonlocal acknowledged
        channel = connection.channel()
        declare_topology(channel, settings)
        channel.confirm_delivery()
        channel.basic_qos(prefetch_count=100 if kind == "environment" else 10)
        window = TemperatureWindow(seconds=settings.window_seconds)

        def flush_window() -> None:
            nonlocal acknowledged
            result = window.summary()
            if result is not None:
                event = make_event("aggregation", kind, "temperature-window", sensor_type="temperature", **result)
                _telemetry(channel, topology, event)
                # Jede Lieferbestätigung erfolgt erst nach allen für diese
                # Messung vorgesehenen bestätigten Veröffentlichungen.
                for pending in window.pending:
                    _telemetry(channel, topology, pending.event)
                    channel.basic_ack(delivery_tag=pending.tag)
                    acknowledged += 1
                    emit_log(
                        "processed", service=kind, id=pending.sensor["message_id"],
                        type=pending.sensor["type"], redelivered=pending.redelivered,
                    )
                emit_log("aggregation", service=kind, type="temperature", **result)
            window.advance()

        emit_log("consumer_ready", service=kind, queue=queue)
        for method, properties, body in channel.consume(queue, inactivity_timeout=0.2, auto_ack=False):
            if window.due():
                flush_window()
            if max_messages and acknowledged >= max_messages:
                break
            if method is None:
                continue
            message_id = _message_id(body, properties)
            emit_log(
                "delivery_received", service=kind, id=message_id,
                redelivered=method.redelivered, routing_key=method.routing_key,
            )
            started = time.perf_counter()
            if processing_delay:
                connection.sleep(processing_delay)
            try:
                sensor = parse_sensor(body, method.routing_key)
            except (ValueError, TypeError, UnicodeDecodeError) as error:
                _reject(channel, topology, kind, method.delivery_tag, message_id, error)
                acknowledged += 1
                continue
            sensor["message_id"] = message_id
            attempts = settings.process_attempts
            for attempt in range(1, attempts + 1):
                try:
                    failure = (getattr(properties, "headers", None) or {}).get("x-demo-fail")
                    if failure == "permanent" or (failure == "transient" and attempt == 1):
                        raise RuntimeError("Absichtlich ausgelöster Verarbeitungsfehler: " + failure)
                    alarm = alarm_for(sensor)
                    if alarm:
                        publish_json(
                            channel, topology.alarms, alarm["level"], alarm,
                            message_id=alarm["alarm_id"], priority=alarm["priority"],
                        )
                    event = make_event(
                        "sensor_processed", kind, message_id,
                        sensor_type=sensor["type"], alarm=bool(alarm),
                        processing_ms=round((time.perf_counter() - started) * 1000, 3),
                        redelivered=method.redelivered,
                    )
                    if sensor["type"] == "temperature":
                        window.add(sensor["value"], PendingDelivery(method.delivery_tag, sensor, event, method.redelivered))
                    else:
                        _telemetry(channel, topology, event)
                        channel.basic_ack(delivery_tag=method.delivery_tag)
                        acknowledged += 1
                        emit_log("processed", service=kind, id=message_id, type=sensor["type"], redelivered=method.redelivered)
                    break
                except SESSION_EXCEPTIONS:
                    raise
                except Exception as error:
                    if attempt == attempts:
                        _reject(
                            channel, topology, kind, method.delivery_tag, message_id, error,
                            sensor_type=sensor["type"], attempts=attempt,
                        )
                        acknowledged += 1
                    else:
                        emit_log("processing_retry", service=kind, id=message_id, attempt=attempt, error=str(error))
                        connection.sleep(min(0.1 * 2 ** (attempt - 1), 1.0))
            if max_messages and acknowledged >= max_messages:
                break
        # Beim bewussten Stop vor Fensterende bleiben Temperaturmessungen
        # unbestätigt und werden durch Connection.close erneut zustellbar.

    _with_reconnect(settings, kind, session)


def run_alarm(settings: Settings, *, max_messages: int = 0) -> None:
    """Verarbeitet die Prioritätsqueue mit Prefetch 1 und manuellen ACKs."""
    if max_messages < 0:
        raise ValueError("max_messages darf nicht negativ sein.")
    topology = Topology(settings.namespace)
    acknowledged = 0

    def session(connection: Any) -> None:
        nonlocal acknowledged
        channel = connection.channel()
        declare_topology(channel, settings)
        channel.confirm_delivery()
        channel.basic_qos(prefetch_count=1)
        emit_log("consumer_ready", service="alarm", queue=topology.alarm_queue)
        for method, properties, body in channel.consume(topology.alarm_queue, inactivity_timeout=0.2, auto_ack=False):
            if method is None:
                continue
            message_id = _message_id(body, properties)
            try:
                alarm = json.loads(body)
                if not isinstance(alarm, dict) or alarm.get("level") not in {"WARNING", "CRITICAL"}:
                    raise ValueError("Alarm benötigt level WARNING oder CRITICAL.")
                expected_priority = 9 if alarm["level"] == "CRITICAL" else 3
                if getattr(properties, "priority", None) != expected_priority:
                    raise ValueError("AMQP-Priorität passt nicht zur Alarmstufe.")
                emit_log("alarm_processed", id=message_id, level=alarm["level"], priority=expected_priority, alarm=alarm)
                event = make_event("alarm_processed", "alarm", message_id, level=alarm["level"], priority=expected_priority)
                _telemetry(channel, topology, event)
                channel.basic_ack(delivery_tag=method.delivery_tag)
            except SESSION_EXCEPTIONS:
                raise
            except (ValueError, TypeError, UnicodeDecodeError) as error:
                _reject(channel, topology, "alarm", method.delivery_tag, message_id, error)
            acknowledged += 1
            if max_messages and acknowledged >= max_messages:
                break

    _with_reconnect(settings, "alarm", session)


def run_monitor(settings: Settings, *, max_messages: int = 0) -> None:
    """Ermittelt Kennzahlen ausschließlich aus RabbitMQ-Telemetrie-Nachrichten."""
    if max_messages < 0:
        raise ValueError("max_messages darf nicht negativ sein.")
    topology = Topology(settings.namespace)
    stats = MonitoringStats()
    acknowledged = 0

    def session(connection: Any) -> None:
        nonlocal acknowledged
        channel = connection.channel()
        declare_topology(channel, settings)
        channel.basic_qos(prefetch_count=100)
        next_report = time.monotonic() + settings.report_seconds
        emit_log("consumer_ready", service="monitoring", queue=topology.monitoring_queue)
        for method, properties, body in channel.consume(topology.monitoring_queue, inactivity_timeout=0.2, auto_ack=False):
            if time.monotonic() >= next_report:
                emit_log("monitoring_report", **stats.snapshot())
                next_report = time.monotonic() + settings.report_seconds
            if method is None:
                continue
            try:
                event = json.loads(body)
                if not isinstance(event, dict):
                    raise ValueError("Telemetrie muss ein JSON-Objekt sein.")
                stats.observe(event)
                channel.basic_ack(delivery_tag=method.delivery_tag)
            except (ValueError, TypeError, UnicodeDecodeError) as error:
                # Kein Telemetrie-Fehlerereignis über denselben Kanal erzeugen:
                # Sonst könnte ungültige Telemetrie eine Rückkopplung auslösen.
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                emit_log("monitoring_rejected", id=_message_id(body, properties), error=str(error))
            acknowledged += 1
            if max_messages and acknowledged >= max_messages:
                break
        emit_log("monitoring_report", **stats.snapshot())

    _with_reconnect(settings, "monitoring", session)
