
"""Analyse-Consumer.

Start:
    python analyse_consumer.py climate      # temperature, humidity
    python analyse_consumer.py pressure     # pressure

Reproduzierbarer Verarbeitungsfehler:
    FAIL_SENSOR_ID=temp-01 python analyse_consumer.py climate
    -> Nachricht von temp-01 schlägt immer fehl: 1. Zustellung wird requeued,
       2. Zustellung (redelivered=True) wird per NACK ohne Requeue in die DLQ geleitet.
"""
import json
import logging
import math
import operator
import os
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone

import pika

from common import (
    ALERT_EXCHANGE,
    MONITOR_EXCHANGE,
    QUEUE_CLIMATE,
    QUEUE_PRESSURE,
    SEVERITY_PRIORITY,
    connect,
    declare_topology,
    publish_json,
)

WINDOW_SECONDS = 10
PREFETCH = 50
RECONNECT_DELAY = 5
FAIL_SENSOR_ID = os.getenv("FAIL_SENSOR_ID")

REQUIRED_FIELDS = ("sensor_id", "timestamp", "type", "value")

OPS = {">": operator.gt, "<": operator.lt}

# (Operator, Schwellwert, Severity). Reihenfolge zählt: erster Treffer gewinnt,
# deshalb steht CRITICAL immer vor WARNING.
RULES = {
    "temperature": [(">", 100, "CRITICAL"), (">", 80, "WARNING")],
    "humidity": [(">", 98, "CRITICAL"), (">", 90, "WARNING")],
    "pressure": [
        ("<", 910, "CRITICAL"), (">", 1090, "CRITICAL"),
        ("<", 920, "WARNING"), (">", 1080, "WARNING"),
    ],
}

PROFILES = {
    "climate": {"queue": QUEUE_CLIMATE, "types": {"temperature", "humidity"}},
    "pressure": {"queue": QUEUE_PRESSURE, "types": {"pressure"}},
}


class InvalidMessage(Exception):
    """Dauerhaft unverarbeitbar: Retry sinnlos, direkt in die DLQ."""


def validate(body, allowed_types):
    """Validiert eine Sensornachricht hinsichtlich JSON-Format, Pflichtfeldern, Sensortyp, Messwert und Zeitstempel."""
    try:
        msg = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        raise InvalidMessage("kein gueltiges JSON")
    if not isinstance(msg, dict):
        raise InvalidMessage("JSON ist kein Objekt")
    # Der aktuelle Producer sendet "sensor_type", die Aufgabenstellung verlangt "type".
    # Alias bleibt, bis der Producer auf "type" umgestellt ist.
    if "type" not in msg and "sensor_type" in msg:
        msg["type"] = msg["sensor_type"]
    missing = [f for f in REQUIRED_FIELDS if f not in msg]
    if missing:
        raise InvalidMessage(f"Felder fehlen: {missing}")
    if msg["type"] not in allowed_types:
        raise InvalidMessage(f"unerwarteter Sensortyp: {msg['type']!r}")
    value = msg["value"]
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise InvalidMessage(f"value nicht numerisch: {value!r}")
    try:
        datetime.fromisoformat(str(msg["timestamp"]).replace("Z", "+00:00"))
    except ValueError:
        raise InvalidMessage(f"timestamp nicht ISO-8601: {msg['timestamp']!r}")
    return msg


def detect_anomaly(msg):
    """Prueft einen Messwert anhand definierter Schwellwerte und gibt bei einer Anomalie Severity und Bedingung zurueck."""
    for op, threshold, severity in RULES.get(msg["type"], []):
        if OPS[op](msg["value"], threshold):
            return severity, f"{op} {threshold}"
    return None


class TumblingWindow:
    """Zusammenhängende 10-Sekunden-Fenster, nach Ablauf wird ein neues begonnen."""

    def __init__(self, seconds):
        """Initialisiert ein Zeitfenster mit der angegebenen Dauer."""
        self.seconds = seconds
        self._reset()

    def _reset(self):
        """Setzt Startzeitpunkte und gespeicherte Messwerte fuer ein neues Zeitfenster zurueck."""
        self.started = time.monotonic()
        self.started_wall = datetime.now(timezone.utc)
        self.values = defaultdict(list)

    def add(self, sensor_type, value):
        """Fuegt einen Messwert des angegebenen Sensortyps zum aktuellen Zeitfenster hinzu."""
        self.values[sensor_type].append(value)

    def due(self):
        """Prueft, ob die festgelegte Dauer des aktuellen Zeitfensters abgelaufen ist."""
        return time.monotonic() - self.started >= self.seconds

    def flush(self):
        """Berechnet Anzahl, Durchschnitt, Minimum und Maximum je Sensortyp und startet ein neues Zeitfenster."""
        results = []
        for sensor_type, vals in self.values.items():
            results.append(
                {
                    "type": sensor_type,
                    "window_start": self.started_wall.isoformat(),
                    "window_seconds": self.seconds,
                    "count": len(vals),
                    "avg": round(sum(vals) / len(vals), 3),
                    "min": min(vals),
                    "max": max(vals),
                }
            )
        self._reset()
        return results


def emit(ch, event, **fields):
    """Monitoring-Event ueber RabbitMQ. Telemetrie darf die Verarbeitung nie stören."""
    payload = {"event": event, "ts": datetime.now(timezone.utc).isoformat(), **fields}
    try:
        print("Monitoring")
        publish_json(ch, MONITOR_EXCHANGE, f"monitor.{event}", payload, persistent=False)
    except pika.exceptions.AMQPError as exc:
        log.warning("Monitoring-Event %s nicht gesendet: %s", event, exc)


def handle(ch, method, body, profile, window, name):
    """Verarbeitet eine Sensornachricht, erkennt Anomalien, versendet Alarme und behandelt Fehler mittels ACK/NACK."""
    tag = method.delivery_tag
    started = time.perf_counter()
    try:
        msg = validate(body, profile["types"])
        if FAIL_SENSOR_ID and msg["sensor_id"] == FAIL_SENSOR_ID:
            raise RuntimeError(f"simulierter Verarbeitungsfehler fuer {FAIL_SENSOR_ID}")

        anomaly = detect_anomaly(msg)
        if anomaly:
            severity, condition = anomaly
            alert = {
                "alert_id": str(uuid.uuid4()),  # Idempotenzschluessel fuer den Alarm-Consumer
                "severity": severity,
                "sensor_id": msg["sensor_id"],
                "type": msg["type"],
                "value": msg["value"],
                "condition": f"{msg['type']} {condition}",
                "measured_at": msg["timestamp"],
                "raised_at": datetime.now(timezone.utc).isoformat(),
                "source": name,
                "source_message_id": msg.get("message_id"),
            }
            # Publisher Confirm + mandatory: Alarm muss im Broker angekommen und geroutet sein,
            # erst danach wird die Quellnachricht bestätigt.
            publish_json(
                ch,
                ALERT_EXCHANGE,
                severity,
                alert,
                persistent=True,
                priority=SEVERITY_PRIORITY[severity],
                mandatory=True,
            )
            log.warning("ALARM %s: %s=%s (Regel: %s) von %s", severity, msg["type"], msg["value"], condition, msg["sensor_id"])
            emit(ch, "alert", consumer=name, severity=severity, type=msg["type"], sensor_id=msg["sensor_id"])

        # Letzter Schritt vor dem ACK, kann nicht fehlschlagen: so wird bei einem Retry
        # kein Wert doppelt ins Fenster aufgenommen.
        window.add(msg["type"], msg["value"])

    except InvalidMessage as exc:
        log.error("Ungueltige Nachricht, NACK ohne Requeue -> DLQ: %s", exc)
        ch.basic_nack(tag, requeue=False)
        emit(ch, "failed", consumer=name, reason="invalid", detail=str(exc))
        return
    except pika.exceptions.AMQPConnectionError:
        raise  # Verbindung weg: nichts bestätigen, Reconnect uebernimmt die äussere Schleife
    except Exception as exc:
        if method.redelivered:
            log.error("Zweiter Fehlschlag (%s), NACK ohne Requeue -> DLQ", exc)
            ch.basic_nack(tag, requeue=False)
            emit(ch, "failed", consumer=name, reason="retries_exhausted", detail=str(exc))
        else:
            log.warning("Verarbeitungsfehler (%s), NACK mit Requeue (1 Wiederholung)", exc)
            ch.basic_nack(tag, requeue=True)
            emit(ch, "failed", consumer=name, reason="transient", detail=str(exc))
        return

    ch.basic_ack(tag)
    emit(
        ch,
        "processed",
        consumer=name,
        type=msg["type"],
        sensor_id=msg["sensor_id"],
        processing_ms=round((time.perf_counter() - started) * 1000, 3),
    )


def flush_window(ch, window, name):
    """Schliesst das aktuelle Zeitfenster ab und veröffentlicht dessen aggregierte Messwerte als Monitoring-Events."""
    for result in window.flush():
        log.info(
            "AGGREGAT %ss %s: n=%d avg=%s min=%s max=%s",
            result["window_seconds"], result["type"], result["count"],
            result["avg"], result["min"], result["max"],
        )
        emit(ch, "aggregate", consumer=name, **result)


def consume(profile, name):
    """Verbindet sich mit RabbitMQ, konsumiert Sensornachrichten und verarbeitet regelmässig abgelaufene Zeitfenster."""
    conn = connect()
    ch = conn.channel()
    declare_topology(ch)
    ch.confirm_delivery()
    ch.basic_qos(prefetch_count=PREFETCH)
    window = TumblingWindow(WINDOW_SECONDS)
    log.info("[%s] warte auf Nachrichten in %s", name, profile["queue"])
    try:
        # inactivity_timeout sorgt dafuer, dass das Fenster auch ohne Traffic abgeschlossen wird
        for method, _props, body in ch.consume(profile["queue"], inactivity_timeout=1):
            if method is not None:
                handle(ch, method, body, profile, window, name)
            if window.due():
                flush_window(ch, window, name)
    finally:
        if conn.is_open:
            conn.close()


def main():
    """Prueft das gewählte Consumer-Profil, konfiguriert das Logging und startet den Consumer mit automatischem Reconnect."""
    if len(sys.argv) != 2 or sys.argv[1] not in PROFILES:
        sys.exit(f"Aufruf: python analyse_consumer.py [{'|'.join(PROFILES)}]")
    name = sys.argv[1]
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")
    global log
    log = logging.getLogger(f"analysis.{name}")
    while True:
        try:
            consume(PROFILES[name], name)
        except KeyboardInterrupt:
            log.info("beendet")
            return
        except pika.exceptions.AMQPError as exc:
            log.warning("Verbindung verloren (%s), Reconnect in %ss", exc, RECONNECT_DELAY)
            time.sleep(RECONNECT_DELAY)


log = logging.getLogger("analysis")

if __name__ == "__main__":
    main()
