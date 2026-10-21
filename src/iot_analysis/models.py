"""Kleine Nachrichtenvertraege und fachliche Alarmregeln."""

from datetime import datetime, timezone
import hashlib
import json
import math
import re
from uuid import uuid4

SENSOR_TYPES = ("temperature", "humidity", "vibration")


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def encode_json(payload):
    return json.dumps(payload, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")


def parse_sensor(body, routing_key):
    try:
        sensor = json.loads(body)
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValueError("Nachricht ist kein gueltiges UTF-8-JSON") from exc
    if not isinstance(sensor, dict):
        raise ValueError("JSON-Objekt erforderlich")
    sensor_id = sensor.get("sensor_id")
    if not isinstance(sensor_id, str) or not re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", sensor_id):
        raise ValueError("sensor_id muss ein Routing-Key-segmentfaehiger Bezeichner sein")
    kind = sensor.get("type")
    if kind not in SENSOR_TYPES:
        raise ValueError("Unbekannter Sensortyp")
    value = sensor.get("value")
    try:
        valid_number = not isinstance(value, bool) and isinstance(value, (int, float)) and math.isfinite(value)
    except OverflowError:
        valid_number = False
    if not valid_number:
        raise ValueError("value muss eine endliche Zahl sein")
    if kind == "humidity" and not 0 <= value <= 100:
        raise ValueError("Luftfeuchtigkeit muss zwischen 0 und 100 liegen")
    if kind == "vibration" and value < 0:
        raise ValueError("Vibration darf nicht negativ sein")
    try:
        stamp = datetime.fromisoformat(sensor["timestamp"].replace("Z", "+00:00"))
        if stamp.tzinfo is None or stamp.utcoffset() is None:
            raise ValueError("Zeitzone fehlt")
    except (KeyError, TypeError, AttributeError, ValueError) as exc:
        raise ValueError("timestamp muss ISO-8601 mit Zeitzone sein") from exc
    if routing_key != f"sensors.{kind}.{sensor_id}":
        raise ValueError("Routing Key und Payload stimmen nicht ueberein")
    # message_id ist eine kompatible Erweiterung, kein zusaetzliches Pflichtfeld.
    if "message_id" not in sensor:
        sensor["message_id"] = hashlib.sha256(body).hexdigest()
    elif not isinstance(sensor["message_id"], str) or not sensor["message_id"]:
        raise ValueError("message_id muss bei Angabe eine nichtleere Zeichenfolge sein")
    return sensor


def alarm_for(sensor):
    value, kind = sensor["value"], sensor["type"]
    level = None
    if kind == "temperature":
        level = "CRITICAL" if value > 100 else "WARNING" if value > 80 else None
    elif kind == "humidity":
        level = "CRITICAL" if value < 15 or value > 85 else "WARNING" if value < 30 or value > 70 else None
    elif kind == "vibration":
        level = "CRITICAL" if value > 10 else "WARNING" if value > 5 else None
    if level is None:
        return None
    alarm_id = f"alarm:{sensor['message_id']}:{level}"
    return {"alarm_id": alarm_id, "message_id": alarm_id,
            "sensor_message_id": sensor["message_id"], "sensor_id": sensor["sensor_id"],
            "timestamp": utc_now(), "type": kind, "value": value, "level": level,
            "priority": 9 if level == "CRITICAL" else 3}


def make_event(kind, service, message_id, **fields):
    return {"event_id": str(uuid4()), "kind": kind, "service": service,
            "message_id": message_id, "timestamp": utc_now(), **fields}
