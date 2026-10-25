"""Producer mit gezielt fehlerhaften Nachrichten: jede 5. Nachricht ist ungueltig,
die Fehlerart wechselt reihum. Erwartung im Consumer: NACK ohne Requeue -> DLQ.

Start: python faulty_producer.py
"""
import json
import random
import time
import uuid
from datetime import datetime, timezone

import pika

from common import SENSOR_EXCHANGE, connect, declare_topology

EVERY = 5
SENSORS = {"temperature": (-10, 110, "temp-01"), "humidity": (30, 100, "hum-01"),
           "pressure": (900, 1100, "pres-01")}


def build(n):
    """Nachricht Nr. n (ab 1) -> (routing_key, body, fehlerhaft)."""
    sensor_type = list(SENSORS)[(n - 1) % 3]
    low, high, sensor_id = SENSORS[sensor_type]
    msg = {"message_id": str(uuid.uuid4()), "sensor_type": sensor_type, "sensor_id": sensor_id,
           "value": round(random.uniform(low, high), 2),
           "timestamp": datetime.now(timezone.utc).isoformat()}
    key = f"sensor.{sensor_type}.{sensor_id}"
    if n % EVERY:
        return key, json.dumps(msg).encode(), False

    kind = (n // EVERY - 1) % 5
    if kind == 0:
        return key, b'{"sensor_id": "x", "value": 2', True        # kein gueltiges JSON
    if kind == 1:
        del msg["value"]                                           # Feld fehlt
    elif kind == 2:
        msg["value"] = "abc"                                       # kein Zahlenwert
    elif kind == 3:
        msg["timestamp"] = "gestern"                               # kein ISO-Zeitstempel
    else:                                                          # Typ aus dem anderen Consumer
        msg["sensor_type"] = "pressure" if sensor_type != "pressure" else "temperature"
    return key, json.dumps(msg).encode(), True


def main():
    ch = connect().channel()
    declare_topology(ch)
    ch.confirm_delivery()
    n = 0
    try:
        while True:
            n += 1
            key, body, faulty = build(n)
            ch.basic_publish(SENSOR_EXCHANGE, key, body,
                             pika.BasicProperties(delivery_mode=2), mandatory=True)
            print("FEHLERHAFT" if faulty else "gesendet  ", key)
            time.sleep(2)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
