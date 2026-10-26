"""Drei Sensoren simulieren; bei Verbindungsfehlern maximal fuenfmal wiederholen."""

from datetime import datetime, timezone
import random
import time
from uuid import uuid4

from common import PUBLISH_ERRORS, close, open_channel, send_sensor

SENSORS = {"temperature": (20, 115), "humidity": (10, 95), "vibration": (0, 13)}


def send_with_retry(connection, channel, sensor):
    for attempt in range(6):  # Ein Erstversuch und maximal fuenf Wiederholungen.
        try:
            if connection is None or connection.is_closed:
                connection, channel = open_channel()
            send_sensor(channel, sensor)
            return connection, channel
        except PUBLISH_ERRORS:
            close(connection)
            connection = channel = None
            if attempt == 5:
                raise
            delay = min(2 ** attempt, 10)
            print(f"Verbindung unterbrochen: Wiederholung {attempt + 1}/5 in {delay}s", flush=True)
            time.sleep(delay)


def main():
    connection = channel = None
    try:
        while True:
            for kind, (low, high) in SENSORS.items():
                sensor = {"sensor_id": f"{kind}-01", "type": kind,
                          "timestamp": datetime.now(timezone.utc).isoformat(),
                          "value": round(random.uniform(low, high), 2), "message_id": str(uuid4())}
                connection, channel = send_with_retry(connection, channel, sensor)
                print(f"GESENDET {kind}: {sensor['value']}", flush=True)
                try:
                    connection.sleep(0.5)  # Pika bedient dabei auch die Heartbeats.
                except PUBLISH_ERRORS:
                    close(connection)
                    connection = channel = None
    finally:
        close(connection)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
