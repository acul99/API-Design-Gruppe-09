"""Kennzahlen ausschliesslich aus den ueber RabbitMQ empfangenen Ereignissen zaehlen."""

from collections import Counter
import json
import math
import time

from common import MONITOR_QUEUE, close, open_channel


def main():
    connection, channel = open_channel()
    channel.basic_qos(prefetch_count=100)
    processed = alarms = failures = 0
    total_ms = 0.0
    per_type = Counter()
    next_report = time.monotonic() + 5
    print("BEREIT monitor", flush=True)
    try:
        for method, _, body in channel.consume(MONITOR_QUEUE, inactivity_timeout=0.2, auto_ack=False):
            if time.monotonic() >= next_report:
                average = total_ms / processed if processed else 0
                print(f"MONITOR processed={processed} per_type={dict(per_type)} "
                      f"alarms={alarms} failures={failures} average_ms={average:.2f}", flush=True)
                next_report = time.monotonic() + 5
            if method is None:
                continue
            try:
                metric = json.loads(body)
                status = metric["status"]
                if status == "processed":
                    kind, elapsed, alarm = metric["sensor_type"], metric["elapsed_ms"], metric["alarm"]
                    if kind not in ("temperature", "humidity", "vibration"):
                        raise ValueError("Unbekannter Sensortyp")
                    if isinstance(elapsed, bool) or not isinstance(elapsed, (int, float)) or not math.isfinite(elapsed) or elapsed < 0:
                        raise ValueError("Ungueltige Verarbeitungszeit")
                    processed += 1
                    per_type[kind] += 1
                    alarms += bool(alarm)
                    total_ms += elapsed
                elif status == "failed":
                    failures += 1
                elif status != "aggregation":
                    raise ValueError("Unbekanntes Ereignis")
            except (ValueError, KeyError, TypeError, OverflowError):
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            else:
                channel.basic_ack(delivery_tag=method.delivery_tag)
    finally:
        close(connection)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
