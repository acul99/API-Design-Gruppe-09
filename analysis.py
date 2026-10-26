"""Zwei Fachanalysen: environment fuer Temperatur/Feuchte, machinery fuer Vibration."""

import argparse
import math
import time

from common import (ENV_QUEUE, MACHINE_QUEUE, close,
                    emit_metric, open_channel, parse_sensor, publish, run_consumer)


def alarm_level(sensor):
    kind, value = sensor["type"], sensor["value"]
    if kind == "temperature":
        return "CRITICAL" if value > 100 else "WARNING" if value > 80 else None
    if kind == "humidity":
        return "CRITICAL" if value < 15 or value > 85 else "WARNING" if value < 30 or value > 70 else None
    return "CRITICAL" if value > 10 else "WARNING" if value > 5 else None


def summarize(values):
    return {"count": len(values), "average": math.fsum(value / len(values) for value in values),
            "min": min(values), "max": max(values)}


def main(kind, delay=0):
    connection, channel = open_channel()
    queue = ENV_QUEUE if kind == "environment" else MACHINE_QUEUE
    window_end = time.monotonic() + 10
    pending = []
    try:
        channel.basic_qos(prefetch_count=100)
        print(f"BEREIT {kind}", flush=True)
        for method, properties, body in channel.consume(queue, inactivity_timeout=0.2, auto_ack=False):
            # Der Timer laeuft auch dann, wenn gerade keine neue Nachricht kommt.
            if time.monotonic() >= window_end:
                if pending:
                    result = summarize([sensor["value"] for _, sensor, _, _, _ in pending])
                    publish(channel, "monitor", {"status": "aggregation", "window_seconds": 10, **result})
                    print(f"Tumbling Window: {result}", flush=True)
                    for tag, sensor, alarm, elapsed, redelivered in pending:
                        emit_metric(channel, "processed", sensor["message_id"], sensor["type"],
                                    alarm, elapsed, redelivered)
                        channel.basic_ack(delivery_tag=tag)
                        print(f"VERARBEITET {sensor['message_id']}", flush=True)
                    pending.clear()
                # Zusammenhaengende Fenster ohne allmaehliche Zeitverschiebung.
                while time.monotonic() >= window_end:
                    window_end += 10
            if method is None:
                continue
            message_id = properties.message_id or "ohne-id"
            print(f"EMPFANG {message_id} redelivered={method.redelivered}", flush=True)
            started = time.perf_counter()
            if delay:
                connection.sleep(delay)  # Nur fuer den reproduzierbaren Crash-Test.
            try:
                sensor = parse_sensor(body, method.routing_key)
                level = alarm_level(sensor)
            except (ValueError, KeyError, TypeError, AttributeError) as error:
                emit_metric(channel, "failed", message_id)
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                print(f"ABGELEHNT {message_id}: {error} -> DLQ", flush=True)
                continue
            if level:
                priority = 9 if level == "CRITICAL" else 3
                publish(channel, level, {**sensor, "level": level, "priority": priority}, priority)
            elapsed = (time.perf_counter() - started) * 1000
            if sensor["type"] == "temperature":
                # Erst nach dem bestaetigten Aggregat bestaetigen, nicht schon im RAM.
                pending.append((method.delivery_tag, sensor, bool(level), elapsed, method.redelivered))
            else:
                emit_metric(channel, "processed", sensor["message_id"], sensor["type"],
                            bool(level), elapsed, method.redelivered)
                channel.basic_ack(delivery_tag=method.delivery_tag)
                print(f"VERARBEITET {sensor['message_id']}", flush=True)
    finally:
        # Bei Publish-/Verbindungsfehlern bleiben unbestaetigte Messungen zustellbar.
        close(connection)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=("environment", "machinery"))
    parser.add_argument("--delay", type=float, default=0, help="Verzoegerung fuer die Crash-Demo")
    args = parser.parse_args()
    if args.delay < 0:
        parser.error("delay darf nicht negativ sein")
    try:
        run_consumer(main, args.kind, args.delay)
    except KeyboardInterrupt:
        pass
