"""Alarme aus der Prioritaetsqueue bearbeiten."""

import json

from common import ALARM_QUEUE, close, emit_metric, open_channel


def main():
    connection, channel = open_channel()
    channel.basic_qos(prefetch_count=1)
    print("BEREIT alarm", flush=True)
    try:
        for method, properties, body in channel.consume(ALARM_QUEUE, auto_ack=False):
            try:
                alarm = json.loads(body)
                level = alarm["level"]
                if level not in ("WARNING", "CRITICAL"):
                    raise ValueError("Unbekannte Alarmstufe")
                if properties.priority != (9 if level == "CRITICAL" else 3):
                    raise ValueError("Falsche AMQP-Prioritaet")
                text = f"ALARM {level}: {alarm['sensor_id']} = {alarm['value']}"
            except (ValueError, KeyError, TypeError) as error:
                emit_metric(channel, "failed", properties.message_id or "ohne-id")
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                print(f"Ungueltiger Alarm -> DLQ: {error}", flush=True)
                continue
            print(text, flush=True)
            channel.basic_ack(delivery_tag=method.delivery_tag)
    finally:
        close(connection)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
