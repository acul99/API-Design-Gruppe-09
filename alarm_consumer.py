"""Alarm-Consumer: liest die Priority Queue "alarms" (CRITICAL vor WARNING).

Start:  python alarm_consumer.py
Demo:   ALARM_WORK_MS=500 python alarm_consumer.py   (macht die Reihenfolge sichtbar)
"""
import json
import logging
import time

import pika

from common import QUEUE_ALARMS, SEVERITY_PRIORITY, connect, declare_topology

WORK_MS = 1000   # simulierte Bearbeitungsdauer
log = logging.getLogger("alarm")
seen = set()   # Duplikaterkennung: Alarme sind At-least-once


def handle(ch, method, body):
    tag = method.delivery_tag
    try:
        alarm = json.loads(body)
        if alarm["severity"] not in SEVERITY_PRIORITY:
            raise ValueError(f"unbekannte Severity {alarm['severity']!r}")
        key = (alarm.get("source_message_id") or alarm["alert_id"], alarm["severity"])
    except (ValueError, KeyError, TypeError) as exc:
        log.error("Ungueltiger Alarm -> DLQ: %r", exc)
        ch.basic_nack(tag, requeue=False)
        return

    if key in seen:
        log.info("Duplikat ignoriert: %s", key)
    else:
        try:
            time.sleep(WORK_MS / 1000)   # hier stünde die echte Reaktion (Mail, Ticket, ...)
            log.warning("ALARM %s %s von %s: %s", alarm["severity"], alarm["type"],
                        alarm["sensor_id"], alarm["value"])
        except Exception as exc:
            # Erster Fehler: einmal neu zustellen, danach in die DLQ
            log.error("Verarbeitungsfehler (%r), requeue=%s", exc, not method.redelivered)
            ch.basic_nack(tag, requeue=not method.redelivered)
            return
        seen.add(key)
    ch.basic_ack(tag)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")
    while True:
        try:
            conn = connect()
            ch = conn.channel()
            declare_topology(ch)
            ch.basic_qos(prefetch_count=1)   # nur so wirkt die Prioritaet
            log.info("warte auf Alarme in %s", QUEUE_ALARMS)
            for method, _props, body in ch.consume(QUEUE_ALARMS):
                handle(ch, method, body)
        except KeyboardInterrupt:
            return
        except pika.exceptions.AMQPError as exc:
            log.warning("Verbindung verloren (%s), neuer Versuch in 5s", exc)
            time.sleep(5)


if __name__ == "__main__":
    main()
