
"""Gemeinsame Konfiguration und RabbitMQ-Topologie fuer alle Komponenten.

declare_topology() ist idempotent und wird von jeder Komponente beim Start
aufgerufen, auch vom Producer. Dadurch existieren die Queues schon, bevor die
erste Nachricht kommt (wichtig wegen mandatory=True), und die Startreihenfolge
ist egal.
"""
import json
import logging
import os
import time

import pika

log = logging.getLogger("common")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")

# Exchanges
SENSOR_EXCHANGE = "iot.events"     # topic: Routing Key = sensor.<type>.<sensor_id>
ALERT_EXCHANGE = "alerts"          # direct: Routing Key = Severity
MONITOR_EXCHANGE = "monitoring"    # topic: monitor.<event>
DLX = "iot.dlx"                    # fanout: sammelt alle toten Nachrichten


# Queues
QUEUE_CLIMATE = "analysis.climate"
QUEUE_PRESSURE = "analysis.pressure"
QUEUE_ALARMS = "alarms"
QUEUE_MONITORING = "monitoring.events"
QUEUE_DLQ = "iot.dlq"

MESSAGE_TTL_MS = 60000
DELIVERY_LIMIT = 5          # Sicherheitsnetz des Brokers gegen Crash-Loops (Quorum Queues)
MAX_PRIORITY = 10
SEVERITY_PRIORITY = {"WARNING": 5, "CRITICAL": 9}

# Filterung passiert hier im Broker (Bindings), nicht im Python-Code.
# Eine Queue pro Analysezweck, nicht pro Sensor: skaliert mit weiteren Sensoren
# desselben Typs, ohne dass neue Queues noetig werden.
BINDINGS = {
    QUEUE_CLIMATE: ["sensor.temperature.*", "sensor.humidity.*"],
    QUEUE_PRESSURE: ["sensor.pressure.*"],
}


def declare_topology(ch):
    """Erstellt die RabbitMQ-Topologie aus Exchanges, Queues und Bindings.

    Eine Topologie beschreibt den Aufbau der Nachrichtenverteilung.
    Ein Exchange nimmt Nachrichten entgegen und verteilt sie anhand
    von Routing-Regeln an Queues (Warteschlangen).
    Bindings verbinden Exchanges mit Queues und legen fest,
    welche Nachrichten in welche Queue gelangen.

    Exchange-Typen:
    - Topic: Verteilt Nachrichten anhand von Routing-Key-Mustern.
    - Direct: Verteilt Nachrichten bei exakt passendem Routing Key.
    - Fanout: Verteilt Nachrichten an alle verbundenen Queues.

    Dead Letter Exchange (DLX):
    Nimmt Nachrichten auf, die nicht mehr regulär verarbeitet werden
    können, und leitet sie an eine Dead Letter Queue (DLQ) weiter.

    Quorum Queue:
    Eine RabbitMQ-Warteschlange, die durch Replikation auf mehreren
    Broker-Knoten eine hohe Ausfallsicherheit ermoeglicht.

    TTL (Time To Live):
    Maximale Lebensdauer einer Nachricht, bevor sie abläuft.

    Priority Queue:
    Warteschlange, in der Nachrichten mit höherer Prioritaet
    bevorzugt ausgeliefert werden.

    Durable bedeutet, dass die Definition eines Exchanges oder
    einer Queue einen Neustart des Brokers uebersteht.

    Idempotent bedeutet, dass wiederholte Aufrufe mit identischer
    Konfiguration keinen anderen Zustand erzeugen.
    """
    # Dead Letter Infrastruktur
    ch.exchange_declare(DLX, "fanout", durable=True)
    ch.queue_declare(QUEUE_DLQ, durable=True, )
    ch.queue_bind(QUEUE_DLQ, DLX)
    dead = {"x-dead-letter-exchange": DLX}

    # Sensordaten
    ch.exchange_declare(SENSOR_EXCHANGE, "topic", durable=True)
    for queue, keys in BINDINGS.items():
        ch.queue_declare(
            queue,
            durable=True,
            arguments={
                "x-queue-type": "quorum",
                "x-message-ttl": MESSAGE_TTL_MS,
                "x-delivery-limit": DELIVERY_LIMIT,
                **dead,
            },
        )
        for key in keys:
            ch.queue_bind(queue, SENSOR_EXCHANGE, key)

    # Alarme (klassische Priority Queue, höhere Priorität wird zuerst ausgeliefert)
    ch.exchange_declare(ALERT_EXCHANGE, "direct", durable=True)
    ch.queue_declare(
        QUEUE_ALARMS,
        durable=True,
        arguments={
            **dead,
            "x-message-ttl": MESSAGE_TTL_MS,
            "x-max-priority": MAX_PRIORITY,
        },
    )
    for severity in SEVERITY_PRIORITY:
        ch.queue_bind(QUEUE_ALARMS, ALERT_EXCHANGE, severity)

    # Monitoring-Events (Telemetrie, darf verloren gehen, aber nicht unbegrenzt wachsen)
    ch.exchange_declare(MONITOR_EXCHANGE, "topic", durable=True)
    ch.queue_declare(
        QUEUE_MONITORING,
        durable=True,
        arguments={
            "x-queue-type": "quorum",
            "x-message-ttl": MESSAGE_TTL_MS,
            "x-dead-letter-exchange": "iot.dlx",
            "x-dead-letter-routing-key": "dead.letter",
        },
    )
    ch.queue_bind(QUEUE_MONITORING, MONITOR_EXCHANGE, "monitor.#")


def connect(max_wait=10):
    """Verbindet mit Exponential Backoff (Obergrenze max_wait Sekunden).

    Stellt eine Verbindung zum RabbitMQ-Broker her.
    Ein Broker ist ein Vermittlungssystem, das Nachrichten
    zwischen Produzenten und Konsumenten transportiert.

    Exponential Backoff bedeutet, dass nach fehlgeschlagenen
    Verbindungsversuchen die Wartezeit jeweils verdoppelt wird
    (1, 2, 4, 8 Sekunden usw.), bis max_wait erreicht ist.

    Heartbeat:
    Regelmaessiges Lebenszeichen zur Erkennung unterbrochener
    Verbindungen.

    blocked_connection_timeout:
    Maximale Wartezeit bei einer vom Broker blockierten Verbindung.
    """
    delay = 1
    while True:
        try:
            return pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    port=RABBITMQ_PORT,
                    credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS),
                    heartbeat=30,
                    blocked_connection_timeout=60,
                )
            )
        except pika.exceptions.AMQPConnectionError as exc:
            log.warning("Verbindung fehlgeschlagen (%s), neuer Versuch in %ss", exc, delay)
            time.sleep(delay)
            delay = min(delay * 2, max_wait)


def publish_json(ch, exchange, routing_key, payload, *, persistent, priority=None, mandatory=False):
    """Veröffentlicht eine JSON-Nachricht ueber einen RabbitMQ-Exchange.

    JSON (JavaScript Object Notation) ist ein textbasiertes
    Datenformat zur strukturierten Uebertragung von Informationen.

    Ein Exchange verteilt Nachrichten anhand eines Routing Keys.
    Der Routing Key ist eine Zeichenkette, die bestimmt,
    an welche angebundenen Queues eine Nachricht weitergeleitet wird.

    Parameter:
    - ch: RabbitMQ-Channel, ein Kommunikationskanal zum Broker.
    - exchange: Exchange, ueber den die Nachricht verteilt wird.
    - routing_key: Schluessel zur Auswahl der Ziel-Queue.
    - payload: Inhalt der zu versendenden Nachricht.
    - persistent: Markiert die Nachricht als dauerhaft zu speichern.
      In Verbindung mit einer dauerhaften Queue kann sie dadurch
      einen Broker-Neustart ueberstehen.
    - priority: Prioritaet fuer die Verarbeitung in Priority Queues.
    - mandatory: Verlangt, dass nicht routbare Nachrichten
      an den Publisher zurueckgegeben werden.
    """
    props = pika.BasicProperties(
        content_type="application/json",
        delivery_mode=2 if persistent else 1,
        priority=priority,
    )
    ch.basic_publish(
        exchange,
        routing_key,
        json.dumps(payload).encode("utf-8"),
        props,
        mandatory=mandatory,
    )
