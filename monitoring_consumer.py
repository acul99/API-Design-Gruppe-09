
# der Alarm COnsumer bracuht den  Exchange Typ Headers: Für type = Critical

import json
import math
import time
from collections import Counter
from contextlib import suppress

import pika


EXCHANGE = "iot.events"
QUEUE = "monitoring.event"

MAX_RETRIES = 5
REPORT_INTERVAL = 5


class MonitoringConsumer:
    def __init__(self):
        self.connection = None
        self.channel = None

        # Statistik seit Programmstart
        self.successful = 0
        self.failed = 0
        self.alarms = 0
        self.messages_by_type = Counter()

        self.total_processing_ms = 0.0
        self.processing_count = 0

        # Fehlerhafte Monitoring-Nachrichten separat zählen.
        self.invalid_events = 0

        # Doppelte Ereignisse während dieser Laufzeit ignorieren.
        self.seen_event_ids = set()

        self.retries = 0

    def connect(self):
        print("Connecting to RabbitMQ")

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host="localhost",
                port=5672,
                credentials=pika.PlainCredentials("guest", "guest"),
                heartbeat=60,
                connection_attempts=1,
                socket_timeout=5,
                stack_timeout=10,
                blocked_connection_timeout=10,
            )
        )

        self.channel = self.connection.channel()

        self.channel.exchange_declare(
            exchange=EXCHANGE,
            exchange_type="topic",
            durable=True,
        )

        # Dieselbe DLX-/DLQ-Konfiguration wie im Producer.
        self.channel.exchange_declare(
            exchange="dlx",
            exchange_type="direct",
            durable=True,
        )

        self.channel.queue_declare(
            queue="dlq",
            durable=True,
        )

        self.channel.queue_bind(
            exchange="dlx",
            queue="dlq",
            routing_key="dead.letter",
        )

        self.channel.queue_declare(
            queue=QUEUE,
            durable=True,
            arguments={
                "x-queue-type": "quorum",
                "x-message-ttl": 60000,
                "x-dead-letter-exchange": "dlx",
                "x-dead-letter-routing-key": "dead.letter",
            },
        )

        self.channel.queue_bind(
            exchange=EXCHANGE,
            queue=QUEUE,
            routing_key="monitoring.#",
        )

        self.channel.basic_qos(prefetch_count=10)

        print("Connection Established")

    def process_message(self, method, body):
        try:
            event = json.loads(body)

            if not isinstance(event, dict):
                raise ValueError("JSON-Objekt erwartet")

            event_id = event["event_id"]
            sensor_type = event["sensor_type"]
            status = event["status"]
            alarm_count = event["alarm_count"]
            processing_ms = event["processing_ms"]

            if not isinstance(event_id, str) or not event_id:
                raise ValueError("Ungültige event_id")

            if not isinstance(sensor_type, str) or not sensor_type:
                raise ValueError("Ungültiger sensor_type")

            if status not in ("success", "failed"):
                raise ValueError("Ungültiger status")

            if type(alarm_count) is not int or alarm_count < 0:
                raise ValueError("Ungültiger alarm_count")

            if (
                type(processing_ms) not in (int, float)
                or not math.isfinite(processing_ms)
                or processing_ms < 0
            ):
                raise ValueError("Ungültige processing_ms")

        except (ValueError, KeyError, TypeError) as error:
            self.invalid_events += 1
            print(f"Ungültiges Monitoring-Ereignis: {error}")

            # Keine endlose erneute Zustellung: Nachricht in die DLQ.
            self.channel.basic_nack(
                delivery_tag=method.delivery_tag,
                requeue=False,
            )
            return

        if event_id not in self.seen_event_ids:
            if status == "success":
                self.successful += 1
            else:
                self.failed += 1

            # Enthält erfolgreiche und fehlgeschlagene Verarbeitungen.
            self.messages_by_type[sensor_type] += 1
            self.alarms += alarm_count

            self.total_processing_ms += processing_ms
            self.processing_count += 1

            self.seen_event_ids.add(event_id)

        # Erst nach Aktualisierung der Statistik bestätigen.
        self.channel.basic_ack(
            delivery_tag=method.delivery_tag,
        )

    def print_statistics(self):
        average_ms = (
            self.total_processing_ms / self.processing_count
            if self.processing_count
            else 0.0
        )

        print("\n--- Monitoring seit Programmstart ---")
        print(f"Gemeldete Verarbeitungen: {self.successful + self.failed}")
        print(f"Erfolgreich:             {self.successful}")
        print(f"Fehlgeschlagen:          {self.failed}")
        print(f"Erzeugte Alarme:         {self.alarms}")
        print(f"Ø Analysezeit:           {average_ms:.2f} ms")

        print("Verarbeitungen je Sensortyp:")
        for sensor_type, count in sorted(self.messages_by_type.items()):
            print(f"  {sensor_type}: {count}")

        print(f"Ungültige Monitoring-Ereignisse: {self.invalid_events}")

    def consume(self):
        next_report = time.monotonic() + REPORT_INTERVAL

        for method, properties, body in self.channel.consume(
            queue=QUEUE,
            auto_ack=False,
            inactivity_timeout=1,
        ):
            if method is not None:
                self.process_message(method, body)

            # Ein erfolgreicher Durchlauf zeigt eine aktive Verbindung.
            self.retries = 0

            # Auch ohne neue Nachrichten regelmäßig ausgeben.
            if time.monotonic() >= next_report:
                self.print_statistics()
                next_report = time.monotonic() + REPORT_INTERVAL

    def run(self):
        while True:
            try:
                self.connect()
                self.consume()
                return

            except (
                pika.exceptions.ProbableAuthenticationError,
                pika.exceptions.ProbableAccessDeniedError,
            ):
                raise

            except (
                pika.exceptions.AMQPConnectionError,
                OSError,
            ) as error:
                self.close()

                if self.retries >= MAX_RETRIES:
                    raise RuntimeError(
                        "Monitoring nach fünf Wiederholungen abgebrochen."
                    ) from error

                delay = min(2 ** self.retries, 10)
                self.retries += 1

                print(
                    f"Verbindungsfehler: {error!r}. "
                    f"Retry {self.retries}/{MAX_RETRIES} "
                    f"in {delay} Sekunden."
                )
                time.sleep(delay)

            finally:
                self.close()

    def close(self):
        if self.channel is not None and self.channel.is_open:
            with suppress(pika.exceptions.AMQPError, OSError):
                self.channel.cancel()

        if self.connection is not None and self.connection.is_open:
            with suppress(pika.exceptions.AMQPError, OSError):
                self.connection.close()

        self.channel = None
        self.connection = None


def main():
    consumer = MonitoringConsumer()

    try:
        consumer.run()
    except KeyboardInterrupt:
        print("\nMonitoring stopped")
    finally:
        consumer.close()
        consumer.print_statistics()


if __name__ == "__main__":
    main()


