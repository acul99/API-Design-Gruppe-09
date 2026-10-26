"""Sichtbare RabbitMQ-Demos: python demo.py invalid oder crash. Queues bleiben erhalten."""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from uuid import uuid4

import pika


def wait_for(condition, timeout=15):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        result = condition()
        if result:
            return result
        time.sleep(0.1)
    raise RuntimeError(f"Die erwartete Beobachtung ist innerhalb von {timeout}s nicht eingetreten.")


def read_evidence(connection, queue, predicate):
    """Aktuellen Queue-Inhalt pruefen und alle gelesenen Nachrichten zurueckgeben."""
    inspection = connection.channel()
    try:
        count = inspection.queue_declare(queue=queue, passive=True).method.message_count
        for _ in range(count):
            method, properties, body = inspection.basic_get(queue, auto_ack=False)
            if method is None:
                break
            result = predicate(properties, body)
            if result:
                return result
        return None
    finally:
        # Ohne ACK gelangen die gelesenen Belege beim Schliessen zurueck in die Queue.
        # Erst nach dem gesamten Durchlauf zurueckgeben, sonst verdecken alte Belege neue.
        if inspection.is_open:
            inspection.close()


def main(scenario, automatic=None):
    if automatic is None:
        automatic = not sys.stdin.isatty()
    # Feste, sichtbare Demo-Topologie. Die normalen iot.*-Queues bleiben getrennt.
    namespace = f"{os.getenv('IOT_DEMO_NAMESPACE', 'iot.demo')}.{scenario}"
    os.environ["IOT_NAMESPACE"] = namespace
    os.environ["IOT_MESSAGE_TTL_MS"] = "none"
    import common as mq
    connection, channel = mq.open_channel()
    processes, streams = [], []
    run_id = uuid4().hex[:12]
    invalid_id, crash_id, after_id = (f"{label}-demo-{run_id}" for label in ("invalid", "crash", "after"))
    print(f"RabbitMQ-Demo {scenario}: Queues mit '{namespace}.' filtern. Lauf: {run_id}", flush=True)
    print("Queues und Belege bleiben erhalten. Demo-Nachrichten haben keine TTL.", flush=True)
    print("Bei 'Get messages' Requeue einschalten, damit Belege erhalten bleiben.", flush=True)

    def checkpoint(message):
        nonlocal connection, channel
        print(f"\nRabbitMQ ansehen: {message}", flush=True)
        if not automatic:
            # Keine blockierte AMQP-Verbindung waehrend einer beliebig langen Pause.
            # Ein laufender Analyseprozess bedient seine eigene Verbindung weiterhin.
            mq.close(connection)
            try:
                input("Weiter mit Enter ... ")
            except EOFError as error:
                raise RuntimeError("Keine Eingabe verfuegbar. Fuer automatischen Ablauf --auto verwenden.") from error
            connection, channel = mq.open_channel()

    with tempfile.TemporaryDirectory() as folder:
        try:
            state = channel.queue_declare(queue=mq.ENV_QUEUE, passive=True).method
            if state.consumer_count or state.message_count:
                raise RuntimeError(f"{mq.ENV_QUEUE} ist noch belegt. Andere Demo beenden und gegebenenfalls "
                                   "nur verbliebene Nachrichten dieser Demo-Queue in RabbitMQ entfernen.")

            def start_worker(wait_file=None):
                path = Path(folder) / f"worker-{len(processes)}.log"
                stream = path.open("w", encoding="utf-8")
                streams.append(stream)
                command = [sys.executable, "-u", "-X", "utf8", "analysis.py", "environment"]
                if wait_file is not None:
                    command.extend(["--wait-file", str(wait_file)])
                worker = subprocess.Popen(command, stdin=subprocess.DEVNULL,
                         cwd=Path(__file__).resolve().parent, stdout=stream, stderr=subprocess.STDOUT,
                         creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
                processes.append(worker)
                wait_for(lambda: "BEREIT environment" in path.read_text(encoding="utf-8"))
                return worker, path

            def send(message_id):
                mq.send_sensor(channel, {"message_id": message_id, "sensor_id": "humidity-demo",
                    "timestamp": datetime.now(timezone.utc).isoformat(), "type": "humidity", "value": 50})

            def processed(message_id):
                def match(properties, body):
                    if properties.message_id != message_id:
                        return None
                    metric = json.loads(body)
                    if metric["status"] == "processed" and metric["message_id"] == message_id:
                        return metric
                    return None
                return read_evidence(connection, mq.MONITOR_QUEUE, match)

            if scenario == "invalid":
                channel.basic_publish(exchange=mq.SENSOR_EXCHANGE, routing_key="sensors.temperature.broken",
                    body=b"{kaputtes JSON", mandatory=True,
                    properties=pika.BasicProperties(delivery_mode=2, message_id=invalid_id))
                checkpoint(f"{mq.ENV_QUEUE}: Ready=1, Unacked=0. ID: {invalid_id}. "
                           "Als Naechstes lehnt die Analyse diese Nachricht ab.")
                worker, path = start_worker()

                def dead_letter():
                    def match(properties, body):
                        if properties.message_id != invalid_id:
                            return None
                        reason = properties.headers["x-death"][0]["reason"]
                        if reason != "rejected":
                            raise RuntimeError(f"Unerwartete DLQ-Ursache: {reason}")
                        print(f"DLQ: {body.decode()} | x-death.reason={reason} | ID={invalid_id}", flush=True)
                        return True
                    return read_evidence(connection, mq.DLQ, match)

                wait_for(dead_letter)
                print("OK: Ungueltige Nachricht per NACK ohne Requeue in DLQ.", flush=True)
                checkpoint(f"{mq.DLQ}: Beleg {invalid_id} mit x-death.reason=rejected. "
                           f"{mq.MONITOR_QUEUE} enthaelt das failed-Ereignis.")
            else:
                send(crash_id)
                checkpoint(f"{mq.ENV_QUEUE}: Ready=1, Unacked=0. ID: {crash_id}. "
                           "Als Naechstes empfaengt die Analyse die Nachricht.")
                gate = Path(folder) / "wait-before-processing"
                gate.touch()
                worker, path = start_worker(wait_file=gate)
                wait_for(lambda: f"EMPFANG {crash_id} " in path.read_text(encoding="utf-8"))
                checkpoint(f"{mq.ENV_QUEUE}: Ready=0, Unacked=1. "
                           "Die Analyse wartet vor Verarbeitung und ACK. Danach wird ihr Prozess beendet.")
                print(f"Consumer hat {crash_id} empfangen. Prozessabbruch vor ACK.", flush=True)
                worker.terminate()
                worker.wait(timeout=5)
                print("Warte auf RabbitMQ: Der Abbruch wird gegebenenfalls erst durch fehlende Heartbeats erkannt.",
                      flush=True)

                def requeued():
                    state = channel.queue_declare(queue=mq.ENV_QUEUE, passive=True).method
                    return state.consumer_count == 0 and state.message_count == 1

                # Bei verzoegert erkanntem TCP-Abbruch kann der Broker bis zu
                # drei 30s-Heartbeatintervalle benoetigen, plus kurze Zustellreserve.
                wait_for(requeued, timeout=95)
                checkpoint(f"{mq.ENV_QUEUE}: Ready=1, Unacked=0, Consumers=0. "
                           "Der Broker hat die unbestaetigte Nachricht erneut eingereiht. Danach startet der Ersatzconsumer.")
                worker, path = start_worker()
                metric = wait_for(lambda: processed(crash_id))
                if not metric["redelivered"]:
                    raise RuntimeError("Die Lieferung wurde nicht als redelivered markiert.")
                print(f"OK: Dieselbe ID {crash_id} erneut verarbeitet, redelivered=True.", flush=True)
            send(after_id)
            wait_for(lambda: processed(after_id))
            # Die Metrik wird vor dem ACK publiziert. Vor dem Beenden auch die
            # nach basic_ack ausgegebene Verarbeitungsbestaetigung abwarten.
            wait_for(lambda: f"VERARBEITET {after_id}" in path.read_text(encoding="utf-8"))
            print("OK: Anschliessende gueltige Messung normal verarbeitet.", flush=True)
            checkpoint(f"{mq.MONITOR_QUEUE}: processed-Belege dieses Laufs ansehen. "
                       f"Folgemessung: {after_id}. Die Quellmessungen wurden bestaetigt. "
                       "Queues und Belege bleiben auch nach dem Beenden sichtbar.")
        finally:
            for worker in processes:
                if worker.poll() is None:
                    worker.terminate()
                worker.wait(timeout=5)
            for stream in streams:
                stream.close()
            mq.close(connection)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", choices=("invalid", "crash"))
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--auto", action="store_true", help="Ohne Pausen ausfuehren")
    mode.add_argument("--step", action="store_true", help="Enter-Pausen auch bei umgeleiteter Eingabe verwenden")
    args = parser.parse_args()
    try:
        main(args.scenario, True if args.auto else False if args.step else None)
    except KeyboardInterrupt:
        print("\nDemo abgebrochen. Queues und vorhandene Belege bleiben erhalten.", flush=True)
