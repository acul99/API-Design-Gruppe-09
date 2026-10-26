"""Zwei reproduzierbare Fehlerszenarien: python demo.py invalid oder crash."""

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


def wait_for(condition):
    end = time.monotonic() + 15
    while time.monotonic() < end:
        result = condition()
        if result:
            return result
        time.sleep(0.1)
    raise RuntimeError("Die erwartete Beobachtung ist innerhalb von 15s nicht eingetreten.")


def main(scenario):
    # Eigene Queues: Die Demo beeinflusst den normalen Datenstrom nicht.
    os.environ["IOT_NAMESPACE"] = "demo_" + uuid4().hex[:8]
    import common as mq
    connection, channel = mq.open_channel()
    processes, streams = [], []
    with tempfile.TemporaryDirectory() as folder:
        try:
            def start_worker(delay=0):
                path = Path(folder) / f"worker-{len(processes)}.log"
                stream = path.open("w", encoding="utf-8")
                streams.append(stream)
                worker = subprocess.Popen([sys.executable, "-u", "analysis.py", "environment", "--delay", str(delay)],
                         cwd=Path(__file__).resolve().parent, stdout=stream, stderr=subprocess.STDOUT,
                         creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
                processes.append(worker)
                wait_for(lambda: "BEREIT environment" in path.read_text(encoding="utf-8"))
                return worker, path

            def send(message_id):
                mq.send_sensor(channel, {"message_id": message_id, "sensor_id": "humidity-demo",
                    "timestamp": datetime.now(timezone.utc).isoformat(), "type": "humidity", "value": 50})

            def processed(message_id):
                method, _, body = channel.basic_get(mq.MONITOR_QUEUE, auto_ack=False)
                if method is None:
                    return None
                metric = json.loads(body)
                channel.basic_ack(delivery_tag=method.delivery_tag)
                if metric["status"] == "processed" and metric["message_id"] == message_id:
                    return metric
                return None

            if scenario == "invalid":
                start_worker()
                channel.basic_publish(exchange=mq.SENSOR_EXCHANGE, routing_key="sensors.temperature.broken",
                    body=b"{kaputtes JSON", mandatory=True,
                    properties=pika.BasicProperties(delivery_mode=2, message_id="invalid-demo"))
                def dead_letter():
                    method, properties, body = channel.basic_get(mq.DLQ, auto_ack=False)
                    if method is None:
                        return None
                    reason = properties.headers["x-death"][0]["reason"]
                    if reason != "rejected":
                        raise RuntimeError(f"Unerwartete DLQ-Ursache: {reason}")
                    print(f"DLQ: {body.decode()} | x-death.reason={reason}", flush=True)
                    channel.basic_ack(delivery_tag=method.delivery_tag)
                    return True
                wait_for(dead_letter)
                print("OK: Ungueltige Nachricht per NACK ohne Requeue in DLQ.", flush=True)
            else:
                worker, path = start_worker(delay=5)
                send("crash-demo")
                wait_for(lambda: "EMPFANG crash-demo" in path.read_text(encoding="utf-8"))
                print("Consumer hat crash-demo empfangen. Prozessabbruch vor ACK.", flush=True)
                worker.terminate()
                worker.wait(timeout=5)
                start_worker()
                metric = wait_for(lambda: processed("crash-demo"))
                if not metric["redelivered"]:
                    raise RuntimeError("Die Lieferung wurde nicht als redelivered markiert.")
                print("OK: Dieselbe ID crash-demo erneut verarbeitet, redelivered=True.", flush=True)
            send("after-demo")
            wait_for(lambda: processed("after-demo"))
            print("OK: Anschliessende gueltige Messung normal verarbeitet.", flush=True)
        finally:
            for worker in processes:
                if worker.poll() is None:
                    worker.terminate()
                worker.wait(timeout=5)
            for stream in streams:
                stream.close()
            for queue in mq.QUEUES:
                channel.queue_delete(queue)
            for exchange in (mq.SENSOR_EXCHANGE, mq.EVENT_EXCHANGE):
                channel.exchange_delete(exchange)
            mq.close(connection)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", choices=("invalid", "crash"))
    main(parser.parse_args().scenario)
