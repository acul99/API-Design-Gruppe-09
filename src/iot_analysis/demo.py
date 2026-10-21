"""Reproduzierbare Broker-Tests in eigenen, automatisch entfernten Namespaces.

Die Tests verwenden echte Python-Prozesse und einen echten RabbitMQ. Sie veraendern
weder vorhandene Projektqueues noch die Konfiguration des lokalen Brokers.
"""

from contextlib import contextmanager
from dataclasses import replace
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from uuid import uuid4

import pika

from .messaging import close_quietly, emit_log, publish_json, retry_connect
from .models import alarm_for, encode_json, utc_now
from .topology import declare_topology, delete_topology


def wait_until(predicate, timeout=20, description="Bedingung"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(0.1)
    raise AssertionError(f"Timeout: {description}")


class Worker:
    def __init__(self, settings, log_dir, *args):
        self.path = log_dir / ("-".join(args).replace(".", "_") + "-" + uuid4().hex[:6] + ".log")
        self.stream = self.path.open("w", encoding="utf-8")
        env = {**os.environ, "AMQP_URL": settings.broker_url, "IOT_NAMESPACE": settings.namespace,
               "PYTHONUNBUFFERED": "1", "PYTHONUTF8": "1"}
        self.process = subprocess.Popen([sys.executable, "-m", "iot_analysis", *args], env=env,
                                        stdout=self.stream, stderr=subprocess.STDOUT,
                                        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))

    def events(self, name=None):
        events = []
        for line in self.path.read_text(encoding="utf-8", errors="replace").splitlines():
            try:
                event = json.loads(line)
                if name is None or event.get("event") == name:
                    events.append(event)
            except (ValueError, AttributeError):
                pass
        return events

    def wait(self, name, count=1, timeout=20):
        def condition():
            events = self.events(name)
            if len(events) >= count:
                return events
            if self.process.poll() is not None:
                raise AssertionError(f"Worker vorzeitig beendet ({self.process.returncode}): {self.path.read_text(encoding='utf-8', errors='replace')}")
            return None
        return wait_until(condition, timeout, f"{name} ({count}) in {self.path.name}")

    def stop(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        self.stream.close()


class BrokerLab:
    def __init__(self, settings, connection, log_dir):
        self.settings, self.connection, self.log_dir = settings, connection, log_dir
        self.channel = connection.channel()
        self.topology = declare_topology(self.channel, settings)
        self.channel.confirm_delivery()
        self.workers = []

    def worker(self, *args):
        worker = Worker(self.settings, self.log_dir, *args)
        self.workers.append(worker)
        worker.wait("consumer_ready")
        return worker

    def sensor(self, kind="humidity", value=50, *, failure=None):
        sensor = {"message_id": str(uuid4()), "sensor_id": f"{kind}-demo", "timestamp": utc_now(),
                  "type": kind, "value": value}
        props = pika.BasicProperties(content_type="application/json", delivery_mode=2,
                                    message_id=sensor["message_id"],
                                    headers={"x-demo-fail": failure} if failure else None)
        self.channel.basic_publish(exchange=self.topology.sensors,
                                   routing_key=f"sensors.{kind}.{sensor['sensor_id']}",
                                   body=encode_json(sensor), properties=props, mandatory=True)
        return sensor

    def ready_count(self, queue):
        self.connection.process_data_events(time_limit=0)
        return self.channel.queue_declare(queue=queue, passive=True).method.message_count

    def dead_letters(self, count=1, timeout=10):
        wait_until(lambda: self.ready_count(self.topology.dlq) >= count, timeout, "Nachrichten in DLQ")
        result = []
        for _ in range(count):
            method, props, body = self.channel.basic_get(queue=self.topology.dlq, auto_ack=False)
            if method is None:
                raise AssertionError("Erwartete DLQ-Nachricht fehlt")
            result.append((props, body))
            self.channel.basic_ack(delivery_tag=method.delivery_tag)
        return result


@contextmanager
def isolated_lab(settings):
    # Der Namespace kommt ausschliesslich aus UUID, nie aus fremden Queue-Namen.
    isolated = replace(settings, namespace="test_" + uuid4().hex)
    log_dir = Path(".state") / isolated.namespace
    log_dir.mkdir(parents=True, exist_ok=False)
    connection = retry_connect(isolated)
    lab = None
    try:
        lab = BrokerLab(isolated, connection, log_dir)
        yield lab
    finally:
        if lab:
            for worker in lab.workers:
                worker.stop()
        close_quietly(connection)
        # Frische Verbindung, auch falls ein Szenario die erste unterbrochen hat.
        cleanup = retry_connect(isolated)
        try:
            delete_topology(cleanup.channel(), isolated)
        finally:
            close_quietly(cleanup)


def scenario_invalid(lab):
    worker = lab.worker("analysis", "--kind", "environment")
    monitor = lab.worker("monitor")
    lab.channel.basic_publish(exchange=lab.topology.sensors, routing_key="sensors.temperature.broken",
                              body=b"{kaputtes JSON", mandatory=True,
                              properties=pika.BasicProperties(delivery_mode=2, message_id="invalid-json"))
    lab.channel.basic_publish(exchange=lab.topology.sensors, routing_key="sensors.temperature.broken",
                              body=encode_json({"sensor_id": "broken", "type": "temperature", "timestamp": utc_now()}),
                              mandatory=True, properties=pika.BasicProperties(delivery_mode=2, message_id="missing-value"))
    letters = lab.dead_letters(2)
    assert all(props.headers["x-death"][0]["reason"] == "rejected" for props, _ in letters)
    for props, body in letters:
        death = props.headers["x-death"][0]
        emit_log("dlq_observed", id=props.message_id, reason=death["reason"],
                 original_queue=death["queue"], body=body.decode("utf-8", errors="replace"))
    lab.sensor(value=50)
    worker.wait("processed")
    assert len(worker.events("rejected")) == 2
    report = wait_until(lambda: [r for r in monitor.events("monitoring_report")
                                 if r["processed"] == 1 and r["failures"] == 2],
                        10, "Monitoring mit zwei Fehlern und erfolgreicher Erholung")[-1]
    return {"dlq": 2, "reason": "rejected", "normal_processing_afterwards": True,
            "monitoring_failures": report["failures"]}


def scenario_processing(lab):
    worker = lab.worker("analysis", "--kind", "environment")
    transient = lab.sensor(value=55, failure="transient")
    permanent = lab.sensor(value=55, failure="permanent")
    lab.sensor(value=50)
    worker.wait("processed", 2)
    letters = lab.dead_letters()
    assert letters[0][0].message_id == permanent["message_id"]
    retries = worker.events("processing_retry")
    assert sum(e["id"] == transient["message_id"] for e in retries) == 1
    assert sum(e["id"] == permanent["message_id"] for e in retries) == 2
    return {"transient_attempts": 2, "permanent_attempts": 3, "dlq": 1, "normal_processing_afterwards": True}


def scenario_crash(lab):
    first = lab.worker("analysis", "--kind", "environment", "--processing-delay", "5")
    sensor = lab.sensor(value=50)
    first.wait("delivery_received")
    assert not first.events("processed"), "Crash muss vor ACK eintreten"
    emit_log("crash_before_ack", id=sensor["message_id"], process_id=first.process.pid)
    first.stop()  # Echter Prozessabbruch innerhalb der Verarbeitung, kein synthetischer NACK.
    replacement = lab.worker("analysis", "--kind", "environment")
    result = replacement.wait("processed")
    assert result[0]["id"] == sensor["message_id"] and result[0]["redelivered"] is True
    emit_log("crash_redelivery_observed", id=result[0]["id"], redelivered=result[0]["redelivered"])
    lab.sensor(value=51)
    replacement.wait("processed", 2)
    assert lab.ready_count(lab.topology.dlq) == 0
    return {"ack_before_crash": False, "redelivered": True, "dlq": 0, "normal_processing_afterwards": True}


def scenario_priority(lab):
    # Zuerst Rueckstau aufbauen, dann den Consumer starten: so ist die
    # Prioritaetsentscheidung des Brokers wirklich beobachtbar.
    for value in (85, 90, 110, 95, 115):
        sensor = {"message_id": str(uuid4()), "sensor_id": "temperature-demo", "timestamp": utc_now(),
                  "type": "temperature", "value": value}
        alarm = alarm_for(sensor)
        publish_json(lab.channel, lab.topology.alarms, alarm["level"], alarm,
                     message_id=alarm["alarm_id"], priority=alarm["priority"])
    worker = lab.worker("alarm")
    alarms = worker.wait("alarm_processed", 5)
    order = [event["level"] for event in alarms]
    assert order == ["CRITICAL", "CRITICAL", "WARNING", "WARNING", "WARNING"], order
    return {"order": order, "prefetch": 1}


def scenario_ttl(lab, full_ttl=False):
    body = encode_json({"sensor_id": "vibration-expired", "timestamp": utc_now(),
                        "type": "vibration", "value": 1})
    lab.channel.basic_publish(exchange=lab.topology.sensors, routing_key="sensors.vibration.vibration-expired",
                              body=body, mandatory=True,
                              properties=pika.BasicProperties(delivery_mode=2, message_id="ttl-demo",
                                                              expiration=None if full_ttl else "1000"))
    started = time.monotonic()
    letters = lab.dead_letters(timeout=70 if full_ttl else 10)
    elapsed = time.monotonic() - started
    assert letters[0][0].headers["x-death"][0]["reason"] == "expired"
    if full_ttl:
        assert elapsed >= 59, elapsed
    return {"reason": "expired", "elapsed_seconds": round(elapsed, 2),
            "queue_ttl_ms": 60000, "message_ttl_ms": None if full_ttl else 1000}


def scenario_end_to_end(lab):
    environment = lab.worker("analysis", "--kind", "environment")
    machinery = lab.worker("analysis", "--kind", "machinery")
    alarms = lab.worker("alarm")
    monitor = lab.worker("monitor")
    for kind, values in (("temperature", (20, 85, 110)), ("humidity", (50, 75, 90)), ("vibration", (1, 6, 11))):
        for value in values:
            lab.sensor(kind, value)
    environment.wait("processed", 6, timeout=25)
    machinery.wait("processed", 3)
    alarms.wait("alarm_processed", 6)
    reports = wait_until(lambda: [r for r in monitor.events("monitoring_report") if r["processed"] == 9],
                         15, "Monitoring fuer alle neun Messungen")
    report = reports[-1]
    assert report["per_type"] == {"temperature": 3, "humidity": 3, "vibration": 3}, report
    assert report["anomalies"] == 6 and report["failures"] == 0, report
    aggregate = environment.events("aggregation")[0]
    assert aggregate["count"] == 3 and abs(aggregate["average"] - 215 / 3) < 1e-9, aggregate
    assert aggregate["window_seconds"] == 10
    return {"processed": 9, "alarms": 6, "average": aggregate["average"], "monitoring": report}


SCENARIOS = {"invalid": scenario_invalid, "processing": scenario_processing,
             "crash": scenario_crash, "priority": scenario_priority, "ttl": scenario_ttl}


def run_demo(settings, scenario="all", *, full_ttl=False):
    selected = SCENARIOS if scenario == "all" else {scenario: SCENARIOS[scenario]}
    for name, function in selected.items():
        with isolated_lab(settings) as lab:
            emit_log("demo_started", scenario=name, namespace=lab.settings.namespace, logs=str(lab.log_dir.resolve()))
            result = function(lab, full_ttl=full_ttl) if name == "ttl" else function(lab)
            emit_log("demo_passed", scenario=name, **result)
