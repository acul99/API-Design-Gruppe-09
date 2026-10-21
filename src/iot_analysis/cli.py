"""Befehlszeilenstart fuer getrennte Dienste und reproduzierbare Demos."""

import argparse
from dataclasses import replace
import json
import os
import subprocess
import sys
import time

import pika

from .config import Settings
from .messaging import close_quietly, emit_log, retry_connect, setup_logging
from .topology import Topology, declare_topology


def _parser():
    parser = argparse.ArgumentParser(description="IoT-Echtzeit-Analyse mit RabbitMQ")
    parser.add_argument("--namespace", help="Eigener Praefix fuer Exchanges und Queues")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("check", help="AMQP-Verbindung pruefen")
    commands.add_parser("topology", help="Exchanges, Queues und Bindings deklarieren")
    commands.add_parser("run", help="Vier Consumer und einen Sensor-Producer lokal starten")
    producer = commands.add_parser("producer", help="Drei Sensoren kontinuierlich simulieren")
    producer.add_argument("--count", type=int, default=0, help="Messungen insgesamt; 0 = unbegrenzt")
    producer.add_argument("--interval", type=float, default=1.0, help="Pause zwischen Messungen in Sekunden")
    producer.add_argument("--seed", type=int, default=42)
    analysis = commands.add_parser("analysis")
    analysis.add_argument("--kind", choices=("environment", "machinery"), required=True)
    analysis.add_argument("--max-messages", type=int, default=0)
    analysis.add_argument("--processing-delay", type=float, default=0, help="Gezielte Verzoegerung fuer Crash-Demo")
    for name in ("alarm", "monitor"):
        command = commands.add_parser(name)
        command.add_argument("--max-messages", type=int, default=0)
    inspect = commands.add_parser("inspect", help="Queue lesen und zuruecklegen; keine Nachrichten entfernen")
    inspect.add_argument("--queue", choices=("dlq", "environment", "machinery", "alarm", "monitoring"), default="dlq")
    inspect.add_argument("--limit", type=int, default=10)
    demo = commands.add_parser("demo", help="Isolierte, automatisch gepruefte Fehlerszenarien")
    demo.add_argument("--scenario", choices=("invalid", "processing", "crash", "priority", "ttl", "all"), default="all")
    demo.add_argument("--full-ttl", action="store_true", help="60s Queue-TTL statt zusaetzlicher 1s Nachrichten-TTL testen")
    return parser


def inspect_queue(settings, name, limit):
    if limit < 1:
        raise ValueError("limit muss positiv sein")
    connection = retry_connect(settings)
    try:
        channel = connection.channel()
        t = declare_topology(channel, settings)
        queue = {"dlq": t.dlq, "environment": t.environment_queue, "machinery": t.machinery_queue,
                 "alarm": t.alarm_queue, "monitoring": t.monitoring_queue}[name]
        count = channel.queue_declare(queue=queue, passive=True).method.message_count
        emit_log("queue_status", queue=queue, ready=count)
        # Lieferungen bis zum Schliessen unbestaetigt halten: kein wiederholtes
        # Lesen derselben Nachricht in einer basic_get/requeue-Schleife.
        for _ in range(min(limit, count)):
            method, props, body = channel.basic_get(queue=queue, auto_ack=False)
            if method is None:
                break
            emit_log("queue_message", id=props.message_id, headers=props.headers,
                     priority=props.priority, body=body.decode("utf-8", errors="replace"))
    finally:
        # RabbitMQ stellt alle gelesenen Nachrichten wieder in die Queue.
        close_quietly(connection)


def run_all(settings):
    connection = retry_connect(settings)
    try:
        declare_topology(connection.channel(), settings)
    finally:
        close_quietly(connection)
    processes = []
    env = {**os.environ, "IOT_NAMESPACE": settings.namespace, "AMQP_URL": settings.broker_url,
           "PYTHONUNBUFFERED": "1", "PYTHONUTF8": "1"}
    try:
        for args in (("analysis", "--kind", "environment"), ("analysis", "--kind", "machinery"),
                     ("alarm",), ("monitor",), ("producer",)):
            processes.append(subprocess.Popen([sys.executable, "-m", "iot_analysis", *args], env=env,
                                               creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0)))
        emit_log("system_started", namespace=settings.namespace, processes=len(processes))
        while True:
            for process in processes:
                if process.poll() is not None:
                    raise RuntimeError(f"Ein Dienst wurde unerwartet beendet (Exit {process.returncode})")
            time.sleep(0.3)
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        emit_log("system_stopped", namespace=settings.namespace)


def main(argv=None):
    setup_logging()
    args = _parser().parse_args(argv)
    settings = Settings.from_env()
    if args.namespace:
        settings = replace(settings, namespace=args.namespace)
    try:
        if args.command in ("analysis", "alarm", "monitor"):
            from .consumers import run_alarm, run_analysis, run_monitor
            if args.max_messages < 0 or getattr(args, "processing_delay", 0) < 0:
                raise ValueError("Anzahlen und Verzoegerungen duerfen nicht negativ sein")
            if args.command == "analysis":
                run_analysis(settings, args.kind, max_messages=args.max_messages, processing_delay=args.processing_delay)
            elif args.command == "alarm":
                run_alarm(settings, max_messages=args.max_messages)
            else:
                run_monitor(settings, max_messages=args.max_messages)
        elif args.command == "producer":
            from .producer import run_producer
            run_producer(settings, count=args.count, interval=args.interval, seed=args.seed)
        elif args.command == "run":
            run_all(settings)
        elif args.command == "inspect":
            inspect_queue(settings, args.queue, args.limit)
        elif args.command == "demo":
            from .demo import run_demo
            run_demo(settings, args.scenario, full_ttl=args.full_ttl)
        else:
            connection = retry_connect(settings)
            try:
                if args.command == "topology":
                    t = declare_topology(connection.channel(), settings)
                    emit_log("topology_ready", namespace=settings.namespace, queues=t.queues)
                else:
                    emit_log("broker_available", namespace=settings.namespace)
            finally:
                close_quietly(connection)
        return 0
    except KeyboardInterrupt:
        emit_log("stopped")
        return 0
    except (pika.exceptions.AMQPError, OSError, ValueError, RuntimeError, AssertionError) as exc:
        # Zugangsdaten aus AMQP_URL werden absichtlich nie ausgegeben.
        emit_log("fatal", error_type=type(exc).__name__, error=str(exc))
        return 1
