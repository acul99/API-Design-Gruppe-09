"""AMQP-Verbindungen, persistente Publikation und begrenzter Backoff."""

import json
import logging
import os
import sys
import time
from uuid import uuid4

import pika

from .models import encode_json

RETRY_DELAYS = (1, 2, 4, 8, 10)
RETRYABLE_EXCEPTIONS = (
    pika.exceptions.AMQPConnectionError,
    pika.exceptions.StreamLostError,
    pika.exceptions.ConnectionClosedByBroker,
    pika.exceptions.ChannelWrongStateError,
    pika.exceptions.ConnectionWrongStateError,
    pika.exceptions.AMQPHeartbeatTimeout,
    pika.exceptions.ConnectionBlockedTimeout,
    OSError,
)


def setup_logging():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "WARNING"), stream=sys.stderr)
    logging.getLogger("pika").setLevel(logging.WARNING)


def emit_log(event, **fields):
    print(json.dumps({"event": event, **fields}, ensure_ascii=False, default=str), flush=True)


def connect(settings):
    params = pika.URLParameters(settings.broker_url)
    params.connection_attempts = 1
    params.socket_timeout = 3
    params.stack_timeout = 5
    params.heartbeat = 30
    params.blocked_connection_timeout = 10
    return pika.BlockingConnection(params)


def with_retry(operation, *, sleep=time.sleep, on_retry=None):
    """Ein Erstversuch plus maximal fuenf Wiederholungen: 1,2,4,8,10s."""
    for attempt in range(len(RETRY_DELAYS) + 1):
        try:
            return operation()
        except RETRYABLE_EXCEPTIONS as exc:
            if attempt == len(RETRY_DELAYS):
                raise
            delay = RETRY_DELAYS[attempt]
            if on_retry:
                on_retry(attempt + 1, delay, exc)
            else:
                emit_log("retry", retry=attempt + 1, delay_seconds=delay,
                         error=type(exc).__name__)
            sleep(delay)


def retry_connect(settings):
    return with_retry(lambda: connect(settings))


def publish_json(channel, exchange, routing_key, payload, message_id=None, priority=0):
    channel.basic_publish(
        exchange=exchange, routing_key=routing_key, body=encode_json(payload), mandatory=True,
        properties=pika.BasicProperties(
            content_type="application/json", content_encoding="utf-8", delivery_mode=2,
            message_id=message_id or payload.get("message_id") or payload.get("event_id") or str(uuid4()),
            priority=priority,
        ),
    )


def close_quietly(connection):
    if connection is not None and connection.is_open:
        try:
            connection.close()
        except (pika.exceptions.AMQPError, OSError):
            pass
