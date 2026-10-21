"""Die komplette RabbitMQ-Topologie an einer zentralen Stelle."""

from dataclasses import dataclass

from .config import Settings

MAIN_TTL_MS = 60_000


@dataclass(frozen=True)
class Topology:
    namespace: str

    def _name(self, suffix):
        return f"{self.namespace}.{suffix}"

    @property
    def sensors(self): return self._name("sensors")
    @property
    def alarms(self): return self._name("alarms")
    @property
    def telemetry(self): return self._name("telemetry")
    @property
    def dlx(self): return self._name("dlx")
    @property
    def environment_queue(self): return self._name("analysis.environment")
    @property
    def machinery_queue(self): return self._name("analysis.machinery")
    @property
    def alarm_queue(self): return self._name("alarm")
    @property
    def monitoring_queue(self): return self._name("monitoring")
    @property
    def dlq(self): return self._name("dlq")

    @property
    def queues(self):
        return (self.environment_queue, self.machinery_queue,
                self.alarm_queue, self.monitoring_queue, self.dlq)

    @property
    def exchanges(self):
        return (self.sensors, self.alarms, self.telemetry, self.dlx)


def declare_topology(channel, settings: Settings):
    t = Topology(settings.namespace)
    for exchange, kind in ((t.sensors, "topic"), (t.alarms, "direct"),
                           (t.telemetry, "fanout"), (t.dlx, "direct")):
        channel.exchange_declare(exchange=exchange, exchange_type=kind, durable=True)
    channel.queue_declare(queue=t.dlq, durable=True, arguments={"x-queue-type": "classic"})
    channel.queue_bind(queue=t.dlq, exchange=t.dlx, routing_key="dead")
    arguments = {"x-queue-type": "classic", "x-message-ttl": MAIN_TTL_MS,
                 "x-dead-letter-exchange": t.dlx, "x-dead-letter-routing-key": "dead"}
    for queue in (t.environment_queue, t.machinery_queue, t.monitoring_queue):
        channel.queue_declare(queue=queue, durable=True, arguments=arguments)
    channel.queue_declare(queue=t.alarm_queue, durable=True,
                          arguments={**arguments, "x-max-priority": 9})
    for sensor_type in ("temperature", "humidity"):
        channel.queue_bind(queue=t.environment_queue, exchange=t.sensors,
                           routing_key=f"sensors.{sensor_type}.*")
    channel.queue_bind(queue=t.machinery_queue, exchange=t.sensors,
                       routing_key="sensors.vibration.*")
    for level in ("WARNING", "CRITICAL"):
        channel.queue_bind(queue=t.alarm_queue, exchange=t.alarms, routing_key=level)
    channel.queue_bind(queue=t.monitoring_queue, exchange=t.telemetry)
    return t


def delete_topology(channel, settings):
    """Nur von Tests mit zuvor selbst erzeugtem, eindeutigem Namespace verwendet."""
    t = Topology(settings.namespace)
    for queue in t.queues:
        channel.queue_delete(queue=queue)
    for exchange in t.exchanges:
        channel.exchange_delete(exchange=exchange)
