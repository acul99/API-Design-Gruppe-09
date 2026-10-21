import json
import uuid
import random
from contextlib import suppress
import pika
from datetime import datetime
import time

MAX_RETRIES = 5
EXCHANGE = "iot.events"

#Sensor Typen und min max an möglichen Measurements definieren
SENSORS = {
    "temperature": [-10.0, 110.0, "temperature", "temp-01"], #°C
    "humidity": [30.0, 100.0, "humidity", "hum-01"],
    "pressure": [ 900.0, 1100.0, "pressure", "pres-01"], # hPa Normal: 1010-1026, Tief: 980- 1010; Hoch: 1025-1045
}


#Neues Measurement generieren
def create_measurement(sensor_type, sensor_id):
    return {
        "message_id": str(uuid.uuid4()),
        "sensor_type": sensor_type,
        "sensor_id": sensor_id,
        "value": round(random.uniform(SENSORS[sensor_type][0], SENSORS[sensor_type][1]), 2),
        "timestamp": datetime.now().isoformat()
    }


class SensorProducer:
    def __init__(self):
        self.connection = None
        self.channel = None

        self.connect()

    def connect(self):
        print("Connecting to RabbitMQ")
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host='localhost',
                port=5672,
                credentials=pika.PlainCredentials('guest', 'guest'),
                heartbeat=60,
                connection_attempts=1,  # Das stuern wir selber
                socket_timeout=5,
                stack_timeout=10,
                blocked_connection_timeout=10,
            )
        )

        self.channel = self.connection.channel()

        #DLX und DLQ erstellen
        self.channel.exchange_declare(
            exchange="iot.dlx",
            exchange_type='fanout',
            durable=True
            )

        self.channel.queue_declare(queue="iot.dlq", durable=True,)
        self.channel.queue_bind(
            exchange="iot.dlx",
            queue="iot.dlq",
            routing_key="dead.letter",
        )


        self.channel.exchange_declare(
            exchange=EXCHANGE,
            exchange_type='topic',
            durable=True,
        )

        #Create queues:
        for queue_name in SENSORS.keys():
            print(f"sensor.{queue_name}.{SENSORS[queue_name][3]}")
            self.channel.queue_declare(
                queue=f"sensor.{queue_name}.{SENSORS[queue_name][3]}",
                durable=True,
                arguments={
                    "x-queue-type": "quorum",
                    "x-message-ttl": 60000,  # TTL 60 Sekunden
                    "x-dead-letter-exchange": "iot.dlx", #dlx
                    "x-dead-letter-routing-key": "dead.letter", #dl Key
                }
            )

            self.channel.queue_bind(
                exchange=EXCHANGE,
                queue=f"sensor.{queue_name}.{SENSORS[queue_name][3]}",
                routing_key=f"sensor.{queue_name}.*",
            )

        self.channel.confirm_delivery()

        print("Connection Established")

    def publish(self, message):
        routing_key = f"sensor.{message['sensor_type']}.{message['sensor_id']}"
        body = json.dumps(message).encode("utf-8")

        properties = pika.BasicProperties(
            content_type='application/json',
            content_encoding='utf-8',
            delivery_mode=2,
            message_id = message["message_id"],
        )

        #Maximale X Wiederholungen bei Übertragungsfehlern, exponentiell steigende Wartezeit zwischen den Wiederholungen, max. 10 Sekunden
        for attempt in range(MAX_RETRIES + 1):
            try:
                if (self.connection is None
                        or self.connection.is_closed
                        or self.channel is None or self.channel.is_closed):
                    print("No Connection!")
                    self.close()
                    self.connect()

                self.channel.basic_publish(
                    exchange=EXCHANGE,
                    routing_key=routing_key,
                    body=body,
                    properties=properties,
                    mandatory=True,

                )
                print(f"Gesendet: {routing_key} -> {message['value']}")

                return
            except pika.exceptions.UnroutableError as error:
                raise RuntimeError(
                    f"Keine passende Queue für {routing_key}. "
                ) from error


            except (
                    pika.exceptions.AMQPConnectionError,
                    pika.exceptions.ChannelWrongStateError,
                    pika.exceptions.NackError,
                    OSError,

            ) as error:
                self.close()

                if attempt == MAX_RETRIES:
                    raise RuntimeError(
                        "Übertragung nach fünf Wiederholungen "
                        f"abgebrochen. Messung: {body.decode('utf-8')}"
                    ) from error

                #Exponentiel steigende Wartezeit zwischen den Wiederholungen, max. 10 Sekunden
                delay = min(2 ** attempt, 10)

                print(
                    f"Übertragungsfehler: {error!r}. "
                    f"Retry {attempt + 1}/{MAX_RETRIES} "
                    f"in {delay} Sekunden."
                )
                time.sleep(delay)


    def close(self):
        if self.connection is not None and self.connection.is_open:
            with suppress(pika.exceptions.AMQPError, OSError):
                self.connection.close()


def main():
    producer = SensorProducer()
    try:
        while True:
            for sensor in SENSORS.keys():
                message = create_measurement(SENSORS[sensor][2], SENSORS[sensor][3])
                producer.publish(message)

            time.sleep(2)
    except KeyboardInterrupt:
        print("Producer stopped")
    finally:
        if producer.connection:
            producer.close()


if __name__ == '__main__':
     main()