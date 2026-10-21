import uuid
import random
from contextlib import suppress

import pika
from datetime import datetime
import time

MAX_RETRIES = 5

SENSORS = {
    "temperature": [-10.0, 110,0], #°C
    "humidity": [30.0, 100,0],
    "pressure": [ 900.0, 1100,0], # hPa Normal: 1010-1026, Tief: 980- 1010; Hoch: 1025-1045
}


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
        self.connection = None,
        self.channel = None

    def connect(self):
        print("Connecting to RabbitMQ")

    def publish(self, message):
        pass

    def close(self):
        if self.connection is not None and self.connection.is_open:
            with suppress(pika.exceptions.AMQPError, OSError):
                self.connection.close()



connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='temperatur', durable=True, arguments={'x-queue-type': 'quorum'})




def main():
    print("Jello")
    producer = SensorProducer()
    try:
        while True:
            for sensor in SENSORS:
                message = create_measurement(*sensor)
                producer.publish(message)

            time.sleep(2)
    except KeyboardInterrupt:
        print("Producer stopped")
    finally:
        if producer.connection:
            producer.close()


if __name__ == '__main__':
     main()