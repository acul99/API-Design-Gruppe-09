import pika




connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='temperatur', durable=True, arguments={'x-queue-type': 'quorum'})


channel.basic_publish(exchange='',)

























