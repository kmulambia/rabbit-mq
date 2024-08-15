#!/usr/bin/env python
import pika
from common.utils.config import AMQ_URL

connection_params = pika.URLParameters(AMQ_URL)
connection = pika.BlockingConnection(connection_params)
channel = connection.channel()

# Declare the RPC queue where the client requests will be sent
channel.queue_declare(queue='rpc_queue')

def on_request(ch, method, props, body):
    name = body.decode()

    # Generate the response
    response = f"Hie {name}!"

    print(f" [.] Got request for '{name}'")

    # Publish the response back to the client's callback queue
    ch.basic_publish(
        exchange='',
        routing_key=props.reply_to,
        properties=pika.BasicProperties(correlation_id=props.correlation_id),
        body=response
    )

    # Acknowledge the request message
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Set up the consumer to handle requests
channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='rpc_queue', on_message_callback=on_request)

print(" [x] Awaiting RPC requests")
channel.start_consuming()
