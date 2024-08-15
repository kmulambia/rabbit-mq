#!/usr/bin/env python
import pika
import uuid
from common.utils.config import AMQ_URL

class GreetingRpcClient:

    def __init__(self):
        self.connection_params = pika.URLParameters(AMQ_URL)
        self.connection = pika.BlockingConnection(self.connection_params)
        self.channel = self.connection.channel()

        # Declare a callback queue where the response will be sent
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        # Start consuming messages from the callback queue
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True
        )

    def on_response(self, ch, method, props, body):
        # Check if the correlation_id matches the one we sent
        if self.corr_id == props.correlation_id:
            self.response = body

    def hello(self, name):
        # Initialize the response and generate a unique correlation ID
        self.response = None
        self.corr_id = str(uuid.uuid4())

        # Publish the message to the 'rpc_queue' with the correlation_id and reply_to queue
        self.channel.basic_publish(
            exchange='',
            routing_key='rpc_queue',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=str(name)
        )

        # Wait for the response
        while self.response is None:
            self.connection.process_data_events(time_limit=None)
        return self.response.decode()

# Create a client instance and make a request
greeting_rpc = GreetingRpcClient()
print(" [x] Requesting greeting")
response = greeting_rpc.hello("Alice")
print(f" [.] Got '{response}'")
