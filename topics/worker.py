# worker.py
import pika
import json
import time
from typing import Any
from common.schemas.base import Job
from common.utils.config import AMQ_URL

def consume_jobs(routing_key: str):
    connection_params = pika.URLParameters(AMQ_URL)
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()

    channel.exchange_declare(exchange='jobs_exchange', exchange_type='topic')

    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(exchange='jobs_exchange', queue=queue_name, routing_key=routing_key)

    print(f' [*] Worker waiting for jobs with routing key {routing_key}. To exit press CTRL+C')

    def callback(ch: Any, method: Any, properties: Any, body: bytes):
        job = Job.parse_raw(body)
        print(f" [x] Received {job}")
        # Simulate work
        time.sleep(job.workload)
        print(f" [x] Done with {job.title}")

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

if __name__ == "__main__":
    consume_jobs(routing_key="tasks.*")