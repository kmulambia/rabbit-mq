# emitter.py
import pika
import json
from common.schemas.base import Job
from common.utils.config import AMQ_URL

def produce_job(job: Job, routing_key: str):
    connection_params = pika.URLParameters(AMQ_URL)
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()

    channel.exchange_declare(exchange='jobs_exchange', exchange_type='topic')

    message = json.dumps(job.dict())

    channel.basic_publish(
        exchange='jobs_exchange',
        routing_key=routing_key,
        body=message
    )

    print(f" [x] Sent {message} with routing key {routing_key}")
    connection.close()

if __name__ == "__main__":
    # Example usage
    produce_job(Job(title="Task A", workload=3), routing_key="tasks.high_priority")
    produce_job(Job(title="Task B", workload=2), routing_key="tasks.low_priority")
    produce_job(Job(title="Task C", workload=1), routing_key="tasks.medium_priority")