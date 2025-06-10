from controller import DataTarget
from typing import Dict, Any, Iterable
import pika
import json

class RabbitmqTarget(DataTarget):
    """RabbitMQ stream target implementation."""
    def __init__(self):
        self.connection = None
        self.channel = None
        self.queue = None
        self.host = None

    def initialize(self, config: Dict[str, Any]) -> None:
        self.queue = config.get('queue')
        self.host = config.get('host', 'localhost')
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue)

    def create_entries(self, entries: Iterable[Dict[str, Any]]) -> None:
        if not self.channel:
            return
        for entry in entries:
            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue,
                body=json.dumps(entry).encode('utf-8')
            )

    def close(self) -> None:
        if self.connection:
            self.connection.close()
