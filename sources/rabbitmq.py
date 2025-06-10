from controller import DataSource
from typing import Dict, Any, List
import pika
import json

class RabbitmqSource(DataSource):
    """RabbitMQ stream source implementation."""
    def __init__(self):
        self.connection = None
        self.channel = None
        self.queue = None
        self.host = None
        self.running = False

    def initialize(self, config: Dict[str, Any]) -> None:
        self.queue = config.get('queue')
        self.host = config.get('host', 'localhost')
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue)
        self.running = True

    def get_entries(self) -> List[Dict[str, Any]]:
        entries = []
        if not self.channel:
            return entries
        while True:
            method, properties, body = self.channel.basic_get(queue=self.queue, auto_ack=True)
            if method is None:
                break
            try:
                entries.append(json.loads(body.decode('utf-8')))
            except Exception:
                continue
        return entries

    def close(self) -> None:
        self.running = False
        if self.connection:
            self.connection.close()
