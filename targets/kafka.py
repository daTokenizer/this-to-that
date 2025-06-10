from controller import DataTarget
from typing import Dict, Any, Iterable
from kafka import KafkaProducer
import json

class KafkaTarget(DataTarget):
    """Kafka stream target implementation."""
    def __init__(self):
        self.producer = None
        self.topic = None
        self.bootstrap_servers = None

    def initialize(self, config: Dict[str, Any]) -> None:
        self.topic = config.get('topic')
        self.bootstrap_servers = config.get('bootstrap_servers')
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def create_entries(self, entries: Iterable[Dict[str, Any]]) -> None:
        if not self.producer:
            return
        for entry in entries:
            self.producer.send(self.topic, value=entry)
        self.producer.flush()

    def close(self) -> None:
        if self.producer:
            self.producer.close()
