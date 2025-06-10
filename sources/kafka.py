from controller import DataSource
from typing import Dict, Any, List
from kafka import KafkaConsumer
import json

class KafkaSource(DataSource):
    """Kafka stream source implementation."""
    def __init__(self):
        self.consumer = None
        self.topic = None
        self.bootstrap_servers = None
        self.group_id = None
        self.running = False

    def initialize(self, config: Dict[str, Any]) -> None:
        self.topic = config.get('topic')
        self.bootstrap_servers = config.get('bootstrap_servers')
        self.group_id = config.get('group_id', 'etl-group')
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        self.running = True

    def get_entries(self) -> List[Dict[str, Any]]:
        entries = []
        if not self.consumer:
            return entries
        for message in self.consumer.poll(timeout_ms=1000).values():
            for record in message:
                entries.append(record.value)
        return entries

    def close(self) -> None:
        self.running = False
        if self.consumer:
            self.consumer.close()
