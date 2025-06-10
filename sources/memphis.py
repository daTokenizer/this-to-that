from controller import DataSource
from typing import Dict, Any, List
from memphis import Memphis
import json

class MemphisSource(DataSource):
    """Memphis stream source implementation."""
    def __init__(self):
        self.connection = None
        self.consumer = None
        self.station = None
        self.consumer_name = None
        self.running = False

    def initialize(self, config: Dict[str, Any]) -> None:
        self.station = config.get('station')
        self.consumer_name = config.get('consumer_name', 'etl-consumer')
        self.host = config.get('host', 'localhost')
        self.username = config.get('username', 'root')
        self.password = config.get('password', 'memphis')
        self.connection = Memphis()
        self.connection.connect(
            host=self.host,
            username=self.username,
            password=self.password
        )
        self.consumer = self.connection.consumer(
            station_name=self.station,
            consumer_name=self.consumer_name,
            consumer_group=self.consumer_name, # defaults to the consumer name
            pull_interval_ms=1000, # defaults to 1000
            batch_size=10, # defaults to 10
            batch_max_time_to_wait_ms=100, # defaults to 100
            max_ack_time_ms=30000, # defaults to 30000
            max_msg_deliveries=2, # defaults to 2
            start_consume_from_sequence=1, # start consuming from a specific sequence. defaults to 1
            last_messages=-1 # consume the last N messages, defaults to -1 (all messages in the station)
        )
        self.running = True

    def get_entries(self) -> List[Dict[str, Any]]:
        entries = []
        if not self.consumer:
            print("XXXX")
            return entries
        messages = self.consumer.fetch()
        for msg in messages:
            try:
                entries.append(json.loads(msg.get_data().decode('utf-8')))
                msg.ack()
            except Exception:
                continue
        return entries

    def close(self) -> None:
        self.running = False
        if self.consumer:
            self.consumer.destroy()
        if self.connection:
            self.connection.close()
