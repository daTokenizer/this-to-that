from controller import DataTarget
from typing import Dict, Any, Iterable
from memphis import Memphis
import json

class MemphisTarget(DataTarget):
    """Memphis stream target implementation."""
    def __init__(self):
        self.connection = None
        self.station = None
        self.host = None
        self.username = None
        self.password = None
        self.producer = None

    def initialize(self, config: Dict[str, Any]) -> None:
        self.station = config.get('station')
        self.host = config.get('host', 'localhost')
        self.username = config.get('username', 'root')
        self.password = config.get('password', 'memphis')
        self.connection = Memphis()
        self.connection.connect(
            host=self.host,
            username=self.username,
            password=self.password
        )
        self.producer = self.connection.producer(
            station_name=self.station
        )

    def create_entries(self, entries: Iterable[Dict[str, Any]]) -> None:
        if not self.producer:
            return
        for entry in entries:
            self.producer.produce(message=json.dumps(entry).encode('utf-8'))

    def close(self) -> None:
        if self.producer:
            self.producer.destroy()
        if self.connection:
            self.connection.close()
