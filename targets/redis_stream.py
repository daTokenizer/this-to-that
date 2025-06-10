from controller import DataTarget
from typing import Dict, Any, Iterable
import redis
import json

class RedisStreamTarget(DataTarget):
    """Redis Streams target implementation."""
    def __init__(self):
        self.client = None
        self.stream = None

    def initialize(self, config: Dict[str, Any]) -> None:
        self.stream = config.get('stream')
        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 6379)
        self.client = redis.Redis(host=self.host, port=self.port, decode_responses=True)

    def create_entries(self, entries: Iterable[Dict[str, Any]]) -> None:
        if not self.client:
            return
        for entry in entries:
            self.client.xadd(self.stream, {'data': json.dumps(entry)})

    def close(self) -> None:
        if self.client:
            self.client.close()
