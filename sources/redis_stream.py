from controller import DataSource
from typing import Dict, Any, List
import redis
import json

class RedisStreamSource(DataSource):
    """Redis Streams source implementation."""
    def __init__(self):
        self.client = None
        self.stream = None
        self.group = None
        self.consumer = None
        self.running = False

    def initialize(self, config: Dict[str, Any]) -> None:
        self.stream = config.get('stream')
        self.group = config.get('group', 'etl-group')
        self.consumer = config.get('consumer', 'etl-consumer')
        self.host = config.get('host', 'localhost')
        self.port = config.get('port', 6379)
        self.client = redis.Redis(host=self.host, port=self.port, decode_responses=True)
        # Create consumer group if not exists
        try:
            self.client.xgroup_create(self.stream, self.group, id='0', mkstream=True)
        except redis.exceptions.ResponseError as e:
            if 'BUSYGROUP' not in str(e):
                raise
        self.running = True

    def get_entries(self) -> List[Dict[str, Any]]:
        entries = []
        if not self.client:
            return entries
        messages = self.client.xreadgroup(self.group, self.consumer, {self.stream: '>'}, count=100, block=1000)
        for _, msgs in messages:
            for msg_id, msg_data in msgs:
                try:
                    # Assume the message is stored under a 'data' field as JSON
                    if 'data' in msg_data:
                        entries.append(json.loads(msg_data['data']))
                    self.client.xack(self.stream, self.group, msg_id)
                except Exception:
                    continue
        return entries

    def close(self) -> None:
        self.running = False
        if self.client:
            self.client.close()
