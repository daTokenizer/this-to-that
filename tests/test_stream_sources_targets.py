import pytest
from unittest.mock import patch, MagicMock
from sources.kafka import KafkaSource
from targets.kafka import KafkaTarget
from sources.rabbitmq import RabbitmqSource
from targets.rabbitmq import RabbitmqTarget
from sources.memphis import MemphisSource
from targets.memphis import MemphisTarget
from sources.redis_stream import RedisStreamSource
from targets.redis_stream import RedisStreamTarget

# Kafka
@patch('sources.kafka.KafkaConsumer')
def test_kafka_source(mock_consumer):
    instance = mock_consumer.return_value
    instance.poll.return_value = {0: [MagicMock(value={'foo': 'bar'})]}
    src = KafkaSource()
    src.initialize({'topic': 't', 'bootstrap_servers': 'localhost:9092'})
    entries = src.get_entries()
    assert entries == [{'foo': 'bar'}]
    src.close()
    instance.close.assert_called_once()

@patch('targets.kafka.KafkaProducer')
def test_kafka_target(mock_producer):
    instance = mock_producer.return_value
    tgt = KafkaTarget()
    tgt.initialize({'topic': 't', 'bootstrap_servers': 'localhost:9092'})
    tgt.create_entries([{'foo': 'bar'}])
    instance.send.assert_called_with('t', value={'foo': 'bar'})
    tgt.close()
    instance.close.assert_called_once()

# RabbitMQ
@patch('sources.rabbitmq.pika.BlockingConnection')
def test_rabbitmq_source(mock_conn):
    channel = MagicMock()
    mock_conn.return_value.channel.return_value = channel
    channel.basic_get.side_effect = [
        (object(), object(), b'{"foo": "bar"}'),
        (None, None, None)
    ]
    src = RabbitmqSource()
    src.initialize({'queue': 'q', 'host': 'localhost'})
    entries = src.get_entries()
    assert entries == [{'foo': 'bar'}]
    src.close()
    mock_conn.return_value.close.assert_called_once()

@patch('targets.rabbitmq.pika.BlockingConnection')
def test_rabbitmq_target(mock_conn):
    channel = MagicMock()
    mock_conn.return_value.channel.return_value = channel
    tgt = RabbitmqTarget()
    tgt.initialize({'queue': 'q', 'host': 'localhost'})
    tgt.create_entries([{'foo': 'bar'}])
    channel.basic_publish.assert_called_with(exchange='', routing_key='q', body=b'{"foo": "bar"}')
    tgt.close()
    mock_conn.return_value.close.assert_called_once()

# Memphis
@patch('sources.memphis.Memphis')
def test_memphis_source(mock_memphis):
    memphis_connection = mock_memphis.return_value.connect
    msg = MagicMock()
    src = MemphisSource()
    src.initialize({'station': 's', 'host': 'localhost'})
    memphis_connection.assert_called_once()
    consumer_instance = mock_memphis.return_value.consumer
    fetch_func = consumer_instance.return_value.fetch
    msg.get_data.return_value = b'{"foo": "bar"}'
    fetch_func.return_value = [msg]
    entries = src.get_entries()
    fetch_func.assert_called_once()
    assert entries == [{'foo': 'bar'}]
    src.close()
    consumer_instance.return_value.destroy.assert_called_once()
    mock_memphis.return_value.close.assert_called_once()

@patch('targets.memphis.Memphis')
def test_memphis_target(mock_memphis):
    memphis_connection = mock_memphis.return_value.connect
    tgt = MemphisTarget()
    tgt.initialize({'station': 's', 'host': 'localhost'})
    memphis_connection.assert_called_once()
    producer_instance = mock_memphis.return_value.producer
    producer_instance.assert_called_once()
    # mock_memphis.return_value.connect.assert_called_once()
    tgt.create_entries([{'foo': 'bar'}])
    produce_func = producer_instance.return_value.produce
    produce_func.assert_called_once()
    produce_func.assert_called_with(message=b'{"foo": "bar"}')
    tgt.close()
    producer_instance.return_value.destroy.assert_called_once()
    mock_memphis.return_value.close.assert_called_once()

# Redis Streams
@patch('sources.redis_stream.redis.Redis')
def test_redis_stream_source(mock_redis):
    client = mock_redis.return_value
    client.xreadgroup.return_value = [
        ('stream', [('id', {'data': '{"foo": "bar"}'})])
    ]
    src = RedisStreamSource()
    src.initialize({'stream': 'stream', 'group': 'g', 'consumer': 'c', 'host': 'localhost'})
    entries = src.get_entries()
    assert entries == [{'foo': 'bar'}]
    src.close()
    client.close.assert_called_once()

@patch('targets.redis_stream.redis.Redis')
def test_redis_stream_target(mock_redis):
    client = mock_redis.return_value
    tgt = RedisStreamTarget()
    tgt.initialize({'stream': 'stream', 'host': 'localhost'})
    tgt.create_entries([{'foo': 'bar'}])
    client.xadd.assert_called_with('stream', {'data': '{"foo": "bar"}'})
    tgt.close()
    client.close.assert_called_once()
