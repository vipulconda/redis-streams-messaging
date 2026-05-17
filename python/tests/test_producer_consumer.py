import fakeredis

from redis_streams_messaging.config import StreamConfig
from redis_streams_messaging.consumer import StreamConsumer
from redis_streams_messaging.producer import StreamProducer


def test_publish_and_consume_success():
    client = fakeredis.FakeRedis(decode_responses=True)
    config = StreamConfig(group_name="g1", consumer_name="c1", max_retries=2)
    stream = "orders"

    producer = StreamProducer(config, client=client)
    consumer = StreamConsumer(config, client=client)

    producer.publish(stream, {"order_id": 42})
    seen = []

    count = consumer.consume(stream, lambda payload: seen.append(payload), max_messages=1)

    assert count == 1
    assert seen == [{"order_id": 42}]


def test_failed_message_moves_to_dlq_after_retries():
    client = fakeredis.FakeRedis(decode_responses=True)
    config = StreamConfig(group_name="g2", consumer_name="c2", max_retries=1)
    stream = "events"

    producer = StreamProducer(config, client=client)
    consumer = StreamConsumer(config, client=client)
    producer.publish(stream, {"event": "fail"})

    def boom(_payload):
        raise RuntimeError("processing failed")

    consumer.consume(stream, boom, max_messages=1)
    # Retry republish + original processed; consume retry until DLQ
    consumer.consume(stream, boom, max_messages=1)

    dlq_len = client.xlen(config.dlq_stream(stream))
    assert dlq_len == 1
