
import click
from confluent_kafka import Consumer, Producer

KAFKA_HOST = 'localhost:9092'
TEST_TOPIC = 'test-topic'
CHAR_ENCODE = 'utf-8'


@click.group()
def cli():
    pass


def produce_callback(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


@cli.command()
def produce():
    '''
    produce message
    '''
    
    producer = Producer({
        'bootstrap.servers': KAFKA_HOST
    })
    producer.poll(0)
    producer.produce(
        TEST_TOPIC,
        'dummy-data'.encode(CHAR_ENCODE)
    )

    producer.flush()


@cli.command()
def consume():
    '''
    consume message
    '''
    consumer = Consumer({
        'bootstrap.servers': KAFKA_HOST,
        'group.id': 'test-group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([TEST_TOPIC])
    message = consumer.poll(1.0)
    
    if message is None:
        print('no message')
    else:
        print('received: {}'.format(message.value().decode(CHAR_ENCODE)))


