
import click
from kafka import KafkaProducer, KafkaConsumer, TopicPartition

from kafka.cluster import ClusterMetadata

KAFKA_HOST = 'localhost:9092'
TEST_TOPIC = 'test'


@click.group()
def cli():
    pass


@cli.command()
def produce():
    '''
    produce message
    '''
    
    producer = KafkaProducer(bootstrap_servers=KAFKA_HOST)
    for i in range(2):
        producer.send(TEST_TOPIC, b'some_bytes')


@cli.command()
def consume():
    '''
    consume message
    '''
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_HOST)
    consumer.assign([TopicPartition(TEST_TOPIC, 0)])
    for msg in consumer:
        print (msg)


@cli.command()
def cluster():
    cluster = ClusterMetadata(bootstrap_servers=KAFKA_HOST)
    print(cluster.brokers())


