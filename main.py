import json
import logging
from json import loads, dumps

import click
from kafka import KafkaConsumer, KafkaProducer

from CreateThingOnMinioAction import CreateThingOnMinioAction

__version__ = '0.0.1'


@click.group()
@click.version_option(__version__)
@click.option('--verbose', '-v',
              is_flag=True,
              help="Print more output.",
              envvar='VERBOSE',
              show_envvar=True,
              )
@click.option('--topic', '-t',
              help="Apache Kafka topic name to subscribe.",
              type=str,
              required=True,
              show_envvar=True,
              envvar='TOPIC'
              )
@click.option('kafka_servers', '--kafka-server', '-k',
              help='Apache Kafka bootstrap server. Multiple occurrences allowed.',
              required=True,
              show_envvar=True,
              multiple=True,
              envvar='KAFKA_SERVERS'
              )
@click.option('--kafka-group-id',
              help='Group ID of Apache Kafka consumer.',
              show_envvar=True,
              default='MyGroupId'
)
@click.pass_context
def cli(ctx, topic, kafka_servers, kafka_group_id, verbose):

    if verbose:
        logging.basicConfig(level=logging.DEBUG)


@cli.command()
@click.pass_context
def run_create_thing_on_minio_action_service(ctx):
    topic = ctx.parent.params['topic']
    kafka_servers = ctx.parent.params['kafka_servers']
    kafka_group_id = ctx.parent.params['kafka_group_id']

    logging.info('Apache kafka servers to connect: {}'.format(''.join(kafka_servers)))

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=kafka_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id=kafka_group_id,
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )

    action = CreateThingOnMinioAction()

    # loop while waiting for messages
    for message in consumer:
        try:
            action.act(message.value)
        except Exception as e:
            logging.critical(e)

        consumer.commit()


@cli.command()
@click.argument('message', type=str)
@click.pass_context
def produce(ctx, message):
    topic = ctx.parent.params['topic']
    kafka_servers = ctx.parent.params['kafka_servers']

    logging.info('Apache kafka servers to connect: {}'.format(''.join(kafka_servers)))

    m = json.loads(message)

    producer = KafkaProducer(
        bootstrap_servers=kafka_servers,
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )

    producer.send(topic=topic, value=m)


if __name__ == '__main__':
    cli(obj={})
