import json
import logging
from json import loads, dumps

import click
from kafka import KafkaConsumer, KafkaProducer

from CreateThingOnMinioAction import CreateThingOnMinioAction
from ProcessNewFileAction import ProcessNewFileAction
from CreateThingInDatabaseAction import CreateThingInDatabaseAction

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
@click.pass_context
def cli(ctx, topic, kafka_servers, verbose):

    # @todo remove topic from cli parameters and set it static per action class

    if verbose:
        logging.basicConfig(level=logging.DEBUG)


@cli.command()
@click.argument(
    'minio_url',
    type=str,
    envvar='MINIO_URL'
)
@click.argument('minio_access_key', type=str, envvar='MINIO_ACCESS_KEY')
@click.argument('minio_secure_key', type=str, envvar='MINIO_SECURE_KEY')
@click.option(
    '--minio_secure',
    type=bool,
    default=True,
    show_envvar=True,
    envvar='MINIO_SECURE',
    help='Use to disable TLS ("HTTPS://") for testing. Do not disable it on production!'
)
@click.pass_context
def run_create_thing_on_minio_action_service(ctx, minio_url, minio_access_key, minio_secure_key,
                                             minio_secure):
    topic = ctx.parent.params['topic']
    kafka_servers = ctx.parent.params['kafka_servers']
    kafka_group_id = 'run_create_thing_on_minio_action_service'

    logging.info('Apache kafka servers to connect: {}'.format(''.join(kafka_servers)))

    action = CreateThingOnMinioAction(topic, kafka_servers, kafka_group_id, minio_settings={
        'minio_url': minio_url,
        'minio_access_key': minio_access_key,
        'minio_secure_key': minio_secure_key,
        'minio_secure': minio_secure
    })

    # loop while waiting for messages
    action.run_loop()


@cli.command()
@click.argument('database_url', type=str, envvar='DATABASE_URL')
# @click.argument('database_user', type=str, envvar='DATABASE_USER')
# @click.argument('database_pass', type=str, envvar='DATABASE_PASS')
@click.pass_context
def run_create_database_schema_action_service(ctx, database_url):
    topic = ctx.parent.params['topic']  # thing_created
    kafka_servers = ctx.parent.params['kafka_servers']
    kafka_group_id = 'run_create_database_schema_action_service'

    action = CreateThingInDatabaseAction(topic, kafka_servers, kafka_group_id, database_settings={
        'url': database_url,
        # 'user': database_user,
        # 'pass': database_pass
    })

    action.run_loop()


@cli.command()
@click.argument('minio_url', type=str, envvar='MINIO_URL')
@click.argument('minio_access_key', type=str, envvar='MINIO_ACCESS_KEY')
@click.argument('minio_secure_key', type=str, envvar='MINIO_SECURE_KEY')
@click.argument('scheduler_endpoint_url', type=str, envvar='SCHEDULER_ENDPOINT_URL')
@click.option(
    '--minio_secure',
    type=bool,
    default=True,
    show_envvar=True,
    envvar='MINIO_SECURE',
    help='Use to disable TLS ("HTTPS://") for testing. Do not disable it on production!'
)
@click.pass_context
def run_process_new_file_service(ctx, minio_url, minio_access_key, minio_secure_key,
                                 scheduler_endpoint_url, minio_secure):
    topic = ctx.parent.params['topic']
    kafka_servers = ctx.parent.params['kafka_servers']
    kafka_group_id = 'run_process_new_file_service'

    logging.info('Apache kafka servers to connect: {}'.format(''.join(kafka_servers)))

    action = ProcessNewFileAction(topic, kafka_servers, kafka_group_id, minio_settings={
        'minio_url': minio_url,
        'minio_access_key': minio_access_key,
        'minio_secure_key': minio_secure_key,
        'minio_secure': minio_secure
    }, scheduler_settings={
        "url": scheduler_endpoint_url
    })

    # loop while waiting for messages
    action.run_loop()


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
