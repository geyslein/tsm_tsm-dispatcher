import json
import logging
import uuid
from json import loads, dumps

import click
import paho.mqtt.client as mqtt

from CreateThingOnMinioAction import CreateThingOnMinioAction
from ProcessNewFileAction import ProcessNewFileAction
from CreateThingInDatabaseAction import CreateThingInDatabaseAction
from MqttDatastreamAction import MqttDatastreamAction
from MqttLoggingAction import MqttLoggingAction
from MqttUserAction import MqttUserAction


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
              help="mqtt topic name to subscribe.",
              type=str,
              required=True,
              show_envvar=True,
              envvar='TOPIC'
              )
@click.option('mqtt_broker', '--mqtt-broker', '-m',
              help='MQTT broker to connect',
              required=True,
              show_envvar=True,
              envvar='MQTT_BROKER'
              )
@click.option('mqtt_user', '--mqtt-user', '-u',
              help='MQTT user',
              required=True,
              show_envvar=True,
              envvar='MQTT_USER'
              )
@click.option('mqtt_password', '--mqtt-password', '-p',
              help='MQTT password',
              required=True,
              show_envvar=True,
              envvar='MQTT_PASSWORD'
              )
@click.pass_context
def cli(ctx, topic, mqtt_broker, mqtt_user, mqtt_password, verbose):
    if verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO
    logging.basicConfig(level=level)


@cli.command()
@click.argument('minio_url', type=str, envvar='MINIO_URL')
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
def run_create_thing_on_minio_action_service(ctx, minio_url, minio_access_key,
                                             minio_secure_key,
                                             minio_secure):
    topic = ctx.parent.params['topic']
    mqtt_broker = ctx.parent.params["mqtt_broker"]
    mqtt_user = ctx.parent.params["mqtt_user"]
    mqtt_password = ctx.parent.params["mqtt_password"]

    logging.info(f'MQTT broker to connect: {mqtt_broker}')

    action = CreateThingOnMinioAction(topic, mqtt_broker, mqtt_user, mqtt_password, minio_settings={
        'minio_url': minio_url,
        'minio_access_key': minio_access_key,
        'minio_secure_key': minio_secure_key,
        'minio_secure': minio_secure
    })

    logging.info(f"Setup ok, starting service '{ctx.command.name}'")
    action.run_loop()


@cli.command()
@click.argument('database_url', type=str, envvar='DATABASE_URL')
# @click.argument('database_user', type=str, envvar='DATABASE_USER')
# @click.argument('database_pass', type=str, envvar='DATABASE_PASS')
@click.pass_context
def run_create_database_schema_action_service(ctx, database_url):
    topic = ctx.parent.params['topic']  # thing_created
    mqtt_broker = ctx.parent.params["mqtt_broker"]
    mqtt_user = ctx.parent.params["mqtt_user"]
    mqtt_password = ctx.parent.params["mqtt_password"]

    action = CreateThingInDatabaseAction(topic, mqtt_broker, mqtt_user, mqtt_password, database_settings={
        'url': database_url,
        # 'user': database_user,
        # 'pass': database_pass
    })

    logging.info(f"Setup ok, starting service '{ctx.command.name}'")
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
    mqtt_broker = ctx.parent.params["mqtt_broker"]
    mqtt_user = ctx.parent.params["mqtt_user"]
    mqtt_password = ctx.parent.params["mqtt_password"]

    logging.info(f'MQTT broker to connect: {mqtt_broker}')

    action = ProcessNewFileAction(
        topic, mqtt_broker, mqtt_user, mqtt_password,
        minio_settings={
            'minio_url': minio_url,
            'minio_access_key': minio_access_key,
            'minio_secure_key': minio_secure_key,
            'minio_secure': minio_secure
        },
        scheduler_settings={
            "url": scheduler_endpoint_url
        }
    )

    logging.info(f"Setup ok, starting service '{ctx.command.name}'")
    action.run_loop()


@cli.command()
@click.option("-t", "--target-uri", type=str, help="datastore uri")
@click.pass_context
def parse_data(ctx, target_uri: str):
    topic = ctx.parent.params['topic']
    mqtt_broker = ctx.parent.params["mqtt_broker"]
    mqtt_user = ctx.parent.params["mqtt_user"]
    mqtt_password = ctx.parent.params["mqtt_password"]

    action = MqttDatastreamAction(topic, mqtt_broker, mqtt_user, mqtt_password, target_uri)

    logging.info(f"Setup ok, starting service '{ctx.command.name}'")
    action.run_loop()


@cli.command()
@click.option("-t", "--target-uri", type=str, help="datastore uri")
@click.pass_context
def persist_log_messages_in_database_service(ctx, target_uri: str):
    topic = ctx.parent.params['topic']
    mqtt_broker = ctx.parent.params["mqtt_broker"]
    mqtt_user = ctx.parent.params["mqtt_user"]
    mqtt_password = ctx.parent.params["mqtt_password"]

    action = MqttLoggingAction(topic, mqtt_broker, mqtt_user, mqtt_password, target_uri)

    logging.info(f"Setup ok, starting service '{ctx.command.name}'")
    action.run_loop()


@cli.command()
@click.argument('database_url', type=str, envvar='DATABASE_URL')
# @click.argument('database_user', type=str, envvar='DATABASE_USER')
# @click.argument('database_pass', type=str, envvar='DATABASE_PASS')
@click.pass_context
def run_create_mqtt_user_action_service(ctx, database_url):
    topic = ctx.parent.params['topic']  # thing_created
    mqtt_broker = ctx.parent.params["mqtt_broker"]
    mqtt_user = ctx.parent.params["mqtt_user"]
    mqtt_password = ctx.parent.params["mqtt_password"]

    action = MqttUserAction(topic, mqtt_broker, mqtt_user, mqtt_password, database_settings={
        'url': database_url,
        # 'user': database_user,
        # 'pass': database_pass
    })

    logging.info(f"Setup ok, starting service '{ctx.command.name}'")
    action.run_loop()


if __name__ == '__main__':
    cli(obj={})
