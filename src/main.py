import json
import logging
import logging.config
import os
import sys
import uuid
import warnings
from json import loads, dumps

import click
import paho.mqtt.client as mqtt
import yaml

from CreateThingOnMinioAction import CreateThingOnMinioAction
from ProcessNewFileAction import ProcessNewFileAction
from CreateThingInDatabaseAction import CreateThingInDatabaseAction
from MqttDatastreamAction import MqttDatastreamAction
from MqttLoggingAction import MqttLoggingAction
from QaqcAction import QaqcAction
from MqttUserAction import MqttUserAction

logger: logging.Logger = None

__version__ = "0.0.1"


@click.group()
@click.version_option(__version__)
@click.option(
    "--log-level",
    "-ll",
    help="Set the verbosity of logging messages.",
    envvar="LOG_LEVEL",
    show_envvar=True,
    default="INFO",
)
@click.option(
    "--topic",
    "-t",
    help="mqtt topic name to subscribe.",
    type=str,
    required=True,
    show_envvar=True,
    envvar="TOPIC",
)
@click.option(
    "mqtt_broker",
    "--mqtt-broker",
    "-m",
    help="MQTT broker to connect",
    required=True,
    show_envvar=True,
    envvar="MQTT_BROKER",
)
@click.option(
    "mqtt_user",
    "--mqtt-user",
    "-u",
    help="MQTT user",
    required=True,
    show_envvar=True,
    envvar="MQTT_USER",
)
@click.option(
    "mqtt_password",
    "--mqtt-password",
    "-p",
    help="MQTT password",
    required=True,
    show_envvar=True,
    envvar="MQTT_PASSWORD",
)
@click.pass_context
def cli(ctx, topic, mqtt_broker, mqtt_user, mqtt_password, log_level):
    global logger
    setup_logging(log_level)
    logger = logging.getLogger("dispatcher-main")
    logger.debug(f"script started: {' '.join(sys.argv)!r}")


def setup_logging(log_level):
    config = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logging.yaml")
    try:
        with open(config) as f:
            cdict = yaml.safe_load(f)
        logging.config.dictConfig(cdict)
    except OSError:
        warnings.warn("No logging configuration ['logging.yaml'] found.")
        logging.basicConfig()
    logging.getLogger().setLevel(log_level)


@cli.command()
@click.argument("minio_url", type=str, envvar="MINIO_URL")
@click.argument("minio_access_key", type=str, envvar="MINIO_ACCESS_KEY")
@click.argument("minio_secure_key", type=str, envvar="MINIO_SECURE_KEY")
@click.option(
    "--minio_secure",
    type=bool,
    default=True,
    show_envvar=True,
    envvar="MINIO_SECURE",
    help='Use to disable TLS ("HTTPS://") for testing. Do not disable it on production!',
)
@click.pass_context
def run_create_thing_on_minio_action_service(
    ctx, minio_url, minio_access_key, minio_secure_key, minio_secure
):
    topic = ctx.parent.params["topic"]
    mqtt_broker = ctx.parent.params["mqtt_broker"]
    mqtt_user = ctx.parent.params["mqtt_user"]
    mqtt_password = ctx.parent.params["mqtt_password"]

    logger.info(f"MQTT broker to connect: {mqtt_broker}")

    action = CreateThingOnMinioAction(
        topic,
        mqtt_broker,
        mqtt_user,
        mqtt_password,
        minio_settings={
            "minio_url": minio_url,
            "minio_access_key": minio_access_key,
            "minio_secure_key": minio_secure_key,
            "minio_secure": minio_secure,
        },
    )

    logger.info(f"Setup ok, starting service '{ctx.command.name}'")
    action.run_loop()


@cli.command()
@click.argument("database_url", type=str, envvar="DATABASE_URL")
# @click.argument('database_user', type=str, envvar='DATABASE_USER')
# @click.argument('database_pass', type=str, envvar='DATABASE_PASS')
@click.pass_context
def run_create_database_schema_action_service(ctx, database_url):
    topic = ctx.parent.params["topic"]  # thing_created
    mqtt_broker = ctx.parent.params["mqtt_broker"]
    mqtt_user = ctx.parent.params["mqtt_user"]
    mqtt_password = ctx.parent.params["mqtt_password"]

    action = CreateThingInDatabaseAction(
        topic,
        mqtt_broker,
        mqtt_user,
        mqtt_password,
        database_settings={
            "url": database_url,
            # 'user': database_user,
            # 'pass': database_pass
        },
    )

    logger.info(f"Setup ok, starting service '{ctx.command.name}'")
    action.run_loop()


@cli.command()
@click.argument("minio_url", type=str, envvar="MINIO_URL")
@click.argument("minio_access_key", type=str, envvar="MINIO_ACCESS_KEY")
@click.argument("minio_secure_key", type=str, envvar="MINIO_SECURE_KEY")
@click.argument("scheduler_endpoint_url", type=str, envvar="SCHEDULER_ENDPOINT_URL")
@click.option(
    "--minio_secure",
    type=bool,
    default=True,
    show_envvar=True,
    envvar="MINIO_SECURE",
    help='Use to disable TLS ("HTTPS://") for testing. Do not disable it on production!',
)
@click.pass_context
def run_process_new_file_service(
    ctx,
    minio_url,
    minio_access_key,
    minio_secure_key,
    scheduler_endpoint_url,
    minio_secure,
):
    topic = ctx.parent.params["topic"]
    mqtt_broker = ctx.parent.params["mqtt_broker"]
    mqtt_user = ctx.parent.params["mqtt_user"]
    mqtt_password = ctx.parent.params["mqtt_password"]

    logger.info(f"MQTT broker to connect: {mqtt_broker}")

    action = ProcessNewFileAction(
        topic,
        mqtt_broker,
        mqtt_user,
        mqtt_password,
        minio_settings={
            "minio_url": minio_url,
            "minio_access_key": minio_access_key,
            "minio_secure_key": minio_secure_key,
            "minio_secure": minio_secure,
        },
        scheduler_settings={"url": scheduler_endpoint_url},
    )

    logger.info(f"Setup ok, starting service '{ctx.command.name}'")
    action.run_loop()


@cli.command()
@click.option("-t", "--target-uri", type=str, help="datastore uri")
@click.pass_context
def parse_data(ctx, target_uri: str):
    topic = ctx.parent.params["topic"]
    mqtt_broker = ctx.parent.params["mqtt_broker"]
    mqtt_user = ctx.parent.params["mqtt_user"]
    mqtt_password = ctx.parent.params["mqtt_password"]

    action = MqttDatastreamAction(
        topic, mqtt_broker, mqtt_user, mqtt_password, target_uri
    )

    logger.info(f"Setup ok, starting service '{ctx.command.name}'")
    action.run_loop()


@cli.command()
@click.argument("scheduler_endpoint_url", type=str, envvar="SCHEDULER_ENDPOINT_URL")
@click.pass_context
def run_QAQC(ctx, scheduler_endpoint_url: str):
    topic = ctx.parent.params["topic"]
    mqtt_broker = ctx.parent.params["mqtt_broker"]
    mqtt_user = ctx.parent.params["mqtt_user"]
    mqtt_password = ctx.parent.params["mqtt_password"]

    logger.info(f"MQTT broker to connect: {mqtt_broker}")

    action = QaqcAction(
        topic,
        mqtt_broker,
        mqtt_user,
        mqtt_password,
        scheduler_settings={"url": scheduler_endpoint_url},
    )

    logger.info(f"Setup ok, starting service '{ctx.command.name}'")
    action.run_loop()


@cli.command()
@click.option("-t", "--target-uri", type=str, help="datastore uri")
@click.pass_context
def persist_log_messages_in_database_service(ctx, target_uri: str):
    topic = ctx.parent.params["topic"]
    mqtt_broker = ctx.parent.params["mqtt_broker"]
    mqtt_user = ctx.parent.params["mqtt_user"]
    mqtt_password = ctx.parent.params["mqtt_password"]

    action = MqttLoggingAction(topic, mqtt_broker, mqtt_user, mqtt_password, target_uri)

    logger.info(f"Setup ok, starting service '{ctx.command.name}'")
    action.run_loop()


@cli.command()
@click.argument("database_url", type=str, envvar="DATABASE_URL")
# @click.argument('database_user', type=str, envvar='DATABASE_USER')
# @click.argument('database_pass', type=str, envvar='DATABASE_PASS')
@click.pass_context
def run_create_mqtt_user_action_service(ctx, database_url):
    topic = ctx.parent.params["topic"]  # thing_created
    mqtt_broker = ctx.parent.params["mqtt_broker"]
    mqtt_user = ctx.parent.params["mqtt_user"]
    mqtt_password = ctx.parent.params["mqtt_password"]

    action = MqttUserAction(
        topic,
        mqtt_broker,
        mqtt_user,
        mqtt_password,
        database_settings={
            "url": database_url,
            # 'user': database_user,
            # 'pass': database_pass
        },
    )

    logger.info(f"Setup ok, starting service '{ctx.command.name}'")
    action.run_loop()


if __name__ == "__main__":
    cli(obj={})
