from __future__ import annotations
import json
import logging
import os.path
import sys
import typing

import fastavro
import paho.mqtt.client as mqtt
from paho.mqtt.client import MQTTMessage
from abc import ABC, abstractmethod


class AbstractAction(ABC):

    SCHEMA_FILE = None  # default, may be overwritten in subclasses

    def __init__(self, topic, mqtt_broker, mqtt_user, mqtt_password):
        self.topic = topic
        self.mqtt_broker = mqtt_broker
        self.mqtt_user = mqtt_user
        self.mqtt_password = mqtt_password

        self.mqtt_host = self.mqtt_broker.split(":")[0]
        self.mqtt_port = int(self.mqtt_broker.split(":")[1])
        self.mqtt_client = mqtt.Client()

    def connect_mqtt(self):
        self.mqtt_client.username_pw_set(self.mqtt_user, self.mqtt_password)
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_log = self.on_log
        self.mqtt_client.connect(self.mqtt_host, self.mqtt_port)

    def subscribe_to_mqtt_topic(self):
        self.connect_mqtt()
        self.mqtt_client.subscribe(self.topic)
        self.mqtt_client.on_message = self.on_message

    def run_loop(self) -> typing.NoReturn:
        self.subscribe_to_mqtt_topic()
        self.mqtt_client.loop_forever()

    def on_log(self, client, userdata, level, buf):
        logging.debug(f"{buf}")

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logging.info(f"Connected to {self.mqtt_broker}")
        else:
            logging.error(f"Failed to connect, return code {rc}\n")

    def on_message(self, client, userdata, message: MQTTMessage):
        logging.info(
            f"{self.__class__.__name__} received message {message.mid} "
            f"on topic '{message.topic}' with QoS {message.qos}"
        )
        logging.debug(f"{message=}")
        try:
            content = self._parse_message(message)
            self.act(content, message)
        except Exception as e:
            logging.error(
                f"Errors occurred, discarding message {message.mid}", exc_info=e
            )

    def _validate_message(self, payload):
        assert self.SCHEMA_FILE is not None
        name = os.path.basename(self.SCHEMA_FILE)
        try:
            schema = fastavro.schema.load_schema(self.SCHEMA_FILE)
            fastavro.validate(payload, schema)
        except fastavro.validation.ValidationError:
            logging.warning(f"Received message does not match avro schema '{name}'")
            raise
        else:
            logging.debug(f"Received message matches avro schema '{name}'")

    def _parse_message(self, message: MQTTMessage):
        decoded: str = message.payload.decode("utf-8")

        if self.SCHEMA_FILE is not None:
            content = json.loads(decoded)
            self._validate_message(content)
            return content

        try:
            # also parse single numeric values
            # and the constants null, +/-Infinity, NoN
            return json.loads(decoded)
        except json.JSONDecodeError:
            # string / datetime / other
            return decoded

    @abstractmethod
    def act(self, content: dict, message: MQTTMessage):
        raise NotImplementedError
