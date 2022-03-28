import logging
import paho.mqtt.client as mqtt
from abc import ABC, abstractmethod
import ast

from AvroSchemaValidator import avro_schema_validator
from fastavro._validate_common import ValidationError


class AbstractAction(ABC):

    def __init__(self, topic, mqtt_broker, mqtt_user, mqtt_password):
        self.topic = topic
        self.mqtt_broker = mqtt_broker
        self.mqtt_host = self.mqtt_broker[0].split(":")[0]
        self.mqtt_port = int(self.mqtt_broker[0].split(":")[1])
        self.mqtt_user = mqtt_user
        self.mqtt_password = mqtt_password
        self.mqtt_client = mqtt.Client()

    def on_message(self, client, userdata, message):
        content = str(message.payload.decode("utf-8"))
        parsed_content = ast.literal_eval(content)
        if avro_schema_validator("thing_event.avsc", parsed_content):
            logging.info(
                "Received message on topic '{topic}' with QoS {qos}:".format(topic=message.topic, qos=message.qos))
            self.act(parsed_content)
        else:
            logging.info("schema mismatch")
            raise ValidationError

    @staticmethod
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            logging.info("Connected to {mqtt_broker}!".format(mqtt_broker=client))
        else:
            logging.info("Failed to connect, return code %d\n", rc)

    @staticmethod
    def on_log(client, userdata, level, buf):
        logging.info("log: ", buf)

    def connect_mqtt(self):
        self.mqtt_client.username_pw_set(self.mqtt_user, self.mqtt_password)
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_log = self.on_log
        self.mqtt_client.connect(self.mqtt_host, self.mqtt_port)

    def subscribe_to_mqtt_topic(self):
        self.connect_mqtt()
        self.mqtt_client.subscribe(self.topic)
        self.mqtt_client.on_message = self.on_message

    def run_loop(self):
        self.subscribe_to_mqtt_topic()
        self.mqtt_client.loop_forever()

    @abstractmethod
    def act(self, message: dict):
        raise NotImplementedError
