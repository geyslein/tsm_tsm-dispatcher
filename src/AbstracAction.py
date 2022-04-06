import json
import logging
import paho.mqtt.client as mqtt
from abc import ABC, abstractmethod

from AvroSchemaValidator import validate_avro_schema
from fastavro._validate_common import ValidationError

logging.basicConfig(format='%(levelname)s: (%(asctime)s) %(message)s', datefmt='%d.%m.%y %H:%M:%S', level=logging.INFO)


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
        parsed_content = json.loads(content)
        if validate_avro_schema(parsed_content, self.has_schema, self.schema_file):
            logging.info(
               "Received message on topic '{topic}' with QoS {qos}!".format(p=parsed_content,topic=message.topic, qos=message.qos))
            self.act(parsed_content)
        else:
            raise ValidationError

    def on_connect(self,client, userdata, flags, rc):
        if rc == 0:
            logging.info("Connected to {mqtt_broker}!".format(mqtt_broker=self.mqtt_broker[0]))
        else:
            logging.info("Failed to connect, return code %d\n", rc)

    @staticmethod
    def on_log(client, userdata, level, buf):
        logging.info("{}".format(buf))

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
