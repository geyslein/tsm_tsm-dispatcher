import logging
import paho.mqtt.client as mqtt
from abc import ABC, abstractmethod
from json import loads
import ast
from AvroSchemaValidator import avro_schema_validator
from fastavro._validate_common import ValidationError


class AbstractAction(ABC):

    def __init__(self, topic, mqtt_broker):
        self.topic = topic
        self.mqtt_broker = mqtt_broker
        self.mqtt_host = self.mqtt_broker[0].split(":")[0]
        self.mqtt_port = int(self.mqtt_broker[0].split(":")[1])
        self.mqtt_client = mqtt.Client()

    def connect_mqtt(self):
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                logging.info("Connected to {mqtt_broker}!".format(mqtt_broker=self.mqtt_broker))
            else:
                logging.info("Failed to connect, return code %d\n", rc)

        def on_log(client, userdata, level, buf):
            logging.info("log: ", buf)

        self.mqtt_client.username_pw_set('testUser', 'password')
        self.mqtt_client.on_connect = on_connect
        self.mqtt_client.on_log = on_log
        self.mqtt_client.connect(self.mqtt_host, self.mqtt_port)

    def publish(self, message):
        self.mqtt_client.loop_start()
        self.mqtt_client.publish(self.topic, message, qos=2, retain=False)

    def subscribe_to_mqtt_topic(self):
        self.connect_mqtt()
        def on_message(client, userdata, message):
            content = str(message.payload.decode("utf-8"))
            parsed_content = ast.literal_eval(content)
            print(parsed_content)
            if avro_schema_validator("thing_event.avsc", parsed_content):
                logging.info(
                    "Received message on topic '{topic}' with QoS {qos}:".format(topic=message.topic, qos=message.qos))
                self.act(parsed_content)
            else:
                logging.info("schema mismatch")
                raise ValidationError

        self.mqtt_client.subscribe(self.topic)
        self.mqtt_client.on_message = on_message

    def deserialize(self, x):
        return loads(x.decode('utf-8'))

    def run_loop(self):
        self.subscribe_to_mqtt_topic()
        self.mqtt_client.loop_forever()

    @abstractmethod
    def act(self, message: dict):
        raise NotImplementedError
