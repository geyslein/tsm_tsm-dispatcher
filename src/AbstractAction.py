import logging
import paho.mqtt.client as mqtt
from abc import ABC, abstractmethod
from MqttHelper import on_message, on_log, on_connect

logging.basicConfig(format='%(levelname)s: (%(asctime)s) %(message)s', datefmt='%d.%m.%y %H:%M:%S', level=logging.INFO)


class AbstractAction(ABC):

    SCHEMA_FILE = None  # default, may be overwritten in subclasses

    def __init__(self, topic, mqtt_broker, mqtt_user, mqtt_password):
        self.topic = topic
        self.mqtt_broker = mqtt_broker
        self.mqtt_user = mqtt_user
        self.mqtt_password = mqtt_password

        self.mqtt_host = self.mqtt_broker[0].split(":")[0]
        self.mqtt_port = int(self.mqtt_broker[0].split(":")[1])
        self.mqtt_client = mqtt.Client()

    def connect_mqtt(self):
        self.mqtt_client.user_data_set({"schema_file": self.SCHEMA_FILE,
                                        "act": self.act,
                                        "mqtt_broker": self.mqtt_broker[0]})
        self.mqtt_client.username_pw_set(self.mqtt_user, self.mqtt_password)
        self.mqtt_client.on_connect = on_connect
        self.mqtt_client.on_log = on_log
        self.mqtt_client.connect(self.mqtt_host, self.mqtt_port)

    def subscribe_to_mqtt_topic(self):
        self.connect_mqtt()
        self.mqtt_client.subscribe(self.topic)
        self.mqtt_client.on_message = on_message

    def run_loop(self):
        self.subscribe_to_mqtt_topic()
        self.mqtt_client.loop_forever()

    @abstractmethod
    def act(self, payload: dict, client: mqtt.Client, userdata: dict, message: mqtt.MQTTMessage):
        raise NotImplementedError
