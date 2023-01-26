import json
import logging
from urllib import request

from paho.mqtt.client import MQTTMessage

from AbstractAction import AbstractAction


class QaqcAction(AbstractAction):

    SCHEMA_FILE = "./avro_schema_files/data_parsed_event.avsc"

    def __init__(
        self, topic, mqtt_broker, mqtt_user, mqtt_password, scheduler_settings: dict
    ):

        super().__init__(topic, mqtt_broker, mqtt_user, mqtt_password)
        self.scheduler_settings = scheduler_settings
        self.request = request.Request(scheduler_settings.get("url"), method="POST")
        self.request.add_header("Content-Type", "application/json")

    def act(self, content: dict, message: MQTTMessage):
        data = {
            "thing_uuid": content["thing_uuid"],
            "target": content["db_uri"],
        }
        data = json.dumps(data)
        data = data.encode()
        r = request.urlopen(self.request, data=data)
        resp = json.loads(r.read())
