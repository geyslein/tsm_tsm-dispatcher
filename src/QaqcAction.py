import json
import logging
from datetime import datetime
from urllib import request

from AbstractAction import AbstractAction


class QaqcAction(AbstractAction):

    SCHEMA_FILE = './avro_schema_files/data_parsed_event.avsc'

    def __init__(self, topic, mqtt_broker, mqtt_user, mqtt_password, scheduler_settings: dict):

        super().__init__(topic, mqtt_broker, mqtt_user, mqtt_password)
        self.scheduler_settings = scheduler_settings
        self.request = request.Request(scheduler_settings.get('url'), method="POST")
        self.request.add_header('Content-Type', 'application/json')

    def act(self, message: dict):

        # skip all messages that are not a put event
        thing_uuid = message['thing_uuid']
        target = message['target_uri']

        data = {
            "thing_uuid": thing_uuid,
            "target": target,
        }

        try:
            data = json.dumps(data)
            data = data.encode()
            r = request.urlopen(self.request, data=data)
            resp = json.loads(r.read())
        except Exception as e:
            logging.error(f"{self.__class__.__name__}", exc_info=e)
