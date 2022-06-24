#! /usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Dict, Callable, List
from AbstractAction import AbstractAction

import paho.mqtt.client as mqtt

from tsm_datastore_lib import get_datastore
from tsm_datastore_lib.Observation import Observation
from tsm_datastore_lib.SqlAlchemyDatastore import SqlAlchemyDatastore

TOPIC_DELIMITER = '/'

def campbell_cr6(payload: dict, origin: str) -> List[Observation]:
    # the basic data massage looked like this
    # {
    #     "type": "Feature",
    #     "geometry": {"type": "Point", "coordinates": [null, null, null]},
    #     "properties": {
    #         "loggerID": "CR6_18341",
    #         "observationNames": ["Batt_volt_Min", "PTemp"],
    #         "observations": {"2022-05-24T08:53:00Z": [11.9, 26.91]}
    #     }
    # }

    properties = payload.get("properties")
    if properties is None:
        return []

    out = []
    for timestamp, values in properties["observations"].items():
        for i, (key, value) in enumerate(zip(properties["observationNames"], values)):
            obs = Observation(
                timestamp=timestamp,
                value=value,
                position=i,
                origin=origin,
                header=key,
            )
            out.append(obs)
    return out


class MqttDatastreamAction(AbstractAction):
    def __init__(self, root_topic, mqtt_broker, mqtt_user, mqtt_password, target_uri):
        super().__init__(root_topic, mqtt_broker, mqtt_user, mqtt_password)

        self.target_uri = target_uri
        self.device_id = ''
        self.schema = ''
        self.datastore = None

    def act(self, message: dict):
        topic = message.get("topic")
        origin = f"{self.mqtt_broker}/{topic}"

        self.__prepare_datastore_by_topic(topic)

        parser = self.__get_parser()
        observations = parser(message, origin)

        self.datastore.store_observations(observations)
        self.datastore.insert_commit_chunk()

    def __prepare_datastore_by_topic(self, topic):
        """
        :param topic: e.g. 'mqtt_ingest/seefo_envimo_cr6_test_002/7ff34ed2-5e56-11ec-9b0a-54e1ad7c5c19'
        """
        schema = topic.split(TOPIC_DELIMITER)[1]
        device_id = topic.split(TOPIC_DELIMITER)[2]

        if self.datastore is None:
            self.datastore = SqlAlchemyDatastore(self.target_uri, device_id, schema)
            self.device_id = device_id
            self.schema = schema
            return

        if self.device_id != device_id or self.schema != schema:
            self.datastore.finalize()
            self.datastore = SqlAlchemyDatastore(self.target_uri, device_id, schema)
            self.device_id = device_id
            self.schema = schema

    def __get_parser(self) -> Callable[[dict], Observation]:
        parser = self.datastore.sqla_thing.properties['default_parser']

        if parser == 'campbell_cr6':
            return campbell_cr6
