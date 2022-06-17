#! /usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Dict, Callable, List
from AbstractAction import AbstractAction

import paho.mqtt.client as mqtt

from tsm_datastore_lib import get_datastore
from tsm_datastore_lib.Observation import Observation
from tsm_datastore_lib.SqlAlchemyDatastore import SqlAlchemyDatastore
from MqttHelper import get_schema_name_from_topic, get_device_id_from_topic


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
        self.datastore = None

    def act(self, payload, client, userdata, message):
        origin = f"{userdata['mqtt_broker']}/{message.topic}"

        self.__prepare_datastore_by_topic(message.topic)

        parser = self.__get_parser()
        observations = parser(payload, origin)

        self.datastore.store_observations(observations)
        self.datastore.finalize()

    def __prepare_datastore_by_topic(self, topic):
        schema = get_schema_name_from_topic (topic)
        device_id = get_device_id_from_topic(topic)

        self.datastore = SqlAlchemyDatastore(self.target_uri, device_id, schema)

    def __get_parser(self) -> Callable[[dict], Observation]:
        parser = self.datastore.sqla_thing.properties['default_parser']

        if parser == 'campbell_cr6':
            return campbell_cr6
