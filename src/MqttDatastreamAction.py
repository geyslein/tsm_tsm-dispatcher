#! /usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import time
import uuid
from typing import Dict, Callable, List
from AbstractAction import AbstractAction

import paho.mqtt.client as mqtt

from tsm_datastore_lib import get_datastore
from tsm_datastore_lib.Observation import Observation
from tsm_datastore_lib.SqlAlchemyDatastore import SqlAlchemyDatastore


def envimo_parser(payload: dict, origin: str) -> List[Observation]:
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


# the parser/mapper 'database'
_SETTINGS: Dict[str, Dict] = {
    "seefo-envimo-cr6-test-001/state/cr6/18341/": {
        "parser": envimo_parser,
        "target_uri": "postgresql://myfirst6185a5b8462711ec910a125e5a40a845:d0ZZ9d3QSDZ6tXIZTnKRY1uVLKIc05GmQh8SA36M@localhost:5432/postgres",
        "device_id": "057d8bba-40b3-11ec-a337-125e5a40a849"
    }
}


def get_datastore_by_topic(topic: str):
    target_uri = _SETTINGS[topic]["target_uri"]
    device_id = _SETTINGS[topic]["device_id"]
    return SqlAlchemyDatastore(target_uri, device_id)


class MqttDatastreamAction(AbstractAction):
    def __init__(self, topic, mqtt_broker, mqtt_user, mqtt_password):
        super().__init__(topic, mqtt_broker, mqtt_user, mqtt_password)
        self.parser_mapping = {}
        self.time_of_settings_update = 0
        self.update_settings()

    def update_settings(self):
        """
        Just a mock of a functionality to dynamically returning:
            - parser
            - respective DB-connection-string
            - and Device-ID
        for a given topic.
        """
        self.parser_mapping = _SETTINGS
        self.time_of_settings_update = time.time()

    def get_parser(self, topic: str) -> Callable[[dict], Observation]:

        time_lapsed = time.time() - self.time_of_settings_update

        if time_lapsed > 60:
            self.update_settings()

        if topic not in _SETTINGS:
            raise ValueError(f"no parser found for topic: {topic}")

        return self.parser_mapping[topic]["parser"]

    def act(self, payload, client, userdata, message):

        datastore = get_datastore_by_topic(message.topic)

        origin = f"{userdata['mqtt_broker']}/{message.topic}"
        parser = self.get_parser(message.topic)

        observations = parser(payload, origin)

        datastore.initiate_connection()
        datastore.store_observations(observations)
        datastore.finalize()
