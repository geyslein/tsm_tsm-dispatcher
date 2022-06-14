#! /usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import uuid
from typing import Dict, Callable, List
from AbstracAction import AbstractAction

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
_PARSERS: Dict[str, Callable[[dict], List[Observation] | None]] = {
    "seefo-envimo-cr6-test-001/state/cr6/18341/" : envimo_parser
}


def get_parser(topic: str) -> Callable[[dict], Observation]:
    """
    Just a mock of a functionality to dynamically returning a parser for a given topic.

    We currently have no concept for this. One possible option would be to store the
    necessary parser/mapping as a property of the thing (like we do with the file based
    parsers) and incorporate the thing UUID into the topic. With this we could query the
    data base for an parser/mapper.
    """
    if topic not in _PARSERS:
        raise ValueError(f"no parser found for topic: {topic}")
    return _PARSERS[topic]


class MqttDatastreamAction(AbstractAction):
    def __init__(self, topic, mqtt_broker, mqtt_user, mqtt_password, target_uri: str, device_id: uuid.UUID):
        super().__init__(topic, mqtt_broker, mqtt_user, mqtt_password)
        self._target_uri = target_uri

        self.datastore = SqlAlchemyDatastore(target_uri, device_id)

    def act(self, payload, client, userdata, message):
        # NOTE: not yet working as the thing is not created as intended
        # datastore = get_datastore(self._target_uri, "32036c37-ba4c-4271-8815-500023374b9e")
        origin = f"{userdata['mqtt_broker']}/{message.topic}"
        parser = get_parser(message.topic)
        observations = parser(payload, origin)

        self.datastore.initiate_connection()
        self.datastore.store_observations(observations)
        self.datastore.finalize()
