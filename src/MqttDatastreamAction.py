#! /usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from functools import lru_cache
from collections import OrderedDict
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

    # The maximum number of datastore instances (database connections) to be held
    # @todo: Get it as optional command line parameter
    DATASTORE_CACHE_SIZE = 100

    def __init__(self, root_topic, mqtt_broker, mqtt_user, mqtt_password, target_uri):
        super().__init__(root_topic, mqtt_broker, mqtt_user, mqtt_password)

        self.target_uri = target_uri
        self.datastores: OrderedDict[SqlAlchemyDatastore] = OrderedDict()

    def act(self, payload, client, userdata, message):
        origin = f"{userdata['mqtt_broker']}/{message.topic}"

        datastore = self.__get_datastore_by_topic(message.topic)

        parser = self.__get_parser(datastore)
        observations = parser(payload, origin)

        datastore.store_observations(observations)
        datastore.insert_commit_chunk()

    @lru_cache(maxsize=DATASTORE_CACHE_SIZE)
    def __get_datastore_by_topic(self, topic):
        """
        :param topic: e.g. 'mqtt_ingest/seefo_envimo_cr6_test_002/7ff34ed2-5e56-11ec-9b0a-54e1ad7c5c19'
        """
        schema, device_id = topic.split(TOPIC_DELIMITER)[:2]
        datastore = SqlAlchemyDatastore(self.target_uri, device_id, schema)
        self.datastores[topic] = datastore
        return datastore

    def __get_parser(self, datastore: SqlAlchemyDatastore) -> Callable[[dict, str], List[Observation]]:
        parser = datastore.sqla_thing.properties['default_parser']

        if parser == 'campbell_cr6':
            return campbell_cr6
