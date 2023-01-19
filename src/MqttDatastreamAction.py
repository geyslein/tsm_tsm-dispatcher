#! /usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from functools import lru_cache
from collections import OrderedDict
from typing import Dict, Callable, List
from AbstractAction import AbstractAction

import paho.mqtt.client as mqtt
import psycopg2
import psycopg2.extras

from datetime import datetime
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


def ydoc_ml417(payload: dict, origin: str) -> List[Observation]:
    # mqtt_ingest/test-logger-pb/test/data/jsn
    # {
    # "device":
    #   {"sn":99073020,"name":"UFZ","v":"4.2B5","imei":353081090730204,"sim":89490200001536167920},
    # "channels":[
    #   {"code":"SB","name":"Signal","unit":"bars"},
    #   {"code":"MINVi","name":"Min voltage","unit":"V"},
    #   {"code":"AVGVi","name":"Average voltage","unit":"V"},
    #   {"code":"AVGCi","name":"Average current","unit":"mA"},
    #   {"code":"P1*","name":"pr2_1_10","unit":"m3/m3"},
    #   {"code":"P2","name":"pr2_1_20","unit":"m3/m3"},
    #   {"code":"P3","name":"pr2_1_30","unit":"m3/m3"},
    #   {"code":"P4","name":"pr2_1_40","unit":"m3/m3"},
    #   {}],
    # "data":[
    #   {"$ts":230116110002,"$msg":"WDT;pr2_1"},
    #   {"$ts":230116110002,"MINVi":3.74,"AVGVi":3.94,"AVGCi":116,"P1*":"0*T","P2":"0*T","P3":"0*T","P4":"0*T"},
    #   {}]}
    if 'data/jsn' not in origin:
        return []

    data = payload['data'][1]
    ts = datetime.strptime(str(data["$ts"]), "%y%m%d%H%M%S")
    ob0 = Observation(ts, data['MINVi'], origin, 0, header="MINVi")
    ob1 = Observation(ts, data['AVGVi'], origin, 1, header="AVGCi")
    ob2 = Observation(ts, data['AVGCi'], origin, 2, header="AVGCi")
    ob3 = Observation(ts, data['P1*'], origin, 3, header="P1*")
    ob4 = Observation(ts, data['P2'], origin, 4, header="P2")
    ob5 = Observation(ts, data['P3'], origin, 5, header="P3")
    ob6 = Observation(ts, data['P4'], origin, 6, header="P4")
    return [ob0, ob1, ob2, ob3, ob4, ob5, ob6]


def brightsky_dwd_api(payload: dict, origin: str) -> List[Observation]:
    weather = payload['weather']
    timestamp = weather.pop('timestamp')
    source = payload['sources'][0]

    out = []

    for property, value in weather.items():
        try:
            obs = Observation(
                timestamp=timestamp,
                value=value,
                position=property,
                origin=origin,
                header=source,
            )
        except Exception as e:
            continue
        out.append(obs)

    return out


class MqttDatastreamAction(AbstractAction):
    # The maximum number of datastore instances (database connections) to be held
    # @todo: Get it as optional command line parameter
    DATASTORE_CACHE_SIZE = 100

    def __init__(self, root_topic, mqtt_broker, mqtt_user, mqtt_password, target_uri):
        super().__init__(root_topic, mqtt_broker, mqtt_user, mqtt_password)

        self.target_uri = target_uri
        self.auth_db = psycopg2.connect(target_uri)

    def act(self, message: dict):
        topic = message.get("topic")
        origin = f"{self.mqtt_broker}/{topic}"

        datastore = self.__get_datastore_by_topic(topic)

        parser = self.__get_parser(datastore)
        observations = parser(message, origin)

        try:
            datastore.store_observations(observations)
            datastore.insert_commit_chunk()
        except Exception as e:
            datastore.session.rollback()

    @lru_cache(maxsize=DATASTORE_CACHE_SIZE)
    def __get_datastore_by_topic(self, topic):
        """
        :param topic: e.g. 'mqtt_ingest/seefo_envimo_cr6_test_002/7ff34ed2-5e56-11ec-9b0a-54e1ad7c5c19'
        """
        mqtt_user = topic.split(TOPIC_DELIMITER)[1]
        sql = "select * from mqtt_auth.mqtt_user u where u.username = %(username)s"
        with self.auth_db:
            with self.auth_db.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor) as c:
                c.execute(sql, {"username": mqtt_user})
                mqtt_auth = c.fetchone()

        datastore = SqlAlchemyDatastore(self.target_uri, mqtt_auth["thing_uuid"],
                                        mqtt_auth["db_schema"])
        return datastore

    def __get_parser(self, datastore: SqlAlchemyDatastore) -> Callable[
        [dict, str], List[Observation]]:
        parser = datastore.sqla_thing.properties['default_parser']

        if parser == 'campbell_cr6':
            return campbell_cr6
        if parser == 'brightsky_dwd_api':
            return brightsky_dwd_api
        if parser == 'ydoc_ml417':
            return ydoc_ml417
