#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from AbstractAction import AbstractAction
from tsm_datastore_lib import SqlAlchemyDatastore
from tsm_datastore_lib.SqlAlchemy.Model import Observation, Thing, Datastream

import logging
import sqlalchemy
from functools import lru_cache
from collections import OrderedDict
from typing import Dict, Callable, List

import pandas as pd
import saqc

TOPIC_DELIMITER = "/"


class QcqaAction(AbstractAction):
    # The maximum number of datastore instances (database connections) to be held
    # @todo: Get it as optional command line parameter
    DATASTORE_CACHE_SIZE = 100

    def __init__(self, root_topic, mqtt_broker, mqtt_user, mqtt_password, target_uri):
        super().__init__(root_topic, mqtt_broker, mqtt_user, mqtt_password)

        self.target_uri = target_uri
        self.datastores: OrderedDict[SqlAlchemyDatastore] = OrderedDict()

    def parse_qcqa_config(self, thing):
        idx = thing.properties["QCQA"]["default"]
        config = thing.properties["QCQA"]["configs"][idx]
        if config["type"] != "SaQC":
            raise NotImplementedError(
                "only configs of type SaQC are supported currently"
            )
        logging.debug(f"{config=}")
        return config

    def get_unique_positions(self, config):
        return set(int(row["position"]) for row in config["tests"])

    def get_datastream(self, datastore, position) -> Datastream:
        thing: Thing = datastore.sqla_thing
        name = f"{thing.name}/{position}"

        # Lookup simple cache at first
        stream = datastore.sqla_datastream_cache.get(name, None)

        # cache miss
        if stream is None:
            stream = (
                datastore.session.query(Datastream)
                .filter(Datastream.name == name)
                .first()
            )
        return stream

    def get_data(
            self,
            datastore: SqlAlchemyDatastore,
            datastream: Datastream,
            window,
            columns=None,
    ) -> pd.DataFrame:

        obs = datastore.session.query(Observation.result_time).filter(
            Observation.datastream == datastream,
            Observation.result_quality == sqlalchemy.JSON.NULL,
        )
        more_recent = obs.order_by(sqlalchemy.desc(Observation.result_time)).first()
        less_recent = obs.order_by(sqlalchemy.asc(Observation.result_time)).first()

        # no new observations
        if more_recent is None:
            return pd.DataFrame()

        # todo: numeric window many entries before less_recent
        # todo: datetime size window before less_recent
        sql = datastore.session.query(
            Observation.result_time, sqlalchemy.text("result_number")
        ).filter(
            Observation.datastream == datastream,
            Observation.result_time <= more_recent[0],
            Observation.result_time >= less_recent[0],
        )

        return pd.read_sql(
            sql.statement, sql.session.bind, parse_dates=True, index_col="result_time"
        )

    def run_config(self, datastore: SqlAlchemyDatastore, config):
        tests = config["tests"]
        window = config["context_window"]
        for test in tests:
            pos = test["position"]
            function = test["function"]
            kwargs = test["kwargs"]
            stream = self.get_datastream(datastore, pos)
            df = self.get_data(datastore, stream, window)
            self.run_saqc_funtion(None, function, kwargs)

    def run_saqc_funtion(self, qc_obj, func, kwargs):
        qc = saqc.SaQC()
        method = getattr(qc, func, None)
        if method is None:
            raise RuntimeError(f"Function {func} not found")
        print(method)

    def act(self, message: dict):
        topic = message.get("topic")
        datastore = self.__get_datastore_by_topic(topic)
        config = self.parse_qcqa_config(datastore.sqla_thing)
        self.run_config(datastore, config)

    @lru_cache(maxsize=DATASTORE_CACHE_SIZE)
    def __get_datastore_by_topic(self, topic):
        """
        :param topic: e.g. 'mqtt_ingest/seefo_envimo_cr6_test_002/7ff34ed2-5e56-11ec-9b0a-54e1ad7c5c19'
        """
        schema, device_id = topic.split(TOPIC_DELIMITER)[1:3]
        datastore = SqlAlchemyDatastore(self.target_uri, device_id, schema)
        self.datastores[topic] = datastore
        return datastore
