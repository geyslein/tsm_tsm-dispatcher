#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import datetime

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

    def parse_window(self, window) -> datetime.timedelta | int:
        # todo: 4 test for (+/-window X offset/numeric)
        if isinstance(window, int) or isinstance(window, str) and window.isnumeric():
            window = int(window)
            is_negative = window < 0
        else:
            window = pd.Timedelta(window).to_pydatetime()
            is_negative = window.days < 0
        if is_negative:
            raise ValueError("window can't be negative")
        return window

    def get_data(
        self,
        datastore: SqlAlchemyDatastore,
        datastream: Datastream,
        window: int | datetime.timedelta,
    ) -> pd.DataFrame | None:

        query = datastore.session.query(Observation.result_time).filter(
            Observation.datastream == datastream,
            Observation.result_quality == sqlalchemy.JSON.NULL,
        )
        more_recent = query.order_by(sqlalchemy.desc(Observation.result_time)).first()
        less_recent = query.order_by(sqlalchemy.asc(Observation.result_time)).first()

        # no new observations
        if more_recent is None:
            # todo: throw warning ?
            return None
        else:
            more_recent = more_recent[0]
            less_recent = less_recent[0]

        base_query = datastore.session.query(Observation).filter(
            Observation.datastream == datastream
        )

        # get all new (unprocessed) data
        query = base_query.filter(
            Observation.result_time <= more_recent,
            Observation.result_time >= less_recent,
        )
        main_data = pd.read_sql(
            query.statement,
            query.session.bind,
            parse_dates=True,
            index_col="result_time",
        )

        if main_data.empty:
            return None

        # numeric window
        # --------------
        # get window-many additional observations
        # preceding the first observation of the new data.
        # the window define a number of observations e.g. `500`
        if isinstance(window, int):

            query = base_query.filter(
                Observation.result_time < less_recent,
            ).order_by(sqlalchemy.desc(Observation.result_time))

            chunks = pd.read_sql(
                query.statement,
                query.session.bind,
                parse_dates=True,
                index_col="result_time",
                chunksize=window,
            )
            try:
                window_data = next(chunks)
            except StopIteration:  # no data
                window_data = pd.DataFrame([])

        # datetime window
        # ---------------
        # get arbitrary many observations, which lies in the
        # window before the first observation. The window is
        # defined by a datetime-offset e.g. `'7d'`
        else:
            window: datetime.timedelta
            query = base_query.filter(
                Observation.result_time < less_recent,
                Observation.result_time >= less_recent - window,
            )
            window_data = pd.read_sql(
                query.statement,
                query.session.bind,
                parse_dates=True,
                index_col="result_time",
            )

        if window_data.empty:
            data = window_data
        else:
            data = pd.concat([main_data, window_data], sort=True)

        return data

    def run_config(self, datastore: SqlAlchemyDatastore, config):
        window = self.parse_window(config["context_window"])
        for test in config["tests"]:
            pos = test["position"]
            function = test["function"]
            kwargs = test["kwargs"]
            stream = self.get_datastream(datastore, pos)
            df = self.get_data(datastore, stream, window)
            if df is None:
                continue
            ctx = dict(stream=stream)
            self.run_saqc_funtion(None, function, kwargs, ctx)

    def run_saqc_funtion(self, qc_obj, func, kwargs, ctx):
        qc = saqc.SaQC()
        method = getattr(qc, func, None)
        if method is None:
            raise RuntimeError(f"Function {func} not found. {ctx=}")
        ctx['func_name'] = method.__name__
        logging.debug(f"{ctx=}")

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
