#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import datetime
import warnings

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
from saqc.core.core import DictOfSeries

TOPIC_DELIMITER = "/"


class QcqaAction(AbstractAction):
    # The maximum number of datastore instances (database connections) to be held
    # @todo: Get it as optional command line parameter
    DATASTORE_CACHE_SIZE = 100

    def __init__(self, root_topic, mqtt_broker, mqtt_user, mqtt_password, target_uri):
        super().__init__(root_topic, mqtt_broker, mqtt_user, mqtt_password)

        self.target_uri = target_uri
        self.datastores: OrderedDict[SqlAlchemyDatastore] = OrderedDict()

    def _parse_qcqa_config(self, datastore):
        thing = datastore.sqla_thing
        idx = thing.properties["QCQA"]["default"]
        config = thing.properties["QCQA"]["configs"][idx]
        if config["type"] != "SaQC":
            raise NotImplementedError(
                "only configs of type SaQC are supported currently"
            )
        logging.debug(f"raw-config: {config=}")

        # config_df:
        #
        config_df = pd.DataFrame(config["tests"])
        config_df["window"] = self._parse_window(config["context_window"])

        return config_df

    def _parse_window(self, window) -> pd.Timedelta | int:
        # todo: add (4x) test for any combination of (+/-window, offset/numeric)
        if isinstance(window, int) or isinstance(window, str) and window.isnumeric():
            window = int(window)
            is_negative = window < 0
        else:
            window = pd.Timedelta(window)
            is_negative = window.days < 0
        if is_negative:
            raise ValueError("window can't be negative")
        return window

    def _get_datastream(self, datastore, position) -> Datastream:
        # todo: integrate this into datastore_lib
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
            # update cache
            if stream is not None:
                datastore.sqla_datastream_cache[name] = stream
        return stream

    def _get_datastream_data(
        self,
        datastore: SqlAlchemyDatastore,
        datastream: Datastream,
        window: int | pd.Timedelta,
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
            window: pd.Timedelta
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
            data = main_data
        else:
            data = pd.concat([main_data, window_data], sort=True)

        return data

    def _get_data(self, datastore: SqlAlchemyDatastore, config: pd.DataFrame):

        if config.empty:
            return

        unique_pos = config['position'].unique()
        data = DictOfSeries(columns=list(map(str, unique_pos)))

        # hint: window is the same for whole config
        window = config.loc[0, 'window']

        for position in unique_pos:
            datastream = self._get_datastream(datastore, position)
            config.loc[config['position'] == position, 'datastream'] = datastream
            if datastream is None:
                warnings.warn(f"no datastream for {position=}")
                continue
            raw = self._get_datastream_data(datastore, datastream, window)

            # todo: evaluate 'result_type'
            data[str(position)] = raw['result_number']

        return saqc.SaQC(data)

    def _run_qcqa(self, data: saqc.SaQC, config: pd.DataFrame):

        for idx, row in config.iterrows():
            # var is position for now, until SMS is inplace
            var = str(row['position'])
            func = row['function']
            kwargs = row['kwargs']
            info = config.loc[idx].to_dict()
            data = self._run_saqc_funtion(data, var, func, kwargs, info)

        return data

    def _run_saqc_funtion(self, qc_obj, var_name, func_name, kwargs, info):
        method = getattr(qc_obj, func_name, None)
        logging.debug(f"running SaQC with {info=}")
        qc_obj = method(var_name, **kwargs)
        return qc_obj

    def act(self, message: dict):
        topic = message.get("topic")
        datastore = self.__get_datastore_by_topic(topic)
        config = self._parse_qcqa_config(datastore)
        data = self._get_data(datastore, config)
        result = self._run_qcqa(data, config)

    @lru_cache(maxsize=DATASTORE_CACHE_SIZE)
    def __get_datastore_by_topic(self, topic):
        """
        :param topic: e.g. 'mqtt_ingest/seefo_envimo_cr6_test_002/7ff34ed2-5e56-11ec-9b0a-54e1ad7c5c19'
        """
        schema, device_id = topic.split(TOPIC_DELIMITER)[1:3]
        datastore = SqlAlchemyDatastore(self.target_uri, device_id, schema)
        self.datastores[topic] = datastore
        return datastore
