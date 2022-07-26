#! /usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

from functools import lru_cache
from collections import OrderedDict
from AbstractAction import AbstractAction

from tsm_datastore_lib.JournalEntry import JournalEntry
from tsm_datastore_lib.SqlAlchemyDatastore import SqlAlchemyDatastore

TOPIC_DELIMITER = '/'


class MqttLoggingAction(AbstractAction):
    # The maximum number of datastore instances (database connections) to be held
    # @todo: Get it as optional command line parameter
    DATASTORE_CACHE_SIZE = 100
    SCHEMA_FILE = './avro_schema_files/log_message.avsc'

    def __init__(self, root_topic, mqtt_broker, mqtt_user, mqtt_password, target_uri):
        super().__init__(root_topic, mqtt_broker, mqtt_user, mqtt_password)

        self.target_uri = target_uri
        self.datastores: OrderedDict[SqlAlchemyDatastore] = OrderedDict()

    def act(self, message: dict):
        topic = message.get("topic")
        log_entry = self.parse(message)
        datastore = self.__get_datastore_by_topic(topic)
        datastore.store_journal_entry(log_entry)
        datastore.insert_commit_chunk()

    def parse(self, message):
        return JournalEntry(
            timestamp=message['timestamp'],
            message=message['message'],
            level=message['level'],
            extra={}
        )

    @lru_cache(maxsize=DATASTORE_CACHE_SIZE)
    def __get_datastore_by_topic(self, topic):
        """
        :param topic: e.g. 'logging/7ff34ed2-5e56-11ec-9b0a-54e1ad7c5c19'
        """
        device_id = topic.split(TOPIC_DELIMITER)[1]
        datastore = SqlAlchemyDatastore(self.target_uri, device_id)
        self.datastores[topic] = datastore
        return datastore
