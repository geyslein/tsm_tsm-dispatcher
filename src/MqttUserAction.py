#! /usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import psycopg2
from psycopg2 import sql as psysql

from AbstractAction import AbstractAction
from thing import Thing

class MqttUserAction(AbstractAction):
    def __init__(self, topic, mqtt_broker, mqtt_user, mqtt_password, database_settings: dict):
        super().__init__(topic, mqtt_broker, mqtt_user, mqtt_password)
        self.db = psycopg2.connect(database_settings.get('url'))

    def act(self, message: dict):
        thing = Thing.get_instance(message)

        # 1. Check, if there is already a database user for this project
        if not self.user_exists(thing):
            # 2.1 Create one if not
            self.create_user(thing)
            # 2.2 Create schema
            self.create_schema(thing)
            # 2.3 Deploy schema on new database
            self.deploy_ddl(thing)

    def create_user(self, thing):
        get_id = 'SELECT MAX(id) FROM mqtt_auth.mqtt_user;'
        sql = 'INSERT INTO mqtt_user (id,thing_uuid,username,password,description,properties) VALUES (%s, %s, %s, ' \
              '%s ,%s ,%s )  ON CONFLICT (thing_uuid) DO UPDATE SET name = EXCLUDED.username,' \
              ' password=EXCLUDED.password' \
              ' description = EXCLUDED.description,' \
              ' properties = EXCLUDED.properties'

        with self.db:
            with self.db.cursor() as c:
                max_id = c.execute(get_id)
                c.execute(
                    sql,
                    (max_id+1, thing.uuid, thing.mqtt_user, thing.password, thing.description, json.dumps(thing.properties))
                )







