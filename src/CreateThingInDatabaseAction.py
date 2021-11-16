import json
import os
import psycopg2

from AbstracAction import AbstractAction
from thing import Thing


class CreateThingInDatabaseAction(AbstractAction):

    def __init__(self, topic, kafka_servers, kafka_group_id, database_settings: dict):
        super().__init__(topic, kafka_servers, kafka_group_id)

        self.db = psycopg2.connect(database_settings.get('url'))

    def act(self, message: dict):

        thing = Thing.get_instance(message)

        # 1. Check, if there is already a database user for this project
        if not self.user_exists(thing):
            # 2.1 Create one of not
            self.create_user(thing)
            # 2.2 Create schema
            self.create_schema(thing)
            # 2.3 Deploy schema on new database
            self.deploy_ddl(thing)

        # 3. Insert thing entity
        self.upsert_thing(thing)

    def create_user(self, thing):
        sql = "CREATE ROLE {} WITH LOGIN PASSWORD '{}'".format(thing.project.slug(),
                                                               thing.database.password)
        with self.db:
            with self.db.cursor() as c:
                c.execute(sql)

    def create_schema(self, thing):
        sql = "CREATE SCHEMA IF NOT EXISTS {} AUTHORIZATION {}".format(
            thing.project.slug(), thing.project.slug()
        )
        with self.db:
            with self.db.cursor() as c:
                c.execute(sql)

    def deploy_ddl(self, thing):
        sql = open(os.path.join(
            os.path.dirname(__file__),
            'CreateThingInDatabaseAction/postgres-ddl.sql'
        )).read()
        with self.db:
            with self.db.cursor() as c:
                c.execute("SET search_path TO {}".format(thing.project.slug()))
                c.execute(sql)

    def upsert_thing(self, thing):
        sql = 'INSERT INTO thing (name, uuid, description, properties) VALUES (%s, %s, %s, ' \
              '%s) ON CONFLICT (uuid) DO UPDATE SET name = EXCLUDED.name, description = ' \
              'EXCLUDED.description, properties = EXCLUDED.properties'
        with self.db:
            with self.db.cursor() as c:
                c.execute("SET search_path TO {}".format(thing.project.slug()))
                c.execute(
                    sql,
                    (thing.name, thing.uuid, thing.description, json.dumps(thing.properties))
                )

    def user_exists(self, thing):
        sql = "SELECT 1 FROM pg_roles WHERE rolname='{}'".format(thing.project.slug())
        with self.db:
            with self.db.cursor() as c:
                c.execute(sql)
                return len(c.fetchall()) > 0