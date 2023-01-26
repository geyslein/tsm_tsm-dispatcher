import json
import os
import psycopg2
from psycopg2 import sql as psysql

from AbstractAction import AbstractAction, MQTTMessage
from thing import Thing


class CreateThingInDatabaseAction(AbstractAction):

    SCHEMA_FILE = './avro_schema_files/thing_event.avsc'

    def __init__(self, topic, mqtt_broker, mqtt_user, mqtt_password, database_settings: dict):
        super().__init__(topic, mqtt_broker, mqtt_user, mqtt_password)
        self.db = psycopg2.connect(database_settings.get('url'))

    def act(self, content: dict, message: MQTTMessage):

        thing = Thing.get_instance(content)

        # 1. Check, if there is already a database user for this project
        if not self.user_exists(thing):
            # 2.1 Create one if not
            self.create_user(thing)
            # 2.2 Create schema
            self.create_schema(thing)
            # 2.3 Deploy schema on new database
            self.deploy_ddl(thing)

        # 3. Insert thing entity
        self.upsert_thing(thing)

    def create_user(self, thing):
        with self.db:
            with self.db.cursor() as c:
                username_identifier = psysql.Identifier(thing.database.username.lower())
                c.execute(psysql.SQL("CREATE ROLE {0} WITH LOGIN PASSWORD {1}").format(
                    username_identifier,
                    psysql.Literal(thing.database.password)
                ))
                c.execute(psysql.SQL("GRANT {user} TO {creator}").format(
                    user=username_identifier,
                    creator=psysql.Identifier(self.db.info.user)
                ))

    def create_schema(self, thing):
        with self.db:
            with self.db.cursor() as c:
                c.execute(
                    psysql.SQL("CREATE SCHEMA IF NOT EXISTS {user} AUTHORIZATION {user}").format(
                        user=psysql.Identifier(thing.database.username.lower())
                    )
                )

    def deploy_ddl(self, thing):
        sql = open(os.path.join(
            os.path.dirname(__file__),
            'CreateThingInDatabaseAction/postgres-ddl.sql'
        )).read()
        with self.db:
            with self.db.cursor() as c:
                username_identifier = psysql.Identifier(thing.database.username.lower())
                # Set search path for current session
                c.execute(psysql.SQL("SET search_path TO {0}").format(
                    username_identifier
                ))
                # Allow tcp connections to database with new user
                c.execute(psysql.SQL(
                    "GRANT CONNECT ON DATABASE {db_name} TO {user}").format(
                        user=username_identifier,
                        db_name=psysql.Identifier(self.db.info.dbname)
                ))
                # Set default schema when connecting as user
                c.execute(psysql.SQL("ALTER ROLE {user} SET search_path to {user}").format(
                    user=username_identifier)
                )
                # Grant schema to new user
                c.execute(psysql.SQL("GRANT USAGE ON SCHEMA {user} TO {user}").format(
                    user=username_identifier)
                )
                # Equip new user with all grants
                c.execute(psysql.SQL("GRANT ALL ON SCHEMA {user} TO {user}").format(
                    user=username_identifier)
                )
                # deploy the tables and indices and so on
                c.execute(sql)

                c.execute(psysql.SQL(
                    "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {user} TO {user}").format(
                        user=username_identifier
                    )
                )

                c.execute(psysql.SQL(
                    "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {user} TO {user}").format(
                        user=username_identifier
                    )
                )

    def upsert_thing(self, thing):
        sql = 'INSERT INTO thing (name, uuid, description, properties) VALUES (%s, %s, %s, ' \
              '%s) ON CONFLICT (uuid) DO UPDATE SET name = EXCLUDED.name, description = ' \
              'EXCLUDED.description, properties = EXCLUDED.properties'
        with self.db:
            with self.db.cursor() as c:
                c.execute(psysql.SQL("SET search_path TO {}").format(
                    psysql.Identifier(thing.database.username.lower()))
                )
                c.execute(
                    sql,
                    (thing.name, thing.uuid, thing.description, json.dumps(thing.properties))
                )

    def user_exists(self, thing):
        with self.db:
            with self.db.cursor() as c:
                c.execute(
                    "SELECT 1 FROM pg_roles WHERE rolname=%s",
                    # don't forget the comma to make it a tuple!
                    (thing.database.username.lower(),))
                return len(c.fetchall()) > 0
