import json
import psycopg2
import json
from psycopg2 import sql as psysql

from AbstractAction import AbstractAction, MQTTMessage
from thing import Thing


class MqttUserAction(AbstractAction):

    SCHEMA_FILE = "./avro_schema_files/thing_event.avsc"

    def __init__(
        self, topic, mqtt_broker, mqtt_user, mqtt_password, database_settings: dict
    ):
        super().__init__(topic, mqtt_broker, mqtt_user, mqtt_password)
        self.db = psycopg2.connect(database_settings.get("url"))

    def act(self, content: dict, message: MQTTMessage):
        thing = Thing.get_instance(content)
        if content["mqtt_authentication_credentials"]:
            user = content["mqtt_authentication_credentials"]["username"]
            pw = content["mqtt_authentication_credentials"]["password_hash"]
            self.create_user(thing, user, pw)

    def create_user(self, thing, user, pw):
        sql = (
            "INSERT INTO mqtt_auth.mqtt_user (project_uuid, thing_uuid, username, "
            "password, description,properties, db_schema) "
            "VALUES (%s, %s, %s, %s ,%s ,%s, %s) "
            "ON CONFLICT (thing_uuid) "
            "DO UPDATE SET"
            " project_uuid = EXCLUDED.project_uuid,"
            " username = EXCLUDED.username,"
            " password=EXCLUDED.password,"
            " description = EXCLUDED.description,"
            " properties = EXCLUDED.properties,"
            " db_schema = EXCLUDED.db_schema"
        )
        with self.db:
            with self.db.cursor() as c:
                c.execute(
                    sql,
                    (
                        thing.project.uuid,
                        thing.uuid,
                        user,
                        pw,
                        thing.description,
                        json.dumps(thing.properties),
                        thing.database.username,
                    ),
                )
