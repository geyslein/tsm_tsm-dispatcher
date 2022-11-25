import json
import psycopg2
import json
from psycopg2 import sql as psysql

from AbstractAction import AbstractAction
from thing import Thing

class MqttUserAction(AbstractAction):

    SCHEMA_FILE = './avro_schema_files/thing_event.avsc'

    def __init__(self, topic, mqtt_broker, mqtt_user, mqtt_password, database_settings: dict):
        super().__init__(topic, mqtt_broker, mqtt_user, mqtt_password)
        self.db = psycopg2.connect(database_settings.get('url'))
        print("innit m8")

    def act(self, message: dict):
        thing = Thing.get_instance(message)
        if message['mqtt_authentication_credentials']:
            user = message['mqtt_authentication_credentials']["username"]
            pw = message['mqtt_authentication_credentials']["password"]
            print(user)
            print(pw)
            self.create_user(thing, user, pw)

    def create_user(self, thing, user, pw):
        get_id = 'SELECT MAX(id) FROM mqtt_auth.mqtt_user;'
        sql = 'INSERT INTO mqtt_auth.mqtt_user (thing_uuid,username,password,description,properties) VALUES ( %s, %s, ' \
              '%s ,%s ,%s ) ON CONFLICT (thing_uuid) DO UPDATE SET username = EXCLUDED.username,' \
              ' password=EXCLUDED.password,' \
              ' description = EXCLUDED.description,' \
              ' properties = EXCLUDED.properties'
        print("db stuff")
        with self.db:
            with self.db.cursor() as c:
                max_id = c.execute(get_id)
                c.execute(
                    sql,
                    ( thing.uuid, user, pw, thing.description,
                     json.dumps(thing.properties))
                )

