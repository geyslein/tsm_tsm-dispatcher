from pwgen import pwgen

from AbstractAction import AbstractAction, MQTTMessage
from minio_cli_wrapper.mc import Mc, MinIoClientError

from thing import Thing


class CreateThingOnMinioAction(AbstractAction):

    SCHEMA_FILE = './avro_schema_files/thing_event.avsc'

    def __init__(self, topic, mqtt_broker, mqtt_user, mqtt_password, minio_settings: dict):
        super().__init__(topic, mqtt_broker, mqtt_user, mqtt_password)

        # Custom minio client wrapper
        self.mcw = Mc(
            minio_settings.get('minio_url'),
            secure=minio_settings.get('minio_secure', True),
            access_key=minio_settings.get('minio_access_key'),
            secret_key=minio_settings.get('minio_secure_key')
        )

    def act(self, content: dict, message: MQTTMessage):

        thing = Thing.get_instance(content)

        # create user
        # not implemented in minio python sdk yet :(
        # so we have to use minio cli client wrapper
        self.mcw.user_add(thing.raw_data_storage.username, thing.raw_data_storage.password)

        # mc admin policy add myminio/ datalogger1-policy /root/iam-policy-datalogger1.json
        # not implemented in minio python sdk yet :(
        self.mcw.policy_add(thing.raw_data_storage.username, {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetBucketLocation",
                        "s3:GetObject",
                        "s3:ListBucket",
                        "s3:PutObject"
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{thing.raw_data_storage.bucket_name}"
                    ]
                }
            ]
        })

        # mc admin policy set myminio/ datalogger1-policy user=datalogger1-user
        # not implemented in minio python sdk yet :(
        self.mcw.policy_set_user(thing.raw_data_storage.username, thing.raw_data_storage.username)

        # Create bucket
        bucket_name = thing.raw_data_storage.bucket_name
        if not self.mcw.bucket_exists(bucket_name):
            try:
                self.mcw.make_locked_bucket(bucket_name)
            except Exception as e:
                raise ValueError(f'Unable to create bucket "{bucket_name}": {e}')
        # set bucket retention config
        self.mcw.set_bucket_100y_retention(bucket_name)

        # enable bucket notifications
        self.mcw.enable_bucket_notification(bucket_name)

        # set bucked tags
        self.mcw.set_bucket_tags(bucket_name, {
            'thing_uuid': thing.uuid,
            'thing_name': thing.name,
            'thing_database_user': thing.database.username,
            'thing_database_pass': thing.database.password,
            'thing_database_url': thing.database.url,
            'thing_properties_default_parser': thing.properties.get('default_parser')
        })
