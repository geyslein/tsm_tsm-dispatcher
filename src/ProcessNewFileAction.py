import json
import logging
from datetime import datetime
from urllib import request

from minio import Minio
from minio.commonconfig import Tags

from AbstractAction import AbstractAction, MQTTMessage


class ProcessNewFileAction(AbstractAction):

    SCHEMA_FILE = './avro_schema_files/new_file_event.avsc'

    def __init__(self, topic, mqtt_broker, mqtt_user, mqtt_password, minio_settings: dict,
                 scheduler_settings: dict):
        super().__init__(topic, mqtt_broker, mqtt_user, mqtt_password)
        self.minio_settings = minio_settings
        self.minio = Minio(
            minio_settings.get('minio_url'),
            secure=minio_settings.get('minio_secure', True),
            access_key=minio_settings.get('minio_access_key'),
            secret_key=minio_settings.get('minio_secure_key')
        )

        self.scheduler_settings = scheduler_settings
        self.request = request.Request(scheduler_settings.get('url'), method="POST")
        self.request.add_header('Content-Type', 'application/json')

    def act(self, content: dict, message: MQTTMessage):

        # skip all messages that are not a put event
        if content['EventName'] != 's3:ObjectCreated:Put':
            return

        filename = content['Records'][0]['s3']['object']['key']
        bucket_name = content['Records'][0]['s3']['bucket']['name']
        tags = self.minio.get_bucket_tags(bucket_name)
        thing_uuid = tags.get('thing_uuid')
        thing_database = {
            'user': tags.get('thing_database_user'),
            'pass': tags.get('thing_database_pass'),
            'url': tags.get('thing_database_url')
        }

        # add object tag with checkpoint and timestamp
        object_tags = Tags.new_object_tags()
        object_tags['thing_uuid'] = thing_uuid
        object_tags['checkpoint_process_new_file_action'] = datetime.now().isoformat()
        self.minio.set_object_tags(bucket_name, filename, object_tags)

        # forward file to basic demo scheduler
        data = {
            "parser": tags.get('thing_properties_default_parser'),
            "target": thing_database.get('url'),
            "source": self.minio.presigned_get_object(bucket_name, filename),
            "thing_uuid": thing_uuid
        }

        try:

            data = json.dumps(data)
            data = data.encode()
            r = request.urlopen(self.request, data=data)
            resp = json.loads(r.read())

            # add object tag with checkpoint and timestamp
            # hint reuse tags object as the existing tags are overwritten otherweise
            object_tags['transmitted_to_scheduler'] = datetime.now().isoformat()
            # Add some information about the scheduler, when we have some useful data from it
            object_tags['scheduled_job_id'] = '23'
            # When using a real scheduler, this will not work anymore because its async...
            object_tags['parser_output'] = resp.get('out')
            self.minio.set_object_tags(bucket_name, filename, object_tags)

        except Exception as e:
            logging.error(f"{self.__class__.__name__}", exc_info=e)
