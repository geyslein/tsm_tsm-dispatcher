import json
import urllib
from datetime import datetime
from urllib import request

from minio import Minio
from minio.commonconfig import Tags

from AbstracAction import AbstractAction


class ProcessNewFileAction(AbstractAction):
    def __init__(self, topic, kafka_servers, kafka_group_id, minio_settings: dict,
                 scheduler_settings: dict):
        super().__init__(topic, kafka_servers, kafka_group_id)
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

    def act(self, message: dict):

        # skip all messages that are not a put event
        if message['EventName'] != 's3:ObjectCreated:Put':
            return

        filename = message['Records'][0]['s3']['object']['key']
        bucket_name = message['Records'][0]['s3']['bucket']['name']
        tags = self.minio.get_bucket_tags(bucket_name)
        thing_uuid = tags.get('thing_uuid')
        thing_database = {
            'thing_database_user': tags.get('thing_database_user'),
            'thing_database_pass': tags.get('thing_database_pass')
        }

        # @todo fetch thing from database to get minio creds. Or can we do this as minioadmin in
        # @todo this case? -> We should use some admin creds as normal thing user should not has 
        # @todo the ability to set bucket and object tags (as we rely on them, here, for example.  

        # add object tag with checkpoint and timestamp
        object_tags = Tags.new_object_tags()
        object_tags['thing_uuid'] = thing_uuid
        object_tags['checkpoint_process_new_file_action'] = datetime.now().isoformat()
        self.minio.set_object_tags(bucket_name, filename, object_tags)

        # forward file to basic demo scheduler
        # @todo get database (host-) name from configuration
        data = {
            "parser": tags.get('thing_properties_default_parser'),
            "target": "postgresql://{username}:"
                      "{password}@postgres/postgres".format(
                username=thing_database.get('thing_database_user'),
                password=thing_database.get('thing_database_pass')
            ),
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
            pass
