from minio import Minio
from AbstracAction import AbstractAction


class ProcessNewFileAction(AbstractAction):
    def __init__(self, topic, kafka_servers, kafka_group_id, minio_settings: dict):
        super().__init__(topic, kafka_servers, kafka_group_id)
        self.minio_settings = minio_settings

        self.minio = Minio(
            minio_settings.get('minio_url'),
            secure=minio_settings.get('minio_secure', True),
            access_key=minio_settings.get('minio_access_key'),
            secret_key=minio_settings.get('minio_secure_key')
        )

    def act(self, message: dict):
        
        filename = message['Records'][0]['s3']['object']['key']
        bucket_name = message['Records'][0]['s3']['bucket']['name']
        tags = self.minio.get_bucket_tags(bucket_name)
        thing_id = tags.get('thing_uuid')
        thing_id_from_bucket_name = bucket_name[-36:]

        # @todo fetch thing from database to get minio creds. Or can we do this as minioadmin in
        #  this case?

        s = self.minio.stat_object(bucket_name, filename)
        self.minio.presigned_get_object(bucket_name, filename)

        # add object tag with checkpoint and timestamp

        pass
