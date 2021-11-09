from pwgen import pwgen
from minio import Minio
from minio.commonconfig import GOVERNANCE
from minio.objectlockconfig import ObjectLockConfig, YEARS

from AbstracAction import AbstractAction
from minio_cli_wrapper.mc import Mc

from thing import Thing


class CreateThingOnMinioAction(AbstractAction):

    def __init__(self):
        super().__init__()

        # Custom minio client wrapper
        self.mcw = Mc(
            "localhost:9000",
            secure=False,
            access_key="minio",
            secret_key="minio123"
        )

    def act(self, message: dict):

        thing = Thing.get_instance(message)
        secret = pwgen(40, symbols=True)

        # create user
        # not implemented in minio python sdk yet :(
        # so we have to use minio cli client wrapper
        self.mcw.user_add(thing.slug(), secret)

        # mc admin policy add myminio/ datalogger1-policy /root/iam-policy-datalogger1.json
        # not implemented in minio python sdk yet :(
        self.mcw.policy_add(thing.slug(), {
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
                        "arn:aws:s3:::{bucket_name}".format(bucket_name=thing.slug())
                    ]
                }
            ]
        })

        # mc admin policy set myminio/ datalogger1-policy user=datalogger1-user
        # not implemented in minio python sdk yet :(
        self.mcw.policy_set_user(thing.slug(), thing.slug())

        # Create service account
        svc_creds = self.mcw.create_service_account(thing.slug())

        # Create bucket
        bucket_name = thing.slug()
        if not self.mcw.bucket_exists(bucket_name):
            try:
                self.mcw.make_locked_bucket(bucket_name)
            except Exception as e:
                raise ValueError('Unable to create bucket "{}": {}'.format(bucket_name, e))
        # set bucket retention config
        self.mcw.set_bucket_100y_retention(bucket_name)
