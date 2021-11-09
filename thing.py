import re


class Thing:
    def __init__(self, uuid: str, name: str):
        self.uuid = uuid
        self.name = name

    def slug(self):
        return re.sub(
            '[^a-z0-9\-]+',
            '',
            '{shortname}-{uuid}'.format(shortname=self.name[0:26].lower(), uuid=self.uuid)
        )

    @staticmethod
    def get_instance(message: dict):
        if 'uuid' and 'name' in message.keys():
            return Thing(message.get('uuid'), message.get('name'))
        else:
            raise ValueError('Unable to get thing instance from message "{}"'.format(message))
