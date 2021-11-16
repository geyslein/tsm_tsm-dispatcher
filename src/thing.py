import re


class Database:
    def __init__(self, password: str = '') -> None:
        self.password = password

    @staticmethod
    def get_instance(message: dict):
        if 'database_password' in message.keys():
            return Database(message.get('database_password'))
        else:
            # @todo add database to messages
            # raise ValueError('Unable to get Database instance from message "{}"'.format(message))
            return Database('Change this!')


class DataProject:
    def __init__(self, uuid: str, name: str) -> None:
        self.uuid = uuid
        self.name = name

    def slug(self):
        return re.sub(
            '[^a-z0-9_]+',
            '',
            '{shortname}_{uuid}'.format(shortname=self.name[0:30].lower(), uuid=self.uuid)
        )

    @staticmethod
    def get_instance(message: dict):
        if 'project_uuid' and 'project_name' in message.keys():
            return DataProject(message.get('project_uuid'), message.get('project_name'))
        else:
            # @todo add project to messages
            # raise ValueError('Unable to get project instance from message "{}"'.format(message))
            return DataProject('6185a5b8-4627-11ec-910a-125e5a40a845', 'My First Project')


class Thing:
    DEMO_PROPERTIES = {
        "parsers": [
            {
                "type": "AnotherCustomParser",
                "settings": {
                    "delimiter": ",",
                    "footlines": 0,
                    "headlines": 1,
                    "timestamp": {
                        "date": {
                            "pattern": "^(\\d{4})-(\\d{2})-(\\d{2})",
                            "position": 1,
                            "replacement": "$1-$2-$3"
                        },
                        "time": {
                            "pattern": "(\\d{2}):(\\d{2}):(\\d{2})$",
                            "position": 1,
                            "replacement": "$1:$2:$3"
                        }
                    }
                }
            },
            {
                "type": "MyCustomParser"
            },
            {
                "type": "CsvParser",
                "settings": {
                    "timestamp_format": "%Y/%m/%d %H:%M:%S",
                    "header": 3,
                    "delimiter": ",",
                    "timestamp_column": 1,
                    "skipfooter": 1
                }
            }
        ]
    }

    def __init__(self, uuid: str, name: str, project: DataProject, database: Database,
                 desc: str = '', properties: dict = {}):
        self.uuid = uuid
        self.name = name
        self.project = project
        self.database = database
        self.description = desc
        self.properties = properties if len(properties) else Thing.DEMO_PROPERTIES

    def slug(self):
        return re.sub(
            '[^a-z0-9\-]+',
            '',
            '{shortname}-{uuid}'.format(shortname=self.name[0:26].lower(), uuid=self.uuid)
        )

    def set_description(self, desc: str):
        self.description = desc

    def set_properties(self, properties: dict):
        self.properties = properties

    @staticmethod
    def get_instance(message: dict):
        if 'uuid' and 'name' in message.keys():
            return Thing(message.get('uuid'), message.get('name'),
                         DataProject.get_instance(message))
        else:
            raise ValueError('Unable to get thing instance from message "{}"'.format(message))

