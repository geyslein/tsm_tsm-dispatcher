class Database:
    def __init__(self, username: str, password: str, url: str) -> None:
        self.username = username
        self.password = password
        self.url = url

    @staticmethod
    def get_instance(message: dict):
        if 'password' and 'username' and 'url' in message.keys():
            return Database(message.get('username'), message.get('password'), message.get('url'))
        else:
            raise ValueError('Unable to get Database instance from message "{}"'.format(message))


class Project:
    def __init__(self, uuid: str, name: str) -> None:
        self.uuid = uuid
        self.name = name

    @staticmethod
    def get_instance(message: dict):
        if 'uuid' and 'name' in message.keys():
            return Project(message.get('uuid'), message.get('name'))
        else:
            raise ValueError('Unable to get project instance from message "{}"'.format(message))


class RawDataStorage:
    def __init__(self, username: str, password: str, bucket_name: str) -> None:
        self.username = username
        self.password = password
        self.bucket_name = bucket_name

    @staticmethod
    def get_instance(message: dict):
        if 'username' and 'password' and 'bucket_name' in message.keys():
            return RawDataStorage(
                message.get('username'),
                message.get('password'),
                message.get('bucket_name')
            )
        else:
            raise ValueError('Unable to get raw data storage instance from message "{}"'.format(
                message))


class Thing:
    def __init__(self, uuid: str, name: str, project: Project, database: Database,
                 raw_data_storage: RawDataStorage, desc: str = '', properties: dict = {}):
        self.uuid = uuid
        self.name = name
        self.project = project
        self.database = database
        self.raw_data_storage = raw_data_storage
        self.description = desc
        self.properties = properties

    def set_description(self, desc: str):
        self.description = desc

    def set_properties(self, properties: dict):
        self.properties = properties

    @staticmethod
    def get_instance(message: dict):
        if 'uuid' and 'name' and 'database' and 'project' and 'raw_data_storage' in message.keys():
            return Thing(
                message.get('uuid'),
                message.get('name'),
                Project.get_instance(message.get('project')),
                Database.get_instance(message.get('database')),
                RawDataStorage.get_instance(message.get('raw_data_storage')),
                desc=message.get('description'),
                properties=message.get('properties'),
            )
        else:
            raise ValueError('Unable to get thing instance from message "{}"'.format(message))

