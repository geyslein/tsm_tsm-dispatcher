class Database:
    def __init__(self, username: str, password: str, url: str) -> None:
        self.username = username
        self.password = password
        self.url = url

    @staticmethod
    def get_instance(message: dict):
        try:
            return Database(message['username'], message['password'], message['url'])
        except KeyError:
            raise ValueError(f'Unable to get Database instance from message "{message}"')


class Project:
    def __init__(self, uuid: str, name: str) -> None:
        self.uuid = uuid
        self.name = name

    @staticmethod
    def get_instance(message: dict):
        try:
            return Project(message['uuid'], message['name'])
        except KeyError:
            raise ValueError(f'Unable to get project instance from message "{message}"')


class RawDataStorage:
    def __init__(self, username: str, password: str, bucket_name: str) -> None:
        self.username = username
        self.password = password
        self.bucket_name = bucket_name

    @staticmethod
    def get_instance(message: dict):
        try:
            return RawDataStorage(
                message['username'],
                message['password'],
                message['bucket_name']
            )
        except KeyError:
            raise ValueError(
                f'Unable to get raw data storage '
                f'instance from message "{message}"'
            )


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
        try:
            return Thing(
                message['uuid'],
                message['name'],
                Project.get_instance(message['project']),
                Database.get_instance(message['database']),
                RawDataStorage.get_instance(message['raw_data_storage']),
                desc=message['description'],
                properties=message['properties'],
            )
        except KeyError:
            raise ValueError(f'Unable to get thing instance from message "{message}"')

