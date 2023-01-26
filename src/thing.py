from __future__ import annotations


class Database:
    def __init__(self, username: str, password: str, url: str):
        self.username = username
        self.password = password
        self.url = url

    @classmethod
    def get_instance(cls, message: dict) -> Database:
        try:
            return cls(message["username"], message["password"], message["url"])
        except KeyError:
            raise ValueError(
                f'Unable to get Database instance from message "{message}"'
            )


class Project:
    def __init__(self, uuid: str, name: str) -> None:
        self.uuid = uuid
        self.name = name

    @classmethod
    def get_instance(cls, message: dict) -> Project:
        try:
            return cls(message["uuid"], message["name"])
        except KeyError:
            raise ValueError(f'Unable to get Project instance from message "{message}"')


class RawDataStorage:
    def __init__(self, username: str, password: str, bucket_name: str):
        self.username = username
        self.password = password
        self.bucket_name = bucket_name

    @classmethod
    def get_instance(cls, message: dict) -> RawDataStorage:
        try:
            return cls(message["username"], message["password"], message["bucket_name"])
        except KeyError:
            raise ValueError(
                f'Unable to get RawDataStorage instance from message "{message}"'
            )


class Thing:
    def __init__(
        self,
        uuid: str,
        name: str,
        project: Project,
        database: Database,
        raw_data_storage: RawDataStorage,
        description: str,
        properties: dict,
    ):
        self.uuid = uuid
        self.name = name
        self.project = project
        self.database = database
        self.raw_data_storage = raw_data_storage
        self.description = description
        self.properties = properties

    @classmethod
    def get_instance(cls, message: dict) -> Thing:
        # Raw data storage is optional, i.e. when using mqtt only
        raw_data_storage = None
        try:

            if "raw_data_storage" in message:
                raw_data_storage = RawDataStorage.get_instance(
                    message["raw_data_storage"]
                )

            return cls(
                message["uuid"],
                message["name"],
                Project.get_instance(message["project"]),
                Database.get_instance(message["database"]),
                raw_data_storage,
                message["description"],
                message["properties"],
            )
        except KeyError as e:
            raise ValueError(
                f'Unable to get Thing instance from message "{message}"', e
            )
