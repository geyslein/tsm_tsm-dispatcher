import logging
from abc import ABC, abstractmethod
from json import loads

from kafka import KafkaConsumer


class AbstractAction(ABC):

    def __init__(self, topic, kafka_servers, kafka_group_id):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id=kafka_group_id,
            value_deserializer=lambda x: self.deserialize(x)
        )

    def deserialize(self, x):
        return loads(x.decode('utf-8'))

    def run_loop(self):
        for message in self.consumer:
            try:
                self.act(message.value)
            except Exception as e:
                logging.critical(e)

            self.consumer.commit()

    @abstractmethod
    def act(self, message: dict):
        raise NotImplementedError
