from abc import ABC, abstractmethod


class AbstractAction(ABC):
    @abstractmethod
    def act(self, message: dict):
        raise NotImplementedError
