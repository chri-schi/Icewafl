from abc import abstractmethod, ABC

from src.Conditions.polluter_condition import PolluterCondition
from src.Logging.polluter_logger import OutputLogger


class PolluterBase(ABC):
    """
    Interface for all polluters.
    """

    @property
    @abstractmethod
    def polluter_condition(self) -> PolluterCondition:
        pass

    @abstractmethod
    def set_condition(self, condition: PolluterCondition):
        pass

    @abstractmethod
    def set_polluter_name(self, polluter_name: str):
        pass

    @property
    @abstractmethod
    def polluter_name(self) -> str:
        pass

    @abstractmethod
    def set_output_logger(self, output_logger: OutputLogger):
        pass

    @property
    @abstractmethod
    def is_logged(self) -> bool:
        pass

    @property
    @abstractmethod
    def output_logger(self) -> OutputLogger:
        pass

    @property
    @abstractmethod
    def polluted_element_type(self) -> str:
        pass

    @property
    @abstractmethod
    def id_attribute(self) -> str:
        pass

    @property
    @abstractmethod
    def parallelism(self) -> int:
        pass

    @abstractmethod
    def set_parallelism(self, parallelism: int):
        pass
