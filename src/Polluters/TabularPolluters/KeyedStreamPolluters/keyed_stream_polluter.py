from abc import abstractmethod
from typing import Union, Callable

from pyflink.datastream import KeyedProcessFunction
from pyflink.datastream.functions import NullByteKeySelector, KeySelector

from src.Conditions.default_condition import DefaultCondition
from src.Polluters.TabularPolluters.polluter_base import PolluterBase
from src.Conditions.polluter_condition import PolluterCondition
from src.Logging.polluter_logger import OutputLogger


class KeyedStreamPolluter(KeyedProcessFunction, PolluterBase):
    """
    Base class for polluters operating on keyed data streams.
    """

    def __init__(self, polluted_element_type: str, id_attribute: str):
        self._polluted_element_type = polluted_element_type
        self._id_attribute = id_attribute
        self._condition = DefaultCondition()
        self._polluter_name = type(self).__name__
        self._output_logger = None
        self._key = NullByteKeySelector()
        self._parallelism = -1

    @abstractmethod
    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        pass

    def set_condition(self, condition: PolluterCondition) -> 'KeyedStreamPolluter':
        self._condition = condition
        return self

    @property
    def polluter_condition(self) -> PolluterCondition:
        return self._condition

    def set_polluter_name(self, polluter_name: str) -> 'KeyedStreamPolluter':
        if not polluter_name:
            raise TypeError("Only non empty string values are allowed")
        self._polluter_name = polluter_name
        return self

    @property
    def polluter_name(self) -> str:
        return self._polluter_name

    @property
    def polluted_element_type(self):
        return self._polluted_element_type

    @property
    def id_attribute(self):
        return self._id_attribute

    @property
    def key(self):
        return self._key

    def set_key(self, key: Union[Callable, KeySelector]):
        self._key = key
        return self

    def set_output_logger(self, output_logger: OutputLogger) -> 'KeyedStreamPolluter':
        pass

    @property
    def is_logged(self) -> bool:
        return False

    @property
    def output_logger(self) -> OutputLogger:
        return None

    def set_parallelism(self, parallelism: int) -> 'KeyedStreamPolluter':
        self._parallelism = parallelism
        return self

    @property
    def parallelism(self) -> int:
        return self._parallelism

