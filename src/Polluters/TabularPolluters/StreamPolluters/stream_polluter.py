from abc import abstractmethod

from pyflink.datastream import ProcessFunction

from src.Conditions.default_condition import DefaultCondition
from src.Polluters.TabularPolluters.polluter_base import PolluterBase
from src.Conditions.polluter_condition import PolluterCondition
from src.Logging.polluter_logger import OutputLogger


class StreamPolluter(ProcessFunction, PolluterBase):
    """
    Base class for all polluters operating on (regular) data streams.
    """

    def __init__(self, polluted_element_type: str, id_attribute: str):
        self._condition = DefaultCondition()
        self._polluter_name = type(self).__name__
        self._polluted_element_type = polluted_element_type
        self._output_logger = None
        self._id_attribute = id_attribute
        self._parallelism = -1

    def set_condition(self, condition: PolluterCondition) -> 'StreamPolluter':
        self._condition = condition
        return self

    @property
    def polluter_condition(self) -> PolluterCondition:
        return self._condition

    def set_polluter_name(self, polluter_name: str) -> 'StreamPolluter':
        if not polluter_name:
            raise TypeError("Only non empty string values are allowed")
        self._polluter_name = polluter_name
        return self

    @property
    def polluter_name(self) -> str:
        return self._polluter_name

    @abstractmethod
    def process_element(self, value, ctx: 'ProcessFunction.Context'):
        pass

    def set_output_logger(self, output_logger: OutputLogger) -> 'StreamPolluter':
        self._output_logger = output_logger
        return self

    @property
    def is_logged(self):
        return self._output_logger is not None

    @property
    def output_logger(self):
        return self._output_logger

    @property
    def polluted_element_type(self):
        return self._polluted_element_type

    @property
    def id_attribute(self):
        return self._id_attribute

    def set_parallelism(self, parallelism: int) -> 'StreamPolluter':
        self._parallelism = parallelism
        return self

    @property
    def parallelism(self) -> int:
        return self._parallelism
