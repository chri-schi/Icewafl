from abc import abstractmethod
from typing import Any

from pyflink.common import Row
from pyflink.datastream import OutputTag, ProcessFunction

from src.Polluters.TabularPolluters.StreamPolluters.stream_polluter import StreamPolluter

from src.Logging.polluter_logger import LoggerConfig


class AttributePolluter(StreamPolluter):
    """
    Base class for stream polluters that manipulate the attributes of a tuple.
    """

    def __init__(self, attributes: set[str], id_attribute: str):
        super().__init__(polluted_element_type="attribute", id_attribute=id_attribute)
        self._attributes = attributes

    @property
    def polluted_attributes(self) -> set[str]:
        return self._attributes.copy()

    @abstractmethod
    def _transform(self, value) -> Any:
        pass

    def _create_polluter_config(self, original_element, processed_element, polluted_element):
        return LoggerConfig(
            polluted_element_type=self._polluted_element_type,
            polluted_element_name=polluted_element,
            element_value_pre=original_element[polluted_element],
            element_value_post=processed_element[polluted_element],
            polluter_name=self._polluter_name,
            tuple_id=original_element[self._output_logger.id_attribute_name]
        )

    def _create_log_output(self, original_attribute_values, processed_attribute_values) -> list[tuple[OutputTag, Row]]:
        if not self._output_logger:
            return []
        return [self._output_logger.get_log_output(
            self._create_polluter_config(original_attribute_values, processed_attribute_values, attribute)
        ) for attribute in self._attributes]

    def transform(self, value) -> tuple[Any, list[tuple[OutputTag, Row]]]:
        original_attribute_values = {attribute: value[attribute] for attribute in self._attributes} | {
            self.id_attribute: value[self.id_attribute]}
        value = self._transform(value)
        log_output = self._create_log_output(original_attribute_values, value)
        return value, log_output

    def process_element(self, value, ctx: 'ProcessFunction.Context'):
        if self._condition.condition(value):
            value, values_side_output = self.transform(value)
            for val in [value] + values_side_output:
                yield val
        else:
            yield value
