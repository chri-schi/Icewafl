from typing import Any

from pyflink.common import Row, Types, TypeInformation
from pyflink.datastream import OutputTag, DataStream


class LoggerConfig:
    """
    Defines which data is logged.
    """

    def __init__(self,
                 polluted_element_type: str,
                 polluted_element_name: str,
                 element_value_pre: str,
                 element_value_post: str,
                 polluter_name: str,
                 tuple_id: str):
        self.polluted_element_type = polluted_element_type
        self.polluted_element_name = polluted_element_name
        self.element_value_pre = element_value_pre
        self.element_value_post = element_value_post
        self.polluter_name = polluter_name
        self.tuple_id = tuple_id

    def log_data(self) -> dict[str, Any]:
        return {key: str(value) for key, value in vars(self).items()}

    @staticmethod
    def logged_attributes() -> list[str]:
        return list(vars(LoggerConfig("", "", "", "", "", "")).keys())


class OutputLogger:
    """
    Manages the logging of pollution steps.
    """

    def __init__(self, id_attribute_name: str, output_tag_name: str):
        self._output_tag_name = output_tag_name
        self._id_attribute_name = id_attribute_name
        self._output_stream: DataStream = None

    @property
    def id_attribute_name(self) -> str:
        return self._id_attribute_name

    @id_attribute_name.setter
    def id_attribute_name(self, value):
        self._id_attribute_name = value

    @property
    def output_tag_name(self) -> str:
        return self._output_tag_name

    @property
    def output_stream(self):
        return self._output_stream

    @output_stream.setter
    def output_stream(self, value):
        self._output_stream = value

    def get_output_type(self, logged_attributes: list) -> TypeInformation:
        return Types.ROW_NAMED(
            logged_attributes, [Types.STRING()] * len(logged_attributes)
        )

    def get_output_tag(self, logged_attributes: list) -> OutputTag:
        return OutputTag(self._output_tag_name, self.get_output_type(logged_attributes))

    def get_log_output(self, config: LoggerConfig) -> tuple[OutputTag, Row]:
        return self.get_output_tag(config.logged_attributes()), Row(**config.log_data())
