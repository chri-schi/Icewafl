from typing import Union

from pyflink.datastream import DataStream, KeyedProcessFunction, ProcessFunction
import operator

from src.Polluters.TabularPolluters.KeyedStreamPolluters.keyed_stream_polluter import KeyedStreamPolluter
from src.Polluters.TabularPolluters.StreamPolluters.stream_polluter import StreamPolluter
from src.Logging.polluter_logger import LoggerConfig, OutputLogger
from abc import ABC, abstractmethod
from functools import reduce


class LogOutputManager:
    """
    Class to manage data streams used for logging.
    """

    def __init__(self, *output_loggers: OutputLogger):
        self._output_streams = {logger.output_tag_name: None for logger in output_loggers}

    @property
    def output_tag_names(self):
        return list(self._output_streams.keys())

    def get_output_stream_by_tag_name(self, output_tag_name: str):
        return self._output_streams[output_tag_name]

    def get_output_stream_by_logger(self, output_logger: OutputLogger):
        return self.get_output_stream_by_tag_name(output_logger.output_tag_name)

    def set_output_stream_for_tag_name(self, output_tag_name: str, stream: DataStream = None):
        self._output_streams[output_tag_name] = stream

    def set_output_stream_for_logger(self, output_logger: OutputLogger, stream: DataStream = None):
        self.set_output_stream_for_tag_name(output_logger.output_tag_name, stream)


class PolluterBuilderComponent(ABC):
    """
    Interface for pollution pipeline components.
    """

    @abstractmethod
    def build(self, stream: DataStream, logger_handler: LogOutputManager = None) -> DataStream:
        pass

    @property
    @abstractmethod
    def registered_polluter_info(self) -> list[str]:
        pass


class PolluterBuilderComposite(PolluterBuilderComponent):
    """
    Class to define and configurate pollution pipelines. A pollution pipeline should contain at least one
    PolluterBuilder instance. The process of building a pollution pipeline is delegated to the registered
    PolluterBuilder instances.
    """

    def __init__(self):
        self._polluters: list[Union[KeyedStreamPolluter, StreamPolluter]] = []
        self._polluter_builders: list[PolluterBuilderComponent] = []

    def __len__(self):
        return len(self._polluters)

    def set_polluters(self, polluters: list[Union[KeyedStreamPolluter, StreamPolluter]]):
        self._polluters = polluters
        self._polluter_builders = [PolluterBuilder(polluter) for polluter in polluters]
        return self

    def add_polluters(self, polluters: list[Union[KeyedStreamPolluter, StreamPolluter]]):
        self._polluters += polluters
        self._polluter_builders += [PolluterBuilder(polluter) for polluter in polluters]
        return self

    @property
    def registered_polluter_info(self) -> list[str]:
        return reduce(operator.add, [polluter.registered_polluter_info for polluter in self._polluter_builders])

    def build(self, stream: DataStream, logger_handler: LogOutputManager = None) -> DataStream:
        for polluter_component in self._polluter_builders:
            stream = polluter_component.build(stream, logger_handler)
        return stream


class PolluterBuilder(PolluterBuilderComponent):
    """
    Atomic building block of a pollution pipeline. Harbours a polluter instance that is added to Flink's streaming data
    flow as transformation operator when the pollution pipeline is built.
    """

    def __init__(self, polluter: Union[KeyedStreamPolluter, StreamPolluter]):
        self._polluter = polluter

    def build(self, stream: DataStream, logger_handler: LogOutputManager = None) -> DataStream:
        if not logger_handler:
            self._polluter.set_output_logger(None)
        if isinstance(self._polluter, KeyedProcessFunction):
            stream = self._build_keyed_stream(stream)
        elif isinstance(self._polluter, ProcessFunction):
            stream = self._build_stream(stream)
        else:
            print("Incompatible Polluter type")
            return stream

        if self._polluter.parallelism:
            stream = stream.set_parallelism(self._polluter.parallelism)
        if self._polluter.is_logged and logger_handler:
            logger_stream = logger_handler.get_output_stream_by_logger(self._polluter.output_logger)
            side_output_stream = stream.get_side_output(
                self._polluter.output_logger.get_output_tag(LoggerConfig.logged_attributes())
            )
            logger_handler.set_output_stream_for_logger(
                self._polluter.output_logger,
                side_output_stream if logger_stream is None else logger_stream.union(side_output_stream)
            )
        return stream

    def _build_stream(self, stream: DataStream) -> DataStream:
        stream = stream.process(self._polluter)
        return stream

    def _build_keyed_stream(self, stream: DataStream) -> DataStream:
        stream = stream.key_by(self._polluter.key).process(self._polluter)
        return stream

    @property
    def registered_polluter_info(self) -> list[str]:
        return [self._polluter.polluter_name]
