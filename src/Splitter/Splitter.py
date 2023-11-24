from abc import abstractmethod

from pyflink.datastream import ProcessFunction, OutputTag


class Splitter(ProcessFunction):

    @abstractmethod
    def process_element(self, value, ctx: 'ProcessFunction.Context'):
        pass

    @property
    @abstractmethod
    def stream_tags(self) -> list[OutputTag]:
        pass

    @property
    @abstractmethod
    def stream_tag_names(self) -> list[str]:
        pass
