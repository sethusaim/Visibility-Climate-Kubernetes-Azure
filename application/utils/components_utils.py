from kfp.components import load_component_from_text
from blob_operations import Blob_Operation

from utils.logger import App_Logger


class Component:
    def __init__(self, log_file):
        self.log_writer = App_Logger()

        self.blob = Blob_Operation()

        self.class_name = self.__class__.__name__

        self.log_file = log_file

    def load_kfp_component(self, fname, container):
        method_name = self.load_kfp_component.__name__

        self.log_writer.start_log("start", self.class_name, method_name)

        try:
            content = self.blob.read_yaml_as_str(fname, container)

            self.log_writer.log(
                f"Got {fname} train component from {container}", self.log_file
            )

            comp = load_component_from_text(content)

            self.log_writer.log(
                f"Loaded {fname} train component from text", self.log_file
            )

            self.log_writer.start_log(
                "exit", self.class_name, method_name, self.log_file
            )

            return comp

        except Exception as e:
            self.log_writer.exception_log(
                e, self.class_name, method_name, self.log_file
            )
