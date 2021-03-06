from os import listdir
from os.path import join
from shutil import rmtree

from blob_operations import Blob_Operation

from utils.logger import App_Logger
from utils.read_params import read_params


class Main_Utils:
    def __init__(self):
        self.class_name = self.__class__.__name__

        self.config = read_params()

        self.container = self.config["blob_container"]

        self.log_file = self.config["log"]["upload"]

        self.log_dir = self.config["log_dir"]

        self.blob = Blob_Operation()

        self.log_writer = App_Logger()

    def upload_logs(self):
        method_name = self.upload_logs.__name__

        self.log_writer.start_log("start", self.class_name, method_name, self.log_file)

        try:
            lst = listdir(self.log_dir)

            self.log_writer.log("Got list of logs from app_logs folder", self.log_file)

            for f in lst:
                local_f = join(self.log_dir, f)

                dest_f = self.log_dir + "/" + f

                self.blob.upload_file(
                    local_f, dest_f, self.container["logs"], self.log_file
                )

            self.log_writer.log(
                f"Uploaded logs to {self.container['logs']}", self.log_file
            )

            rmtree(self.log_dir)

            self.log_writer.start_log(
                "exit", self.class_name, method_name, self.log_file
            )

        except Exception as e:
            self.log_writer.exception_log(
                e, self.class_name, method_name, self.log_file
            )
