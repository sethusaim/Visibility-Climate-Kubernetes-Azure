from shutil import rmtree

from blob_operations import Blob_Operation

from utils.logger import App_Logger
from utils.read_params import read_params


class Main_Utils:
    def __init__(self):
        self.class_name = self.__class__.__name__

        self.blob = Blob_Operation()

        self.log_writer = App_Logger()

        self.config = read_params()

        self.log_dir = self.config["log_dir"]

        self.dir = self.config["dir"]

    def upload_logs(self):
        method_name = self.upload_logs.__name__

        self.log_writer.start_log("start", self.class_name, method_name, "upload")

        try:
            self.blob.upload_folder(self.log_dir, "upload")

            self.log_writer.log("Uploaded logs to logs container", "upload")

            self.log_writer.start_log("exit", self.class_name, method_name, "upload")

            rmtree(self.log_dir)

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, "upload")

    def get_filename(self, key, fname, log_file):
        method_name = self.get_filename.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            pred_fname = self.dir[key] + "/" + fname

            self.log_writer.log(f"Got the pred file name for {key}", log_file)

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

            return pred_fname

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)
