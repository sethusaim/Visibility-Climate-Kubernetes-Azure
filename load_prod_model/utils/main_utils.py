from os import listdir
from os.path import join
from shutil import rmtree

from blob_operations import Blob_Operation

from utils.logger import App_Logger
from utils.read_params import read_params


class Main_Utils:
    def __init__(self):
        self.blob = Blob_Operation()

        self.log_writer = App_Logger()

        self.config = read_params()

        self.container = self.config["blob_container"]

        self.models_dir = self.config["models_dir"]

        self.log_file = self.config["log"]["upload"]

        self.log_dir = self.config["log_dir"]

        self.file_format = self.config["model_save_format"]

        self.class_name = self.__class__.__name__

    def upload_logs(self):
        method_name = self.upload_logs.__name__

        self.log_writer.start_log("start", self.class_name, method_name, self.log_file)

        try:
            lst = listdir(self.log_dir)

            self.log_writer.log(
                "Got list of logs from train_logs folder", self.log_file
            )

            for f in lst:
                local_f = join(self.log_dir, f)

                dest_f = self.log_dir + "/" + f

                self.s3.upload_file(
                    local_f, dest_f, self.container["logs"], self.log_file
                )

            self.log_writer.log(
                f"Uploaded logs to {self.container['logs']}", self.log_file
            )

            self.log_writer.start_log(
                "exit", self.class_name, method_name, self.log_file
            )

            rmtree(self.log_dir)

        except Exception as e:
            self.log_writer.exception_log(
                e, self.class_name, method_name, self.log_file
            )

    def get_model_file(self, key, model_name, log_file):
        method_name = self.get_model_file.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            model_file = self.models_dir[key] + "/" + model_name + self.file_format

            self.log_writer.log(f"Got model file for {key}", log_file)

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

            return model_file

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)

    def create_prod_and_stag_dirs(self, container, log_file):
        """
        Method Name :   create_prod_and_stag_dirs
        Description :   This method creates folders for production and staging container

        Output      :   Folders for production and staging are created in s3 container
        On Failure  :   Write an exception log and then raise an exception

        Version     :   1.2
        
        Revisions   :   moved setup to cloud
        """
        method_name = self.create_prod_and_stag_dirs.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            self.s3.create_folder(self.models_dir["prod"], container, log_file)

            self.s3.create_folder(self.models_dir["stag"], container, log_file)

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)
