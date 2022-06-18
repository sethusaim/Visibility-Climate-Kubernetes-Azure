from os import listdir
from os.path import join
from shutil import rmtree

from blob_operations import Blob_Operation

from utils.logger import App_Logger
from utils.read_params import read_params


class Main_Utils:
    """
    Description :   This class is used for main utility functions required in core functions of the service
    
    Version     :   1.2
    Revisions   :   Moved to setup to cloud 
    """

    def __init__(self):
        self.blob = Blob_Operation()

        self.log_writer = App_Logger()

        self.config = read_params()

        self.models_dir = self.config["models_dir"]

        self.log_dir = self.config["log_dir"]

        self.file_format = self.config["model_save_format"]

        self.class_name = self.__class__.__name__

    def upload_logs(self):
        """
        Method Name :   upload_logs
        Description :   This method uploads the logs to blob container
        
        Output      :   The logs are uploaded to blob container
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.upload_logs.__name__

        self.log_writer.start_log("start", self.class_name, method_name, "upload")

        try:
            lst = listdir(self.log_dir)

            self.log_writer.log("Got list of logs from train_logs folder", "upload")

            for f in lst:
                local_f = join(self.log_dir, f)

                dest_f = self.log_dir + "/" + f

                self.s3.upload_file(local_f, dest_f, "logs", "upload")

            self.log_writer.log("Uploaded logs to logs container", "upload")

            self.log_writer.start_log("exit", self.class_name, method_name, "upload")

            rmtree(self.log_dir)

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, "upload")

    def get_model_file(self, key, model_name, log_file):
        """
        Method Name :   get_model_file
        Description :   This method gets the model file based on key and model name, where key represents trained,stag and prod dir
        
        Output      :   The model file is returned based on the key and model name
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
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
