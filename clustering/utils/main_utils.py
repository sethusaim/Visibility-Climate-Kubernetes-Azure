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

        self.container = self.config["blob_container"]

        self.log_dir = self.config["log_dir"]

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

                self.blob.upload_file(local_f, dest_f, "logs", "upload")

            self.log_writer.log("Uploaded logs to logs container", "upload")

            self.log_writer.start_log("exit", self.class_name, method_name, "upload")

            rmtree(self.log_dir)

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, "upload")

    def get_cluster_fname(self, fname, idx, log_file):
        """
        Method Name :   get_cluster_fname
        Description :   This method gets the cluster file name based on cluster number
        
        Output      :   The cluster file name is returned based on cluster number
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_cluster_fname.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            cluster_fname = fname.replace(".csv", "-" + str(idx) + ".csv")

            self.log_writer.log(
                f"Got cluster file name for cluster {idx} of file {fname}", log_file,
            )

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

            return cluster_fname

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)
