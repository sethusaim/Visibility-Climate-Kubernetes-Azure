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

        self.log_dir = self.config["log_dir"]

        self.files = self.config["files"]

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

    def get_training_data(self, key, log_file):
        """
        Method Name :   get_training_data
        Description :   This method gets the training data from blob container based on the key
        
        Output      :   The cluster data is returned as dataframe from blob container based on the key
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_training_data.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            data = self.blob.read_csv(self.files[key], "feature_store", log_file)

            self.log_writer.log(
                f"Got the training data based on {key} from feature store container",
                log_file,
            )

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

            return data

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)

    def upload_cluster_data(self, idx, cluster_data, log_file, key=None):
        """
        Method Name :   upload_cluster_data
        Description :   This method uploads the cluster data based on the idx and key
        
        Output      :   The cluster data is uploaded to s3 container based on idx and key
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.upload_cluster_data.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            cluster_fname = self.get_cluster_fname(self.files[key], idx, log_file)

            self.log_writer.log(
                f"Got cluster file name for {key} and with cluster number as {idx}",
                log_file,
            )

            self.blob.upload_df_as_csv(
                cluster_data, cluster_fname, cluster_fname, "feature_store", log_file
            )

            self.log_writer.log(
                f"Uploaded {cluster_fname} file to feature store container", log_file,
            )

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)
