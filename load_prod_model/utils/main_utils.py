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

        self.dir = self.config["dir"]

        self.log_dir = self.config["dir"]["log"]

        self.feats_pattern = self.config["feature_pattern"]

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
            self.blob.upload_folder(self.log_dir, "logs", "upload")

            self.log_writer.log("Uploaded logs to logs container", "upload")

            self.log_writer.start_log("exit", self.class_name, method_name, "upload")

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
            model_file = self.dir[key] + "/" + model_name + self.file_format

            self.log_writer.log(f"Got model file for {key}", log_file)

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

            return model_file

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)

    def get_number_of_clusters(self, log_file):
        """
        Method Name :   get_number_of_cluster
        Description :   This method gets the number of clusters based on training data on which clustering algorithm was used
        
        Output      :   The number of clusters for the given training data is returned
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_number_of_clusters.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            feat_fnames = self.blob.get_files_from_folder(
                self.feats_pattern, "feature_store", log_file
            )

            self.log_writer.log(
                f"Got features file names from feature store container based on feature pattern",
                log_file,
            )

            num_clusters = len(feat_fnames)

            self.log_writer.log(
                f"Got the number of clusters as {num_clusters}", log_file
            )

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

            return num_clusters

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)
