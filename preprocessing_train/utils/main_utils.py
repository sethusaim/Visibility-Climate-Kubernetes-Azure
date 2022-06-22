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

        self.log_dir = self.config["dir"]["log"]

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

            self.log_writer.log(f"Uploaded logs to logs container", "upload")

            self.log_writer.start_log("exit", self.class_name, method_name, "upload")

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, "upload")

    def upload_preprocessed_data(self, data, log_file):
        method_name = self.upload_preprocessed_data.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            self.blob.upload_df_as_csv(
                data,
                "pred_input_preprocess",
                "pred_input_preprocess",
                "feature_store",
                log_file,
            )

            self.log_writer.log("Uploaded preprocessed data to s3 container", log_file)

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)
