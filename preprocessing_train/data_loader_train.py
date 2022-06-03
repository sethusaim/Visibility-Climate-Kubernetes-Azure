from blob_operations import Blob_Operation
from utils.logger import App_Logger
from utils.read_params import read_params


class Data_Getter_Train:
    """
    Description :   This class shall be used for obtaining the df from the input files blob container where the trainiction file is present
    
    Version     :   1.2
    Revisions   :   Moved to setup to cloud 
    """

    def __init__(self, log_file):
        self.config = read_params()

        self.log_file = log_file

        self.files = self.config["files"]

        self.container = self.config["blob_container"]

        self.blob = Blob_Operation()

        self.log_writer = App_Logger()

        self.class_name = self.__class__.__name__

    def get_data(self):
        """
        Method Name :   get_data
        Description :   This method reads the data from the input files blob container where the trainiction file is present
        
        Output      :   A pandas dataframe
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_data.__name__

        self.log_writer.start_log("start", self.class_name, method_name, self.log_file)

        try:
            df = self.blob.read_csv(
                self.files["train_input"],
                self.container["feature_store"],
                self.log_file,
            )

            self.log_writer.log(
                f"Data loaded from file {self.files['train_input']} and container {self.container['feature_store']}",
                self.log_file,
            )

            self.log_writer.start_log(
                "exit", self.class_name, method_name, self.log_file
            )

            return df

        except Exception as e:
            self.log_writer.exception_log(
                e, self.class_name, method_name, self.log_file
            )
