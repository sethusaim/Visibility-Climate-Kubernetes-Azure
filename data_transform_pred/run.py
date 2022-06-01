from data_transformation_pred import Data_Transform_Pred
from utils.logger import App_Logger
from utils.main_utils import Main_Utils
from utils.read_params import read_params


class Run:
    """
    Description :   This class is used for running the data transformation prediction service
    
    Version     :   1.2
    Revisions   :   Moved to setup to cloud 
    """
    def __init__(self):
        self.config = read_params()

        self.class_name = self.__class__.__name__

        self.pred_main_log = self.config["log"]["data_transform_main"]

        self.log_writer = App_Logger()

        self.data_transform = Data_Transform_Pred()

    def pred_data_transform(self):
        """
        Method Name :   pred_data_transform
        Description :   This method is used for performing data transformation operations on prediction data
        
        Output      :   Data transformation is done on prediction data and artifacts are stored in s3 buckets
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.pred_data_transform.__name__

        try:
            self.log_writer.log("Starting Data Transformation", self.pred_main_log)

            self.data_transform.add_quotes_to_string()

            self.log_writer.log("Data Transformation completed !!", self.pred_main_log)

            self.log_writer.start_log(
                "exit", self.class_name, method_name, self.pred_main_log
            )

        except Exception as e:
            self.log_writer.exception_log(
                e, self.class_name, method_name, self.pred_main_log,
            )


if __name__ == "__main__":
    try:
        run = Run()

        run.pred_data_transform()

    except Exception as e:
        raise e

    finally:
        utils = Main_Utils()

        utils.upload_logs()
