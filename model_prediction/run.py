from blob_operations import Blob_Operation
from utils.logger import App_Logger
from utils.main_utils import Main_Utils


class Run:
    """
    Description :   This class is used for running the model prediction service
    
    Version     :   1.2
    Revisions   :   Moved to setup to cloud 
    """

    def __init__(self):
        self.class_name = self.__class__.__name__

        self.log_writer = App_Logger()

        self.utils = Main_Utils()

        self.blob = Blob_Operation()

    def predict_from_model(self):
        """
        Method Name :   predict_from_model
        Description :   This method performs the model prediction on new data
        
        Output      :   Model prediction are done on the new data, and results and artifacts are stored in s3 containers
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   Moved to setup to cloud 
        """
        method_name = self.predict_from_model.__name__

        self.log_writer.start_log("start", self.class_name, method_name, "pred")

        try:
            unique_clusters = self.utils.get_unique_clusters("pred")

            for i in unique_clusters:
                result = self.utils.get_predictions(i, "pred")

                self.utils.upload_results(result, "pred")

            self.log_writer.log(
                f"Prediction file is created at io files container", "pred"
            )

            self.log_writer.log("End of prediction", "pred")

            self.log_writer.start_log("exit", self.class_name, method_name, "pred")

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, "pred")


if __name__ == "__main__":
    try:
        run = Run()

        run.predict_from_model()

    except Exception as e:
        raise e

    finally:
        utils = Main_Utils()

        utils.upload_logs()
