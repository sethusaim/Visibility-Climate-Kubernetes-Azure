from data_transformation_pred import Data_Transform_Pred
from utils.logger import App_Logger
from utils.main_utils import Main_Utils
from utils.read_params import read_params


class Run:
    def __init__(self):
        self.config = read_params()

        self.class_name = self.__class__.__name__

        self.pred_main_log = self.config["log"]["data_transform_main"]

        self.col = self.config["col"]

        self.log_writer = App_Logger()

        self.data_transform = Data_Transform_Pred()

    def pred_data_transform(self):
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
