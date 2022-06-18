from data_transformation_train import Data_Transform_Train
from utils.logger import App_Logger
from utils.main_utils import Main_Utils


class Run:
    def __init__(self):
        self.class_name = self.__class__.__name__

        self.log_writer = App_Logger()

        self.data_transform = Data_Transform_Train()

    def train_data_transform(self):
        method_name = self.train_data_transform.__name__

        try:
            self.log_writer.log("Starting Data Transformation", "data_transform_main")

            self.data_transform.add_quotes_to_string()

            self.log_writer.log(
                "Data Transformation completed !!", "data_transform_main"
            )

            self.log_writer.start_log(
                "exit", self.class_name, method_name, "data_transform_main"
            )

        except Exception as e:
            self.log_writer.exception_log(
                e, self.class_name, method_name, "data_transform_main",
            )


if __name__ == "__main__":
    try:
        run = Run()

        run.train_data_transform()

    except Exception as e:
        raise e

    finally:
        utils = Main_Utils()

        utils.upload_logs()
