from data_loader_train import Data_Getter_Train
from preprocessing import Preprocessor
from utils.logger import App_Logger
from utils.main_utils import Main_Utils


class Run:
    def __init__(self):
        self.class_name = self.__class__.__name__

        self.data_getter_train = Data_Getter_Train("preprocess_train")

        self.preprocess = Preprocessor("preprocess_train")

        self.log_writer = App_Logger()

        self.utils = Main_Utils()

    def run_preprocess(self):
        method_name = self.run_preprocess.__name__

        self.log_writer.start_log(
            "start", self.class_name, method_name, "preprocess_train"
        )

        try:
            data = self.data_getter_train.get_data()

            is_null_present = self.preprocess.is_null_present(data)

            self.log_writer.log(
                f"Preprocessing function is_null_present returned null values present to be {is_null_present}",
                "preprocess_train",
            )

            self.log_writer.log(
                "Imputing missing values for the data", "preprocess_train"
            )

            if is_null_present:
                data = self.preprocess.impute_missing_values(data)

            self.log_writer.log(
                "Imputed missing values for the data", "preprocess_train"
            )

            cols_to_drop = self.preprocess.get_columns_with_zero_std_deviation(data)

            self.log_writer.log(
                "Got columns with zero standard deviation", "preprocess_train"
            )

            data = self.preprocess.remove_columns(data, cols_to_drop)

            self.log_writer.log(
                "Removed columns with zero standard deviation", "preprocess_train"
            )

            self.utils.upload_preprocessed_data(data, "preprocess_train")

            self.log_writer.log(
                "Completed preprocessing for training data", "preprocess_train"
            )

            self.log_writer.start_log(
                "exit", self.class_name, method_name, "preprocess_train"
            )

        except Exception as e:
            self.log_writer.exception_log(
                e, self.class_name, method_name, "preprocess_train"
            )


if __name__ == "__main__":
    try:
        run = Run()

        run.run_preprocess()

    except Exception as e:
        raise e

    finally:
        utils = Main_Utils()

        utils.upload_logs()
