from pred_data_validation import Raw_Pred_Data_Validation
from utils.logger import App_Logger
from utils.main_utils import Main_Utils
from utils.read_params import read_params


class Run:
    def __init__(self):
        self.config = read_params()

        self.class_name = self.__class__.__name__

        self.log_writer = App_Logger()

        self.pred_main_log = self.config["log"]["raw_pred_main"]

        self.container = self.config["blob_container"]

        self.raw_data = Raw_Pred_Data_Validation()

    def raw_pred_data_validation(self):
        """
        Method Name :   raw_pred_data_validation
        Description :   This method is used for validating the preding batch files
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.raw_pred_data_validation.__name__

        self.log_writer.start_log(
            "start", self.class_name, method_name, self.pred_main_log,
        )

        try:
            self.log_writer.log("Raw Data Validation started !!", self.pred_main_log)

            (
                LengthOfDateStampInFile,
                LengthOfTimeStampInFile,
                _,
                noofcolumns,
            ) = self.raw_data.values_from_schema()

            regex = self.raw_data.get_regex_pattern()

            self.raw_data.validate_raw_fname(
                regex, LengthOfDateStampInFile, LengthOfTimeStampInFile
            )

            self.raw_data.validate_col_length(noofcolumns)

            self.raw_data.validate_missing_values_in_col()

            self.log_writer.log("Raw Data Validation Completed !!", self.pred_main_log)

            self.log_writer.start_log(
                "exit", self.class_name, method_name, self.pred_main_log,
            )

        except Exception as e:
            self.log_writer.exception_log(
                e, self.class_name, method_name, self.pred_main_log,
            )


if __name__ == "__main__":
    try:
        run = Run()

        run.raw_pred_data_validation()

    except Exception as e:
        raise e

    finally:
        utils = Main_Utils()

        utils.upload_logs()