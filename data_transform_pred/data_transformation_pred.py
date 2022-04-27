from blob_operations import Blob_Operation
from utils.logger import App_Logger
from utils.read_params import read_params


class Data_Transform_Pred:
    """
    Description :  This class shall be used for transforming the prediction batch data before loading it in Database!!.

    Version     :   1.2
    Revisions   :   None
    """

    def __init__(self):
        self.config = read_params()

        self.container = self.config["blob_container"]

        self.blob = Blob_Operation()

        self.log_writer = App_Logger()

        self.data_dir = self.config["data_dir"]

        self.class_name = self.__class__.__name__

        self.pred_data_transform_log = self.config["log"]["data_transform"]

    def add_quotes_to_string(self):
        """
        Method Name :   add_quotes_to_string
        Description :   This method addes the quotes to the string data present in columns

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.add_quotes_to_string.__name__

        self.log_writer.start_log(
            "start", self.class_name, method_name, self.pred_data_transform_log
        )

        try:
            lst = self.blob.read_csv_from_folder(
                self.data_dir["good"],
                self.container["pred_data"],
                self.pred_data_transform_log,
            )

            for idx, f in enumerate(lst):
                df = f[idx][0]

                file = f[idx][1]

                abs_f = f[idx][2]

                df["DATE"] = df["DATE"].apply(lambda x: "'" + str(x) + "'")

                self.log_writer.log(
                    f"Quotes added for the file {file}", self.pred_data_transform_log
                )

                self.blob.upload_df_as_csv(
                    df,
                    abs_f,
                    file,
                    self.container["pred_data"],
                    self.pred_data_transform_log,
                )

            self.log_writer.start_log(
                key="exit", class_name=self.class_name, method_name=method_name,
            )

            self.log_writer.start_log(
                "exit", self.class_name, method_name, self.pred_data_transform_log
            )

        except Exception as e:
            self.log_writer.exception_log(
                e, self.class_name, method_name, self.pred_data_transform_log
            )
