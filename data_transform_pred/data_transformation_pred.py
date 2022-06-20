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

        self.blob = Blob_Operation()

        self.log_writer = App_Logger()

        self.data_dir = self.config["data_dir"]

        self.class_name = self.__class__.__name__

    def add_quotes_to_string(self):
        """
        Method Name :   add_quotes_to_string
        Description :   This method is used for adding quotes to string values present in the dataframe
        
        Output      :   Quotes are added to string values present in the dataframe
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.add_quotes_to_string.__name__

        self.log_writer.start_log(
            "start", self.class_name, method_name, "data_transform"
        )

        try:
            lst = self.blob.read_csv_from_folder(
                self.data_dir["good"], "pred_data", "data_transform"
            )

            for _, f in enumerate(lst):
                df = f[0]

                file = f[1]

                abs_f = f[2]

                self.log_writer.log(
                    "Got dataframe,file name and absolute file name from list of tuples",
                    "data_transform",
                )

                df["DATE"] = df["DATE"].apply(lambda x: "'" + str(x) + "'")

                self.log_writer.log(
                    f"Quotes added for the file {file}", "data_transform"
                )

                self.blob.upload_df_as_csv(
                    df, abs_f, file, "pred_data", "data_transform"
                )

            self.log_writer.start_log(
                "exit", self.class_name, method_name, "data_transform"
            )

        except Exception as e:
            self.log_writer.exception_log(
                e, self.class_name, method_name, "data_transform"
            )
