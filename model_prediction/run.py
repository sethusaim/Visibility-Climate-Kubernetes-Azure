from pandas import DataFrame

from blob_operations import Blob_Operation
from utils.logger import App_Logger
from utils.main_utils import Main_Utils
from utils.read_params import read_params


class Run:
    """
    Description :   This class is used for running the model prediction service
    
    Version     :   1.2
    Revisions   :   Moved to setup to cloud 
    """

    def __init__(self):
        self.class_name = self.__class__.__name__

        self.config = read_params()

        self.pred_log = self.config["log"]["pred"]

        self.model_dir = self.config["model_dir"]

        self.files = self.config["files"]

        self.container = self.config["blob_container"]

        self.save_format = self.config["save_format"]

        self.log_writer = App_Logger()

        self.utils = Main_Utils()

        self.blob = Blob_Operation()

    def predict_from_model(self):
        """
        Method Name :   predict_from_model
        Description :   This method performs the model prediction on new data
        
        Output      :   Model prediction are done on the new data, and results and artifacts are stored in s3 buckets
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   Moved to setup to cloud 
        """
        method_name = self.predict_from_model.__name__

        self.log_writer.start_log("start", self.class_name, method_name, self.pred_log)

        try:
            data = self.blob.read_csv(
                self.files["pred_input_file_preprocess"],
                self.container["feature_store"],
                self.pred_log,
            )

            kmeans_model = self.blob.load_model(
                "KMeans",
                self.container["model"],
                self.pred_log,
                format=self.save_format,
                model_dir=self.model_dir["prod"],
            )

            clusters = kmeans_model.predict(data.drop(["climate"], axis=1))

            data["clusters"] = clusters

            unique_clusters = data["clusters"].unique()

            for i in unique_clusters:
                cluster_data = data[data["clusters"] == i]

                climate_names = list(cluster_data["climate"])

                cluster_data = data.drop(labels=["climate"], axis=1)

                cluster_data = cluster_data.drop(["clusters"], axis=1)

                model_name = self.utils.find_correct_model_file(
                    i, self.container["model"], self.pred_log
                )

                model = self.blob.load_model(
                    model_name,
                    self.container["model"],
                    self.pred_log,
                    format=self.save_format,
                )

                result = list(model.predict(cluster_data))

                result = DataFrame(
                    list(zip(climate_names, result)), columns=["climate", "Prediction"]
                )

                self.blob.upload_df_as_csv(
                    result,
                    self.files["pred_output"],
                    self.files["pred_output"],
                    self.container["io_files"],
                    self.pred_log,
                )

            self.log_writer.log(
                f"Prediction file is created with {self.files['pred_output']} in {self.container['io_files']}",
                self.pred_log,
            )

            self.log_writer.log("End of prediction", self.pred_log)

            self.log_writer.start_log(
                "exit", self.class_name, method_name, self.pred_log
            )

        except Exception as e:
            self.log_writer.exception_log(
                e, self.class_name, method_name, self.pred_log
            )


if __name__ == "__main__":
    try:
        run = Run()

        run.predict_from_model()

    except Exception as e:
        raise e

    finally:
        utils = Main_Utils()

        utils.upload_logs()
