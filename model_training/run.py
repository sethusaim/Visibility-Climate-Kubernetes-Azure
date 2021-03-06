from mlflow import end_run, start_run

from blob_operations import Blob_Operation
from mlflow_operations import MLFlow_Operation
from tuner import Model_Finder
from utils.logger import App_Logger
from utils.main_utils import Main_Utils
from utils.read_params import read_params


class Run:
    """
    Description :   This class is used for running the model training service
    
    Version     :   1.2
    Revisions   :   Moved to setup to cloud 
    """

    def __init__(self):
        self.class_name = self.__class__.__name__

        self.config = read_params()

        self.mlflow_config = self.config["mlflow_config"]

        self.model = Model_Finder("model_train")

        self.utils = Main_Utils()

        self.blob = Blob_Operation()

        self.log_writer = App_Logger()

        self.mlflow_op = MLFlow_Operation("model_train")

    def training_model(self):
        """
        Method Name :   training_model
        Description :   This method performs the trains the models based on the training data
        
        Output      :   Model training is done, along with logging of models, metrics and parameters to mlflow and artifacts are stored
                        blob container
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   Moved to setup to cloud 
        """
        method_name = self.training_model.__name__

        self.log_writer.start_log("start", self.class_name, method_name, "model_train")

        try:
            lst_clusters = self.utils.get_number_of_clusters("model_train")

            self.log_writer.log(
                f"Found the number of cluster to be {lst_clusters}", "model_train",
            )

            kmeans_model = self.blob.load_model(
                "KMeans", "model", "model_train", model_dir="train"
            )

            kmeans_model_name = kmeans_model.__class__.__name__

            self.mlflow_op.set_mlflow_tracking_uri()

            self.mlflow_op.set_mlflow_experiment("exp_name")

            with start_run(run_name=self.mlflow_config["run_name"]):
                self.mlflow_op.log_sklearn_model(kmeans_model, kmeans_model_name)

                end_run()

            for i in range(lst_clusters):
                cluster_feat = self.utils.get_cluster_features(i, "model_train")

                cluster_label = self.utils.get_cluster_targets(i, "model_train")

                self.log_writer.log(
                    "Got cluster features and cluster labels dataframe from feature store container",
                    "model_train",
                )

                with start_run(run_name=self.mlflow_config["run_name"] + str(i)):
                    self.model.train_and_log_models(
                        cluster_feat, cluster_label, "model_train", idx=i,
                    )

            self.log_writer.log(
                "Completed model and training and logging of the models to mlflow",
                "model_train",
            )

            self.log_writer.start_log(
                "exit", self.class_name, method_name, "model_train"
            )

        except Exception as e:
            self.log_writer.exception_log(
                e, self.class_name, method_name, "model_train"
            )


if __name__ == "__main__":
    try:
        run = Run()

        run.training_model()

    except Exception as e:
        raise e

    finally:
        utils = Main_Utils()

        utils.upload_logs()
