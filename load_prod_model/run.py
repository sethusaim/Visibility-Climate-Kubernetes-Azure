from blob_operations import Blob_Operation
from mlflow_operations import MLFlow_Operation
from utils.logger import App_Logger
from utils.main_utils import Main_Utils
from utils.read_params import read_params


class Load_Prod_Model:
    """
    Description :   This class shall be used for loading the production model
    
    Version     :   1.2
    Revisions   :   Moved to setup to cloud 
    """

    def __init__(self):
        self.config = read_params()

        self.class_name = self.__class__.__name__

        self.container = self.config["blob_container"]

        self.load_prod_model_log = self.config["log"]["load_prod_model"]

        self.mlflow_config = self.config["mlflow_config"]

        self.blob = Blob_Operation()

        self.log_writer = App_Logger()

        self.utils = Main_Utils()

        self.mlflow_op = MLFlow_Operation(self.load_prod_model_log)

    def load_production_model(self):
        """
        Method Name :   load_production_model
        Description :   This method is responsible for finding the best model based on metrics and then transitioned them to thier stages

        Output      :   The best models are put in production and rest are put in staging
        On Failure  :   Write an exception log and then raise an exception

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.load_production_model.__name__

        self.log_writer.start_log(
            "start", self.class_name, method_name, self.load_prod_model_log
        )

        try:
            self.utils.create_prod_and_stag_dirs(
                self.container["model"], self.load_prod_model_log
            )

            self.mlflow_op.set_mlflow_tracking_uri()

            exp = self.mlflow_op.get_experiment_from_mlflow(
                self.mlflow_config["exp_name"]
            )

            runs = self.mlflow_op.get_runs_from_mlflow(exp.experiment_id)

            feat_fnames = self.blob.get_files_from_folder(
                self.config["feature_pattern"],
                self.container["feature_store"],
                self.load_prod_model_log,
            )

            num_clusters = len(feat_fnames)

            """
            Code Explaination: 
            num_clusters - Dynamically allocated based on the number of clusters created using elbow plot

            Here, we are trying to iterate over the number of clusters and then dynamically create the cols 
            where in the best model names can be found, and then copied to production or staging depending on
            the condition

            Eg- metrics.XGBoost1-best_score
            """
            reg_model_names = self.mlflow_op.get_mlflow_models()

            cols = [
                "metrics." + str(model) + "-best_score"
                for model in reg_model_names
                if model != "KMeans"
            ]

            self.log_writer.log(
                "Created cols for all registered model", self.load_prod_model_log
            )

            runs_cols = runs.filter(cols).max().sort_values(ascending=False)

            self.log_writer.log(
                "Sorted the runs cols in descending order", self.load_prod_model_log
            )

            metrics_dict = runs_cols.to_dict()

            self.log_writer.log("Converted runs cols to dict", self.load_prod_model_log)

            """ 
            Eg-output: For 3 clusters, 
            
            [
                metrics.XGBoost0-best_score,
                metrics.XGBoost1-best_score,
                metrics.XGBoost2-best_score,
                metrics.RandomForest0-best_score,
                metrics.RandomForest1-best_score,
                metrics.RandomForest2-best_score
            ] 

            Eg- runs_dataframe: I am only showing for 3 cols,actual runs dataframe will be different
                                based on the number of clusters
                
                since for every run cluster values changes, rest two cols will be left as blank,
                so only we are taking the max value of each col, which is nothing but the value of the metric
                

run_number  metrics.XGBoost0-best_score metrics.RandomForest1-best_score metrics.XGBoost1-best_score
    0                   1                       0.5
    1                                                                                   1                 
    2                                                                           
            """

            best_metrics_names = [
                max(
                    [
                        (file, metrics_dict[file])[0]
                        for file in metrics_dict
                        if str(i) in file
                    ]
                )
                for i in range(0, num_clusters)
            ]

            self.log_writer.log(
                f"Got top model names based on the metrics of clusters",
                self.load_prod_model_log,
            )

            ## best_metrics will store the value of metrics, but we want the names of the models,
            ## so best_metrics.index will return the name of the metric as registered in mlflow

            ## Eg. metrics.XGBoost1-best_score

            ## top_mn_lst - will store the top 3 model names

            top_mn_lst = [mn.split(".")[1].split("-")[0] for mn in best_metrics_names]

            self.log_writer.log(f"Got the top model names", self.load_prod_model_log)

            results = self.mlflow_op.search_mlflow_models("DESC")

            ## results - This will store all the registered models in mlflow
            ## Here we are iterating through all the registered model and for every latest registered model
            ## we are checking if the model name is in the top 3 model list, if present we are putting that
            ## model into production or staging

            for res in results:
                for mv in res.latest_versions:
                    if mv.name in top_mn_lst:
                        self.mlflow_op.transition_mlflow_model(
                            mv.version,
                            "Production",
                            mv.name,
                            self.container["model"],
                            self.container["model"],
                        )

                    ## In the registered models, even kmeans model is present, so during Prediction,
                    ## this model also needs to be in present in production, the code logic is present below

                    elif mv.name == "KMeans":
                        self.mlflow_op.transition_mlflow_model(
                            mv.version,
                            "Production",
                            mv.name,
                            self.container["model"],
                            self.container["model"],
                        )

                    else:
                        self.mlflow_op.transition_mlflow_model(
                            mv.version,
                            "Staging",
                            mv.name,
                            self.container["model"],
                            self.container["model"],
                        )

            self.log_writer.log(
                "Transitioning of models based on scores successfully done",
                self.load_prod_model_log,
            )

            self.log_writer.start_log(
                "exit", self.class_name, method_name, self.load_prod_model_log,
            )

        except Exception as e:
            self.log_writer.exception_log(
                e, self.class_name, method_name, self.load_prod_model_log,
            )


if __name__ == "__main__":
    try:
        run = Load_Prod_Model()

        run.load_production_model()

    except Exception as e:
        raise e

    finally:
        utils = Main_Utils()

        utils.upload_logs()
