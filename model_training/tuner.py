from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier

from blob_operations import Blob_Operation
from mlflow_operations import MLFlow_Operation
from utils.logger import App_Logger
from utils.model_utils import Model_Utils
from utils.read_params import read_params


class Model_Finder:
    """
    Description :   This class shall  be used to find the model with best accuracy and AUC score.
    
    Version     :   1.2
    Revisions   :   Moved to setup to cloud 
    """

    def __init__(self, log_file):
        self.log_file = log_file

        self.class_name = self.__class__.__name__

        self.config = read_params()

        self.split_kwargs = self.config["base"]

        self.model_dir = self.config["models_dir"]

        self.container = self.config["blob_container"]

        self.save_format = self.config["save_format"]

        self.mlflow_op = MLFlow_Operation(self.log_file)

        self.model_utils = Model_Utils()

        self.log_writer = App_Logger()

        self.blob = Blob_Operation()

        self.rf_model = RandomForestClassifier()

        self.xgb_model = XGBClassifier(
            objective="binary:logistic", eval_metric="logloss"
        )

    def get_rf_model(self, train_x, train_y):
        """
        Method Name :   get_rf_model
        Description :   get the parameters for Random Forest Algorithm which give the best accuracy.
                        Use Hyper Parameter Tuning.
        
        Output      :   The model with the best parameters
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_rf_model.__name__

        self.log_writer.start_log("start", self.class_name, method_name, self.log_file)

        try:
            self.rf_model_name = self.rf_model.__class__.__name__

            self.rf_best_params = self.model_utils.get_model_params(
                self.rf_model, train_x, train_y, self.log_file
            )

            self.log_writer.log(
                f"{self.rf_model_name} model best params are {self.rf_best_params}",
                self.log_file,
            )

            self.rf_model.set_params(**self.rf_best_params)

            self.log_writer.log(
                f"Initialized {self.rf_model_name} with {self.rf_best_params} as params",
                self.log_file,
            )

            self.rf_model.fit(train_x, train_y)

            self.log_writer.log(
                f"Created {self.rf_model_name} based on the {self.rf_best_params} as params",
                self.log_file,
            )

            self.log_writer.start_log(
                "exit", self.class_name, method_name, self.log_file
            )

            return self.rf_model

        except Exception as e:
            self.log_writer.exception_log(
                e, self.class_name, method_name, self.log_file
            )

    def get_xgboost_model(self, train_x, train_y):
        """
        Method Name :   get_xgboost_model
        Description :   get the parameters for XGBoost Algorithm which give the best accuracy.
                        Use Hyper Parameter Tuning.

        Output      :   The model with the best parameters
        On Failure  :   Write an exception log and then raise an exception

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_xgboost_model.__name__

        self.log_writer.start_log("start", self.class_name, method_name, self.log_file)

        try:
            self.xgb_model_name = self.xgb_model.__class__.__name__

            self.xgb_best_params = self.model_utils.get_model_params(
                self.xgb_model, train_x, train_y, self.log_file
            )

            self.log_writer.log(
                f"{self.xgb_model_name} model best params are {self.xgb_best_params}",
                self.log_file,
            )

            self.xgb_model.set_params(**self.xgb_best_params)

            self.log_writer.log(
                f"Initialized {self.xgb_model_name} model with best params as {self.xgb_best_params}",
                self.log_file,
            )

            self.xgb_model.fit(train_x, train_y)

            self.log_writer.log(
                f"Created {self.xgb_model_name} model with best params as {self.xgb_best_params}",
                self.log_file,
            )

            self.log_writer.start_log(
                "exit", self.class_name, method_name, self.log_file
            )

            return self.xgb_model

        except Exception as e:
            self.log_writer.exception_log(
                e, self.class_name, method_name, self.log_file
            )

    def get_trained_models(self, train_x, train_y, test_x, test_y):
        """
        Method Name :   get_trained_models
        Description :   Find out the Model which has the best score.
        
        Output      :   The best model name and the model object
        On Failure  :   Write an exception log and then raise an exception

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_trained_models.__name__

        self.log_writer.start_log("start", self.class_name, method_name, self.log_file)

        try:
            self.xgb_model = self.get_xgboost_model(train_x, train_y)

            self.log_writer.log(
                f"Got trained {self.xgb_model.__class__.__name__} model", self.log_file
            )

            self.xgb_model_score = self.model_utils.get_model_score(
                self.xgb_model, test_x, test_y, self.log_file
            )

            self.log_writer.log(
                f"{self.xgb_model.__class__.__name__} model score is {self.xgb_model_score}",
                self.log_file,
            )

            self.rf_model = self.get_rf_model(train_x, train_y)

            self.log_writer.log(
                f"Got trained {self.rf_model.__class__.__name__} model", self.log_file
            )

            self.rf_model_score = self.model_utils.get_model_score(
                self.rf_model, test_x, test_y, self.log_file
            )

            self.log_writer.log(
                f"{self.rf_model.__class__.__name__} model score is {self.rf_model_score}",
                self.log_file,
            )

            lst = [
                (self.xgb_model, self.xgb_model_score),
                (self.rf_model, self.rf_model_score),
            ]

            self.log_writer.log(
                "Got list of tuples consisting of trained models and model scores",
                self.log_file,
            )

            self.log_writer.start_log(
                "exit", self.class_name, method_name, self.log_file
            )

            return lst

        except Exception as e:
            self.log_writer.exception_log(
                e, self.class_name, method_name, self.log_file
            )

    def train_and_log_models(self, X_data, Y_data, log_file, idx):
        """
        Method Name :   train_and_log_models
        Description :   The methods gets the trained models and performs logging of models,parameters and metrics to mlflow server 
        
        Output      :   The trained models along with thier parameters and metrics are logged into mlflow server and artifacts are stored
                        blob container                
        On Failure  :   Write an exception log and then raise an exception

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.train_and_log_models.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            x_train, x_test, y_train, y_test = train_test_split(
                X_data, Y_data, **self.split_kwargs
            )

            self.log_writer.log(
                f"Performed train test split with kwargs as {self.split_kwargs}",
                log_file,
            )

            lst = self.get_trained_models(x_train, y_train, x_test, y_test)

            self.log_writer.log("Got trained models", log_file)

            for _, tm in enumerate(lst):
                model = tm[0]

                model_score = tm[1]

                self.blob.save_model(
                    model,
                    self.model_dir["train"],
                    "model",
                    self.save_format,
                    log_file,
                    idx=idx,
                )

                self.mlflow_op.log_all_for_model(model, model_score, idx)

            self.log_writer.log(
                "Saved and logged all trained models to mlflow", log_file
            )

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)
