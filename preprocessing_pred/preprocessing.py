from numpy import asarray
from numpy import nan as np_nan
from pandas import DataFrame, get_dummies
from sklearn.impute import KNNImputer
from sklearn.preprocessing import StandardScaler

from blob_operations import Blob_Operation
from utils.logger import App_Logger
from utils.read_params import read_params


class Preprocessor:
    """
    Description :   This class shall be used to clean and transform the data before training
    Version     :   1.2
    
    Revisions   :   Moved to setup to cloud 
    """

    def __init__(self, log_file):
        self.class_name = self.__class__.__name__

        self.config = read_params()

        self.log_file = log_file

        self.files = self.config["files"]

        self.imputer_params = self.config["knn_imputer"]

        self.container = self.config["blob_container"]

        self.blob = Blob_Operation()

        self.log_writer = App_Logger()

    def remove_columns(self, data, columns):
        """
        Method Name :   remove_columns
        Description :   This method removes the given columns from a pandas dataframe.

        Output      :   A pandas dataframe after removing the specified columns.
        On Failure  :   Write an exception log and then raise an exception

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.remove_columns.__name__

        self.log_writer.start_log(
            "start", self.class_name, method_name, self.log_file,
        )

        try:
            self.data = data

            self.columns = columns

            self.useful_data = self.data.drop(labels=self.columns, axis=1)

            self.log_writer.log("Column removal Successful", self.log_file)

            self.log_writer.start_log(
                "exit", self.class_name, method_name, self.log_file,
            )

            return self.useful_data

        except Exception as e:
            self.log_writer.log("Column removal Unsuccessful", self.log_file)

            self.log_writer.exception_log(
                e, self.class_name, method_name, self.log_file,
            )

    def separate_label_feature(self, data, label_column_name):
        """
        Method Name :   separate_label_feature
        Description :   This method separates the features and a Label Coulmns.

        Output      :   Returns two separate Dataframes, one containing features and the other containing Labels .
        On Failure  :   Write an exception log and then raise an exception

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.separate_label_feature.__name__

        self.log_writer.start_log(
            "start", self.class_name, method_name, self.log_file,
        )

        try:
            self.X = data.drop(labels=label_column_name, axis=1)

            self.Y = data[label_column_name]

            self.log_writer.log("Label Separation Successful", self.log_file)

            self.log_writer.start_log(
                "exit", self.class_name, method_name, self.log_file,
            )

            return self.X, self.Y

        except Exception as e:
            self.log_writer.log("Label Separation Unsuccessful", self.log_file)

            self.log_writer.exception_log(
                e, self.class_name, method_name, self.log_file,
            )

    def drop_unnecessary_columns(self, data, cols):
        """
        Method Name :   drop_unnecessary_columns
        Description :   This method drop unnecessary columns in the dataframe

        Output      :   Unnecessary columns are dropped in the dataframe
        On Failure  :   Write an exception log and then raise an exception

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.drop_unnecessary_columns.__name__

        self.log_writer.start_log(
            "start", self.class_name, method_name, self.log_file,
        )

        try:
            data = data.drop(cols, axis=1)

            self.log_writer.log("Dropped unnecessary columns", self.log_file)

            self.log_writer.start_log(
                "exit", self.class_name, method_name, self.log_file,
            )

            return data

        except Exception as e:
            self.log_writer.exception_log(
                e, self.class_name, method_name, self.log_file,
            )

    def replace_invalid_with_null(self, data):
        """
        Method Name :   replace_invalid_with_null
        Description :   This method replaces invalid values with null

        Output      :   A dataframe where invalid values are replaced with null
        On Failure  :   Write an exception log and then raise an exception

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.replace_invalid_with_null.__name__

        self.log_writer.start_log(
            "start", self.class_name, method_name, self.log_file,
        )

        try:
            for column in data.columns:
                count = data[column][data[column] == "?"].count()

                if count != 0:
                    data[column] = data[column].replace("?", np_nan)

            self.log_writer.log("Replaced invalid values with nan", self.log_file)

            self.log_writer.start_log(
                "exit", self.class_name, method_name, self.log_file,
            )

            return data

        except Exception as e:
            self.log_writer.exception_log(
                e, self.class_name, method_name, self.log_file,
            )

    def is_null_present(self, data):
        """
        Method Name :   is_null_present
        Description :   This method checks whether there are null values present in the pandas dataframe or not.

        Output      :   Returns True if null values are present in the dataframe, False if they are not present and
                        returns the list of columns for which null values are present.
        On Failure  :   Write an exception log and then raise an exception

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.is_null_present.__name__

        self.log_writer.start_log(
            "start", self.class_name, method_name, self.log_file,
        )

        try:

            self.null_present = False

            self.cols_with_missing_values = []

            self.cols = data.columns

            self.null_counts = data.isna().sum()

            for i in range(len(self.null_counts)):
                if self.null_counts[i] > 0:
                    self.null_present = True

                    self.cols_with_missing_values.append(self.cols[i])

            if self.null_present:
                self.null_df = DataFrame()

                self.null_df["columns"] = data.columns

                self.null_df["missing values count"] = asarray(data.isna().sum())

            self.log_writer.log("Created data frame with null values", self.log_file)

            self.blob.upload_df_as_csv(
                self.null_df,
                self.files["null_values"],
                self.files["null_values"],
                self.container["io_files"],
                self.log_file,
            )

            self.log_writer.start_log(
                "exit", self.class_name, method_name, self.log_file
            )

            return self.null_present

        except Exception as e:
            self.log_writer.log("Finding missing values failed", self.log_file)

            self.log_writer.exception_log(
                e, self.class_name, method_name, self.log_file
            )

    def encode_target_cols(self, data):
        """
        Method Name :   encode_target_cols
        Description :   This method encodes all the categorical values in the training set.

        Output      :   A dataframe which has all the categorical values encoded.
        On Failure  :   Write an exception log and then raise an exception

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.encode_target_cols.__name__

        self.log_writer.start_log(
            "start", self.class_name, method_name, self.log_file,
        )

        try:
            data["class"] = data["class"].map({"p": 1, "e": 2})

            for column in data.drop(["class"], axis=1).columns:
                data = get_dummies(data, columns=[column])

            self.log_writer.log("Encoded target columns", self.log_file)

            self.log_writer.start_log(
                "exit", self.class_name, method_name, self.log_file,
            )

            return data

        except Exception as e:
            self.log_writer.exception_log(
                e, self.class_name, method_name, self.log_file,
            )

    def apply_standard_scaler(self, X):
        """
        Method Name : apply_standard_scaler
        Description : This method replaces all the missing values in the dataframe using KNN Imputer.

        Output      : A dataframe which has all the missing values imputed.
        On Failure  : Raise Exception

        Version     : 1.2
        Revisions   : moved setup to cloud
        """
        method_name = self.apply_standard_scaler.__name__

        self.log_writer.start_log(
            "start", self.class_name, method_name, self.log_file,
        )

        try:
            scalar = StandardScaler()

            X_scaled = scalar.fit_transform(X)

            self.log_writer.log(
                f"Transformed data using {scalar.__class__.__name__}", self.log_file
            )

            self.log_writer.start_log(
                "exit", self.class_name, method_name, self.log_file,
            )

            return X_scaled

        except Exception as e:
            self.log_writer.exception_log(
                e, self.class_name, method_name, self.log_file,
            )

    def impute_missing_values(self, data):
        """
        Method Name : impute_missing_values
        Description : This method replaces all the missing values in the dataframe using KNN Imputer.

        Output      : A dataframe which has all the missing values imputed.
        On Failure  : Raise Exception

        Version     : 1.2
        Revisions   : moved setup to cloud
        """
        method_name = self.impute_missing_values.__name__

        self.log_writer.start_log(
            "start", self.class_name, method_name, self.log_file,
        )

        try:
            self.data = data

            imputer = KNNImputer(missing_values=np_nan, **self.imputer_params)

            self.log_writer.log(
                f"Initialized {imputer.__class__.__name__}", self.log_file
            )

            self.new_array = imputer.fit_transform(self.data)

            self.log_writer.log(
                "Imputed missing values using KNN imputer", self.log_file
            )

            self.new_data = DataFrame(data=(self.new_array), columns=self.data.columns)

            self.log_writer.log(
                "Created new dataframe with imputed values", self.log_file
            )

            self.log_writer.log("Imputing missing values Successful", self.log_file)

            self.log_writer.start_log(
                "exit", self.class_name, method_name, self.log_file,
            )

            return self.new_data

        except Exception as e:
            self.log_writer.exception_log(
                e, self.class_name, method_name, self.log_file,
            )

    def get_columns_with_zero_std_deviation(self, data):
        """
        Method Name :   get_columns_with_zero_std_deviation
        Description :   This method finds out the columns which have a standard deviation of zero.

        Output      :   List of the columns with standard deviation of zero
        On Failure  :   Write an exception log and then raise an exception

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_columns_with_zero_std_deviation.__name__

        self.log_writer.start_log(
            "start", self.class_name, method_name, self.log_file,
        )

        try:
            self.columns = data.columns

            self.data_n = data.describe()

            self.col_drop = [x for x in self.columns if self.data_n[x]["std"] == 0]

            self.log_writer.log(
                "Column search for Standard Deviation of Zero Successful", self.log_file
            )

            self.log_writer.start_log(
                "exit", self.class_name, method_name, self.log_file,
            )

            return self.col_drop

        except Exception as e:
            self.log_writer.log(
                "Column search for Standard Deviation of Zero Failed", self.log_file
            )

            self.log_writer.exception_log(
                e, self.class_name, method_name, self.log_file,
            )
