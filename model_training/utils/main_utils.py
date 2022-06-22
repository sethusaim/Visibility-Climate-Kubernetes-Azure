from blob_operations import Blob_Operation

from utils.logger import App_Logger
from utils.read_params import read_params


class Main_Utils:
    """
    Description :   This class is used for main utility functions required in core functions of the service
    
    Version     :   1.2 
    Revisions   :   Moved to setup to cloud 
    """

    def __init__(self):
        self.class_name = self.__class__.__name__

        self.config = read_params()

        self.log_dir = self.config["dir"]["log"]

        self.file_pattern = self.config["file_pattern"]

        self.blob = Blob_Operation()

        self.log_writer = App_Logger()

    def upload_logs(self):
        """
        Method Name :   upload_logs
        Description :   This method uploads the logs to blob container
        
        Output      :   The logs are uploaded to blob container
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.upload_logs.__name__

        self.log_writer.start_log("start", self.class_name, method_name, "upload")

        try:
            self.blob.upload_folder(self.log_dir, "logs", "upload")

            self.log_writer.log("Uploaded logs to logs container", "upload")

            self.log_writer.start_log("exit", self.class_name, method_name, "upload")

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, "upload")

    def get_cluster_fname(self, key, idx, log_file):
        """
        Method Name :   get_cluster_fname
        Description :   This method gets the file name based on the cluster number
        
        Output      :   File name based on cluster number is returned
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_cluster_fname.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            cluster_fname = "climate_" + key + f"-{idx}.csv"

            self.log_writer.log(f"Got the cluster file name for {key}", log_file)

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

            return cluster_fname

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)

    def get_targets_csv_as_numpy_array(self, fname, container, log_file):
        """
        Method Name :   get_targets_csv_as_numpy_array
        Description :   This method gets the targets csv file present in blob container as numpy array
        
        Output      :   The targets csv file is returned as numpy array
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_targets_csv_as_numpy_array.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            df = self.blob.read_csv(fname, container, log_file)

            self.log_writer.log(
                "Got dataframe from {container} with file as {fname}", log_file
            )

            targets = df["Labels"]

            self.log_writer.log("Got Labels col from dataframe", log_file)

            np_array = targets.to_numpy(dtype=int)

            self.log_writer.log("Converted targets dataframe to numpy array", log_file)

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

            return np_array

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)

    def get_features_csv_as_numpy_array(self, fname, container, log_file):
        """
        Method Name :   get_features_csv_as_numpy_array
        Description :   This method gets the features csv file present in blob container as numpy array
        
        Output      :   The features csv file is returned as numpy array
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_features_csv_as_numpy_array.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            df = self.blob.read_csv(fname, container, log_file)

            self.log_writer.log(
                f"Got the dataframe from {container} with file name as {fname}",
                log_file,
            )

            np_array = df.to_numpy()

            self.log_writer.log(
                f"Converted the dataframe to numpy array", log_file,
            )

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

            return np_array

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)

    def get_targets_csv(self, fname, container, log_file):
        """
        Method Name :   get_targets_csv
        Description :   This method gets the targets csv file present in blob container as numpy array
        
        Output      :   The targets csv file is returned as numpy array
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_targets_csv.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            df = self.blob.read_csv(fname, container, log_file)["Labels"]

            self.log_writer.log(
                "Got dataframe from {container} with file as {fname}", log_file
            )

            self.log_writer.log("Got Labels col from dataframe", log_file)

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

            return df

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)

    def get_features_csv(self, fname, log_file):
        """
        Method Name :   get_features_csv
        Description :   This method gets the features csv file present in blob container as numpy array
        
        Output      :   The features csv file is returned as numpy array
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_features_csv.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            df = self.blob.read_csv(fname, "feature_store", log_file)

            self.log_writer.log(
                f"Got the dataframe from feature store with file name as {fname}",
                log_file,
            )

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

            return df

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)

    def get_cluster_features(self, cluster_num, log_file):
        """
        Method Name :   get_cluster_features
        Description :   This method gets the cluster features based on the cluster number 
        
        Output      :   The cluster features are returned based on the cluster number
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_cluster_features.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            feat_name = self.get_cluster_fname("features", cluster_num, log_file)

            self.log_writer.log(
                "Got cluster feature file name based on cluster number", log_file
            )

            cluster_feat = self.get_features_csv(feat_name, log_file)

            self.log_writer.log(
                "Got cluster features based on the cluster file name", log_file
            )

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

            return cluster_feat

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)

    def get_cluster_targets(self, cluster_num, log_file):
        """
        Method Name :   get_cluster_targets
        Description :   This method gets the cluster targets based on the cluster number 
        
        Output      :   The cluster targets are returned based on the cluster number
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_cluster_targets.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            label_name = self.get_cluster_fname("targets", cluster_num, log_file)

            self.log_writer.log(
                "Got cluster targets file name based on cluster number", log_file
            )

            cluster_label = self.get_targets_csv(label_name, "feature_store", log_file)

            self.log_writer.log(
                "Got cluster targets based on the cluster file name", log_file
            )

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

            return cluster_label

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)

    def get_number_of_clusters(self, log_file):
        """
        Method Name :   get_number_of_cluster
        Description :   This method gets the number of clusters based on training data on which clustering algorithm was used
        
        Output      :   The number of clusters for the given training data is returned
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_number_of_clusters.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            feat_fnames = self.blob.get_files_from_folder(
                self.file_pattern, "feature_store", log_file
            )

            self.log_writer.log(
                f"Got features file names from s3 container based on {self.file_pattern}",
                log_file,
            )

            num_clusters = len(feat_fnames)

            self.log_writer.log(
                f"Got the number of clusters as {num_clusters}", log_file
            )

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

            return num_clusters

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)
