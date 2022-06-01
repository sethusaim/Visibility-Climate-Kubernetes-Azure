from blob_operations import Blob_Operation
from clustering import KMeans_Clustering
from utils.logger import App_Logger
from utils.main_utils import Main_Utils
from utils.read_params import read_params


class Run:
    """
    Description :   This class is used for running the clustering service
    
    Version     :   1.2
    Revisions   :   Moved to setup to cloud 
    """
    def __init__(self):
        self.config = read_params()

        self.files = self.config["files"]

        self.container = self.config["blob_container"]

        self.clustering_log = self.config["log"]["clustering"]

        self.utils = Main_Utils()

        self.log_writer = App_Logger()

        self.kmeans_op = KMeans_Clustering(self.clustering_log)

        self.blob = Blob_Operation()

        self.class_name = self.__class__.__name__

    def run_clustering(self):
        """
        Method Name :   run_clustering
        Description :   This method performs the clustering operation on training data
        
        Output      :   An elbow plot figure saved to input files container
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   Moved to setup to cloud 
        """
        method_name = self.run_clustering.__name__

        self.log_writer.start_log(
            "start", self.class_name, method_name, self.clustering_log
        )

        try:
            X = self.blob.read_csv(
                self.files["features"],
                self.container["feature_store"],
                self.clustering_log,
            )

            self.log_writer.log(
                f"Read the features file for training from {self.container['feature_store']} container",
                self.clustering_log,
            )

            Y = self.blob.read_csv(
                self.files["targets"],
                self.container["feature_store"],
                self.clustering_log,
            )

            self.log_writer.log(
                f"Read the labels for training from {self.container['feature_store']} container",
                self.clustering_log,
            )

            num_clusters = self.kmeans_op.draw_elbow_plot(X)

            X = self.kmeans_op.create_clusters(X, num_clusters)

            X["Labels"] = Y

            list_of_clusters = X["Cluster"].unique()

            self.log_writer.log(
                f"Got the {list_of_clusters} unique clusters", self.clustering_log
            )

            for i in list_of_clusters:
                cluster_data = X[X["Cluster"] == i]

                cluster_features = cluster_data.drop(["Labels", "Cluster"], axis=1)

                cluster_label = cluster_data["Labels"]

                cluster_feats_fname = self.utils.get_cluster_fname(
                    self.files["features"], i, self.clustering_log
                )

                cluster_label_fname = self.utils.get_cluster_fname(
                    self.files["targets"], i, self.clustering_log
                )

                self.blob.upload_df_as_csv(
                    cluster_features,
                    cluster_feats_fname,
                    cluster_feats_fname,
                    self.container["feature_store"],
                    self.clustering_log,
                )

                self.blob.upload_df_as_csv(
                    cluster_label,
                    cluster_label_fname,
                    cluster_label_fname,
                    self.container["feature_store"],
                    self.clustering_log,
                )

            self.log_writer.start_log(
                "exit", self.class_name, method_name, self.clustering_log
            )

        except Exception as e:
            self.log_writer.exception_log(
                e, self.class_name, method_name, self.clustering_log
            )


if __name__ == "__main__":
    try:
        run = Run()

        run.run_clustering()

    except Exception as e:
        raise e

    finally:
        utils = Main_Utils()

        utils.upload_logs()
