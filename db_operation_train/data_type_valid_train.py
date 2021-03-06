from blob_operations import Blob_Operation
from mongo_db_operations import MongoDB_Operation
from utils.logger import App_Logger


class DB_Operation_Train:
    """
    Description :    This class shall be used for handling all the db operations
    Version     :   1.2
    
    Revisions   :   Moved to setup to cloud 
    """

    def __init__(self):
        self.class_name = self.__class__.__name__

        self.blob = Blob_Operation()

        self.mongo = MongoDB_Operation()

        self.log_writer = App_Logger()

    def insert_good_data_as_record(self, good_data_db_name, good_data_collection_name):
        """
        Method Name :   insert_good_data_as_record
        Description :   This method inserts the good data in MongoDB as collection

        Output      :   A MongoDB collection is created with good data present in it
        On Failure  :   Write an exception log and then raise an exception

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.insert_good_data_as_record.__name__

        self.log_writer.start_log("start", self.class_name, method_name, "db_insert")

        try:
            lst = self.blob.read_csv_from_folder(
                "train_good_data", "train_data", "db_insert"
            )

            for _, f in enumerate(lst):
                df = f[0]

                self.mongo.insert_dataframe_as_record(
                    df, good_data_db_name, good_data_collection_name, "db_insert"
                )

                self.log_writer.log(
                    "Inserted dataframe as collection record in mongodb", "db_insert"
                )

            self.log_writer.start_log("exit", self.class_name, method_name, "db_insert")

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, "db_insert")

    def export_collection_to_csv(self, good_data_db_name, good_data_collection_name):
        """
        Method Name :   export_collection_to_csv
        Description :   This method inserts the good data in MongoDB as collection

        Output      :   A csv file stored in input files container, containing good data which was stored in MongoDB
        On Failure  :   Write an exception log and then raise an exception

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.export_collection_to_csv.__name__

        self.log_writer.start_log("start", self.class_name, method_name, "export_csv")

        try:
            df = self.mongo.get_collection_as_dataframe(
                good_data_db_name, good_data_collection_name, "export_csv"
            )

            self.blob.upload_df_as_csv(
                df, "train_export", "train_export", "feature_store", "export_csv"
            )

            self.log_writer.log("Exported dataframe to csv file", "export_csv")

            self.log_writer.start_log(
                "exit", self.class_name, method_name, "export_csv"
            )

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, "export_csv")
