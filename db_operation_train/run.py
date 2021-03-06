from data_type_valid_train import DB_Operation_Train
from utils.logger import App_Logger
from utils.main_utils import Main_Utils


class Run:
    """
    Description :   This class is used for running the database operation trainiction pipeline
    Version     :   1.2
    
    Revisions   :   Moved to setup to cloud 
    """

    def __init__(self):
        self.class_name = self.__class__.__name__

        self.log_writer = App_Logger()

        self.db_operation = DB_Operation_Train()

    def train_data_type_valid(self):
        """
        Method Name :   train_data_type_valid
        Description :   This method performs the database operations for trainiction data

        Output      :   The dataframe is inserted in database collection
        On Failure  :   Write an exception log and then raise an exception

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.train_data_type_valid.__name__

        self.log_writer.start_log("start", self.class_name, method_name, "db_main")

        try:
            self.log_writer.log("Data type validation operation started !!", "db_main")

            self.db_operation.insert_good_data_as_record("db_name", "collection_name")

            self.db_operation.export_collection_to_csv("db_name", "collection_name")

            self.log_writer.log(
                "Data type validation Operation completed !!", "db_main"
            )

            self.log_writer.start_log("exit", self.class_name, method_name, "db_main")

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, "db_main")


if __name__ == "__main__":
    try:
        run = Run()

        run.train_data_type_valid()

    except Exception as e:
        raise e

    finally:
        utils = Main_Utils()

        utils.upload_logs()
