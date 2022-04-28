from os import listdir
from os.path import join
from shutil import rmtree

from boto3 import resource
from botocore.exceptions import ClientError


from utils.logger import App_Logger
from utils.read_params import read_params


class Main_Utils:
    def __init__(self):
        self.blob = S3_Operation()

        self.log_writer = App_Logger()

        self.config = read_params()

        self.s3_resource = resource("blob")

        self.files = self.config["files"]

        self.container = self.config["blob_container"]

        self.model_dir = self.config["model_dir"]

        self.log_dir = self.config["log_dir"]

        self.log_file = self.config["log"]["upload"]

        self.class_name = self.__class__.__name__

    def upload_logs(self):
        method_name = self.upload_logs.__name__

        self.log_writer.start_log("start", self.class_name, method_name, self.log_file)

        try:
            lst = listdir(self.log_dir)

            self.log_writer.log(
                f"Got list of logs from {self.log_dir} folder", self.log_file
            )

            for f in lst:
                local_f = join(self.log_dir, f)

                dest_f = self.log_dir + "/" + f

                self.blob.upload_file(
                    local_f, dest_f, self.container["logs"], self.log_file
                )

            self.log_writer.log(
                f"Uploaded logs to {self.container['logs']}", self.log_file
            )

            self.log_writer.start_log(
                "exit", self.class_name, method_name, self.log_file
            )

            rmtree(self.log_dir)

        except Exception as e:
            self.log_writer.exception_log(
                e, self.class_name, method_name, self.log_file
            )

    def find_correct_model_file(self, cluster_number, container, log_file):
        """
        Method Name :   find_correct_model_file
        Description :   This method gets correct model file based on cluster number during trainiction
        Output      :   A correct model file is found 
        On Failure  :   Write an exception log and then raise an exception
        Version     :   1.2
        
        Revisions   :   moved setup to cloud
        """
        method_name = self.find_correct_model_file.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            list_of_files = self.blob.get_files_from_folder(
                self.model_dir["prod"], container, log_file
            )

            for file in list_of_files:
                try:
                    if file.index(str(cluster_number)) != -1:
                        model_name = file

                except:
                    continue

            model_name = model_name.split(".")[0]

            self.log_writer.log(
                f"Got {model_name} from {self.prod_model_dir} folder in {container} container",
                log_file,
            )

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

            return model_name

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)

    def delete_train_file(self, log_file):
        """
        Method Name :   delete_train_file
        Description :   This method deletes the existing trainiction file for the model trainiction starts
        
        Output      :   An existing trainiction file is deleted
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.delete_train_file.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            self.s3_resource.Object(
                self.container["io_files"], self.files["train_file"]
            ).load()

            self.log_writer.log(
                f"Found existing trainiction batch file. Deleting it.", log_file
            )

            self.blob.delete_file(
                self.files["train_file"], self.container["io_files"], log_file
            )

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                pass

            else:
                self.log_writer.exception_log(
                    e, self.class_name, method_name, log_file,
                )
