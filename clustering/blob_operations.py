from io import StringIO
from os import environ, remove
from pickle import dump

from azure.storage.blob import BlobServiceClient, ContainerClient
from pandas import read_csv

from utils.logger import App_Logger
from utils.read_params import read_params


class Blob_Operation:
    """
    Description :   This class is used for performing blob operations required by the service
    
    Version     :   1.2
    Revisions   :   Moved to setup to cloud 
    """

    def __init__(self):
        self.config = read_params()

        self.connection_string = environ["AZURE_CONN_STR"]

        self.class_name = self.__class__.__name__

        self.container = self.config["blob_container"]

        self.log_writer = App_Logger()

    def get_container_client(self, container, log_file):
        """
        Method Name :   get_container_client
        Description :   This method gets the container client for the specified container name
        
        Output      :   The container client for the particular container name is returned
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_container_client.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            container_client = ContainerClient.from_connection_string(
                conn_str=self.connection_string,
                container_name=self.container[container],
            )

            self.log_writer.log("Got container client from connection string", log_file)

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

            return container_client

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)

    def get_blob_client(self, blob_fname, container, log_file):
        """
        Method Name :   get_blob_client
        Description :   This method gets the blob client for the specified container name
        
        Output      :   The blob client for the particular container name is returned
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """

        method_name = self.get_blob_client.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            client = BlobServiceClient.from_connection_string(self.connection_string)

            blob_client = client.get_blob_client(
                container=self.container[container], blob=blob_fname
            )

            return blob_client

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)

    def load_file(self, fname, container, log_file):
        """
        Method Name :   load_file
        Description :   This method loads the file from a blob container
        
        Output      :   The file is loaded from the blob container
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.load_file.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            blob_client = self.get_blob_client(fname, container, log_file)

            self.log_writer.log("Got blob client from blob service client", log_file)

            f = blob_client.exists()

            self.log_writer.log(f"{fname} file exists is {f}", log_file)

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

            return f

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)

    def delete_file(self, fname, container, log_file):
        """
        Method Name :   delete_file
        Description :   This method deletes the file from a blob container
        
        Output      :   The file is deleted from the blob container
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.delete_file.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            client = self.get_container_client(container, log_file)

            client.delete_blob(fname)

            self.log_writer.log(
                f"Deleted {fname} file from {container} container", log_file
            )

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)

    def upload_file(
        self,
        local_fname,
        container_fname,
        container,
        log_file,
        delete=True,
        replace=True,
    ):
        """
        Method Name :   upload_file
        Description :   This method uploades the file based on parameters of delete and replace,
                        - delete parameter removes the local copy of the file, by default delete is set to True, since we do not want to 
                        have any information within docker container
                        - replace parameter replaces the existing file in the container, by default replace is set to True, because we are 
                        updating old file with new data.
        
        Output      :   The file is uploaded to the blob container,with default parameters
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.upload_file.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            client = self.get_container_client(container, log_file)

            if replace is True:
                f = self.load_file(container_fname, container, log_file)

                self.log_writer.log(
                    f"{container_fname} file exists is {f}, and replace option is set to {replace}..Deleting the file",
                    log_file,
                )

                if f is True:
                    self.delete_file(container_fname, container, log_file)

                else:
                    self.log_writer.log(
                        f"{container_fname} file exists is {f}", log_file
                    )

                with open(local_fname, "rb") as f:
                    client.upload_blob(data=f, name=container_fname)

                self.log_writer.log(
                    f"Uploaded {local_fname} to {container} container with name as {container_fname} file",
                    log_file,
                )

            else:
                self.log_writer.log(
                    f"Replace option is set to {replace}, not replacing the {container_fname} file in {container} container",
                    log_file,
                )

            if delete is True:
                remove(local_fname)

                self.log_writer.log(
                    f"delete option is set to {delete}, deleted {local_fname} from local",
                    log_file,
                )

            else:
                self.log_writer.log(
                    f"deleted option is set to {delete}, not removing the {local_fname} from local",
                    log_file,
                )

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)

    def save_model(self, model, model_dir, save_format, container, log_file, idx=None):
        """
        Method Name :   save_model
        Description :   This method saves the model to a blob container, in particular folder and save format
        
        Output      :   The model is saved to blob container in particular folder and save format
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.save_model.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            model_name = model.__class__.__name__

            func = (
                lambda: model_name + save_format
                if model_name == "KMeans"
                else model_name + str(idx) + save_format
            )

            model_file = func()

            self.log_writer.log(
                f"Local copy of {model_name} model file name is created", log_file
            )

            dir_func = (
                lambda: model_dir + "/" + model_file
                if model_dir is not None
                else model_file
            )

            container_model_file = dir_func()

            self.log_writer.log(
                f"Container location of {model_name} model file name is created ",
                log_file,
            )

            with open(model_file, "wb") as f:
                dump(model, f)

            self.log_writer.log(f"Saved local copy of {model_name} model", log_file)

            self.upload_file(model_file, container_model_file, container, log_file)

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)

    def get_object(self, fname, container, log_file):
        """
        Method Name :   get_object
        Description :   This method get the file objects from container
        
        Output      :   The file object/objects are returned from the container 
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_object.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            client = self.get_container_client(container, log_file)

            f = client.download_blob(blob=fname)

            self.log_writer.log(
                f"Got {fname} info from {container} container", log_file
            )

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

            return f

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)

    def read_object(self, object, log_file, decode=True, make_readable=False):
        """
        Method Name :   read_object
        Description :   This method reads the object, with decode and make_readable as the parameters 
        
        Output      :   The file object/objects are read from the container, with decode and make_readable as parameters
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.read_object.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            func = (
                lambda: object.readall().decode()
                if decode is True
                else object.readall()
            )

            self.log_writer.log(
                f"Read {object} object with decode as {decode}", log_file
            )

            conv_func = lambda: StringIO(func()) if make_readable is True else func()

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

            return conv_func()

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)

    def read_csv(self, fname, container, log_file):
        """
        Method Name :   read_csv
        Description :   This method reads the csv file from container
        
        Output      :   The csv file is read from the container and returned as dataframe
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.read_csv.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            csv_obj = self.get_object(fname, container, log_file)

            content = self.read_object(csv_obj, log_file, make_readable=True)

            df = read_csv(content)

            self.log_writer.log(
                f"Read {fname} csv file from {container} container", log_file
            )

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

            return df

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)

    def upload_df_as_csv(
        self, dataframe, local_fname, container_fname, container, log_file
    ):
        """
        Method Name :   upload_df_as_csv
        Description :   This method uploads the dataframe as csv file to blob container
        
        Output      :   The dataframe is uploaded to blob container as a csv file
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.upload_df_as_csv.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            dataframe.to_csv(local_fname, index=None, header=True)

            self.log_writer.log(
                f"Created a local copy of dataframe with name {local_fname}", log_file
            )

            self.upload_file(local_fname, container_fname, container, log_file)

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)
