from os import environ, listdir
from os.path import join
from shutil import rmtree

from azure.storage.blob import BlobServiceClient, ContainerClient

from utils.logger import App_Logger
from utils.read_params import read_params


class Blob_Operation:
    """
    Description :   This class is used for performing blob operations required by the service
    
    Version     :   1.2
    Revisions   :   Moved to setup to cloud 
    """

    def __init__(self):
        self.class_name = self.__class__.__name__

        self.config = read_params()

        self.container = self.config["blob_container"]

        self.log_writer = App_Logger()

        self.connection_string = environ["AZURE_CONN_STR"]

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
                self.connection_string, container=self.container[container]
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

            self.log_writer.log(
                "Got BlobServiceClient from connection string", log_file
            )

            blob_client = client.get_blob_client(
                container=self.container[container], blob=blob_fname
            )

            self.log_writer.log(
                f"Got blob client for {blob_fname} blob {container} container", log_file
            )

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

            return blob_client

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)

    def get_blob_url(self, fname, container, log_file):
        """
        Method Name :   get_blob_url
        Description :   This method gets the blob url for the specified file name present in the container
        
        Output      :   The blob url for the particular file name is returned present in the container
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_blob_url.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            blob_client = self.get_blob_client(fname, container)

            self.log_writer.log(
                f"Got {fname} blob from {container} container", log_file
            )

            f = blob_client.url

            self.log_writer.log(f"Got {fname} blob url", log_file)

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

            return f

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)

    def copy_data(self, from_fname, from_container, to_fname, to_container, log_file):
        """
        Method Name :   copy_data
        Description :   This method copies the data (from_fname) from one container to other container with to_fname as file name 
        
        Output      :   The data is copied from one container to other container with to_fname as file name
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.copy_data.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            dest_client = self.get_container_client(to_container, log_file)

            from_blob = self.get_blob_url(from_fname, from_container, log_file)

            to_blob = dest_client.get_blob_client(blob=to_fname)

            to_blob.start_copy_from_url(from_blob)

            self.log_writer.log(
                f"Copied {from_fname} file from {from_container} container to {to_fname} file from {to_container}",
                log_file,
            )

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)

    def get_files_from_folder(self, folder_name, container, log_file):
        """
        Method Name :   get_files_from_folder
        Description :   This method get the files from folder present in container
        
        Output      :   A list of files are returned from folder_name present in container 
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_files_from_folder.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            client = self.get_container_client(container, log_file)

            blob_list = client.list_blobs(name_starts_with=folder_name + "/")

            f_name_lst = [f.name for f in blob_list]

            self.log_writer.log(
                f"Got files from {folder_name} folder from {container} container",
                log_file,
            )

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

            return f_name_lst

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)

    def upload_folder(self, folder, container, log_file, delete=True):
        """
        Method Name :   upload_folder
        Description :   This method uploads the given folder to container
        
        Output      :   The folder is uploaded to container
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.upload_folder.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            lst = listdir(folder)

            self.log_writer.log(f"Got list of files from the {folder} folder", log_file)

            self.log_writer.log(
                f"Uploading files from {folder} folder to container", log_file
            )

            for f in lst:
                local_f = join(folder, f)

                dest_f = folder + "/" + f

                self.upload_file(local_f, dest_f, container, log_file)

            if delete is True:
                rmtree(folder)

            else:
                pass

            self.log_writer.log(f"Uploaded {folder} folder to container", log_file)

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)
