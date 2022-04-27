from os import environ, remove

from azure.storage.blob import BlobServiceClient, ContainerClient

from utils.logger import App_Logger
from utils.read_params import read_params


class Blob_Operation:
    def __init__(self):
        self.class_name = self.__class__.__name__

        self.config = read_params()

        self.log_writer = App_Logger()

        self.connection_string = environ["AZURE_CONN_STR"]

    def get_container_client(self, container, log_file):
        method_name = self.get_container_client.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            container_client = ContainerClient.from_connection_string(
                conn_str=self.connection_string, container=container
            )

            self.log_writer.log("Got container client from connection string", log_file)

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

            return container_client

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)

    def get_blob_client(self, blob_fname, container, log_file):
        method_name = self.get_blob_client.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            client = BlobServiceClient.from_connection_string(
                conn_str=self.connection_string
            )

            self.log_writer.log(
                "Got BlobServiceClient from connection string", log_file
            )

            blob_client = client.get_blob_client(container=container, blob=blob_fname)

            self.log_writer.log(
                f"Got blob client for {blob_fname} blob from {container} container",
                log_file,
            )

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

            return blob_client

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)

    def load_file(self, fname, container, log_file):
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
                    self.delete_file(
                        fname=container_fname, container=container,
                    )

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
