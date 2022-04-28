from io import StringIO
from json import loads
from os import environ, remove

from azure.storage.blob import BlobServiceClient, ContainerClient
from pandas import read_csv

from utils.logger import App_Logger
from utils.read_params import read_params


class Blob_Operation:
    def __init__(self):
        self.class_name = self.__class__.__name__

        self.config = read_params()

        self.connection_string = environ["AZURE_CONN_STR"]

        self.log_writer = App_Logger()

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
            client = BlobServiceClient.from_connection_string(self.connection_string)

            self.log_writer.log(
                "Got BlobServiceClient from connection string", log_file
            )

            blob_client = client.get_blob_client(container, blob_fname)

            self.log_writer.log(
                f"Got blob client for {blob_fname} blob present in {container} container",
                log_file,
            )

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

            return blob_client

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)

    def get_object(self, fname, container, log_file):
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

    def read_json(self, fname, container, log_file):
        method_name = self.read_json.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            f_obj = self.get_object(fname, container, log_file)

            json_content = self.read_object(f_obj, log_file)

            dic = loads(json_content)

            self.log_writer.log(
                f"Read {fname} json file from {container} container", log_file
            )

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

            return dic

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)

    def read_text(self, fname, container, log_file):
        method_name = self.read_text.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            f_obj = self.get_object(fname, container, log_file)

            content = self.read_object(f_obj, log_file)

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

            return content

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

    def get_files_from_folder(self, folder_name, container, log_file):
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

    def get_blob_url(self, fname, container, log_file):
        method_name = self.get_blob_url.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            blob_client = self.get_blob_client(fname, container, log_file)

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

    def get_df_from_object(self, object, log_file):
        method_name = self.get_df_from_object.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            content = self.read_object(object, log_file, make_readable=True,)

            df = read_csv(content)

            self.log_writer.log(f"Got dataframe from {object} object", log_file)

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

            return df

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)

    def read_csv(self, fname, container, log_file):
        method_name = self.read_csv.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            csv_obj = self.get_object(fname, container, log_file)

            df = self.get_df_from_object(csv_obj, log_file)

            self.log_writer.log(
                f"Read {fname} csv file from {container} container", log_file
            )

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

            return df

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)

    def read_csv_from_folder(self, folder_name, container, log_file):
        method_name = self.read_csv_from_folder.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            files = self.get_files_from_folder(folder_name, container, log_file)

            lst = [
                (self.read_csv(f, container, log_file), f, f.split("/")[-1],)
                for f in files
            ]

            self.log_writer.log(
                f"Read csv files from {folder_name} folder from {container} container",
                log_file,
            )

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

            return lst

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)

    def move_data(self, from_fname, from_container, to_fname, to_container, log_file):
        method_name = self.move_data.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            self.copy_data(from_fname, from_container, to_fname, to_container, log_file)

            self.delete_file(from_fname, from_container, log_file)

            self.log_writer.log(
                f"Moved {from_fname} file from {from_container} container to {to_container} container,with {to_fname} file as name",
                log_file,
            )

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)

    def upload_df_as_csv(
        self, dataframe, local_fname, container_fname, container, log_file
    ):
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
