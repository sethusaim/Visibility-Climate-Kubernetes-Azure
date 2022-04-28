from curses.ascii import ETB
from io import StringIO
from os import environ, remove
from pickle import loads, dump

from azure.storage.blob import BlobServiceClient, ContainerClient

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
                self.connection_string, container
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

            blob_client = client.get_blob_client(container=container, blob=blob_fname)

            self.log_writer.log(
                f"Got blob client for {blob_fname} blob from {container} container",
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

            self.log_writer.log(
                f"Read {object} with make_readable as {make_readable}", log_file
            )

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

            return conv_func()

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
                    self.log_writer.log(f"{container_fname} file exists is {f}",)

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

    def load_model(self, model_name, container, save_format, log_file, model_dir=None):
        method_name = self.load_model.__name__

        self.log_writer.start_log("start", self.class_name, method_name, log_file)

        try:
            func = (
                lambda: model_name + save_format
                if model_dir is None
                else model_dir + "/" + model_name + save_format
            )

            model_file = func()

            self.log_writer.log(f"Got {model_file} as model file", log_file)

            f_obj = self.get_object(model_file, container, log_file)

            model_content = self.read_object(f_obj, log_file, decode=False)

            model = loads(model_content)

            self.log_writer.log(
                f"Loaded {model_name} model from {container} container", log_file
            )

            self.log_writer.start_log("exit", self.class_name, method_name, log_file)

            return model

        except Exception as e:
            self.log_writer.exception_log(e, self.class_name, method_name, log_file)

    def save_model(self, model, model_dir, container, save_format, log_file, idx=None):
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
