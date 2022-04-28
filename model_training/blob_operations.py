from utils.read_params import read_params
from utils.logger import App_Logger


class Blob_Operation:
    def __init__(self):
        self.class_name = self.__class__.__name__

        self.config = read_params()

        self.logger = App_Logger()
