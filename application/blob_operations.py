from utils.read_params import read_params
from os import environ

class Blob_Operation:
    def __init__(self):
        self.config = read_params()
        
        self.connection_string = environ["AZURE_CONN_STR"]
        
        self.class_name = self.__class__.__name__
                