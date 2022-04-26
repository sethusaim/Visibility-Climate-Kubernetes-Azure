from utils.logger import App_Logger
from utils.read_params import read_params

class Blob_Operation:
    def __init__(self):
        self.class_name = self.__class__.__name__
        
        self.config = read_params()
        
        self.log_writer = App_Logger()
        
        