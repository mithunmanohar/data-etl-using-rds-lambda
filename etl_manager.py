"""
ETL manager class provides an interface to manage e2e process of
downloading file from remote location, loading data to RDS

"""
# global imports
import os
import sys
import logging

# init logger
file_handler = logging.FileHandler(filename='logs/etl.log')
stdout_handler = logging.StreamHandler(sys.stdout)
handlers = [file_handler, stdout_handler]
logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] [%(filename)s:%(lineno)d] '
                           '%(levelname)s - %(message)s',
                    handlers=handlers)

logger = logging.getLogger(__name__)
logger.info("Initialsed ETL job")

# local imports
from models.file import FileApi
from configs.config import conf

class EtlManager(object):
    def __init__(self, configs):
        self.configs = configs
        self.file_handler = FileApi(self.configs)

    def start_etl(self):
        file_to_process = self.file_handler.download_file(self.configs.file_url)
        if file_to_process:
            print("kk")
        else:
            logging.critical("Unable to download file. Exiting..")
            return
        pass


if __name__ == '__main__':
    ENV = os.getenv('ENVIRONMENT', 'DEV')
    configs = conf[ENV]
    etl_obj = EtlManager(configs)
    etl_obj.start_etl()
