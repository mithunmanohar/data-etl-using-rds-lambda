"""
ETL manager class provides an interface to manage e2e process of
downloading file from remote location, processing, loading data to RDS

"""
# global imports
import os
import sys
import gzip
import json
import logging
import psycopg2
from psycopg2.extensions import AsIs

# local imports
from src.file import FileApi
from configs.config import conf
from src.database import PgDb

# init logger
file_handler = logging.FileHandler(filename='logs/etl.log')
stdout_handler = logging.StreamHandler(sys.stdout)
handlers = [file_handler, stdout_handler]
logging.basicConfig(level=logging.DEBUG,
                    format='[%(asctime)s] [%(filename)s:%(lineno)d] '
                           '%(levelname)s - %(message)s',
                    handlers=handlers)

logging.getLogger(__name__)
logging.debug("JJJJJJJJ")

class EtlManager(object):
    def __init__(self, configs):
        self.configs = configs
        self.file_handler = FileApi(self.configs)
        self.pg_con = PgDb(self.configs.pg_db['pg_data_lake'])

    def start_file_injestion(self, file_list):
        batch_size = self.configs.batch_size
        for fl in file_list:
            try:
                if fl.endswith('.gz'):
                    with gzip.open(fl, 'r') as fl_obj:
                        try:
                            for rec in fl_obj:
                                rec = json.loads(rec)
                                if 'user' in rec:
                                    rec['t_user'] = rec.pop('user')
                                columns = list(rec.keys())

                                values = [rec[x] if type(rec[x]) != dict
                                          else json.dumps(rec[x]) for
                                          x in columns]
                                print('Inseting ')
                                insert_statement = 'insert into twitter (%s) values %s'
                                self.pg_con.get_cursor().execute(
                                    insert_statement, (AsIs(','.join(columns)),
                                                       tuple(values)))
                        except Exception as e:
                            logging.exception("exception in processing "
                                              "record %s. error %s " % (rec, e))
                else:
                    logging.warning("Invalid file %s", fl)

            except Exception as e:
                logging.exception("exception in processing file %s error %s " % (fl, e))



    def start_etl(self):
        logging.info("Downloading file from %s to local folder" %
                     self.configs.file_url)
        file_to_process = self.file_handler.download_file(self.configs.file_url)
        if file_to_process:
            unzip_folder = self.file_handler.unzip_file(file_to_process)
            file_list = self.file_handler.get_file_list(unzip_folder)
            logging.info("%s files to process" % str(len(file_list)))
            self.start_file_injestion(file_list)
        else:
            logging.critical("Unable to download file. Exiting..")


if __name__ == '__main__':
    ENV = os.getenv('ENVIRONMENT', 'DEV')                               # fetch platform from env
    configs = conf[ENV]                                                 # init platform configs
    etl_obj = EtlManager(configs)
    etl_obj.start_etl()
