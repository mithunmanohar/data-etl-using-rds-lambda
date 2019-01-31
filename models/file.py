"""
Interface for related operations
"""
import os
import urllib
import shutil
import logging
import requests

logger = logging.getLogger(__name__)

class FileApi(object):
    def __init__(self, configs):
        self.configs = configs

    def _parse_drop_box_link(self, url):
        url = urllib.parse.unquote(url)
        url = url.split("?")
        return url[0]

    def download_file(self, url):
        try:
            parsed_url = self._parse_drop_box_link(url)
            local_filename = self.configs.download_folder + os.sep \
                             + parsed_url.split('/')[-1]
            print(local_filename)
            logger.debug(
                'downloading files to %s folder' % local_filename)
            r = requests.get(url, stream=True)
            with open(local_filename, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
            logger.debug(
                'downloaded files to %s folder' % local_filename)
            return local_filename
        except Exception as e:
            logger.exception("Exeception in downloading file due to %s"
                              % e)


    def extract_file(self, file_path):
        pass

