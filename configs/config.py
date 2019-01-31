#!usr/bin/env python

"""
Config manager for etl

"""

import logging
import os

logger = logging.getLogger(__name__)

class Config(object):
    pass

class DevelopmentConfig(Config):
    logger.info("Loaded DEV configs")
    DEBUG = True
    AWS_REGION = "us-east-1"

    download_folder = os.getcwd() + os.sep + "data"
    file_url = ""

    LOG_LEVEL = logging.INFO
    LOGS_ROOT = "/logs"

    postgres_db = {}



class ProductionConfig(Config):
    pass


class TestingConfig(Config):
    pass

conf = {
    'DEV': DevelopmentConfig,
    'PROD': ProductionConfig,
    'TEST': TestingConfig
}

