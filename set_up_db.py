#!usr/bin/env python

""""
Script for initial database setup
"""
import sys
import os

ENV = os.getenv('ENVIRONMENT', 'TEST')

# local imports
from src.database import PgDb
from configs.config import conf

class SetupDb(object):
    def __init__(self, configs):
        self.conn = PgDb(configs)

    def create_db_tables(self):
        q_create_twitter_table = """
        CREATE TABLE if not exists twitter (
        id BIGINT  PRIMARY KEY NOT NULL,
        created_at DATE NOT NULL,
        id_str VARCHAR,
        text VARCHAR NOT NULL,
        display_text_range integer[],
        source VARCHAR,
        truncated BOOLEAN,
        in_reply_to_status_id BIGINT,
        in_reply_to_status_id_str VARCHAR,
        in_reply_to_user_id BIGINT,
        in_reply_to_user_id_str VARCHAR,
        in_reply_to_screen_name VARCHAR,
        t_user jsonb,
        geo jsonb,
        coordinates JSONB,
        place JSONB,
        contributors VARCHAR,
        quoted_status_id BIGINT,
        quoted_status_id_str VARCHAR,
        quoted_status JSONB,
        retweeted_status JSONB,
        is_quote_status BOOLEAN,
        extended_tweet JSONB,
        retweet_count BIGINT,
        quote_count BIGINT,
        reply_count BIGINT,
        favorite_count BIGINT,
        entities JSONB,
        extended_entities JSONB,
        favorited BOOLEAN,
        retweeted BOOLEAN,
        possibly_sensitive BOOLEAN,
        filter_level VARCHAR,
        lang VARCHAR,
        matching_rules JSONB,
        timestamp_ms BIGINT );
    """

        self.conn.write_query(q_create_twitter_table)


if __name__ == "__main__":
    ENV = os.getenv('ENVIRONMENT', 'DEV')
    configs = conf[ENV]
    setup_inst = SetupDb(configs.pg_db['pg_data_lake'])
    setup_inst.create_db_tables()